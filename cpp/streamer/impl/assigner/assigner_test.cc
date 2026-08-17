#include <gtest/gtest.h>
#include <algorithm>
#include <limits>
#include <memory>
#include <vector>
#include <string>
#include <set>
#include "streamer/impl/assigner/assigner.h"
#include "streamer/impl/config/config.h"
#include "common/exception/exception.h"
#include "common/response_code/response_code.h"
#include "utils/random/random.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl {

struct AssignerTest : public ::testing::Test
{
 protected:
    void SetUp() override
    {
        config = std::make_shared<Config>();
        config->concurrency = utils::random::number(1, 20);
        config->s3_concurrency = utils::random::number(1, 20);
    }

    // A file whose ranges tile [offset, offset + sum(sizes)) and are packed consecutively from dst -
    // the layout the previous single-destination API implied. Coalesces to exactly one transfer.
    static FileRanges contiguous_file(const std::string & path, size_t offset, const std::vector<size_t> & sizes, char * dst)
    {
        FileRanges file;
        file.path = path;
        file.ranges.reserve(sizes.size());

        size_t o = offset;
        char * d = dst;
        for (const auto size : sizes)
        {
            file.ranges.push_back(ReadRange{ o, size, d });
            o += size;
            d += size;
        }
        return file;
    }

    static FileRanges file_with(const std::string & path, const std::vector<ReadRange> & ranges)
    {
        FileRanges file;
        file.path = path;
        file.ranges = ranges;
        return file;
    }

    // The Assigner never dereferences a destination - it only does pointer arithmetic and comparison -
    // so tests that care only about layout can use a fabricated base.
    static char * fake_base()
    {
        return reinterpret_cast<char *>(static_cast<uintptr_t>(utils::random::number(1, 1000)) * 4096);
    }

    std::shared_ptr<Config> config;
};

TEST_F(AssignerTest, Empty_Inputs)
{
    const std::vector<FileRanges> request;

    // Empty input should not throw, just produce no transfers
    EXPECT_NO_THROW(Assigner(request, config));

    const Assigner assigner(request, config);
    EXPECT_TRUE(assigner.transfers().empty());
    EXPECT_EQ(assigner.num_workloads(), 0);
}

TEST_F(AssignerTest, File_Without_Ranges)
{
    // A file carrying no ranges is legal: it owes no responses and contributes no transfer
    std::vector<FileRanges> request(1);
    request[0].path = utils::random::string();

    const Assigner assigner(request, config);
    EXPECT_TRUE(assigner.transfers().empty());
}

// ---------------------------------------------------------------------------------------------
// Coalescing - runs before any work is divided between workers
// ---------------------------------------------------------------------------------------------

TEST_F(AssignerTest, Coalesce_Adjacent_Ranges)
{
    char * const base = fake_base();
    const std::vector<size_t> sizes = { 100, 200, 300, 400 };

    std::vector<FileRanges> request;
    request.push_back(contiguous_file(utils::random::string(), 4096, sizes, base));

    const Assigner assigner(request, config);

    ASSERT_EQ(assigner.transfers().size(), 1);

    const auto & transfer = assigner.transfers()[0];
    EXPECT_EQ(transfer.file_index, 0);
    EXPECT_EQ(transfer.offset, 4096);
    EXPECT_EQ(transfer.size, 1000);
    EXPECT_EQ(transfer.destination, base);
    EXPECT_EQ(transfer.first_range_index, 0);
    EXPECT_EQ(transfer.range_sizes, sizes);
}

TEST_F(AssignerTest, Coalesce_Broken_By_File_Gap)
{
    char * const base = fake_base();

    // adjacent in the destination, but a hole in the file
    std::vector<FileRanges> request;
    request.push_back(file_with(utils::random::string(), {
        ReadRange{ 0,    100, base },
        ReadRange{ 500,  100, base + 100 },   // file gap: 100 != 500
    }));

    const Assigner assigner(request, config);

    ASSERT_EQ(assigner.transfers().size(), 2);
    EXPECT_EQ(assigner.transfers()[0].first_range_index, 0);
    EXPECT_EQ(assigner.transfers()[0].offset, 0);
    EXPECT_EQ(assigner.transfers()[1].first_range_index, 1);
    EXPECT_EQ(assigner.transfers()[1].offset, 500);
    EXPECT_EQ(assigner.transfers()[1].destination, base + 100);
}

TEST_F(AssignerTest, Coalesce_Broken_By_Destination_Gap)
{
    char * const base = fake_base();

    // adjacent in the file, but a hole in the destination - one read cannot fill two buffers
    std::vector<FileRanges> request;
    request.push_back(file_with(utils::random::string(), {
        ReadRange{ 0,   100, base },
        ReadRange{ 100, 100, base + 4096 },   // destination gap
    }));

    const Assigner assigner(request, config);

    ASSERT_EQ(assigner.transfers().size(), 2);
    EXPECT_EQ(assigner.transfers()[0].destination, base);
    EXPECT_EQ(assigner.transfers()[1].destination, base + 4096);
    EXPECT_EQ(assigner.transfers()[1].first_range_index, 1);
}

TEST_F(AssignerTest, Coalesce_Never_Spans_Files)
{
    char * const base = fake_base();

    // two files whose ranges would look adjacent if the file boundary were ignored
    std::vector<FileRanges> request;
    request.push_back(contiguous_file(utils::random::string(), 0, { 100 }, base));
    request.push_back(contiguous_file(utils::random::string(), 100, { 100 }, base + 100));

    const Assigner assigner(request, config);

    ASSERT_EQ(assigner.transfers().size(), 2);
    EXPECT_EQ(assigner.transfers()[0].file_index, 0);
    EXPECT_EQ(assigner.transfers()[1].file_index, 1);
    EXPECT_EQ(assigner.transfers()[1].first_range_index, 0);
}

TEST_F(AssignerTest, Coalesce_Unordered_Ranges_Are_Not_Merged)
{
    char * const base = fake_base();

    // the same two ranges as Coalesce_Adjacent_Ranges, listed back to front. Coalescing works on the
    // order given, so nothing merges - the DEBUG log records the lost coalescing.
    std::vector<FileRanges> request;
    request.push_back(file_with(utils::random::string(), {
        ReadRange{ 100, 100, base + 100 },
        ReadRange{ 0,   100, base },
    }));

    const Assigner assigner(request, config);

    EXPECT_EQ(assigner.transfers().size(), 2);
}

TEST_F(AssignerTest, Coalesce_With_Single_Worker)
{
    // Coalescing must not depend on the worker count: it happens before any assignment, so a
    // single-worker streamer still reads a coalesced group with one read rather than one per range.
    config->concurrency = 1;
    config->s3_concurrency = 1;

    char * const base = fake_base();
    const std::vector<size_t> sizes = { 10, 20, 30, 40, 50 };

    std::vector<FileRanges> request;
    request.push_back(contiguous_file(utils::random::string(), 0, sizes, base));

    const Assigner assigner(request, config);

    ASSERT_EQ(assigner.transfers().size(), 1);
    EXPECT_EQ(assigner.transfers()[0].range_sizes.size(), sizes.size());

    // one worker, so the transfer is never split
    ASSERT_EQ(assigner.transfers()[0].tasks.size(), 1);
    EXPECT_EQ(assigner.transfers()[0].tasks[0].size, 150);
    EXPECT_EQ(assigner.num_workloads(), 1);
}

TEST_F(AssignerTest, Transfer_Split_Across_Workers)
{
    // A transfer larger than a worker's target is split; the slices must tile it exactly, with the
    // offset and destination advancing together so each slice stays contiguous.
    config->concurrency = 8;

    char * const base = fake_base();
    const size_t size = config->fs_sync_read_block_bytesize * 64;

    std::vector<FileRanges> request;
    request.push_back(contiguous_file(utils::random::string(), 0, { size }, base));

    const Assigner assigner(request, config);

    ASSERT_EQ(assigner.transfers().size(), 1);
    const auto & transfer = assigner.transfers()[0];
    EXPECT_GT(transfer.tasks.size(), 1);

    size_t expected_offset = 0;
    std::set<unsigned> worker_indices;
    for (const auto & task : transfer.tasks)
    {
        EXPECT_EQ(task.offset_in_file, expected_offset);
        EXPECT_EQ(task.destination, base + expected_offset);
        EXPECT_EQ(worker_indices.count(task.workload_index), 0);
        worker_indices.insert(task.workload_index);
        expected_offset += task.size;
    }
    EXPECT_EQ(expected_offset, size);
}

TEST_F(AssignerTest, First_Range_Index_Of_Later_Transfer)
{
    // The response carries the index within the FILE, so a transfer that does not start at range 0 must
    // report where it begins - otherwise every response after the first hole is misattributed.
    char * const base = fake_base();

    std::vector<FileRanges> request;
    request.push_back(file_with(utils::random::string(), {
        ReadRange{ 0,    10, base },
        ReadRange{ 10,   10, base + 10 },
        ReadRange{ 1000, 10, base + 20 },   // hole in the file: starts a new transfer
        ReadRange{ 1010, 10, base + 30 },
    }));

    const Assigner assigner(request, config);

    ASSERT_EQ(assigner.transfers().size(), 2);
    EXPECT_EQ(assigner.transfers()[0].first_range_index, 0);
    EXPECT_EQ(assigner.transfers()[0].range_sizes.size(), 2);
    EXPECT_EQ(assigner.transfers()[1].first_range_index, 2);
    EXPECT_EQ(assigner.transfers()[1].range_sizes.size(), 2);
}

// ---------------------------------------------------------------------------------------------
// Assignment
// ---------------------------------------------------------------------------------------------

TEST_F(AssignerTest, Valid_Inputs)
{
    for (bool is_object_storage : {true, false})
    {
        const size_t num_files = utils::random::number(1, 10);

        std::vector<FileRanges> request;
        std::vector<size_t> sizes;
        std::vector<size_t> offsets;

        char * dst = fake_base();
        size_t total_size = 0;
        for (size_t i = 0; i < num_files; ++i)
        {
            const size_t file_size = utils::random::number<size_t>(100000, 100000000);
            const size_t offset = utils::random::number<size_t>(0, 100);

            request.push_back(contiguous_file((is_object_storage ? "s3://bucket/" : "") + utils::random::string(),
                                              offset, { file_size }, dst));
            sizes.push_back(file_size);
            offsets.push_back(offset);
            dst += file_size;
            total_size += file_size;
        }

        EXPECT_NO_THROW(Assigner(request, config));
        const Assigner assigner(request, config);

        // one range per file, so one transfer per file
        ASSERT_EQ(assigner.transfers().size(), num_files);

        std::set<unsigned> worker_indices;
        for (size_t i = 0; i < num_files; ++i)
        {
            const auto & transfer = assigner.transfers()[i];
            EXPECT_EQ(transfer.file_index, i);
            EXPECT_EQ(transfer.offset, offsets[i]);
            EXPECT_EQ(transfer.tasks[0].offset_in_file, offsets[i]);

            size_t total = 0;
            std::set<unsigned> file_worker_indices;
            for (const auto & task : transfer.tasks)
            {
                EXPECT_EQ(file_worker_indices.count(task.workload_index), 0);
                file_worker_indices.insert(task.workload_index);
                worker_indices.insert(task.workload_index);
                total += task.size;
            }
            EXPECT_EQ(total, sizes[i]);
        }

        // check that the number of assignment is the same as the number of workers
        EXPECT_EQ(assigner.get_num_workers(), (is_object_storage ? config->s3_concurrency : config->concurrency));

        const auto concurrency = is_object_storage ? config->s3_concurrency : config->concurrency;
        const auto block_bytesize = is_object_storage ? config->s3_block_bytesize : config->fs_sync_read_block_bytesize;
        const size_t expected_workers = std::max(std::min(total_size / block_bytesize, static_cast<size_t>(concurrency)), 1UL);
        ASSERT_EQ(worker_indices.size(), static_cast<unsigned>(expected_workers));
        ASSERT_EQ(assigner.num_workloads(), static_cast<unsigned>(expected_workers));
    }
}

TEST_F(AssignerTest, Overflow_Check)
{
    char * const base = fake_base();

    std::vector<FileRanges> request;
    request.push_back(file_with("file1", { ReadRange{ 0, std::numeric_limits<size_t>::max(), base } }));
    request.push_back(file_with("file2", { ReadRange{ 0, std::numeric_limits<size_t>::max(), base } }));

    EXPECT_THROW(Assigner(request, config), runai::llm::streamer::common::Exception);
}

TEST_F(AssignerTest, Zero_Size_Ranges)
{
    const size_t num_files = utils::random::number(1, 50);

    std::vector<FileRanges> request;
    std::vector<size_t> sizes;
    std::vector<size_t> offsets;

    char * dst = fake_base();
    size_t total_size = 0;
    for (size_t i = 0; i < num_files; ++i)
    {
        const size_t size = utils::random::boolean() ? 0 : utils::random::number<size_t>(1, 1000);
        const size_t offset = utils::random::number<size_t>(0, 100);

        request.push_back(contiguous_file(utils::random::string(), offset, { size }, dst));
        sizes.push_back(size);
        offsets.push_back(offset);
        dst += size;
        total_size += size;
    }

    const Assigner assigner(request, config);

    // every range yields a transfer, zero sized or not - each still owes exactly one response
    ASSERT_EQ(assigner.transfers().size(), num_files);

    std::set<unsigned> worker_indices;
    for (size_t i = 0; i < num_files; ++i)
    {
        const auto & transfer = assigner.transfers()[i];
        EXPECT_EQ(transfer.offset, offsets[i]);
        EXPECT_EQ(transfer.size, sizes[i]);

        // a zero sized transfer still gets exactly one task, so its range is still answered
        if (sizes[i] == 0)
        {
            ASSERT_EQ(transfer.tasks.size(), 1);
            EXPECT_EQ(transfer.tasks[0].size, 0);
            EXPECT_EQ(transfer.tasks[0].offset_in_file, offsets[i]);
        }

        size_t total = 0;
        for (const auto & task : transfer.tasks)
        {
            worker_indices.insert(task.workload_index);
            total += task.size;
        }
        EXPECT_EQ(total, sizes[i]);
    }

    EXPECT_EQ(assigner.get_num_workers(), config->concurrency);

    const size_t expected_workers = std::max(std::min(total_size / config->fs_sync_read_block_bytesize, static_cast<size_t>(config->concurrency)), 1UL);
    ASSERT_EQ(worker_indices.size(), static_cast<unsigned>(expected_workers));
    ASSERT_EQ(assigner.num_workloads(), static_cast<unsigned>(expected_workers));
}

TEST_F(AssignerTest, Only_Zero_Size_Ranges)
{
    const size_t num_files = utils::random::number(1, 10);

    std::vector<FileRanges> request;
    std::vector<size_t> offsets;

    char * const base = fake_base();
    for (size_t i = 0; i < num_files; ++i)
    {
        const size_t offset = utils::random::number<size_t>(0, 100);
        request.push_back(contiguous_file(utils::random::string(), offset, { 0 }, base));
        offsets.push_back(offset);
    }

    const Assigner assigner(request, config);

    ASSERT_EQ(assigner.transfers().size(), num_files);

    std::set<unsigned> worker_indices;
    for (size_t i = 0; i < num_files; ++i)
    {
        const auto & transfer = assigner.transfers()[i];
        ASSERT_EQ(transfer.tasks.size(), 1);
        EXPECT_EQ(transfer.tasks[0].size, 0);
        EXPECT_EQ(transfer.tasks[0].offset_in_file, offsets[i]);
        worker_indices.insert(transfer.tasks[0].workload_index);
    }

    EXPECT_EQ(assigner.get_num_workers(), config->concurrency);

    // no bytes to read, so everything lands on a single worker
    ASSERT_EQ(worker_indices.size(), 1);
    ASSERT_EQ(assigner.num_workloads(), 1);
}

}  // namespace runai::llm::streamer::impl
