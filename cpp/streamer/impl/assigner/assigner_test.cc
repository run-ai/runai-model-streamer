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

    std::shared_ptr<Config> config;
};

TEST_F(AssignerTest, Empty_Inputs)
{
    std::vector<std::string> paths;
    std::vector<size_t> offsets;
    std::vector<size_t> sizes;
    std::vector<void*> dsts;

    // Empty input should not throw, just create empty assignments
    EXPECT_NO_THROW(Assigner(paths, offsets, sizes, dsts, config));

    Assigner assigner(paths, offsets, sizes, dsts, config);
    EXPECT_THROW(assigner.file_assignments(0), std::exception);
}

TEST_F(AssignerTest, Mismatched_Input_Sizes)
{
    std::vector<std::string> paths = {utils::random::string()};
    std::vector<size_t> offsets = {0};
    std::vector<size_t> sizes = {utils::random::number(100, 1000)};
    std::vector<void*> dsts;  // Empty dsts

    EXPECT_THROW(Assigner(paths, offsets, sizes, dsts, config), runai::llm::streamer::common::Exception);
}

TEST_F(AssignerTest, Valid_Inputs)
{
    for (bool is_object_storage : {true, false})
    {
        const size_t num_files = utils::random::number(1, 10);
        std::vector<std::string> paths;
        std::vector<size_t> offsets;
        std::vector<size_t> sizes;
        std::vector<void*> dsts;

        size_t total_size = 0;
        for (size_t i = 0; i < num_files; ++i)
        {
            const size_t file_size = utils::random::number<size_t>(100000, 100000000);
            paths.push_back((is_object_storage ? "s3://bucket/" : "") + utils::random::string());
            offsets.push_back(utils::random::number<size_t>(0, 100));
            sizes.push_back(file_size);
            dsts.push_back(reinterpret_cast<void *>(utils::random::number()));
            total_size += file_size;
        }

        EXPECT_NO_THROW(Assigner(paths, offsets, sizes, dsts, config));
        const Assigner assigner(paths, offsets, sizes, dsts, config);

        for (size_t i = 0; i < num_files; ++i)
        {
            const auto & tasks = assigner.file_assignments(i);
            EXPECT_EQ(tasks[0].offset_in_file, offsets[i]);
            size_t total = 0;
            std::set<unsigned> worker_indices;
            for (const auto & task : tasks)
            {
                EXPECT_EQ(worker_indices.count(task.worker_index), 0);
                worker_indices.insert(task.worker_index);
                total += task.size;
            }
            EXPECT_EQ(total, sizes[i]);
        }

        // check that the number of assignment is the same as the number of workers
        EXPECT_EQ(assigner.get_num_workers(), (is_object_storage ? config->s3_concurrency : config->concurrency));

        std::set<unsigned> worker_indices;
        for (size_t i = 0; i < num_files; ++i)
        {
            const auto & tasks = assigner.file_assignments(i);
            for (const auto & task : tasks)
            {
                LOG(SPAM) << "file " << i << " file size: " << sizes[i] << " task.worker_index: " << task.worker_index << " task.size: " << task.size;
                worker_indices.insert(task.worker_index);
            }
        }

        const auto concurrency = is_object_storage ? config->s3_concurrency : config->concurrency;
        const auto block_bytesize = is_object_storage ? config->s3_block_bytesize : config->fs_block_bytesize;
        const size_t expected_workers = std::max(std::min(total_size / block_bytesize, static_cast<size_t>(concurrency)), 1UL);
        ASSERT_EQ(worker_indices.size(), static_cast<unsigned>(expected_workers));
        ASSERT_EQ(assigner.num_workloads(), static_cast<unsigned>(expected_workers));
    }
}

TEST_F(AssignerTest, Overflow_Check)
{
    std::vector<std::string> paths = {"file1", "file2"};
    std::vector<size_t> offsets = {0, 0};
    std::vector<size_t> sizes = {std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max()};
    std::vector<void*> dsts = {nullptr, nullptr};

    EXPECT_THROW(Assigner(paths, offsets, sizes, dsts, config), runai::llm::streamer::common::Exception);
}

TEST_F(AssignerTest, Zero_Size_Files)
{
    const size_t num_files = utils::random::number(1, 50);
    std::vector<std::string> paths;
    std::vector<size_t> offsets;
    std::vector<size_t> sizes;
    std::vector<void *> dsts;
    size_t total_size = 0;
    for (size_t i = 0; i < num_files; ++i)
    {
        paths.push_back(utils::random::string());
        offsets.push_back(utils::random::number<size_t>(0, 100));
        if (utils::random::boolean())
        {
            sizes.push_back(0);
        }
        else
        {
            sizes.push_back(utils::random::number<size_t>(1, 1000));
        }
        dsts.push_back(nullptr);
        total_size += sizes[i];
    }
    dsts[0] = reinterpret_cast<void *>(utils::random::number());

    const Assigner assigner(paths, offsets, sizes, dsts, config);

    char * global_dst = static_cast<char *>(dsts[0]);
    for (size_t i = 0; i < num_files; ++i)
    {
        const auto & tasks = assigner.file_assignments(i);
        LOG(SPAM) << "file " << i << " tasks size: " << tasks.size();
        if (sizes[i] == 0)
        {
            EXPECT_EQ(tasks.size(), 1);
            EXPECT_EQ(tasks[0].size, 0);
            EXPECT_EQ(tasks[0].offset_in_file, offsets[i]);
            EXPECT_EQ(tasks[0].destination, global_dst);
            continue;
        }

        EXPECT_EQ(tasks[0].offset_in_file, offsets[i]);
        EXPECT_EQ(tasks[0].destination, global_dst);
        global_dst += sizes[i];

        size_t total = 0;
        std::set<unsigned> worker_indices;
        for (const auto & task : tasks)
        {
            EXPECT_EQ(worker_indices.count(task.worker_index), 0);
            worker_indices.insert(task.worker_index);
            total += task.size;
        }
        EXPECT_EQ(total, sizes[i]);
    }

    // check that the number of assignment is the same as the number of workers
    EXPECT_EQ(assigner.get_num_workers(), config->concurrency);

    std::set<unsigned> worker_indices;
    for (size_t i = 0; i < num_files; ++i)
    {
        const auto & tasks = assigner.file_assignments(i);
        for (const auto & task : tasks)
        {
            LOG(SPAM) << "file " << i << " file size: " << sizes[i] << " task.worker_index: " << task.worker_index << " task.size: " << task.size;
            worker_indices.insert(task.worker_index);
        }
    }

    const auto block_bytesize = config->fs_block_bytesize;
    const size_t expected_workers = std::max(std::min(total_size / block_bytesize, static_cast<size_t>(config->concurrency)), 1UL);
    ASSERT_EQ(worker_indices.size(), static_cast<unsigned>(expected_workers));
    ASSERT_EQ(assigner.num_workloads(), static_cast<unsigned>(expected_workers));
}

TEST_F(AssignerTest, Only_Zero_Size_Files)
{
    const size_t num_files = utils::random::number(1, 10);
    std::vector<std::string> paths;
    std::vector<size_t> offsets;
    std::vector<size_t> sizes;
    std::vector<void *> dsts;
    size_t total_size = 0;
    for (size_t i = 0; i < num_files; ++i)
    {
        paths.push_back(utils::random::string());
        offsets.push_back(utils::random::number<size_t>(0, 100));
        sizes.push_back(0);
        dsts.push_back(nullptr);
    }
    dsts[0] = reinterpret_cast<void *>(utils::random::number());

    const Assigner assigner(paths, offsets, sizes, dsts, config);

    char * global_dst = static_cast<char *>(dsts[0]);
    for (size_t i = 0; i < num_files; ++i)
    {
        const auto & tasks = assigner.file_assignments(i);
        LOG(SPAM) << "file " << i << " tasks size: " << tasks.size();
        if (sizes[i] == 0)
        {
            EXPECT_EQ(tasks.size(), 1);
            EXPECT_EQ(tasks[0].size, 0);
            EXPECT_EQ(tasks[0].offset_in_file, offsets[i]);
            EXPECT_EQ(tasks[0].destination, global_dst);
            continue;
        }

        EXPECT_EQ(tasks[0].offset_in_file, offsets[i]);
        EXPECT_EQ(tasks[0].destination, global_dst);
        global_dst += sizes[i];

        size_t total = 0;
        std::set<unsigned> worker_indices;
        for (const auto & task : tasks)
        {
            EXPECT_EQ(worker_indices.count(task.worker_index), 0);
            worker_indices.insert(task.worker_index);
            total += task.size;
        }
        EXPECT_EQ(total, sizes[i]);
    }

    // check that the number of assignment is the same as the number of workers
    EXPECT_EQ(assigner.get_num_workers(), config->concurrency);

    std::set<unsigned> worker_indices;
    for (size_t i = 0; i < num_files; ++i)
    {
        const auto & tasks = assigner.file_assignments(i);
        for (const auto & task : tasks)
        {
            LOG(SPAM) << "file " << i << " file size: " << sizes[i] << " task.worker_index: " << task.worker_index << " task.size: " << task.size;
            worker_indices.insert(task.worker_index);
        }
    }

    ASSERT_EQ(worker_indices.size(), 1);
    ASSERT_EQ(assigner.num_workloads(), 1);
}

}  // namespace runai::llm::streamer::impl
