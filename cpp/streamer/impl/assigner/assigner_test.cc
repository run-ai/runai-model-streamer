#include <gtest/gtest.h>
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
    const size_t num_files = utils::random::number(1, 10);
    std::vector<std::string> paths;
    std::vector<size_t> offsets;
    std::vector<size_t> sizes;
    std::vector<void*> dsts;

    for (size_t i = 0; i < num_files; ++i)
    {
        const size_t file_size = utils::random::number<size_t>(100000, 100000000);
        paths.push_back(utils::random::string());
        offsets.push_back(utils::random::number<size_t>(0, 100));
        sizes.push_back(file_size);
        dsts.push_back(reinterpret_cast<void *>(utils::random::number()));
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
}

}  // namespace runai::llm::streamer::impl
