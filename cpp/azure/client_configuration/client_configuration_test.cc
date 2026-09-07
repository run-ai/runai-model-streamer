#include "azure/client_configuration/client_configuration.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>

#include "utils/temp/env/env.h"

namespace runai::llm::streamer::impl::azure
{

namespace
{

// The formula the configuration implements, restated so the tests assert against the machine they run
// on rather than a literal thread count.
unsigned expected(unsigned readers, unsigned group_size = 1)
{
    const unsigned nprocs = std::thread::hardware_concurrency();
    const unsigned floor = nprocs == 0 ? 8U : 1U;
    return std::max(floor, nprocs * 2 / (readers * group_size));
}

} // namespace

class AzureConcurrencyTest : public ::testing::Test
{
 protected:
    void SetUp() override
    {
        _obj = std::make_unique<utils::temp::UnsetEnv>(std::string("RUNAI_STREAMER_OBJ_CONCURRENCY"));
        _legacy = std::make_unique<utils::temp::UnsetEnv>(std::string("RUNAI_STREAMER_CONCURRENCY"));
        _group = std::make_unique<utils::temp::UnsetEnv>(std::string("RUNAI_STREAMER_PROCESS_GROUP_SIZE"));
    }

    std::unique_ptr<utils::temp::UnsetEnv> _obj;
    std::unique_ptr<utils::temp::UnsetEnv> _legacy;
    std::unique_ptr<utils::temp::UnsetEnv> _group;
};

// The threads are sized by the number of CLIENTS the streamer will build, which is the object-storage
// concurrency - so it has to follow the specific variable, not only the legacy one.
TEST_F(AzureConcurrencyTest, Threads_Follow_Obj_Concurrency)
{
    for (const unsigned readers : { 2U, 4U, 8U, 16U })
    {
        utils::temp::Env obj(std::string("RUNAI_STREAMER_OBJ_CONCURRENCY"), static_cast<unsigned long>(readers));
        EXPECT_EQ(ClientConfiguration().max_concurrency, expected(readers)) << readers;
    }
}

// The property the divisor exists for: N clients with nprocs*2/N threads each is the same total
// whatever N is. Reading the wrong variable breaks exactly this, and nothing else.
//
// Only reader counts that DIVIDE the total are checked. The division truncates otherwise, and that
// loss belongs to the formula rather than to which variable it read - asserting on it would fail on
// hosts whose core count happens not to divide.
TEST_F(AzureConcurrencyTest, The_Total_Thread_Count_Does_Not_Move)
{
    const unsigned nprocs = std::thread::hardware_concurrency();
    ASSERT_GT(nprocs, 0u) << "the formula falls back to a fixed 8 with no cores to divide";

    const unsigned total = nprocs * 2;
    unsigned checked = 0;

    for (unsigned readers = 2; readers <= total; ++readers)
    {
        if (total % readers != 0)
        {
            continue;
        }

        utils::temp::Env obj(std::string("RUNAI_STREAMER_OBJ_CONCURRENCY"), static_cast<unsigned long>(readers));
        EXPECT_EQ(readers * ClientConfiguration().max_concurrency, total) << readers;
        ++checked;
    }

    EXPECT_GT(checked, 0u) << "no reader count divided " << total << ", so nothing was asserted";
}

// The legacy variable still drives object storage when the specific one is unset, and loses when both
// are set - the same precedence the streamer resolves the client count by.
TEST_F(AzureConcurrencyTest, Obj_Concurrency_Wins_Over_The_Legacy_Variable)
{
    {
        utils::temp::Env legacy(std::string("RUNAI_STREAMER_CONCURRENCY"), 4UL);
        EXPECT_EQ(ClientConfiguration().max_concurrency, expected(4));
    }

    utils::temp::Env legacy(std::string("RUNAI_STREAMER_CONCURRENCY"), 4UL);
    utils::temp::Env obj(std::string("RUNAI_STREAMER_OBJ_CONCURRENCY"), 16UL);
    EXPECT_EQ(ClientConfiguration().max_concurrency, expected(16));
}

TEST_F(AzureConcurrencyTest, Unset_Uses_The_Object_Storage_Default)
{
    EXPECT_EQ(ClientConfiguration().max_concurrency, expected(8));
}

// A divisor of zero used to be an integer division by zero rather than a misconfiguration.
TEST_F(AzureConcurrencyTest, Zero_Is_Floored_Rather_Than_Dividing_By_Zero)
{
    utils::temp::Env obj(std::string("RUNAI_STREAMER_OBJ_CONCURRENCY"), 0UL);
    EXPECT_EQ(ClientConfiguration().max_concurrency, expected(1));
}

// Azure divides by the process group size too, so one setting describes the node at any tensor
// parallel size.
TEST_F(AzureConcurrencyTest, The_Process_Group_Divides_As_Well)
{
    utils::temp::Env obj(std::string("RUNAI_STREAMER_OBJ_CONCURRENCY"), 4UL);
    utils::temp::Env group(std::string("RUNAI_STREAMER_PROCESS_GROUP_SIZE"), 2UL);

    EXPECT_EQ(ClientConfiguration().max_concurrency, expected(4, 2));
}

}; // namespace runai::llm::streamer::impl::azure
