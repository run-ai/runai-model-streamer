#include "obj_store/obj_store_init/obj_store_init.h"

#include <gtest/gtest.h>

#include <memory>

namespace runai::llm::streamer::impl::obj_store
{

TEST(Creation, Sanity)
{
    std::unique_ptr<S3Init> init;
    EXPECT_NO_THROW(init = std::make_unique<S3Init>());
}

}; // namespace runai::llm::streamer::impl::obj_store