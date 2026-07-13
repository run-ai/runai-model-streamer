#include "streamer/impl/batch/batch.h"

#include <gtest/gtest.h>
#include <sys/stat.h>

#include "utils/random/random.h"
#include "utils/temp/file/file.h"

#include "common/s3_wrapper/s3_wrapper.h"

#include "streamer/impl/cuda/cuda_loader.h"

namespace runai::llm::streamer::impl
{

namespace
{

bool real_cuda_driver_present()
{
    struct stat st;
    return stat("/dev/nvidiactl", &st) == 0;
}

} // anonymous namespace

// N tensors of random sizes written to a contiguous destination buffer.
// Also exercises multi-chunk reads by using a random fs_block_bytesize.
TEST(ReadCuda, Sanity)
{
    if (real_cuda_driver_present())
        GTEST_SKIP() << "Real CUDA driver present; mock test skipped on GPU machine";

    ASSERT_NE(cuda::CudaDriver::get(), nullptr)
        << "Mock libcuda.so.1 not loaded — check DT_RPATH in BUILD";

    const unsigned num_tensors = utils::random::number(1, 8);
    const size_t   file_start  = utils::random::number<size_t>(0, 512);

    std::vector<size_t> sizes;
    size_t total = 0;
    for (unsigned i = 0; i < num_tensors; ++i)
    {
        sizes.push_back(utils::random::number<size_t>(1, 4096));
        total += sizes.back();
    }

    const auto src = utils::random::buffer(file_start + total);
    utils::temp::File file(src);

    std::vector<char> dst(total, '\0');

    const auto chunk_bytesize = utils::random::number<size_t>(1, total);
    const auto config = std::make_shared<Config>(
        utils::random::number(1, 4), chunk_bytesize,
        utils::random::number<size_t>(1, chunk_bytesize),
        false /* do not force minimum chunk size */);

    auto responder = std::make_shared<common::Responder>(1);
    auto request   = std::make_shared<Request>(file_start, 0, 0, num_tensors, total, dst.data());

    Tasks tasks;
    size_t file_off = file_start;
    size_t rel_off  = 0;
    for (unsigned i = 0; i < num_tensors; ++i)
    {
        tasks.emplace_back(request, file_off, sizes[i], rel_off);
        file_off += sizes[i];
        rel_off  += sizes[i];
    }

    common::s3::S3ClientWrapper::Params params;
    Batch batch(0, 0, file.path, params, std::move(tasks), responder, config, /*cuda=*/true);

    std::atomic<bool> stopped(false);
    ASSERT_NO_THROW(batch.execute(stopped));
    EXPECT_EQ(responder->pop().ret, common::ResponseCode::Success);

    size_t pos = file_start;
    for (unsigned i = 0; i < num_tensors; ++i)
    {
        for (size_t j = 0; j < sizes[i]; ++j)
            EXPECT_EQ(dst[pos - file_start + j], static_cast<char>(src[pos + j]))
                << "mismatch tensor=" << i << " byte=" << j;
        pos += sizes[i];
    }
}

// Tensors written to 512-byte aligned offsets within a larger buffer, matching
// the layout that the Python alignment layer produces for real CUDA streaming.
// Padding bytes between tensors must remain untouched.
TEST(ReadCuda, AlignedDestinations)
{
    if (real_cuda_driver_present())
        GTEST_SKIP() << "Real CUDA driver present; mock test skipped on GPU machine";

    ASSERT_NE(cuda::CudaDriver::get(), nullptr)
        << "Mock libcuda.so.1 not loaded — check DT_RPATH in BUILD";

    constexpr size_t kAlign = 512;

    // Sizes deliberately not multiples of kAlign
    const std::vector<size_t> sizes     = {100, 200, 300};
    const size_t              file_start = 0;

    size_t file_total = 0;
    for (auto s : sizes) file_total += s;

    const auto src = utils::random::buffer(file_total);
    utils::temp::File file(src);

    // Compute per-tensor destination offsets as the Python layer would
    auto align_up = [](size_t v, size_t a){ return (v + a - 1) & ~(a - 1); };

    std::vector<size_t> dst_offsets;
    size_t dst_cursor = 0;
    for (size_t i = 0; i < sizes.size(); ++i)
    {
        dst_offsets.push_back(dst_cursor);
        dst_cursor += align_up(sizes[i], kAlign);
    }
    const size_t dst_total = dst_cursor;

    // Sentinel fill so any write into padding is detectable
    constexpr char kSentinel = static_cast<char>(0xAB);
    std::vector<char> dst(dst_total, kSentinel);

    const auto config = std::make_shared<Config>();

    auto responder = std::make_shared<common::Responder>(1);
    // request->dst is the base; task relative_offset lands each tensor at its
    // aligned slot: destination() == dst.data() + dst_offsets[i]
    auto request   = std::make_shared<Request>(file_start, 0, 0, sizes.size(), file_total, dst.data());

    Tasks tasks;
    size_t file_off = file_start;
    for (size_t i = 0; i < sizes.size(); ++i)
    {
        tasks.emplace_back(request, file_off, sizes[i], dst_offsets[i]);
        file_off += sizes[i];
    }

    common::s3::S3ClientWrapper::Params params;
    Batch batch(0, 0, file.path, params, std::move(tasks), responder, config, /*cuda=*/true);

    std::atomic<bool> stopped(false);
    ASSERT_NO_THROW(batch.execute(stopped));
    EXPECT_EQ(responder->pop().ret, common::ResponseCode::Success);

    size_t src_pos = file_start;
    for (size_t i = 0; i < sizes.size(); ++i)
    {
        // Tensor data must match the source file
        for (size_t j = 0; j < sizes[i]; ++j)
            EXPECT_EQ(dst[dst_offsets[i] + j], static_cast<char>(src[src_pos + j]))
                << "data mismatch tensor=" << i << " byte=" << j;

        // Padding between this tensor and the next must be untouched
        const size_t pad_start = dst_offsets[i] + sizes[i];
        const size_t pad_end   = (i + 1 < sizes.size()) ? dst_offsets[i + 1] : dst_total;
        for (size_t j = pad_start; j < pad_end; ++j)
            EXPECT_EQ(dst[j], kSentinel)
                << "padding corrupted at dst byte=" << j << " (between tensor " << i << " and " << i + 1 << ")";

        src_pos += sizes[i];
    }
}

}; // namespace runai::llm::streamer::impl
