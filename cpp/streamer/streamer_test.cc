#include "streamer/streamer.h"

#include <unistd.h>

#include <gtest/gtest.h>
#include <map>
#include <string>
#include <vector>
#include <chrono>
#include <set>

#include "common/response_code/response_code.h"

#include "utils/logging/logging.h"
#include "utils/random/random.h"
#include "utils/fd/fd.h"
#include "utils/temp/env/env.h"
#include "utils/temp/file/file.h"

namespace runai::llm::streamer
{

namespace
{

struct StreamerTest : ::testing::Test
{
    StreamerTest() :
        _size("RUNAI_STREAMER_CONCURRENCY", utils::random::number<int>(1, 16)),
        _chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", utils::random::number<int>(1, 1024))
    {}

    int runai_request_file(void * streamer, const char * path, size_t offset, size_t size, void * dst)
    {
        std::vector<size_t> sizes;
        sizes.push_back(size);
        std::vector<size_t *> internal_sizes;
        internal_sizes.push_back(sizes.data());
        std::vector<unsigned> num_sizes;
        num_sizes.push_back(1);
        return runai::llm::streamer::runai_request(streamer, 1, &path, &offset, &size, &dst, num_sizes.data(), internal_sizes.data(), nullptr, nullptr, nullptr, nullptr, nullptr);
    }

    int runai_read_file(void * streamer, const char * path, size_t offset, size_t size, void * dst)
    {
        std::vector<size_t> sizes;
        sizes.push_back(size);
        std::vector<size_t *> internal_sizes;
        internal_sizes.push_back(sizes.data());
        std::vector<unsigned> num_sizes;
        num_sizes.push_back(1);
        auto res = runai::llm::streamer::runai_request(streamer, 1, &path, &offset, &size, &dst, num_sizes.data(), internal_sizes.data(), nullptr, nullptr, nullptr, nullptr, nullptr);
        if (res != static_cast<int>(runai::llm::streamer::common::ResponseCode::Success))
        {
            return res;
        }

        unsigned file_index;
        unsigned index;
        return runai_response(streamer, &file_index, &index);
    }

 protected:
    utils::temp::Env _size;
    utils::temp::Env _chunk_bytesize;
    utils::temp::Env _block_bytesize;
};

} // namespace

TEST_F(StreamerTest, Creation)
{
    void * streamer = nullptr;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));
    EXPECT_NE(streamer, nullptr);

    EXPECT_NO_THROW(runai_end(streamer));
}

TEST(Creation, Invalid_Parameter)
{
    void * streamer = nullptr;

    {
        utils::temp::Env size("RUNAI_STREAMER_CONCURRENCY", 0);
        utils::temp::Env chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", utils::random::number<int>(1, 2000));
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::InvalidParameterError));
        EXPECT_EQ(streamer, nullptr);
    }

    {
        utils::temp::Env size("RUNAI_STREAMER_CONCURRENCY", utils::random::number<int>(1, 10));
        utils::temp::Env chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", 0);
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::InvalidParameterError));
        EXPECT_EQ(streamer, nullptr);
    }

    {
        utils::temp::Env size("RUNAI_STREAMER_CONCURRENCY", utils::random::number<int>(1, 10));
        utils::temp::Env chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", 0);
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::InvalidParameterError));
        EXPECT_EQ(streamer, nullptr);
    }
}

TEST_F(StreamerTest, Read)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    std::vector<unsigned char> v(size);
    res = runai_read_file(streamer, file.path.c_str(), 0, size, v.data());
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(v[i], expected[i]);
        if (v[i] != expected[i])
        {
            break;
        }
    }

    runai_end(streamer);
}

TEST_F(StreamerTest, Async)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    EXPECT_EQ(runai_request_file(streamer, file.path.c_str(), 0, size, dst.data()), static_cast<int>(common::ResponseCode::Success));
    unsigned r = utils::random::number();
    unsigned rfile = utils::random::number();
    EXPECT_EQ(runai_response(streamer, &rfile, &r), static_cast<int>(common::ResponseCode::Success));
    EXPECT_EQ(r, 0);
    EXPECT_EQ(rfile, 0);
    EXPECT_EQ(runai_response(streamer, &rfile, &r), static_cast<int>(common::ResponseCode::FinishedError));

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(dst[i], expected[i]);
        if (dst[i] != expected[i])
        {
            break;
        }
    }

    runai_end(streamer);
}

TEST_F(StreamerTest, Error)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(utils::random::number(1, size-1));
    utils::temp::File file(data);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    std::vector<char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    const auto request_ret = runai_request_file(streamer, file.path.c_str(), 0, size, dst.data());
    if (request_ret == static_cast<int>(common::ResponseCode::EofError))
    {
        return;
    }

    EXPECT_EQ(request_ret, static_cast<int>(common::ResponseCode::Success));

    unsigned value = utils::random::number();
    unsigned r = value;
    unsigned file_index = utils::random::number();
    EXPECT_EQ(runai_response(streamer, &file_index, &r), static_cast<int>(common::ResponseCode::EofError));
    EXPECT_EQ(r, 0);
    EXPECT_EQ(file_index, 0);
    EXPECT_EQ(runai_response(streamer, &file_index, &r), static_cast<int>(common::ResponseCode::FinishedError));

    runai_end(streamer);
}

TEST(Response, Description)
{
    auto __strings = std::map<int, std::string>
    {
        { static_cast<int>(common::ResponseCode::FileAccessError),                "File access error"                       },
        { static_cast<int>(common::ResponseCode::EofError),                       "End of file reached"                     },
        { static_cast<int>(common::ResponseCode::InvalidParameterError),          "Invalid request parameters"              },
        { static_cast<int>(common::ResponseCode::EmptyRequestError),              "Empty request parameters"                },
        { static_cast<int>(common::ResponseCode::BusyError),                      "Streamer is handling previous request"   },
        { static_cast<int>(common::ResponseCode::UnknownError),                   "Unknown Error"                           },
        { static_cast<int>(common::ResponseCode::FinishedError),                  "Finished all responses"                  },
        { static_cast<int>(common::ResponseCode::Success),                        "Request sent successfuly"                },
    };

    // errors

    for (auto response_code : {common::ResponseCode::FileAccessError, common::ResponseCode::EofError, common::ResponseCode::InvalidParameterError, common::ResponseCode::EmptyRequestError, common::ResponseCode::BusyError, common::ResponseCode::UnknownError, common::ResponseCode::FinishedError} )
    {
        std::string str = runai_response_str(static_cast<int>(response_code));

        const auto it = __strings.find(static_cast<int>(response_code));
        EXPECT_NE(it, __strings.end());

        EXPECT_EQ(str, it->second);
    }
}

TEST_F(StreamerTest, S3_Library_Not_Found)
{
    auto size = utils::random::number(100, 1000);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    const auto s3_path = "s3://" + utils::random::string() + "/" + utils::random::string();

    std::vector<char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    EXPECT_EQ(runai_request_file(streamer, s3_path.c_str(), 0, size, dst.data()), static_cast<int>(common::ResponseCode::Success));
    unsigned r = utils::random::number();
    EXPECT_EQ(runai_response(streamer, &r, &r), static_cast<int>(common::ResponseCode::S3NotSupported));

    runai_end(streamer);
}

TEST_F(StreamerTest, GCS_Library_Not_Found)
{
    auto size = utils::random::number(100, 1000);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    const auto s3_path = "gs://" + utils::random::string() + "/" + utils::random::string();

    std::vector<char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    EXPECT_EQ(runai_request_file(streamer, s3_path.c_str(), 0, size, dst.data()), static_cast<int>(common::ResponseCode::Success));
    unsigned r = utils::random::number();
    EXPECT_EQ(runai_response(streamer, &r, &r), static_cast<int>(common::ResponseCode::GCSNotSupported));

    runai_end(streamer);
}

TEST_F(StreamerTest, End_Before_Read)
{
    auto size = utils::random::number(10000000, 100000000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);

    EXPECT_EQ(runai_request_file(streamer, file.path.c_str(), 0, size, dst.data()), static_cast<int>(common::ResponseCode::Success));

    ::usleep(utils::random::number(400));

    const auto start_time = std::chrono::steady_clock::now();
    runai_end(streamer);
    const auto time_ = std::chrono::steady_clock::now();
    const auto duration  = std::chrono::duration_cast<std::chrono::milliseconds>(time_ - start_time);
    EXPECT_LT(duration.count(), 1000);
}

TEST_F(StreamerTest, Multiple_Files)
{
    auto num_files = utils::random::number(1, 50);
    std::vector<utils::temp::File> files(num_files);
    std::vector<const char *> file_paths(num_files);
    std::vector<size_t> file_offsets(num_files);

    std::vector<std::vector<uint8_t>> buffers(num_files);
    std::vector<std::vector<uint8_t>> expected(num_files);
    std::vector<size_t> sizes(num_files);

    // We assume that all files are written to a single continous cpu buffer
    std::vector<void *> dsts(num_files);
    std::vector<unsigned> num_ranges(num_files);
    std::vector<std::vector<size_t>> range_sizes(num_files);
    std::vector<size_t *> internal_sizes(num_files);

    std::vector<std::set<unsigned>> expected_response(num_files);

    size_t num_expected_responses = 0;
    size_t dst_size = 0;
    for (unsigned i = 0; i < num_files; ++i)
    {
        sizes[i] = utils::random::number(1000000, 10000000);
        dst_size += sizes[i];

        buffers[i] = utils::random::buffer(sizes[i]);
        files[i] = utils::temp::File(buffers[i]);
        file_paths[i] = files[i].path.c_str();
        file_offsets[i] = 0;
        expected[i] = utils::Fd::read(files[i].path);
        EXPECT_EQ(expected[i].size(), sizes[i]);

        auto num_non_zero_chunks = utils::random::number(1, 100);
        auto num_zero_chunks = utils::random::number(0, 2);
        num_ranges[i] = num_non_zero_chunks + num_zero_chunks;
        auto chunks = utils::random::chunks(sizes[i], num_non_zero_chunks);

        // add zero size chunks
        for (unsigned k = 0; k < num_non_zero_chunks || num_zero_chunks > 0;)
        {
            bool add_zero = utils::random::boolean();
            if (num_zero_chunks > 0 && add_zero)
            {
                range_sizes[i].push_back(0);
                --num_zero_chunks;
            }
            else if (k < num_non_zero_chunks)
            {
                range_sizes[i].push_back(chunks[k]);
                ++k;
            }
        }

        ASSERT_EQ(range_sizes[i].size(), num_ranges[i]);

        internal_sizes[i] = range_sizes[i].data();

        num_expected_responses += num_ranges[i];

        for (unsigned request_index = 0; request_index < num_ranges[i]; ++request_index)
        {
            expected_response[i].insert(request_index);
        }
    }
    std::vector<unsigned char> dst(dst_size);
    dsts[0] = static_cast<void *>(dst.data());

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    EXPECT_EQ(runai_request(streamer, num_files, file_paths.data(), file_offsets.data(), sizes.data(), dsts.data(), num_ranges.data(), internal_sizes.data(), nullptr, nullptr, nullptr, nullptr, nullptr), static_cast<int>(common::ResponseCode::Success));

    // wait for all the responses to arrive
    unsigned r;
    unsigned file_index;
    for (unsigned i = 0; i < num_expected_responses; ++i)
    {
        r = utils::random::number();
        file_index = utils::random::number();
        EXPECT_EQ(runai_response(streamer, &file_index, &r), static_cast<int>(common::ResponseCode::Success));
        EXPECT_LT(file_index, num_files);
        EXPECT_EQ(expected_response[file_index].count(r), 1);
        expected_response[file_index].erase(r);
    }

    EXPECT_EQ(runai_response(streamer, &file_index, &r), static_cast<int>(common::ResponseCode::FinishedError));

    // verify
    size_t offset = 0;
    for (unsigned file_index = 0; file_index < num_files; ++file_index)
    {
        EXPECT_EQ(expected_response[file_index].size(), 0);
        EXPECT_EQ(expected[file_index].size(), sizes[file_index]);
        for (size_t j = 0; j < sizes[file_index]; ++j)
        {
            EXPECT_LT(offset + j, dst.size());
            EXPECT_EQ(dst[offset + j], expected[file_index][j]);
            if (dst[offset + j] != expected[file_index][j])
            {
                LOG(ERROR) << "offset = " << offset + j << " expected = " << expected[file_index][j] << " dst = " << dst[offset + j];
                break;
            }
        }
        offset += sizes[file_index];
    }

    runai_end(streamer);
}

}; // namespace runai::llm::streamer
