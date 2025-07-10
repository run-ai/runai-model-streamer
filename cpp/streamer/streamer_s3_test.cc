#include "streamer/streamer.h"

#include <gtest/gtest.h>
#include <map>
#include <string>
#include <vector>

#include "common/response_code/response_code.h"
#include "common/s3_credentials/s3_credentials.h"

#include "utils/logging/logging.h"
#include "utils/random/random.h"
#include "utils/env/env.h"
#include "utils/dylib/dylib.h"
#include "utils/temp/env/env.h"
#include "utils/fdlimit/fdlimit.h"

namespace runai::llm::streamer
{

namespace
{

struct StreamerTest : ::testing::Test
{
    StreamerTest() :
        _concurrency(utils::random::number<int>(1, 16)),
        _size("RUNAI_STREAMER_CONCURRENCY", _concurrency),
        _chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", utils::random::number<int>(1, 1024)),
        s3_path("s3://" + utils::random::string() + "/" + utils::random::string()),
        credentials(
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr)),
        credentials_c(credentials),
        num_files(utils::random::number(1, 10)),
        s3_paths(num_files),
        file_names(num_files),
        file_offsets(num_files),
        sizes(num_files),
        dsts(num_files),
        num_ranges(num_files),
        range_sizes(num_files),
        internal_sizes(num_files),
        expected_response(num_files)
    {
        LOG(DEBUG) << "num_files: " << num_files;
        size_t dst_size = 0;
        num_expected_responses = 0;
        std::string bucket_name = utils::random::string();
        for (unsigned i = 0; i < num_files; ++i)
        {
            s3_paths[i] = "s3://" + bucket_name + "/" + utils::random::string();
            file_names[i] = s3_paths[i].c_str();
            sizes[i] = utils::random::number(10000000, 200000000);
            LOG(DEBUG) << "sizes[i]: " << sizes[i];
            dst_size += sizes[i];

            file_offsets[i] = utils::random::number<size_t>(0, sizes[i] - 1);

            num_ranges[i] = utils::random::number(1, 100);
            range_sizes[i] =  utils::random::chunks(sizes[i], num_ranges[i]);
            internal_sizes[i] = range_sizes[i].data();

            num_expected_responses += num_ranges[i];

            for (unsigned request_index = 0; request_index < num_ranges[i]; ++request_index)
            {
                expected_response[i].insert(request_index);
            }
        }
        LOG(DEBUG) << "num_expected_responses: " << num_expected_responses;
        dst.resize(dst_size);
        dsts[0] = static_cast<void *>(dst.data());
    }

    ~StreamerTest()
    {
        utils::Dylib dylib("libstreamers3.so");
        auto mock_cleanup = dylib.dlsym<void(*)()>("runai_mock_s3_cleanup");
        mock_cleanup();
    }

 protected:
    int _concurrency;
    utils::temp::Env _size;
    utils::temp::Env _chunk_bytesize;
    utils::temp::Env _block_bytesize;
    std::string s3_path;
    common::s3::Credentials credentials;
    common::s3::Credentials_C credentials_c;

    unsigned num_files;
    std::vector<std::string> s3_paths;
    std::vector<const char *> file_names;
    std::vector<size_t> file_offsets;
    std::vector<size_t> sizes;
    std::vector<void *> dsts;
    std::vector<unsigned> num_ranges;
    std::vector<std::vector<size_t>> range_sizes;
    std::vector<size_t *> internal_sizes;
    unsigned num_expected_responses;
    std::vector<std::set<unsigned>> expected_response;
    std::vector<unsigned char> dst;
};

} // namespace


TEST_F(StreamerTest, Async_Read)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    auto mock_cleanup = dylib.dlsym<void(*)()>("runai_mock_s3_cleanup");

    bool use_credentials = utils::random::boolean();
    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    if (use_credentials)
    {
        res = runai_request(streamer,
                        num_files,
                        file_names.data(),
                        file_offsets.data(),
                        sizes.data(),
                        dsts.data(),
                        num_ranges.data(),
                        internal_sizes.data(),
                        credentials_c.access_key_id,
                        credentials_c.secret_access_key,
                        credentials_c.session_token,
                        credentials_c.region,
                        credentials_c.endpoint);
    }
    else
    {
        res = runai_request(streamer,
                        num_files,
                        file_names.data(),
                        file_offsets.data(),
                        sizes.data(),
                        dsts.data(),
                        num_ranges.data(),
                        internal_sizes.data(),
                        nullptr,
                        nullptr,
                        nullptr,
                        nullptr,
                        nullptr);
    }

    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    // wait for all the responses to arrive
    unsigned r;
    unsigned file_index;
    for (unsigned i = 0; i < num_expected_responses; ++i)
    {
        r = utils::random::number();
        file_index = utils::random::number();
        auto response_code = runai_response(streamer, &file_index, &r);
        EXPECT_EQ(response_code, static_cast<int>(common::ResponseCode::Success));
        if (response_code != static_cast<int>(common::ResponseCode::Success))
        {
            break;
        }
        EXPECT_LT(file_index, num_files);
        EXPECT_EQ(expected_response[file_index].count(r), 1);
        expected_response[file_index].erase(r);
    }
    runai_end(streamer);
    mock_cleanup();
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(StreamerTest, Increase_Insufficient_Fd_Limit)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    auto mock_cleanup = dylib.dlsym<void(*)()>("runai_mock_s3_cleanup");

    for (bool use_credentials : { true, false })
    {
        auto concurrency = utils::getenv<int>("RUNAI_STREAMER_CONCURRENCY");

        const auto insufficient_fd_limit = utils::random::number<rlim_t>(50, concurrency * 64 -1);
        utils::FdLimitSetter fd_limit(insufficient_fd_limit);
        void * streamer;
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

        if (use_credentials)
        {
            res = runai_request(streamer,
                            num_files,
                            file_names.data(),
                            file_offsets.data(),
                            sizes.data(),
                            dsts.data(),
                            num_ranges.data(),
                            internal_sizes.data(),
                            credentials_c.access_key_id,
                            credentials_c.secret_access_key,
                            credentials_c.session_token,
                            credentials_c.region,
                            credentials_c.endpoint);
        }
        else
        {
            res = runai_request(streamer,
                            num_files,
                            file_names.data(),
                            file_offsets.data(),
                            sizes.data(),
                            dsts.data(),
                            num_ranges.data(),
                            internal_sizes.data(),
                            nullptr,
                            nullptr,
                            nullptr,
                            nullptr,
                            nullptr);
        }
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

        runai_end(streamer);
        EXPECT_EQ(verify_mock(), 0);

        // verify that fd limit was restored
        EXPECT_EQ(utils::get_cur_file_descriptors(), insufficient_fd_limit);
        mock_cleanup();
    }
}

TEST_F(StreamerTest, Stop_Before_Async_Read)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    auto stop_mock = dylib.dlsym<common::backend_api::ResponseCode_t(*)()>("obj_cancel_all_reads");
    auto mock_cleanup = dylib.dlsym<void(*)()>("runai_mock_s3_cleanup");

    for (bool use_credentials : { true, false })
    {
        void * streamer;
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

        stop_mock();

        if (use_credentials)
        {
            res = runai_request(streamer,
                            num_files,
                            file_names.data(),
                            file_offsets.data(),
                            sizes.data(),
                            dsts.data(),
                            num_ranges.data(),
                            internal_sizes.data(),
                            credentials_c.access_key_id,
                            credentials_c.secret_access_key,
                            credentials_c.session_token,
                            credentials_c.region,
                            credentials_c.endpoint);
        }
        else
        {
            res = runai_request(streamer,
                            num_files,
                            file_names.data(),
                            file_offsets.data(),
                            sizes.data(),
                            dsts.data(),
                            num_ranges.data(),
                            internal_sizes.data(),
                            nullptr,
                            nullptr,
                            nullptr,
                            nullptr,
                            nullptr);
        }

        // request was not sent to the S3 server
        unsigned r;
        unsigned file_index;
        EXPECT_EQ(runai_response(streamer, &file_index, &r), static_cast<int>(common::ResponseCode::FinishedError));

        runai_end(streamer);
        EXPECT_EQ(verify_mock(), 0);

        mock_cleanup();
    }
}

TEST_F(StreamerTest, End_During_Async_Read)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    auto mock_cleanup = dylib.dlsym<common::backend_api::ResponseCode_t(*)()>("obj_remove_all_clients");

    for (bool use_credentials : { true, false })
    {
        auto mock_response_time = dylib.dlsym<void(*)(unsigned)>("runai_mock_s3_set_response_time_ms");
        unsigned delay_ms = 1000;
        mock_response_time(delay_ms);

        void * streamer;
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

        if (use_credentials)
        {
            res = runai_request(streamer,
                            num_files,
                            file_names.data(),
                            file_offsets.data(),
                            sizes.data(),
                            dsts.data(),
                            num_ranges.data(),
                            internal_sizes.data(),
                            credentials_c.access_key_id,
                            credentials_c.secret_access_key,
                            credentials_c.session_token,
                            credentials_c.region,
                            credentials_c.endpoint);
        }
        else
        {
            res = runai_request(streamer,
                            num_files,
                            file_names.data(),
                            file_offsets.data(),
                            sizes.data(),
                            dsts.data(),
                            num_ranges.data(),
                            internal_sizes.data(),
                            nullptr,
                            nullptr,
                            nullptr,
                            nullptr,
                            nullptr);
        }

        ::usleep(utils::random::number(300));

        runai_end(streamer);

        EXPECT_EQ(verify_mock(), 0);

        mock_cleanup();
    }
}

TEST_F(StreamerTest, Multiple_Files)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");

    void * streamer;
    EXPECT_EQ(runai_start(&streamer), static_cast<int>(common::ResponseCode::Success));

    auto res = runai_request(streamer,
                             num_files,
                             file_names.data(),
                             file_offsets.data(),
                             sizes.data(),
                             dsts.data(),
                             num_ranges.data(),
                             internal_sizes.data(),
                             credentials_c.access_key_id,
                             credentials_c.secret_access_key,
                             credentials_c.session_token,
                             credentials_c.region,
                             credentials_c.endpoint);

    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    // wait for all the responses to arrive
    unsigned r;
    unsigned file_index;
    for (unsigned i = 0; i < num_expected_responses; ++i)
    {
        r = utils::random::number();
        file_index = utils::random::number();
        auto response_code = runai_response(streamer, &file_index, &r);
        EXPECT_EQ(response_code, static_cast<int>(common::ResponseCode::Success));
        if (response_code != static_cast<int>(common::ResponseCode::Success))
        {
            break;
        }
        EXPECT_LT(file_index, num_files);
        EXPECT_EQ(expected_response[file_index].count(r), 1);
        expected_response[file_index].erase(r);
    }

    runai_end(streamer);
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(StreamerTest, Multiple_Files_Error)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");

    const auto error_code = common::ResponseCode::FileAccessError;
    utils::temp::Env env_rc("RUNAI_STREAMER_S3_MOCK_RESPONSE_CODE", static_cast<int>(error_code));

    void * streamer;
    EXPECT_EQ(runai_start(&streamer), static_cast<int>(common::ResponseCode::Success));


    auto res = runai_request(streamer,
                             num_files,
                             file_names.data(),
                             file_offsets.data(),
                             sizes.data(),
                             dsts.data(),
                             num_ranges.data(),
                             internal_sizes.data(),
                             credentials_c.access_key_id,
                             credentials_c.secret_access_key,
                             credentials_c.session_token,
                             credentials_c.region,
                             credentials_c.endpoint);

    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    // wait for all the responses to arrive
    unsigned r;
    unsigned file_index;
    for (unsigned i = 0; i < num_expected_responses; ++i)
    {
        r = utils::random::number();
        file_index = utils::random::number();
        const auto response_code = runai_response(streamer, &file_index, &r);
        EXPECT_EQ(response_code, static_cast<int>(error_code));
        EXPECT_LT(file_index, num_files);
        EXPECT_EQ(expected_response[file_index].count(r), 1);
        if (response_code != static_cast<int>(error_code))
        {
            break;
        }
        expected_response[file_index].erase(r);
    }

    const auto response_code = runai_response(streamer, &file_index, &r);
    EXPECT_EQ(response_code, static_cast<int>(common::ResponseCode::FinishedError));

    runai_end(streamer);
    EXPECT_EQ(verify_mock(), 0);
}

}; // namespace runai::llm::streamer
