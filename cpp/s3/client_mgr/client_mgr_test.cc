
#include "s3/client_mgr/client_mgr.h"

#include <gtest/gtest.h>

#include <set>
#include <string>

#include "common/backend_api/object_storage/utils.h"
#include "utils/random/random.h"

namespace runai::llm::streamer::impl::s3
{

struct Helper : S3ClientBase
{
    Helper(const common::backend_api::ObjectClientConfig_t & config) :
        S3ClientBase(config),
        counter(++global_counter),
        id(utils::random::number<size_t>())
    {
    }

    using S3ClientBase::verify_credentials;

    const std::optional<std::string> & key()
    {
        return _key;
    }

    const std::optional<std::string> & secret()
    {
        return _secret;
    }

    const std::optional<std::string> & token()
    {
        return _token;
    }

    const std::optional<std::string> & region()
    {
        return _region;
    }

    const std::optional<std::string> & endpoint()
    {
        return _endpoint;
    }

    static size_t global_counter;
    const size_t counter;

    const size_t id;
};

size_t Helper::global_counter = 0;

struct ClientMgrTest : ::testing::Test
{
    ClientMgrTest() :
        uri(create_uri()),
        credentials(
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr)),
        params(uri, credentials, utils::random::number<size_t>())
    {}

    void TearDown() override
    {
        ClientMgr<Helper>::clear();

        EXPECT_EQ(ClientMgr<Helper>::size(), 0);
        EXPECT_EQ(ClientMgr<Helper>::unused(), 0);
    }

    static std::shared_ptr<common::s3::StorageUri> create_uri()
    {
        return std::make_shared<common::s3::StorageUri>("s3://" + utils::random::string() + "/" + utils::random::string());
    }

    std::shared_ptr<common::s3::StorageUri> uri;
    common::s3::Credentials credentials;
    common::s3::S3ClientWrapper::Params params;
};

TEST_F(ClientMgrTest, Creation_Sanity)
{
    EXPECT_EQ(ClientMgr<Helper>::size(), 0);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);
}

TEST_F(ClientMgrTest, Create_Client)
{
    std::vector<common::backend_api::ObjectConfigParam_t> initial_params;
    const auto config = params.to_config(initial_params);
    Helper * helper = ClientMgr<Helper>::pop(config);

    EXPECT_EQ(helper->key(), credentials.access_key_id);
    EXPECT_EQ(helper->secret(), credentials.secret_access_key);
    EXPECT_EQ(helper->token(), credentials.session_token);
    EXPECT_EQ(helper->region(), credentials.region);
    EXPECT_EQ(helper->endpoint(), credentials.endpoint);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    // used clients are not cleared
    ClientMgr<Helper>::clear();
    EXPECT_EQ(ClientMgr<Helper>::size(), 1);

    ClientMgr<Helper>::push(helper);
    ClientMgr<Helper>::clear();
    EXPECT_EQ(ClientMgr<Helper>::size(), 0);
}

TEST_F(ClientMgrTest, Reuse_Client)
{
    std::vector<common::backend_api::ObjectConfigParam_t> initial_params;
    const auto config = params.to_config(initial_params);
    Helper * helper = ClientMgr<Helper>::pop(config);

    EXPECT_EQ(helper->key(), credentials.access_key_id);
    EXPECT_EQ(helper->secret(), credentials.secret_access_key);
    EXPECT_EQ(helper->token(), credentials.session_token);
    EXPECT_EQ(helper->region(), credentials.region);
    EXPECT_EQ(helper->endpoint(), credentials.endpoint);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    ClientMgr<Helper>::push(helper);
    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 1);

    const auto expected = helper->counter;

    unsigned n = utils::random::number(2, 20);
    for (unsigned i = 0; i < n; ++i)
    {
        const std::string path = utils::random::string();
        params.uri->path = path;
        std::vector<common::backend_api::ObjectConfigParam_t> initial_params;
        const auto config = params.to_config(initial_params);
        Helper * helper = ClientMgr<Helper>::pop(config);
        EXPECT_EQ(helper->key(), credentials.access_key_id);
        EXPECT_EQ(helper->secret(), credentials.secret_access_key);
        EXPECT_EQ(helper->token(), credentials.session_token);
        EXPECT_EQ(helper->region(), credentials.region);
        EXPECT_EQ(helper->endpoint(), credentials.endpoint);

        EXPECT_TRUE(helper->verify_credentials(config));
        EXPECT_EQ(helper->counter, expected);

        EXPECT_EQ(ClientMgr<Helper>::size(), 1);
        EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

        ClientMgr<Helper>::push(helper);
        EXPECT_EQ(ClientMgr<Helper>::size(), 1);
        EXPECT_EQ(ClientMgr<Helper>::unused(), 1);
    }
}

TEST_F(ClientMgrTest, Credentials_Changed)
{
    std::vector<common::backend_api::ObjectConfigParam_t> initial_params;
    const auto config = params.to_config(initial_params);
    Helper * helper = ClientMgr<Helper>::pop(config);

    EXPECT_EQ(helper->key(), credentials.access_key_id);
    EXPECT_EQ(helper->secret(), credentials.secret_access_key);
    EXPECT_EQ(helper->token(), credentials.session_token);
    EXPECT_EQ(helper->region(), credentials.region);
    EXPECT_EQ(helper->endpoint(), credentials.endpoint);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    ClientMgr<Helper>::push(helper);
    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 1);

    auto expected = helper->counter;

    unsigned n = utils::random::number(2, 20);
    for (unsigned i = 0; i < n; ++i)
    {
        common::s3::Credentials new_credentials(
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr));

        auto new_uri = std::make_shared<common::s3::StorageUri>(*uri);
        new_uri->path = utils::random::string();
        common::s3::S3ClientWrapper::Params new_params(new_uri, new_credentials, utils::random::number<size_t>());
        std::vector<common::backend_api::ObjectConfigParam_t> initial_params;
        const auto new_config = new_params.to_config(initial_params);
        bool changed = !helper->verify_credentials(new_config);
        Helper * helper = ClientMgr<Helper>::pop(new_config);
        EXPECT_EQ(helper->key(), new_credentials.access_key_id);
        EXPECT_EQ(helper->secret(), new_credentials.secret_access_key);
        EXPECT_EQ(helper->token(), new_credentials.session_token);
        EXPECT_EQ(helper->region(), new_credentials.region);
        EXPECT_EQ(helper->endpoint(), new_credentials.endpoint);

        EXPECT_TRUE(helper->verify_credentials(new_config));

        EXPECT_EQ(helper->counter != expected, changed);

        expected = helper->counter;

        EXPECT_EQ(ClientMgr<Helper>::size(), 1);
        EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

        ClientMgr<Helper>::push(helper);
        EXPECT_EQ(ClientMgr<Helper>::size(), 1);
        EXPECT_EQ(ClientMgr<Helper>::unused(), 1);
    }
}

TEST_F(ClientMgrTest, Change_Bucket)
{
    std::set<Helper *> used;
    unsigned m = utils::random::number(2, 20);
    for (unsigned i = 0; i < m; ++i)
    {
        std::vector<common::backend_api::ObjectConfigParam_t> initial_params;
        const auto config = params.to_config(initial_params);
        Helper * helper = ClientMgr<Helper>::pop(config);

        EXPECT_EQ(ClientMgr<Helper>::size(), i + 1);
        EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

        used.insert(helper);
    }

    for (auto helper : used)
    {
        ClientMgr<Helper>::push(helper);
    }
    EXPECT_EQ(ClientMgr<Helper>::unused(), m);

    unsigned n = utils::random::number(2, 20);
    auto uri_ = create_uri();
    while (uri_->bucket == uri->bucket)
    {
        uri_ = create_uri();
    }

    used.clear();
    for (unsigned i = 0; i < n; ++i)
    {
        const std::string path = utils::random::string();
        uri_->path = path;
        common::s3::S3ClientWrapper::Params new_params(uri_, credentials, utils::random::number<size_t>());
        std::vector<common::backend_api::ObjectConfigParam_t> initial_params;
        const auto new_config = new_params.to_config(initial_params);
        Helper * helper = ClientMgr<Helper>::pop(new_config);
        EXPECT_EQ(ClientMgr<Helper>::unused(), (m > i + 1 ? m - i - 1 : 0));

        EXPECT_EQ(ClientMgr<Helper>::size(), (m > i + 1 ? m : i + 1));
        used.insert(helper);
    }

    for (auto helper : used)
    {
        ClientMgr<Helper>::push(helper);
    }
}

TEST_F(ClientMgrTest, Remove_Client)
{
    std::vector<common::backend_api::ObjectConfigParam_t> initial_params;
    const auto config = params.to_config(initial_params);
    Helper * helper = ClientMgr<Helper>::pop(config);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    ClientMgr<Helper>::push(helper);
}

TEST_F(ClientMgrTest, Remove_Client_Is_Reentrant)
{
    std::vector<common::backend_api::ObjectConfigParam_t> initial_params;
    const auto config = params.to_config(initial_params);
    Helper * helper = ClientMgr<Helper>::pop(config);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    ClientMgr<Helper>::push(helper);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 1);

    // push is reentrant
    EXPECT_NO_THROW(ClientMgr<Helper>::push(helper));
    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 1);
}

}; //namespace runai::llm::streamer::impl::s3
