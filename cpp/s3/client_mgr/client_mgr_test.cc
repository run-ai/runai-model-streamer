
#include "s3/client_mgr/client_mgr.h"

#include <gtest/gtest.h>

#include <set>
#include <string>

#include "utils/random/random.h"

namespace runai::llm::streamer::impl::s3
{

struct Helper : S3ClientBase
{
    Helper(const common::s3::Path & path, const common::s3::Credentials_C & credentials) :
        S3ClientBase(path, credentials),
        counter(++global_counter),
        id(utils::random::number<size_t>())
    {
    }

    using S3ClientBase::bucket;
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
        s3_path(uri),
        credentials(
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr)
        )
    {}

    void TearDown() override
    {
        ClientMgr<Helper>::clear();

        EXPECT_EQ(ClientMgr<Helper>::size(), 0);
        EXPECT_EQ(ClientMgr<Helper>::unused(), 0);
        EXPECT_EQ(ClientMgr<Helper>::current_bucket(), "");
    }

    static common::s3::StorageUri create_uri()
    {
        return common::s3::StorageUri("s3://" + utils::random::string() + "/" + utils::random::string());
    }

    common::s3::StorageUri uri;
    common::s3::Path s3_path;
    common::s3::Credentials credentials;
};

TEST_F(ClientMgrTest, Creation_Sanity)
{
    EXPECT_EQ(ClientMgr<Helper>::size(), 0);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), "");
}

TEST_F(ClientMgrTest, Create_Client)
{
    Helper * helper = ClientMgr<Helper>::pop(s3_path, credentials);
    EXPECT_EQ(helper->bucket(), uri.bucket);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

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
    Helper * helper = ClientMgr<Helper>::pop(s3_path, credentials);
    EXPECT_EQ(helper->bucket(), uri.bucket);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

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
        uri.path = path;
        const common::s3::Path s3_path(uri);
        Helper * helper = ClientMgr<Helper>::pop(s3_path, credentials);
        EXPECT_EQ(helper->bucket(), uri.bucket);
        EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);
        EXPECT_EQ(helper->key(), credentials.access_key_id);
        EXPECT_EQ(helper->secret(), credentials.secret_access_key);
        EXPECT_EQ(helper->token(), credentials.session_token);
        EXPECT_EQ(helper->region(), credentials.region);
        EXPECT_EQ(helper->endpoint(), credentials.endpoint);

        EXPECT_TRUE(helper->verify_credentials(credentials));
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
    Helper * helper = ClientMgr<Helper>::pop(s3_path, credentials);
    EXPECT_EQ(helper->bucket(), uri.bucket);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

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

        bool changed = !helper->verify_credentials(new_credentials);
        std::string path = utils::random::string();
        uri.path = path;
        const common::s3::Path s3_path(uri);
        Helper * helper = ClientMgr<Helper>::pop(s3_path, new_credentials);
        EXPECT_EQ(helper->bucket(), uri.bucket);
        EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);
        EXPECT_EQ(helper->key(), new_credentials.access_key_id);
        EXPECT_EQ(helper->secret(), new_credentials.secret_access_key);
        EXPECT_EQ(helper->token(), new_credentials.session_token);
        EXPECT_EQ(helper->region(), new_credentials.region);
        EXPECT_EQ(helper->endpoint(), new_credentials.endpoint);

        EXPECT_TRUE(helper->verify_credentials(new_credentials));

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
        Helper * helper = ClientMgr<Helper>::pop(s3_path, credentials);
        EXPECT_EQ(helper->bucket(), uri.bucket);
        EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

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
    while (uri_.bucket == uri.bucket)
    {
        uri_ = create_uri();
    }

    used.clear();
    for (unsigned i = 0; i < n; ++i)
    {
        const std::string path = utils::random::string();
        uri_.path = path;
        const common::s3::Path s3_path_(uri_);
        Helper * helper = ClientMgr<Helper>::pop(s3_path_, credentials);
        EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

        EXPECT_EQ(helper->bucket(), uri_.bucket);
        EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri_.bucket);

        EXPECT_EQ(ClientMgr<Helper>::size(), i + 1);
        used.insert(helper);
    }

    for (auto helper : used)
    {
        ClientMgr<Helper>::push(helper);
    }
}

TEST_F(ClientMgrTest, Remove_Client)
{
    Helper * helper = ClientMgr<Helper>::pop(s3_path, credentials);

    EXPECT_EQ(helper->bucket(), uri.bucket);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    ClientMgr<Helper>::push(helper);
}

TEST_F(ClientMgrTest, Remove_Client_Is_Reentrant)
{
    Helper * helper = ClientMgr<Helper>::pop(s3_path, credentials);

    EXPECT_EQ(helper->bucket(), uri.bucket);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

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
