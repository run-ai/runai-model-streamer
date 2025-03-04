
#include "s3/client_mgr/client_mgr.h"

#include <gtest/gtest.h>

#include <set>
#include <string>

#include "utils/random/random.h"

namespace runai::llm::streamer::impl::s3
{

struct Helper
{
    Helper(const common::s3::StorageUri & uri, const std::string & access_key_id, const std::string & secret_access_key, const std::string & session_token) :
        counter(++global_counter),
        id(utils::random::number<size_t>()),
        uri(uri),
        key(access_key_id.c_str(), access_key_id.size()),
        secret(secret_access_key.c_str(), secret_access_key.size()),
        token(session_token.c_str(), session_token.size())
    {
    }

    void path(const std::string & str)
    {
        uri.path = str;
    }

    const std::string & path() const
    {
        return uri.path;
    }

    const std::string & bucket() const
    {
        return uri.bucket;
    }

    bool verify_credentials(const std::string & access_key_id, const std::string & secret_access_key, const std::string & session_token) const
    {
        return (access_key_id == key && secret_access_key == secret && session_token == token);
    }

    static size_t global_counter;
    const size_t counter;

    const size_t id;
    common::s3::StorageUri uri;
    const Aws::String key;
    const Aws::String secret;
    const Aws::String token;
};

size_t Helper::global_counter = 0;

struct ClientMgrTest : ::testing::Test
{
    ClientMgrTest() :
        uri(create_uri()),
        access_key_id(utils::random::string()),
        secret_access_key(utils::random::string()),
        session_token(utils::random::string())
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
    std::string access_key_id;
    std::string secret_access_key;
    std::string session_token;
};

TEST_F(ClientMgrTest, Creation_Sanity)
{
    EXPECT_EQ(ClientMgr<Helper>::size(), 0);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), "");
}

TEST_F(ClientMgrTest, Create_Client)
{
    Helper * helper = ClientMgr<Helper>::pop(uri, access_key_id, secret_access_key, session_token);
    EXPECT_EQ(helper->bucket(), uri.bucket);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

    EXPECT_EQ(helper->key, access_key_id);
    EXPECT_EQ(helper->secret, secret_access_key);
    EXPECT_EQ(helper->token, session_token);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    ClientMgr<Helper>::clear();
    EXPECT_EQ(ClientMgr<Helper>::size(), 1);

    ClientMgr<Helper>::push(helper);
}

TEST_F(ClientMgrTest, Reuse_Client)
{
    Helper * helper = ClientMgr<Helper>::pop(uri, access_key_id, secret_access_key, session_token);
    EXPECT_EQ(helper->bucket(), uri.bucket);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

    EXPECT_EQ(helper->key, access_key_id);
    EXPECT_EQ(helper->secret, secret_access_key);
    EXPECT_EQ(helper->token, session_token);

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
        Helper * helper = ClientMgr<Helper>::pop(uri, access_key_id, secret_access_key, session_token);
        EXPECT_EQ(helper->bucket(), uri.bucket);
        EXPECT_EQ(helper->path(), path);
        EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);
        EXPECT_EQ(helper->key, access_key_id);
        EXPECT_EQ(helper->secret, secret_access_key);
        EXPECT_EQ(helper->token, session_token);
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
    Helper * helper = ClientMgr<Helper>::pop(uri, access_key_id, secret_access_key, session_token);
    EXPECT_EQ(helper->bucket(), uri.bucket);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

    EXPECT_EQ(helper->key, access_key_id);
    EXPECT_EQ(helper->secret, secret_access_key);
    EXPECT_EQ(helper->token, session_token);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    ClientMgr<Helper>::push(helper);
    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 1);

    auto expected = helper->counter;

    unsigned n = utils::random::number(2, 20);
    for (unsigned i = 0; i < n; ++i)
    {
        auto new_key = utils::random::string();    
        auto new_secret = utils::random::string();    
        auto new_token = utils::random::string();

        bool changed = (new_key != access_key_id || new_secret != secret_access_key || new_token != session_token);

        const std::string path = utils::random::string();
        uri.path = path;
        Helper * helper = ClientMgr<Helper>::pop(uri, new_key, new_secret, new_token);
        EXPECT_EQ(helper->bucket(), uri.bucket);
        EXPECT_EQ(helper->path(), path);
        EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);
        EXPECT_EQ(helper->key, new_key);
        EXPECT_EQ(helper->secret, new_secret);
        EXPECT_EQ(helper->token, new_token);
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
        Helper * helper = ClientMgr<Helper>::pop(uri, access_key_id, secret_access_key, session_token);
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
        Helper * helper = ClientMgr<Helper>::pop(uri_, access_key_id, secret_access_key, session_token);
        EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

        EXPECT_EQ(helper->bucket(), uri_.bucket);
        EXPECT_EQ(helper->path(), path);
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
    Helper * helper = ClientMgr<Helper>::pop(uri, access_key_id, secret_access_key, session_token);

    EXPECT_EQ(helper->bucket(), uri.bucket);
    EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    ClientMgr<Helper>::push(helper);
}

TEST_F(ClientMgrTest, Remove_Client_Is_Reentrant)
{
    Helper * helper = ClientMgr<Helper>::pop(uri, access_key_id, secret_access_key, session_token);

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
