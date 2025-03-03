
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
        id(utils::random::number<size_t>()),
        uri(uri)
    {}

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

    const size_t id;
    common::s3::StorageUri uri;
};

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

    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 0);

    ClientMgr<Helper>::push(helper);
    EXPECT_EQ(ClientMgr<Helper>::size(), 1);
    EXPECT_EQ(ClientMgr<Helper>::unused(), 1);

    unsigned n = utils::random::number(2, 20);
    for (unsigned i = 0; i < n; ++i)
    {
        const std::string path = utils::random::string();
        uri.path = path;
        Helper * helper = ClientMgr<Helper>::pop(uri, access_key_id, secret_access_key, session_token);
        EXPECT_EQ(helper->bucket(), uri.bucket);
        EXPECT_EQ(helper->path(), path);
        EXPECT_EQ(ClientMgr<Helper>::current_bucket(), uri.bucket);

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
