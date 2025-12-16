#include "utils/env/env.h"

#include <unistd.h>
#include <gtest/gtest.h>

#include <climits>

#include "utils/temp/env/env.h"
#include "utils/random/random.h"

namespace runai::llm::streamer::utils
{

TEST(EnvExists, No)
{
    EXPECT_FALSE(env_exists(random::string()));
}

TEST(EnvExists, Yes)
{
    temp::Env env;
    EXPECT_TRUE(env_exists(env.name));
}

TEST(try_getenv__Default, Non_Existing)
{
    std::string s;
    EXPECT_FALSE(try_getenv(random::string(), /* out */ s));
}

TEST(try_getenv__Default, Existing)
{
    temp::Env e;

    std::string s;
    EXPECT_TRUE(try_getenv(e.name, /* out */ s));
    EXPECT_EQ(s, e.value);
}

TEST(try_getenv__String, Non_Existing)
{
    std::string s;
    EXPECT_FALSE(try_getenv<std::string>(random::string(), /* out */ s));
}

TEST(try_getenv__String, Existing)
{
    temp::Env e;

    std::string s;
    EXPECT_TRUE(try_getenv<std::string>(e.name, /* out */ s));
    EXPECT_EQ(s, e.value);
}

TEST(try_getenv__Int, Non_Existing)
{
    int i;
    EXPECT_FALSE(try_getenv<int>(random::string(), /* out */ i));
}

TEST(try_getenv__Int, Existing)
{
    const auto name = random::string();
    const auto value = random::number<int>();

    temp::Env e(name, value);

    int i;
    EXPECT_TRUE(try_getenv<int>(name, /* out */ i));
    EXPECT_EQ(i, value);
}

TEST(try_getenv__Bool, Non_Existing)
{
    bool b;
    EXPECT_FALSE(try_getenv<bool>(random::string(), /* out */ b));
}

TEST(try_getenv__Bool, Existing)
{
    const auto name = random::string();
    const auto value = random::boolean();

    temp::Env e(name, value);

    bool b;
    EXPECT_TRUE(try_getenv<bool>(name, /* out */ b));
    EXPECT_EQ(b, value);
}

TEST(Getenv, Sanity)
{
    temp::Env env;
    EXPECT_EQ(getenv(env.name), env.value);
}

TEST(Getenv, NonExisting)
{
    EXPECT_THROW(getenv(random::string()), std::exception);
}

TEST(Getenv_String, Default)
{
    const auto def = random::string();

    EXPECT_EQ(getenv(random::string(), def), def);
    EXPECT_EQ(getenv<std::string>(random::string(), def), def);
}

TEST(Getenv_Int, Sanity)
{
    const int value = random::number();

    temp::Env env(std::to_string(value));

    EXPECT_EQ(getenv<int>(env.name), value);
    EXPECT_EQ(getenv<int>(env.name, random::number()), value);
}

TEST(Getenv_Int, Negative)
{
    temp::Env env("-1");

    EXPECT_EQ(getenv<int>(env.name), -1);
}

TEST(Getenv_Int, NotInt)
{
    temp::Env env("a");

    EXPECT_THROW(getenv<int>(env.name), std::exception);
    EXPECT_THROW(getenv<int>(env.name, random::number()), std::exception);
}

TEST(Getenv_Unsigned_long, Sanity)
{
    const unsigned long value = random::number<unsigned long>();

    temp::Env env(std::to_string(value));

    EXPECT_EQ(getenv<unsigned long>(env.name), value);
    EXPECT_EQ(getenv<unsigned long>(env.name, random::number<unsigned long>()), value);
}

TEST(Getenv_Unsigned_long, Negative)
{
    temp::Env env("-1");

    EXPECT_EQ(getenv<unsigned long>(env.name), ULONG_MAX);
}

TEST(Getenv_Unsigned_long, NotUnsignedLong)
{
    temp::Env env("a");

    EXPECT_THROW(getenv<unsigned long>(env.name), std::exception);
    EXPECT_THROW(getenv<unsigned long>(env.name, random::number()), std::exception);
}

TEST(Getenv_Bool, True)
{
    temp::Env env("1");

    EXPECT_TRUE(getenv<bool>(env.name));

    for (auto def : { true, false })
    {
        EXPECT_TRUE(getenv<bool>(env.name, def));
    }
}

TEST(Getenv_Bool, False)
{
    temp::Env env("0");

    EXPECT_FALSE(getenv<bool>(env.name));

    for (auto def : { true, false })
    {
        EXPECT_FALSE(getenv<bool>(env.name, def));
    }
}

TEST(Getenv_Bool, Default)
{
    for (auto def : { true, false })
    {
        EXPECT_EQ(getenv<bool>(random::string(), def), def);
    }
}

TEST(Getenv_Bool, NotBool)
{
    temp::Env env;

    EXPECT_THROW(getenv<bool>(env.name), std::exception);

    for (auto def : { true, false })
    {
        EXPECT_THROW(getenv<bool>(env.name, def), std::exception);
    }
}

} // namespace runai::llm::streamer::utils
