#include "s3/client/client.h"

#include <gtest/gtest.h>

namespace runai::llm::streamer::impl::s3
{

TEST(ParseEndpointScheme, HttpPrefix)
{
    auto result = parse_endpoint_scheme("http://10.1.254.10");
    EXPECT_EQ(result.host, "10.1.254.10");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTP);
}

TEST(ParseEndpointScheme, HttpPrefixWithPort)
{
    auto result = parse_endpoint_scheme("http://10.1.254.10:9000");
    EXPECT_EQ(result.host, "10.1.254.10:9000");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTP);
}

TEST(ParseEndpointScheme, HttpsPrefix)
{
    auto result = parse_endpoint_scheme("https://s3.amazonaws.com");
    EXPECT_EQ(result.host, "s3.amazonaws.com");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTPS);
}

TEST(ParseEndpointScheme, HttpsPrefixWithPort)
{
    auto result = parse_endpoint_scheme("https://cwobject.com:443");
    EXPECT_EQ(result.host, "cwobject.com:443");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTPS);
}

TEST(ParseEndpointScheme, NoScheme)
{
    auto result = parse_endpoint_scheme("10.1.254.10");
    EXPECT_EQ(result.host, "10.1.254.10");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTPS);
}

TEST(ParseEndpointScheme, NoSchemeWithPort)
{
    auto result = parse_endpoint_scheme("localhost:9000");
    EXPECT_EQ(result.host, "localhost:9000");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTPS);
}

TEST(ParseEndpointScheme, EmptyString)
{
    auto result = parse_endpoint_scheme("");
    EXPECT_EQ(result.host, "");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTPS);
}

TEST(ParseEndpointScheme, HttpInHostname)
{
    // Ensure we only strip the scheme prefix, not "http" appearing elsewhere
    auto result = parse_endpoint_scheme("httpbin.org");
    EXPECT_EQ(result.host, "httpbin.org");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTPS);
}

TEST(ParseEndpointScheme, HttpUpperCase)
{
    auto result = parse_endpoint_scheme("HTTP://10.1.254.10");
    EXPECT_EQ(result.host, "10.1.254.10");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTP);
}

TEST(ParseEndpointScheme, HttpsMixedCase)
{
    auto result = parse_endpoint_scheme("Https://s3.example.com");
    EXPECT_EQ(result.host, "s3.example.com");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTPS);
}

TEST(ParseEndpointScheme, HttpSchemeOnly)
{
    auto result = parse_endpoint_scheme("http://");
    EXPECT_EQ(result.host, "");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTP);
}

TEST(ParseEndpointScheme, HttpsSchemeOnly)
{
    auto result = parse_endpoint_scheme("https://");
    EXPECT_EQ(result.host, "");
    EXPECT_EQ(result.scheme, Aws::Http::Scheme::HTTPS);
}

}; // namespace runai::llm::streamer::impl::s3
