#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <vector>
#include <dlfcn.h>

#include "azure/azcache_provider/runai_azcache_provider.h"
#include "azure/azcache_provider/azcache_provider_loader.h"

namespace runai::llm::streamer::impl::azure::testing
{

namespace fs = std::filesystem;

class SimpleFileCacheTest : public ::testing::Test
{
protected:
    fs::path cache_dir_;
    fs::path so_path_;

    void SetUp() override
    {
        // Create a temp cache directory
        cache_dir_ = fs::temp_directory_path() / ("runai_cache_test_" + std::to_string(getpid()));
        fs::create_directories(cache_dir_);

        // Locate the example .so built by Bazel
        // Bazel puts genrule outputs relative to the runfiles directory
        const char* test_srcdir = getenv("TEST_SRCDIR");
        const char* test_workspace = getenv("TEST_WORKSPACE");
        if (test_srcdir && test_workspace)
        {
            so_path_ = fs::path(test_srcdir) / test_workspace / "azure/azcache_provider/libsimple_file_cache_test.so";
        }

        // Fallback: try relative path from working directory
        if (!fs::exists(so_path_))
        {
            so_path_ = "azure/azcache_provider/libsimple_file_cache_test.so";
        }
    }

    void TearDown() override
    {
        fs::remove_all(cache_dir_);
        unsetenv("RUNAI_CACHE_DIR");
    }

    // Write a test blob file into the cache directory
    void populate_cache(const std::string& container, const std::string& blob,
                        const std::vector<uint8_t>& data)
    {
        fs::path blob_path = cache_dir_ / container / blob;
        fs::create_directories(blob_path.parent_path());
        std::ofstream ofs(blob_path, std::ios::binary);
        ofs.write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    // Directly dlopen the example .so and return the function pointer
    blob_read_fn load_cache_fn()
    {
        void* handle = dlopen(so_path_.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (!handle)
        {
            ADD_FAILURE() << "dlopen failed: " << dlerror();
            return nullptr;
        }
        auto fn = reinterpret_cast<blob_read_fn>(
            dlsym(handle, BLOB_READ_SYMBOL));
        if (!fn)
        {
            ADD_FAILURE() << "dlsym failed: " << dlerror();
        }
        return fn;
    }
};

TEST_F(SimpleFileCacheTest, ReadFullBlob)
{
    // Populate cache with test data
    std::vector<uint8_t> data(1024);
    for (size_t i = 0; i < data.size(); ++i)
    {
        data[i] = static_cast<uint8_t>(i & 0xFF);
    }
    populate_cache("test-container", "model/weights.bin", data);

    setenv("RUNAI_CACHE_DIR", cache_dir_.c_str(), 1);

    auto cache_read = load_cache_fn();
    ASSERT_NE(cache_read, nullptr);

    // Read the full blob
    std::vector<uint8_t> buf(1024);
    char* error = nullptr;
    ssize_t result = cache_read("test-account", "test-container", "model/weights.bin",
                                buf.data(), 0, 1024, &error);
    ASSERT_EQ(result, 1024) << (error ? error : "no error detail");
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(buf, data);

    if (error) free(error);
}

TEST_F(SimpleFileCacheTest, ReadRange)
{
    // Populate cache with sequential bytes
    std::vector<uint8_t> data(4096);
    for (size_t i = 0; i < data.size(); ++i)
    {
        data[i] = static_cast<uint8_t>(i % 251);  // prime to avoid aliasing
    }
    populate_cache("models", "llm/shard-0001.safetensors", data);

    setenv("RUNAI_CACHE_DIR", cache_dir_.c_str(), 1);

    auto cache_read = load_cache_fn();
    ASSERT_NE(cache_read, nullptr);

    // Read a range from the middle
    size_t offset = 1000;
    size_t length = 512;
    std::vector<uint8_t> buf(length);
    char* error = nullptr;

    ssize_t result = cache_read("test-account", "models", "llm/shard-0001.safetensors",
                                buf.data(), offset, length, &error);
    ASSERT_EQ(result, static_cast<ssize_t>(length)) << (error ? error : "no error");
    EXPECT_EQ(error, nullptr);

    // Verify data matches the expected range
    for (size_t i = 0; i < length; ++i)
    {
        EXPECT_EQ(buf[i], data[offset + i]) << "mismatch at offset " << (offset + i);
    }

    if (error) free(error);
}

TEST_F(SimpleFileCacheTest, MissingBlobReturnsError)
{
    setenv("RUNAI_CACHE_DIR", cache_dir_.c_str(), 1);

    auto cache_read = load_cache_fn();
    ASSERT_NE(cache_read, nullptr);

    char buf[100];
    char* error = nullptr;

    ssize_t result = cache_read("test-account", "no-such-container", "no-such-blob",
                                buf, 0, 100, &error);
    EXPECT_EQ(result, -1);
    EXPECT_NE(error, nullptr);
    if (error)
    {
        EXPECT_NE(std::string(error).find("open failed"), std::string::npos);
        free(error);
    }
}

TEST_F(SimpleFileCacheTest, ShortReadReturnsError)
{
    // Populate cache with a small file
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    populate_cache("c", "small.bin", data);

    setenv("RUNAI_CACHE_DIR", cache_dir_.c_str(), 1);

    auto cache_read = load_cache_fn();
    ASSERT_NE(cache_read, nullptr);

    // Try to read more than the file contains
    char buf[100];
    char* error = nullptr;

    ssize_t result = cache_read("test-account", "c", "small.bin", buf, 0, 100, &error);
    EXPECT_EQ(result, -1);
    EXPECT_NE(error, nullptr);
    if (error)
    {
        EXPECT_NE(std::string(error).find("short read"), std::string::npos);
        free(error);
    }
}

TEST_F(SimpleFileCacheTest, NullArgumentsReturnError)
{
    auto cache_read = load_cache_fn();
    ASSERT_NE(cache_read, nullptr);

    char* error = nullptr;
    char buf[10];

    EXPECT_EQ(cache_read(nullptr, "c", "blob", buf, 0, 10, &error), -1);
    if (error) { free(error); error = nullptr; }

    EXPECT_EQ(cache_read("a", nullptr, "blob", buf, 0, 10, &error), -1);
    if (error) { free(error); error = nullptr; }

    EXPECT_EQ(cache_read("a", "c", nullptr, buf, 0, 10, &error), -1);
    if (error) { free(error); error = nullptr; }

    EXPECT_EQ(cache_read("a", "c", "b", nullptr, 0, 10, &error), -1);
    if (error) { free(error); error = nullptr; }
}

TEST_F(SimpleFileCacheTest, MultipleContainers)
{
    // Populate two containers with different data
    std::vector<uint8_t> data_a = {10, 20, 30, 40};
    std::vector<uint8_t> data_b = {50, 60, 70, 80};
    populate_cache("container-a", "file.bin", data_a);
    populate_cache("container-b", "file.bin", data_b);

    setenv("RUNAI_CACHE_DIR", cache_dir_.c_str(), 1);

    auto cache_read = load_cache_fn();
    ASSERT_NE(cache_read, nullptr);

    char buf[4];
    char* error = nullptr;

    ASSERT_EQ(cache_read("test-account", "container-a", "file.bin", buf, 0, 4, &error), 4);
    EXPECT_EQ(memcmp(buf, data_a.data(), 4), 0);

    ASSERT_EQ(cache_read("test-account", "container-b", "file.bin", buf, 0, 4, &error), 4);
    EXPECT_EQ(memcmp(buf, data_b.data(), 4), 0);

    if (error) free(error);
}

TEST_F(SimpleFileCacheTest, PathTraversalRejected)
{
    setenv("RUNAI_CACHE_DIR", cache_dir_.c_str(), 1);

    auto cache_read = load_cache_fn();
    ASSERT_NE(cache_read, nullptr);

    char buf[10];
    char* error = nullptr;

    // ".." in container
    EXPECT_EQ(cache_read("a", "../etc", "passwd", buf, 0, 10, &error), -1);
    ASSERT_NE(error, nullptr);
    EXPECT_NE(std::string(error).find("path traversal"), std::string::npos);
    free(error); error = nullptr;

    // ".." in blob
    EXPECT_EQ(cache_read("a", "c", "../../etc/passwd", buf, 0, 10, &error), -1);
    ASSERT_NE(error, nullptr);
    EXPECT_NE(std::string(error).find("path traversal"), std::string::npos);
    free(error); error = nullptr;

    // ".." embedded in path
    EXPECT_EQ(cache_read("a", "c", "sub/../../../etc/shadow", buf, 0, 10, &error), -1);
    ASSERT_NE(error, nullptr);
    EXPECT_NE(std::string(error).find("path traversal"), std::string::npos);
    free(error); error = nullptr;

    // "..." (not traversal) should NOT be rejected — will fail with open error instead
    EXPECT_EQ(cache_read("a", "c", "...", buf, 0, 10, &error), -1);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(std::string(error).find("path traversal"), std::string::npos);
    free(error); error = nullptr;
}

} // namespace runai::llm::streamer::impl::azure::testing
