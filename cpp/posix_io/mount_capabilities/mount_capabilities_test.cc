#include "posix_io/mount_capabilities/mount_capabilities.h"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <linux/magic.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <string>

#include "common/exception/exception.h"
#include "utils/random/random.h"
#include "utils/temp/file/file.h"

namespace runai::llm::streamer::posix_io
{

namespace
{

// Answered from the FILESYSTEM, never from the code under test. A skip must depend on the
// environment; deciding it from MountCapability would let a broken probe skip instead of fail.
bool is_tmpfs(const std::string & path)
{
    struct statfs fs;
    return ::statfs(path.c_str(), &fs) == 0 && fs.f_type == TMPFS_MAGIC;
}

dev_t device_of(const std::string & path)
{
    struct stat st;
    return ::stat(path.c_str(), &st) == 0 ? st.st_dev : 0;
}

} // namespace

TEST(MountCapabilities, Regular_File_Is_Not_Memory_Backed)
{
    utils::temp::File file(utils::random::buffer(64));
    MountCapabilities mounts;

    const auto capability = mounts.of_path(file.path);

    EXPECT_NE(capability.dev, 0);
    EXPECT_FALSE(capability.memory_backed);
}

// The probe that keeps tmpfs out of the O_DIRECT cell. Nothing else detects it: an O_DIRECT open
// SUCCEEDS on tmpfs and fails later at read time, so statfs magic is the only working test.
TEST(MountCapabilities, Tmpfs_Is_Memory_Backed)
{
    if (!is_tmpfs("/dev/shm"))
    {
        GTEST_SKIP() << "/dev/shm is not tmpfs here";
    }

    const std::string path = "/dev/shm/runai_mount_test_" + utils::random::string();
    const int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0600);
    ASSERT_GE(fd, 0);
    ::close(fd);

    MountCapabilities mounts;
    const auto capability = mounts.of_path(path);
    ::unlink(path.c_str());

    EXPECT_TRUE(capability.memory_backed);
}

// The point of the class: a model's shards share a mount, so only the first costs a probe.
TEST(MountCapabilities, Probes_Once_Per_Mount)
{
    utils::temp::File a(utils::random::buffer(64));
    utils::temp::File b(utils::random::buffer(64));

    MountCapabilities mounts;
    EXPECT_EQ(mounts.size(), 0);

    const auto first = mounts.of_path(a.path);
    EXPECT_EQ(mounts.size(), 1);

    const auto second = mounts.of_path(b.path);
    EXPECT_EQ(mounts.size(), 1) << "two files on one mount must not be two entries";

    EXPECT_EQ(first.dev, second.dev);
    EXPECT_EQ(first.memory_backed, second.memory_backed);
}

// of_fd is verification on an fd the caller was opening anyway - it must agree with of_path and must
// not add an entry for a mount already known.
TEST(MountCapabilities, Of_Fd_Agrees_With_Of_Path)
{
    utils::temp::File file(utils::random::buffer(64));
    MountCapabilities mounts;

    const auto by_path = mounts.of_path(file.path);
    EXPECT_EQ(mounts.size(), 1);

    const int fd = ::open(file.path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    const auto by_fd = mounts.of_fd(fd);
    ::close(fd);

    EXPECT_EQ(mounts.size(), 1);
    EXPECT_EQ(by_fd.dev, by_path.dev);
    EXPECT_EQ(by_fd.memory_backed, by_path.memory_backed);
}

// Two mounts must be two entries, or one would answer for the other - which is what selects the
// engine, so a collapse here would put both mounts on one worker and lose the isolation.
TEST(MountCapabilities, Distinct_Mounts_Are_Distinct_Entries)
{
    utils::temp::File regular(utils::random::buffer(64));

    const std::string shm = "/dev/shm/runai_mount_test_" + utils::random::string();
    const int fd = ::open(shm.c_str(), O_CREAT | O_RDWR, 0600);
    ASSERT_GE(fd, 0);
    ::close(fd);

    // Whether these are two mounts is a fact about the machine, so ask the machine.
    if (device_of(regular.path) == device_of(shm))
    {
        ::unlink(shm.c_str());
        GTEST_SKIP() << "/dev/shm and the temp directory are the same mount here";
    }

    MountCapabilities mounts;
    const auto a = mounts.of_path(regular.path);
    const auto b = mounts.of_path(shm);
    ::unlink(shm.c_str());

    EXPECT_EQ(mounts.size(), 2);
    EXPECT_NE(a.dev, b.dev);
}

TEST(MountCapabilities, Missing_Path_Throws)
{
    MountCapabilities mounts;
    EXPECT_THROW(mounts.of_path("/no/such/path/" + utils::random::string()), common::Exception);
    EXPECT_EQ(mounts.size(), 0) << "a failed probe must not leave an entry";
}

TEST(MountCapabilities, Bad_Fd_Throws)
{
    MountCapabilities mounts;
    EXPECT_THROW(mounts.of_fd(-1), common::Exception);
}

// The answer must come from the FILESYSTEM, not from the code under test, or a broken probe would
// agree with itself. So this opens the file directly and compares.
TEST(MountCapabilities, Direct_Support_Matches_A_Real_Open)
{
    const auto data = utils::random::buffer(4096);
    utils::temp::File file(data);

    int fd = ::open(file.path.c_str(), O_RDONLY | O_DIRECT);
    const bool really_supported = fd >= 0;
    if (fd >= 0)
    {
        ::close(fd);
    }

    MountCapabilities mounts;
    const auto support = mounts.direct_support(device_of(file.path), file.path);

    EXPECT_EQ(support, really_supported ? DirectSupport::Yes : DirectSupport::No);
}

// One open per mount, however many files sit on it. A model's shards share a mount, so without the
// cache a 200-shard load would open and close 200 extra times.
TEST(MountCapabilities, Direct_Support_Is_Probed_Once_Per_Mount)
{
    const auto data = utils::random::buffer(4096);
    utils::temp::File first(data);
    utils::temp::File second(data);

    const dev_t dev = device_of(first.path);
    ASSERT_EQ(dev, device_of(second.path)) << "both temp files should be on one mount";

    MountCapabilities mounts;
    const auto answer = mounts.direct_support(dev, first.path);

    // The second call names a path that does not exist. If it probed, it would report Unknown - so
    // getting the first answer back proves it did not.
    EXPECT_EQ(mounts.direct_support(dev, "/no/such/path/" + utils::random::string()), answer);
}

// A file we cannot open says nothing about its mount. Reporting No would send every other file on
// that mount to the synchronous reader because of one bad path.
TEST(MountCapabilities, A_Missing_File_Leaves_Direct_Support_Unknown)
{
    MountCapabilities mounts;
    const auto path = "/no/such/path/" + utils::random::string();

    EXPECT_EQ(mounts.direct_support(device_of("/tmp"), path), DirectSupport::Unknown);

    // And nothing was remembered, so a readable file on the same mount can still answer.
    const auto data = utils::random::buffer(4096);
    utils::temp::File file(data);
    EXPECT_NE(mounts.direct_support(device_of(file.path), file.path), DirectSupport::Unknown);
}

// A directory can never be opened with O_DIRECT - measured, EINVAL - so probing with one would report
// every mount as incapable. This pins that the probe is given a file.
TEST(MountCapabilities, A_Directory_Is_Not_A_Valid_Probe_Target)
{
    const auto data = utils::random::buffer(4096);
    utils::temp::File file(data);

    MountCapabilities mounts;
    const auto by_file = mounts.direct_support(device_of(file.path), file.path);

    // A fresh instance, so the cache cannot answer for it.
    MountCapabilities by_directory_mounts;
    const auto by_directory = by_directory_mounts.direct_support(device_of("/tmp"), "/tmp");

    EXPECT_EQ(by_directory, DirectSupport::No)
        << "a directory refuses O_DIRECT whatever the mount can do";

    if (by_file == DirectSupport::Yes)
    {
        EXPECT_NE(by_file, by_directory) << "which is exactly why the probe must be given a file";
    }
}

}; // namespace runai::llm::streamer::posix_io
