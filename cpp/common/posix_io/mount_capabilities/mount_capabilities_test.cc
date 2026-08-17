#include "common/posix_io/mount_capabilities/mount_capabilities.h"

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

namespace runai::llm::streamer::common::posix_io
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

}; // namespace runai::llm::streamer::common::posix_io
