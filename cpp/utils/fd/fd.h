#pragma once

#include <ostream>
#include <set>
#include <map>
#include <string>
#include <vector>

namespace runai::llm::streamer::utils
{

struct Fd
{
    enum class Read : char
    {
        Exactly, // read an exact number of bytes
        UpTo,    // read up to a certain number of bytes
        Eof,     // read until eof has been reached
    };

    Fd() = default; // empty creation
    Fd(int fd);
    virtual ~Fd();

    Fd(Fd && other);
    Fd & operator=(Fd &&);

    Fd(const Fd &)              = delete;
    Fd & operator=(const Fd &)  = delete;

    // getters
    int fd() const;
    operator int() const;

    // reading and writing to device in blocks
    size_t read(size_t size, /* out */ void * data, Read mode);
    size_t read(size_t size, /* out */ void * data, Read mode, size_t chunk_size);

    void write(void const * const data, size_t size);
    void write(void const * const data, size_t size, size_t chunk_size);

    // go to an offset from the start of the file
    void seek(off_t offset) const;

    size_t size() const;

    // basic reading and writing

    std::vector<uint8_t> read(size_t size, Read mode = Read::Exactly);
    void write(const std::vector<uint8_t> & data);

    // list regular file names in path
    static std::vector<std::string> list(const std::string & path);

    // read the entire file - used for testing

    static std::vector<uint8_t> read(const std::string & path);
    static std::vector<uint8_t> read(const std::string & path, off_t offset, size_t bytes);

    static void write(const std::string & path, const std::vector<uint8_t> & data, int flags, mode_t mode);

 protected:
    // wrappers of OS read/write methods.
    // by default, `read()` and `write()` are used, respectively.

    virtual ssize_t _os_read(void * buf, size_t count);
    virtual ssize_t _os_write(void const * buf, size_t count);

    int _fd = -1;

 private:
    // try closing the fd if needed;
    // this does not throw exceptions on failure.
    void _close();
};

inline bool operator<(const Fd & lhs, const Fd & rhs)
{
    return lhs.fd() < rhs.fd();
}

inline std::ostream & operator<<(std::ostream & os, const Fd & fd)
{
    return os << fd.fd();
}

} // namespace runai::llm::streamer::utils
