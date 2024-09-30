#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <vector>
#include "utils/fd/fd.h"
#include "utils/logging/logging.h"


namespace runai::llm::streamer
{
struct State {
    utils::Fd file;
    std::vector<size_t> read_item_sizes;
    unsigned total_items = 0;
    unsigned current_item = 0;
    char* destination = nullptr;
    unsigned current_dst_offset = 0;
};

State __state;

extern "C" int runai_start(void ** streamer)
{
    __state = State{};
    *streamer = reinterpret_cast<void*>(0x123456789ABCDEF0);
    return 0;
}

extern "C" void runai_end(void * streamer)
{
}

extern "C" int runai_read(void * streamer, const char * path, size_t file_offset, size_t bytesize, char * dst)
{
    auto file = utils::Fd(::open(path, O_RDONLY));
    if (file.fd() == -1) {
        LOG(ERROR) << "Error opening file: " << path;
        return -1;
    }

    try
    {
        file.seek(file_offset);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Error seek in file: " << path << " to: " << file_offset;
        return -1;
    }

    try
    {
        size_t result = file.read(bytesize, dst, utils::Fd::Read::Eof);
        if (result != bytesize)
        {
            LOG(ERROR) << "Reached EOF";
            return -1;
        }
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Failed to read from file";
        return -1;
    }
    return 0;
}

extern "C" int runai_request(void * streamer, const char * path, size_t file_offset, size_t bytesize, char * dst, unsigned num_sizes, size_t * internal_sizes)
{
    __state = State{};
    __state.file = utils::Fd(::open(path, O_RDONLY));
    if (__state.file.fd() == -1) {
        LOG(ERROR) << "Error opening file: " << path;
        return -1;
    }

    try
    {
        __state.file.seek(file_offset);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Error seek in file: " << path << " to: " << file_offset;
        return -1;
    }

    __state.read_item_sizes.resize(num_sizes);
    std::memcpy(__state.read_item_sizes.data(), internal_sizes, num_sizes * sizeof(size_t));

    __state.total_items = num_sizes;
    __state.destination = dst;
    return 0;
}

extern "C" int runai_response(void * streamer, unsigned * index)
{
    size_t result = 0;
    auto to_read = __state.read_item_sizes[__state.current_item];
    auto to_dst = __state.destination + __state.current_dst_offset;
    try
    {
        result = __state.file.read(to_read, to_dst, utils::Fd::Read::Eof);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Failed to read from file";
        return -1;
    }

    if (result != to_read)
    {
        LOG(ERROR) << "Reached EOF";
        return -1;
    }

    __state.current_dst_offset += to_read;
    *index = __state.current_item;
    __state.current_item++;
    return 0;
}

extern "C" const char * runai_response_str(int response_code)
{
    return 0;
}
} // namespace runai::llm::streamer
