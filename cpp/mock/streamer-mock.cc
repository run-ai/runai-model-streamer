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
std::vector<State> __multi_state;
unsigned __multi_file_count = 0;
unsigned __current_multi_file = 0;


int request(void * streamer, const char * path, size_t file_offset, size_t bytesize, char * dst, unsigned num_sizes, size_t * internal_sizes, State * state)
{
    state->file = utils::Fd(::open(path, O_RDONLY));
    if (state->file.fd() == -1) {
        LOG(ERROR) << "Error opening file: " << path;
        return -1;
    }

    try
    {
        state->file.seek(file_offset);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Error seek in file: " << path << " to: " << file_offset;
        return -1;
    }

    state->read_item_sizes.resize(num_sizes);
    std::memcpy(state->read_item_sizes.data(), internal_sizes, num_sizes * sizeof(size_t));

    state->total_items = num_sizes;
    state->destination = dst;
    return 0;
}

int response(void * streamer, unsigned * index, State * state)
{
    size_t result = 0;
    auto to_read = state->read_item_sizes[state->current_item];
    auto to_dst = state->destination + state->current_dst_offset;
    try
    {
        result = state->file.read(to_read, to_dst, utils::Fd::Read::Eof);
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

    state->current_dst_offset += to_read;
    *index = state->current_item;
    state->current_item++;
    return 0;
}

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

extern "C" int runai_read_with_credentials(void * streamer, const char * path, size_t file_offset, size_t bytesize, char * dst, const char * key, const char * secret, const char * token, const char * region,  const char * endpoint)
{
    return runai_read(streamer, path, file_offset, bytesize, dst);
}


extern "C" int runai_request(void * streamer, const char * path, size_t file_offset, size_t bytesize, char * dst, unsigned num_sizes, size_t * internal_sizes)
{
    return request(streamer, path, file_offset, bytesize, dst, num_sizes, internal_sizes, &__state);
}

extern "C" int runai_request_with_credentials(void * streamer, const char * path, size_t file_offset, size_t bytesize, char * dst, unsigned num_sizes, size_t * internal_sizes, const char * key, const char * secret, const char * token, const char * region, const char * endpoint)
{
    return request(streamer, path, file_offset, bytesize, dst, num_sizes, internal_sizes, &__state);
}

extern "C" int runai_response(void * streamer, unsigned * index)
{
    return response(streamer, index, &__state);
}

extern "C" int runai_request_multi(
    void * streamer,
    unsigned num_files,
    const char ** paths,
    size_t * file_offsets,
    size_t * bytesizes,
    void ** dsts,
    unsigned * num_sizes,
    size_t ** internal_sizes,
    const char * key,
    const char * secret,
    const char * token,
    const char * region,
    const char * endpoint
)
{
    __multi_state.clear();
    __current_multi_file = 0;
    __multi_file_count = num_files;

    int buffer_start = 0;
    for (unsigned i = 0; i < num_files; ++i) {
        State state;
        request(streamer, paths[i], file_offsets[i], bytesizes[i], (char*)((char*)dsts[0] + buffer_start), num_sizes[i], internal_sizes[i], &state);
        buffer_start = buffer_start + bytesizes[i];
        __multi_state.push_back(std::move(state));
    }

    return 0;
}

extern "C" int runai_response_multi(void * streamer, unsigned * file_index, unsigned * index)
{
    if (__current_multi_file >= __multi_state.size()) {
        return -1; // All files processed
    }

    State& state = __multi_state[__current_multi_file];

    if (state.current_item >= state.total_items) {
        ++__current_multi_file;
        return runai_response_multi(streamer, file_index, index); // recurse to next file
    }

    *file_index = __current_multi_file;
    return response(streamer, index, &state);
}

extern "C" const char * runai_response_str(int response_code)
{
    return 0;
}
} // namespace runai::llm::streamer
