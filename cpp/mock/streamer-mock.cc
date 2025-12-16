#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <vector>
#include <limits>
#include <sys/stat.h>
#include "utils/fd/fd.h"
#include "utils/logging/logging.h"
#include "common/response_code/response_code.h"


namespace runai::llm::streamer
{
struct State {
    utils::Fd file;
    std::vector<size_t> read_item_sizes;
    unsigned total_items = 0;
    unsigned current_item = 0;
    char* destination = nullptr;
    size_t current_dst_offset = 0;
    size_t file_size = 0;
    size_t file_offset = 0;
    std::string file_path;
    bool is_valid = false;
};

// Global state for managing multiple file requests
std::vector<State> __multi_state;
unsigned __current_multi_file = 0;
bool __request_in_progress = false;

// Helper function to get file size
static size_t get_file_size(const char* path)
{
    struct stat st;
    if (stat(path, &st) == 0)
    {
        return static_cast<size_t>(st.st_size);
    }
    return 0;
}

// Helper function to validate and initialize a single file request
static int validate_and_init_request(
    void * streamer,
    const char * path,
    size_t file_offset,
    size_t bytesize,
    char * dst,
    unsigned num_sizes,
    size_t * internal_sizes,
    State * state)
{
    // Validate null pointers
    if (path == nullptr)
    {
        LOG(ERROR) << "Path is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (dst == nullptr)
    {
        LOG(ERROR) << "Destination buffer is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (internal_sizes == nullptr && num_sizes > 0)
    {
        LOG(ERROR) << "Internal sizes array is null but num_sizes > 0";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    // Validate empty request
    if (bytesize == 0 && num_sizes == 0)
    {
        LOG(ERROR) << "Empty request - no response will be sent";
        return static_cast<int>(common::ResponseCode::EmptyRequestError);
    }

    if (num_sizes == 0 || bytesize == 0)
    {
        LOG(ERROR) << "Total bytes to read is " << bytesize << " but number of sub requests is " << num_sizes;
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    // Validate internal_sizes sum equals bytesize
    size_t total_internal_size = 0;
    for (unsigned i = 0; i < num_sizes; ++i)
    {
        if (internal_sizes[i] == 0)
        {
            LOG(ERROR) << "Internal size at index " << i << " is zero";
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }
        
        // Check for overflow
        if (total_internal_size > std::numeric_limits<size_t>::max() - internal_sizes[i])
        {
            LOG(ERROR) << "Internal sizes sum overflow";
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }
        
        total_internal_size += internal_sizes[i];
    }

    if (total_internal_size != bytesize)
    {
        LOG(ERROR) << "Internal sizes sum (" << total_internal_size 
                   << ") does not match bytesize (" << bytesize << ")";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    // Check if file exists and get its size
    if (!utils::Fd::exists(path))
    {
        LOG(ERROR) << "File does not exist: " << path;
        return static_cast<int>(common::ResponseCode::FileAccessError);
    }

    size_t file_size = get_file_size(path);
    if (file_size == 0 && bytesize > 0)
    {
        LOG(ERROR) << "File is empty or cannot determine size: " << path;
        return static_cast<int>(common::ResponseCode::FileAccessError);
    }

    // Validate file offset and bytesize don't exceed file size
    if (file_offset > file_size)
    {
        LOG(ERROR) << "File offset (" << file_offset 
                   << ") exceeds file size (" << file_size << ") for file: " << path;
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (file_offset + bytesize > file_size)
    {
        LOG(ERROR) << "Requested range [" << file_offset << ", " << file_offset + bytesize
                   << ") exceeds file size (" << file_size << ") for file: " << path;
        return static_cast<int>(common::ResponseCode::EofError);
    }

    // Open file
    state->file = utils::Fd(::open(path, O_RDONLY));
    if (state->file.fd() == -1)
    {
        LOG(ERROR) << "Error opening file: " << path;
        return static_cast<int>(common::ResponseCode::FileAccessError);
    }

    // Seek to file offset
    try
    {
        state->file.seek(file_offset);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Error seeking in file: " << path << " to offset: " << file_offset;
        return static_cast<int>(common::ResponseCode::EofError);
    }

    // Initialize state
    state->read_item_sizes.resize(num_sizes);
    std::memcpy(state->read_item_sizes.data(), internal_sizes, num_sizes * sizeof(size_t));
    state->total_items = num_sizes;
    state->current_item = 0;
    state->destination = dst;
    state->current_dst_offset = 0;
    state->file_size = file_size;
    state->file_offset = file_offset;
    state->file_path = std::string(path);
    state->is_valid = true;

    return static_cast<int>(common::ResponseCode::Success);
}

// Helper function to read a single chunk
static int read_chunk(State * state, unsigned * index)
{
    if (!state->is_valid)
    {
        LOG(ERROR) << "Invalid state for reading";
        return static_cast<int>(common::ResponseCode::UnknownError);
    }

    if (state->current_item >= state->total_items)
    {
        LOG(ERROR) << "All items already read";
        return static_cast<int>(common::ResponseCode::FinishedError);
    }

    size_t to_read = state->read_item_sizes[state->current_item];
    char* to_dst = state->destination + state->current_dst_offset;

    // Validate we're not reading beyond the file
    size_t current_file_position = state->file_offset + state->current_dst_offset;
    if (current_file_position + to_read > state->file_size)
    {
        LOG(ERROR) << "Read would exceed file size. Position: " << current_file_position
                   << ", To read: " << to_read << ", File size: " << state->file_size;
        return static_cast<int>(common::ResponseCode::EofError);
    }

    size_t result = 0;
    try
    {
        result = state->file.read(to_read, to_dst, utils::Fd::Read::Exactly);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Failed to read from file: " << state->file_path;
        return static_cast<int>(common::ResponseCode::UnknownError);
    }

    if (result != to_read)
    {
        LOG(ERROR) << "Read " << result << " bytes, expected " << to_read 
                   << " bytes from file: " << state->file_path;
        return static_cast<int>(common::ResponseCode::EofError);
    }

    state->current_dst_offset += to_read;
    *index = state->current_item;
    state->current_item++;

    return static_cast<int>(common::ResponseCode::Success);
}

extern "C" int runai_start(void ** streamer)
{
    if (streamer == nullptr)
    {
        LOG(ERROR) << "Streamer output parameter is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    // Clean up any previous state
    __multi_state.clear();
    __current_multi_file = 0;
    __request_in_progress = false;

    *streamer = reinterpret_cast<void*>(0x123456789ABCDEF0);
    return static_cast<int>(common::ResponseCode::Success);
}

extern "C" void runai_end(void * streamer)
{
    // Clean up all file descriptors
    __multi_state.clear();
    __current_multi_file = 0;
    __request_in_progress = false;
}

extern "C" int runai_request(
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
    // Validate streamer pointer
    if (streamer == nullptr)
    {
        LOG(ERROR) << "Streamer pointer is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    // Validate null pointers
    if (paths == nullptr)
    {
        LOG(ERROR) << "Paths array is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (file_offsets == nullptr)
    {
        LOG(ERROR) << "File offsets array is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (bytesizes == nullptr)
    {
        LOG(ERROR) << "Bytesizes array is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (dsts == nullptr)
    {
        LOG(ERROR) << "Destination buffers array is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (num_sizes == nullptr)
    {
        LOG(ERROR) << "Num sizes array is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (internal_sizes == nullptr)
    {
        LOG(ERROR) << "Internal sizes array is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    // Validate num_files
    if (num_files == 0)
    {
        LOG(ERROR) << "Number of files is zero";
        return static_cast<int>(common::ResponseCode::EmptyRequestError);
    }

    // Validate destination buffer
    if (dsts[0] == nullptr)
    {
        LOG(ERROR) << "Destination buffer is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    // Check if a request is already in progress
    if (__request_in_progress)
    {
        LOG(ERROR) << "Streamer is handling previous request";
        return static_cast<int>(common::ResponseCode::BusyError);
    }

    // Clean up previous state
    __multi_state.clear();
    __current_multi_file = 0;
    __request_in_progress = true;

    // Validate array bounds and initialize each file request
    size_t buffer_offset = 0;
    for (unsigned i = 0; i < num_files; ++i)
    {
        // Validate array access
        if (paths[i] == nullptr)
        {
            LOG(ERROR) << "Path at index " << i << " is null";
            __multi_state.clear();
            __request_in_progress = false;
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }

        // Check for buffer overflow when calculating buffer offset
        if (buffer_offset > std::numeric_limits<size_t>::max() - bytesizes[i])
        {
            LOG(ERROR) << "Buffer offset calculation overflow for file " << i;
            __multi_state.clear();
            __request_in_progress = false;
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }

        State state;
        int result = validate_and_init_request(
            streamer,
            paths[i],
            file_offsets[i],
            bytesizes[i],
            reinterpret_cast<char*>(dsts[0]) + buffer_offset,
            num_sizes[i],
            internal_sizes[i],
            &state
        );

        if (result != static_cast<int>(common::ResponseCode::Success))
        {
            // Clean up on error
            __multi_state.clear();
            __request_in_progress = false;
            return result;
        }

        __multi_state.push_back(std::move(state));
        buffer_offset += bytesizes[i];
    }

    return static_cast<int>(common::ResponseCode::Success);
}

extern "C" int runai_response(void * streamer, unsigned * file_index, unsigned * index)
{
    // Validate null pointers
    if (streamer == nullptr)
    {
        LOG(ERROR) << "Streamer pointer is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (file_index == nullptr)
    {
        LOG(ERROR) << "File index output parameter is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    if (index == nullptr)
    {
        LOG(ERROR) << "Index output parameter is null";
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    // Check if no request is in progress
    if (!__request_in_progress || __multi_state.empty())
    {
        return static_cast<int>(common::ResponseCode::FinishedError);
    }

    // Find the next available file/item to read
    while (__current_multi_file < __multi_state.size())
    {
        State& state = __multi_state[__current_multi_file];

        if (state.current_item >= state.total_items)
        {
            // This file is done, move to next file
            ++__current_multi_file;
            continue;
        }

        // Read the next chunk from this file
        *file_index = __current_multi_file;
        int result = read_chunk(&state, index);

        // If we've finished all files, mark request as complete
        if (__current_multi_file >= __multi_state.size() || 
            (__current_multi_file == __multi_state.size() - 1 && 
             state.current_item >= state.total_items))
        {
            __request_in_progress = false;
        }

        return result;
    }

    // All files processed
    __request_in_progress = false;
    return static_cast<int>(common::ResponseCode::FinishedError);
}

extern "C" const char * runai_response_str(int response_code)
{
    return common::description(response_code);
}
} // namespace runai::llm::streamer
