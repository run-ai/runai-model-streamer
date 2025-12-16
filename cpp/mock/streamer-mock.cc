#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <memory>
#include <algorithm>

#include "utils/fd/fd.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer
{

// Response code enum matching the real implementation
enum class ResponseCode : int
{
    Success = 0,
    FinishedError,
    FileAccessError,
    EofError,
    S3NotSupported,
    GlibcPrerequisite,
    InsufficientFdLimit,
    InvalidParameterError,
    EmptyRequestError,
    BusyError,
    CaFileNotFound,
    UnknownError,
    ObjPluginLoadError,
    GCSNotSupported,
    __Max,
};

// Response code descriptions matching the real implementation
constexpr const char* RESPONSE_MESSAGES[] = {
    "Request sent successfuly",
    "Finished all responses",
    "File access error",
    "End of file reached",
    "S3 not supported",
    "GLIBC version should be at least 2.29",
    "Increase process fd limit or decrease the concurrency level. Recommended value for the streamer alone is the concurrency multiplied by 64, in addition to your application fd usage",
    "Invalid request parameters",
    "Empty request parameters",
    "Streamer is handling previous request",
    "CA bundle file not found",
    "Unknown Error",
    "Error loading object storage plugin",
    "GCS not supported",
};

const char* get_response_message(int response_code)
{
    if (response_code < 0 || response_code >= static_cast<int>(ResponseCode::__Max))
    {
        return "Invalid response code";
    }
    return RESPONSE_MESSAGES[response_code];
}

// State for tracking a single file read operation
struct FileReadState
{
    utils::Fd file;
    std::string path;
    size_t file_offset;
    size_t total_bytesize;
    void* destination;
    size_t destination_offset;
    std::vector<size_t> chunk_sizes;
    unsigned current_chunk_index;
    bool is_complete;

    FileReadState()
        : file(-1)
        , file_offset(0)
        , total_bytesize(0)
        , destination(nullptr)
        , destination_offset(0)
        , current_chunk_index(0)
        , is_complete(false)
    {}

    FileReadState(FileReadState&& other) noexcept
        : file(std::move(other.file))
        , path(std::move(other.path))
        , file_offset(other.file_offset)
        , total_bytesize(other.total_bytesize)
        , destination(other.destination)
        , destination_offset(other.destination_offset)
        , chunk_sizes(std::move(other.chunk_sizes))
        , current_chunk_index(other.current_chunk_index)
        , is_complete(other.is_complete)
    {}

    FileReadState& operator=(FileReadState&& other) noexcept
    {
        if (this != &other)
        {
            file = std::move(other.file);
            path = std::move(other.path);
            file_offset = other.file_offset;
            total_bytesize = other.total_bytesize;
            destination = other.destination;
            destination_offset = other.destination_offset;
            chunk_sizes = std::move(other.chunk_sizes);
            current_chunk_index = other.current_chunk_index;
            is_complete = other.is_complete;
        }
        return *this;
    }
};

// Global state for the mock streamer
struct MockStreamerState
{
    std::vector<FileReadState> file_states;
    unsigned current_file_index;
    bool has_active_request;

    MockStreamerState()
        : current_file_index(0)
        , has_active_request(false)
    {}
};

// Global state instance
static MockStreamerState g_state;

// Helper function to read a chunk from a file
int read_chunk(FileReadState& state, unsigned* chunk_index)
{
    if (state.is_complete)
    {
        return static_cast<int>(ResponseCode::FinishedError);
    }

    if (state.current_chunk_index >= state.chunk_sizes.size())
    {
        state.is_complete = true;
        return static_cast<int>(ResponseCode::FinishedError);
    }

    const size_t chunk_size = state.chunk_sizes[state.current_chunk_index];
    char* dst = static_cast<char*>(state.destination) + state.destination_offset;

    try
    {
        const size_t bytes_read = state.file.read(chunk_size, dst, utils::Fd::Read::Eof);
        
        if (bytes_read != chunk_size)
        {
            LOG(ERROR) << "Failed to read complete chunk. Expected " << chunk_size 
                       << " bytes, got " << bytes_read << " bytes from " << state.path;
            return static_cast<int>(ResponseCode::EofError);
        }

        *chunk_index = state.current_chunk_index;
        state.destination_offset += chunk_size;
        state.current_chunk_index++;

        // Check if all chunks for this file are complete
        if (state.current_chunk_index >= state.chunk_sizes.size())
        {
            state.is_complete = true;
        }

        return static_cast<int>(ResponseCode::Success);
    }
    catch (const std::exception& e)
    {
        LOG(ERROR) << "Exception while reading chunk from " << state.path << ": " << e.what();
        return static_cast<int>(ResponseCode::FileAccessError);
    }
}

// Initialize a file read state
int initialize_file_read(
    const char* path,
    size_t file_offset,
    size_t bytesize,
    void* destination,
    unsigned num_chunks,
    const size_t* chunk_sizes,
    FileReadState& state)
{
    // Validate inputs
    if (path == nullptr || destination == nullptr)
    {
        return static_cast<int>(ResponseCode::InvalidParameterError);
    }

    if (num_chunks == 0 || chunk_sizes == nullptr)
    {
        return static_cast<int>(ResponseCode::EmptyRequestError);
    }

    // Verify total bytesize matches sum of chunks
    size_t total_chunk_size = 0;
    for (unsigned i = 0; i < num_chunks; ++i)
    {
        total_chunk_size += chunk_sizes[i];
    }

    if (total_chunk_size != bytesize)
    {
        LOG(ERROR) << "Total chunk size (" << total_chunk_size 
                   << ") does not match bytesize (" << bytesize << ")";
        return static_cast<int>(ResponseCode::InvalidParameterError);
    }

    // Open file
    state.file = utils::Fd(::open(path, O_RDONLY));
    if (state.file.fd() == -1)
    {
        LOG(ERROR) << "Failed to open file: " << path;
        return static_cast<int>(ResponseCode::FileAccessError);
    }

    // Seek to offset
    try
    {
        state.file.seek(file_offset);
    }
    catch (const std::exception& e)
    {
        LOG(ERROR) << "Failed to seek to offset " << file_offset << " in file " << path << ": " << e.what();
        return static_cast<int>(ResponseCode::FileAccessError);
    }

    // Initialize state
    state.path = path;
    state.file_offset = file_offset;
    state.total_bytesize = bytesize;
    state.destination = destination;
    state.destination_offset = 0;
    state.chunk_sizes.assign(chunk_sizes, chunk_sizes + num_chunks);
    state.current_chunk_index = 0;
    state.is_complete = false;

    return static_cast<int>(ResponseCode::Success);
}

} // namespace runai::llm::streamer

extern "C" {

int runai_start(void** streamer)
{
    using namespace runai::llm::streamer;
    
    if (streamer == nullptr)
    {
        return static_cast<int>(ResponseCode::InvalidParameterError);
    }

    // Reset global state
    g_state = MockStreamerState();
    
    // Return a dummy pointer (not actually used, but kept for compatibility)
    *streamer = reinterpret_cast<void*>(0x123456789ABCDEF0);
    
    return static_cast<int>(ResponseCode::Success);
}

void runai_end(void* streamer)
{
    using namespace runai::llm::streamer;
    
    // Clean up any open files
    g_state.file_states.clear();
    g_state.current_file_index = 0;
    g_state.has_active_request = false;
}

int runai_request(
    void* streamer,
    unsigned num_files,
    const char** paths,
    size_t* file_offsets,
    size_t* bytesizes,
    void** dsts,
    unsigned* num_sizes,
    size_t** internal_sizes,
    const char* key,
    const char* secret,
    const char* token,
    const char* region,
    const char* endpoint)
{
    using namespace runai::llm::streamer;

    // Validate inputs
    if (streamer == nullptr)
    {
        return static_cast<int>(ResponseCode::InvalidParameterError);
    }

    if (num_files == 0)
    {
        return static_cast<int>(ResponseCode::EmptyRequestError);
    }

    if (paths == nullptr || file_offsets == nullptr || bytesizes == nullptr || 
        dsts == nullptr || num_sizes == nullptr || internal_sizes == nullptr)
    {
        return static_cast<int>(ResponseCode::InvalidParameterError);
    }

    // Check if there's an active request
    if (g_state.has_active_request)
    {
        return static_cast<int>(ResponseCode::BusyError);
    }

    // Note: credentials (key, secret, token, region, endpoint) are ignored in the mock
    // as we read from local filesystem

    // Clear previous state
    g_state.file_states.clear();
    g_state.current_file_index = 0;
    g_state.has_active_request = true;

    // For CPU memory, only dsts[0] is used as a single buffer
    void* destination_buffer = dsts[0];
    size_t buffer_offset = 0;

    // Initialize read state for each file
    for (unsigned i = 0; i < num_files; ++i)
    {
        FileReadState file_state;
        
        int result = initialize_file_read(
            paths[i],
            file_offsets[i],
            bytesizes[i],
            static_cast<char*>(destination_buffer) + buffer_offset,
            num_sizes[i],
            internal_sizes[i],
            file_state);

        if (result != static_cast<int>(ResponseCode::Success))
        {
            // Clean up on error
            g_state.file_states.clear();
            g_state.has_active_request = false;
            return result;
        }

        g_state.file_states.push_back(std::move(file_state));
        buffer_offset += bytesizes[i];
    }

    return static_cast<int>(ResponseCode::Success);
}

int runai_response(void* streamer, unsigned* file_index, unsigned* index)
{
    using namespace runai::llm::streamer;

    // Validate inputs
    if (streamer == nullptr || file_index == nullptr || index == nullptr)
    {
        return static_cast<int>(ResponseCode::InvalidParameterError);
    }

    if (!g_state.has_active_request)
    {
        return static_cast<int>(ResponseCode::FinishedError);
    }

    // Find the next available chunk to read
    while (g_state.current_file_index < g_state.file_states.size())
    {
        FileReadState& file_state = g_state.file_states[g_state.current_file_index];
        
        if (file_state.is_complete)
        {
            // Move to next file
            g_state.current_file_index++;
            continue;
        }

        // Read the next chunk from this file
        unsigned chunk_index;
        int result = read_chunk(file_state, &chunk_index);
        
        if (result == static_cast<int>(ResponseCode::Success))
        {
            *file_index = g_state.current_file_index;
            *index = chunk_index;
            return result;
        }
        else if (result == static_cast<int>(ResponseCode::FinishedError))
        {
            // This file is complete, try next file
            g_state.current_file_index++;
            continue;
        }
        else
        {
            // Error occurred
            g_state.has_active_request = false;
            return result;
        }
    }

    // All files are complete
    g_state.has_active_request = false;
    return static_cast<int>(ResponseCode::FinishedError);
}

const char* runai_response_str(int response_code)
{
    using namespace runai::llm::streamer;
    return get_response_message(response_code);
}

} // extern "C"