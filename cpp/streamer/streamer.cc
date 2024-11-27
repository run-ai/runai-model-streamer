#include "streamer/streamer.h"

#include <memory>

#include "common/exception/exception.h"
#include "common/response_code/response_code.h"
#include "streamer/impl/streamer/streamer.h"

namespace runai::llm::streamer
{

// Library for reading a large file concurrently to a given host memory buffer
// Reads a single file at a time
// NOT THREAD SAFE - caller must not send requests and responses in parallel

// creates streamer object with threadpool of the given size
// returns streamer response code Success or error code
// chunk_bytesize : number of bytes to read by each thread before sending response to the caller (in case there are new completed sub requests)
// block_bytesize : maximal number of bytes to read from the storage in a single read call

_RUNAI_EXTERN_C int runai_start(void ** streamer)
{
    // verify configuration
    std::unique_ptr<impl::Config> config;
    try
    {
        config = std::make_unique<impl::Config>();
    }
    catch(...)
    {
        return static_cast<int>(common::ResponseCode::InvalidParameterError);
    }

    try
    {
        *streamer = new impl::Streamer(*config);
    }
    catch(...)
    {
        return static_cast<int>(common::ResponseCode::UnknownError);
    }
    return static_cast<int>(common::ResponseCode::Success);
}

// destroys streamer object

_RUNAI_EXTERN_C void runai_end(void * streamer)
{
    try
    {
        auto s = static_cast<impl::Streamer *>(streamer);
        if (s != nullptr)
        {
            delete s;
        }
    }
    catch(...)
    {
    }
}

// send a read request and wait until finished
// return true if the exact number of bytes was read

_RUNAI_EXTERN_C int runai_read(void * streamer, const char * path, size_t file_offset, size_t bytesize, void * dst)
{
    try
    {
        auto s = static_cast<impl::Streamer *>(streamer);
        if (s == nullptr)
        {
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }
        return static_cast<int>(s->request(path, file_offset, bytesize, dst));
    }
    catch(...)
    {
    }
    return static_cast<int>(common::ResponseCode::UnknownError);
}

// send asynchronous read request with a list of consecutive sub requests, and receive response for each sub request when ready
// num_sizes : number of sub requests
// internal_sizes : a list containing the size of each sub request, where the first sub request starts at the given file offset and each sub request starts at the end of the previous one

_RUNAI_EXTERN_C int runai_request(void * streamer, const char * path, size_t file_offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes)
{
    try
    {
        auto s = static_cast<impl::Streamer *>(streamer);
        if (s == nullptr)
        {
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }
        return static_cast<int>(s->request(path, file_offset, bytesize, dst, num_sizes, internal_sizes));
    }
    catch(...)
    {
    }
    return static_cast<int>(common::ResponseCode::UnknownError);
}

// wait until the next sub request is ready
// returns -1 when there are no more responses

_RUNAI_EXTERN_C int runai_response(void * streamer, unsigned * index)
{
    try
    {
        if (streamer == nullptr || index == nullptr)
        {
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }

        auto * s = static_cast<impl::Streamer *>(streamer);
        auto r = s->response();
        if (r.ret == common::ResponseCode::Success)
        {
            *index = r.index;
        }
        return static_cast<int>(r.ret);
    }
    catch(...)
    {
    }
    return static_cast<int>(common::ResponseCode::UnknownError);
}

const char * unexpected_error = "Unexpected error occured";

_RUNAI_EXTERN_C const char * runai_response_str(int response_code)
{
    try
    {
        return common::description(response_code);
    }
    catch(const std::exception& e)
    {
    }

    return unexpected_error;
}

_RUNAI_EXTERN_C int runai_list_objects(void * streamer, const char * path, char*** object_keys, size_t * object_count)
{
    try
    {
        if (streamer == nullptr || path == nullptr)
        {
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }

        auto * s = static_cast<impl::Streamer *>(streamer);
        auto ret = s->list_objects(path, object_keys, object_count);
        return static_cast<int>(ret);
    }
    catch(...)
    {
    }
    return static_cast<int>(common::ResponseCode::UnknownError);
}

// free list
_RUNAI_EXTERN_C int runai_free_list_objects(char** object_keys, size_t object_count)
{
    try
    {
        auto ret = impl::Streamer::free_list_objects(object_keys, object_count);
        return static_cast<int>(ret);
    }
    catch(...)
    {
    }
    return static_cast<int>(common::ResponseCode::UnknownError);
}

int runai_read_object_to_file(void * streamer, const char * s3_path, const char * fs_path)
{
    try
    {
        if (streamer == nullptr)
        {
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }

        auto * s = static_cast<impl::Streamer *>(streamer);
        auto ret = s->request(s3_path, fs_path);
        return static_cast<int>(ret);
    }
    catch(common::Exception & exception)
    {
        return static_cast<int>(exception.error());
    }
    catch(...)
    {
    }
    return static_cast<int>(common::ResponseCode::UnknownError);
}

} // namespace runai::llm::streamer
