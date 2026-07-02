#include "streamer/streamer.h"

#include <memory>
#include <string>
#include <vector>

#include "common/exception/exception.h"
#include "common/response_code/response_code.h"
#include "common/s3_credentials/s3_credentials.h"
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

// send asynchronous read request to read multiple files
//
// num_files : number of files to read
// paths : list of files paths
// file_offsets : offset for each file path, from which to start reading
// bytesizes : size of each destination buffer
// dsts : destination buffers
//        for reading to CPU memory, dsts[0] only is used as a single buffer to contain all the files in the order specified by paths
// num_sizes : number of sub requests for each file
// internal_sizes : a list containing the size of each sub request, where the first sub request starts at the given file offset and each sub request starts at the end of the previous one
// return Success if request is valid

_RUNAI_EXTERN_C int runai_request(
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
    try
    {
        auto s = static_cast<impl::Streamer *>(streamer);
        if (s == nullptr)
        {
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }

        common::s3::Credentials credentials(key, secret, token, region, endpoint);

        std::vector<std::string> paths_v(paths, paths + num_files);
        std::vector<size_t> file_offsets_v(file_offsets, file_offsets + num_files);
        std::vector<size_t> bytesizes_v(bytesizes, bytesizes + num_files);
        std::vector<void *> dsts_v(dsts, dsts + num_files);
        std::vector<unsigned> num_sizes_v(num_sizes, num_sizes + num_files);
        std::vector<size_t *> internal_sizes_v(internal_sizes, internal_sizes + num_files);

        std::vector<std::vector<size_t>> internal_sizes_vv(num_files);
        for (unsigned i = 0; i < num_files; ++i)
        {
            internal_sizes_vv[i] = std::vector<size_t>(internal_sizes_v[i], internal_sizes_v[i] + num_sizes_v[i]);
        }

        return static_cast<int>(s->async_request(paths_v, file_offsets_v, bytesizes_v, dsts_v, num_sizes_v, internal_sizes_vv, credentials));
    }
    catch(...)
    {
    }
    return static_cast<int>(common::ResponseCode::UnknownError);
}

// receive response for each sub request when ready
//
// streamer : streamer object
// file_index : index of the file that the response belongs to
// index : index of the sub request that the response belongs to
// return Success if response is valid

_RUNAI_EXTERN_C int runai_response(void * streamer, unsigned * file_index /* return parameter */, unsigned * index /* return parameter */)
{
    try
    {
        if (streamer == nullptr || index == nullptr)
        {
            return static_cast<int>(common::ResponseCode::InvalidParameterError);
        }

        auto * s = static_cast<impl::Streamer *>(streamer);
        auto r = s->response();

        *index = r.index;
        *file_index = r.file_index;
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

_RUNAI_EXTERN_C int runai_list_files(
    const char *          prefix,
    int                   is_recursive,
    const char **         allow_patterns,
    unsigned              num_allow_patterns,
    const char **         ignore_patterns,
    unsigned              num_ignore_patterns,
    RunaiFileListCallback callback,
    void *                user_data,
    const char **         param_keys,
    const char **         param_values,
    unsigned              num_params)
{
    try
    {
        if (!prefix || !callback)
            return static_cast<int>(common::ResponseCode::InvalidParameterError);

        const char *key = nullptr, *secret = nullptr, *token = nullptr,
                   *region = nullptr, *endpoint = nullptr;
        for (unsigned i = 0; i < num_params; ++i)
        {
            if (!param_keys || !param_keys[i]) continue;
            const std::string k(param_keys[i]);
            const char* v = param_values ? param_values[i] : nullptr;
            if      (k == "key")      key      = v;
            else if (k == "secret")   secret   = v;
            else if (k == "token")    token    = v;
            else if (k == "region")   region   = v;
            else if (k == "endpoint") endpoint = v;
        }
        common::s3::Credentials credentials(key, secret, token, region, endpoint);

        std::vector<std::string> allow, ignore;
        for (unsigned i = 0; allow_patterns && i < num_allow_patterns; ++i) allow.emplace_back(allow_patterns[i]);
        for (unsigned i = 0; ignore_patterns && i < num_ignore_patterns; ++i) ignore.emplace_back(ignore_patterns[i]);

        impl::Streamer streamer;
        const auto files = streamer.list_files(prefix, is_recursive != 0, allow, ignore, credentials);
        for (const auto & entry : files)
        {
            callback(entry.first.c_str(), entry.second, user_data);
        }

        return static_cast<int>(common::ResponseCode::Success);
    }
    catch (const common::Exception & e)
    {
        return static_cast<int>(e.error());
    }
    catch (...)
    {
    }
    return static_cast<int>(common::ResponseCode::UnknownError);
}

} // namespace runai::llm::streamer
