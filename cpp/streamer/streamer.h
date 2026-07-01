#pragma once

#include <stddef.h>

namespace runai::llm::streamer
{

#ifdef _RUNAI_STREAMER_SO
    #define _RUNAI_EXTERN_C extern "C"
#else
    #define _RUNAI_EXTERN_C
#endif

typedef void (*RunaiFileListCallback)(const char* path, size_t file_size, void* user_data);

// Library for reading a large file concurrently to a given host memory buffer
// Reads a single file at a time
// NOT THREAD SAFE - caller must not send requests and responses in parallel

// creates streamer object with threadpool of the given size
// return streamer response code Success or error code
// chunk_bytesize : number of bytes to read by each thread before sending response to the caller (in case there are new completed sub requests)
// block_bytesize : maximal number of bytes to read from the storage in a single read call

_RUNAI_EXTERN_C int runai_start(void ** streamer /* return parameter */);

// destroys streamer object

_RUNAI_EXTERN_C void runai_end(void * streamer);

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
);

_RUNAI_EXTERN_C int runai_response(void * streamer, unsigned * file_index /* return parameter */, unsigned * index /* return parameter */);

_RUNAI_EXTERN_C const char * runai_response_str(int response_code);

// List files at the given object storage prefix.
//
// For each matching entry the callback is invoked as:
//   callback(path, file_size, user_data)
// where path is the full object URI and file_size is the size in bytes.
// user_data is passed through to every callback invocation unchanged.
//
// Example:
//   struct Result { std::vector<std::pair<std::string,size_t>> files; };
//   Result result;
//   runai_list_files("s3://my-bucket/models/", 1,
//       nullptr, 0, nullptr, 0,
//       [](const char* p, size_t sz, void* ud) {
//           static_cast<Result*>(ud)->files.emplace_back(p, sz);
//       }, &result,
//       keys, vals, num_params);
//
// allow_patterns / ignore_patterns are fnmatch(3) patterns; NULL means no filter.
// param_keys / param_values are parallel arrays of credential/config key-value pairs
// (recognised keys: "key", "secret", "token", "region", "endpoint").
_RUNAI_EXTERN_C int runai_list_files(
    const char *             prefix,
    int                      is_recursive,
    const char **            allow_patterns,
    unsigned                 num_allow_patterns,
    const char **            ignore_patterns,
    unsigned                 num_ignore_patterns,
    RunaiFileListCallback    callback,
    void *                   user_data,
    const char **            param_keys,
    const char **            param_values,
    unsigned                 num_params
);

} // namespace runai::llm::streamer
