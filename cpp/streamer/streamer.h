#pragma once

#include <stddef.h>

namespace runai::llm::streamer
{

#ifdef _RUNAI_STREAMER_SO
    #define _RUNAI_EXTERN_C extern "C"
#else
    #define _RUNAI_EXTERN_C
#endif

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

// send a read request to a file destination and wait until finished
// return Success if succeeded

_RUNAI_EXTERN_C int runai_read_file(void * streamer, const char * path, const char * dst);

// send a read request to copy s3 object to a file destination and wait until finished
// return Success if succeeded

_RUNAI_EXTERN_C int runai_read_object_to_file(void * streamer, const char * s3_path, const char * fs_path);

// send a read request and wait until finished
// return Success if the exact number of bytes was read

_RUNAI_EXTERN_C int runai_read(void * streamer, const char * path, size_t file_offset, size_t bytesize, void * dst);

// send asynchronous read request with a list of consecutive sub requests, and receive response for each sub request when ready
// num_sizes : number of sub requests
// internal_sizes : a list containing the size of each sub request, where the first sub request starts at the given file offset and each sub request starts at the end of the previous one
// return Success if request is valid

_RUNAI_EXTERN_C int runai_request(void * streamer, const char * path, size_t file_offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes);

// wait until the next sub request is ready
// return FinishedError when there are no more responses

_RUNAI_EXTERN_C int runai_response(void * streamer, unsigned * index /* return parameter */);

_RUNAI_EXTERN_C const char * runai_response_str(int response_code);

// list files or object keys
_RUNAI_EXTERN_C int runai_list_objects(void * streamer, const char * path, char*** object_keys, size_t * object_count);
// free list
_RUNAI_EXTERN_C int runai_free_list_objects(char** object_keys, size_t object_count);

} // namespace runai::llm::streamer
