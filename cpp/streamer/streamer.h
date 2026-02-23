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
    const char * endpoint,
    int cuda  /* 0 = read to CPU host memory (dsts), non-zero = read to CUDA device memory (dsts) */
);

_RUNAI_EXTERN_C int runai_response(void * streamer, unsigned * file_index /* return parameter */, unsigned * index /* return parameter */);

_RUNAI_EXTERN_C const char * runai_response_str(int response_code);

} // namespace runai::llm::streamer
