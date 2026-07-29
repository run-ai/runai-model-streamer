#pragma once

#include <stddef.h>

#include "common/submission/submission_id.h"

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

// Set the streamer's object-storage credentials as a general key/value dictionary (param_keys /
// param_values / num_params). Keys are the plugin's canonical config-parameter names (e.g.
// "access_key_id", "secret_access_key", "session_token", "region", "endpoint"); arbitrary keys are
// carried through to the backend. Credentials are streamer-scoped and set once: setting the same
// credentials again returns Success; a different set after the first returns CredentialsAlreadySet (create
// a new streamer for a different identity). Call this before submitting object-storage reads / listing.
_RUNAI_EXTERN_C int runai_set_credentials(
    void * streamer,
    const char ** param_keys,
    const char ** param_values,
    unsigned num_params
);

// Multi-request submit: read multiple files concurrently.
//
// num_files : number of files to read
// paths : list of files paths
// file_offsets : offset for each file path, from which to start reading
// bytesizes : size of each destination buffer
// dsts : destination buffers; for reading to CPU memory, dsts[0] only is used as a single buffer to contain
//        all the files in the order specified by paths
// num_sizes : number of sub requests for each file
// internal_sizes : a list containing the size of each sub request, where the first sub request starts at the
//                  given file offset and each subsequent sub request starts at the end of the previous one
// Credentials are NOT passed here - set them once via runai_set_credentials.
//  out_submission_id : always set to this submission's id once one is assigned, and left 0 only
//                      if the call fails before that (e.g. invalid parameters). On Success it
//                      identifies the submission; use it to demux responses from
//                      runai_response. If the call fails after the submission was committed,
//                      its responses are still delivered and can be drained by this id.
_RUNAI_EXTERN_C int runai_request(
    void * streamer,
    SubmissionId * out_submission_id /* return parameter */,
    unsigned num_files,
    const char ** paths,
    size_t * file_offsets,
    size_t * bytesizes,
    void ** dsts,
    unsigned * num_sizes,
    size_t ** internal_sizes
);

// Multi-request response. Returns the next ready sub-range from any in-flight submission.
//  out_submission_id : set to the owning submission's id (which submission this response is for).
//  file_index, index : the file and sub-range within that submission.
//  submission_done   : set to 1 iff this was the submission's last response (it is now complete).
//  timeout_ms        : max time to wait for a response; 0 blocks indefinitely.
// ret is the truthful per-sub-range code (Success or a specific error), TimedOut on timeout, or
// FinishedError on teardown.
_RUNAI_EXTERN_C int runai_response(
    void * streamer,
    SubmissionId * out_submission_id /* return parameter */,
    unsigned * file_index /* return parameter */,
    unsigned * index /* return parameter */,
    int * submission_done /* return parameter */,
    unsigned timeout_ms
);

_RUNAI_EXTERN_C const char * runai_response_str(int response_code);

// List files at the given object storage prefix.
//
// streamer is a handle from runai_start; listing reuses its object-storage clients, backend handle and
// credentials (set once via runai_set_credentials) rather than creating throwaway ones.
//
// For each matching entry the callback is invoked as:
//   callback(path, file_size, user_data)
// where path is the full object URI and file_size is the size in bytes.
// user_data is passed through to every callback invocation unchanged.
//
// Example:
//   struct Result { std::vector<std::pair<std::string,size_t>> files; };
//   Result result;
//   runai_list_files(streamer, "s3://my-bucket/models/", 1,
//       nullptr, 0, nullptr, 0,
//       [](const char* p, size_t sz, void* ud) {
//           static_cast<Result*>(ud)->files.emplace_back(p, sz);
//       }, &result);
//
// allow_patterns / ignore_patterns are fnmatch(3) patterns; NULL means no filter.
_RUNAI_EXTERN_C int runai_list_files(
    void *                   streamer,
    const char *             prefix,
    int                      is_recursive,
    const char **            allow_patterns,
    unsigned                 num_allow_patterns,
    const char **            ignore_patterns,
    unsigned                 num_ignore_patterns,
    RunaiFileListCallback    callback,
    void *                   user_data
);

} // namespace runai::llm::streamer
