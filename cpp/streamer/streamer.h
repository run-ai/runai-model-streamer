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

// Library for reading files concurrently into host memory buffers
// A single submission (runai_request) may cover many files, and many submissions may be in flight at
// once - each response carries the id of the submission it belongs to
// NOT THREAD SAFE - caller must not send requests and responses in parallel

// Creates a streamer object; returns Success or an error code.
// Takes no configuration arguments - the streamer configures itself from the environment. Each variable
// below sets BOTH backends; when a variable is unset the two backends fall back to different defaults:
//   RUNAI_STREAMER_CONCURRENCY    : number of worker threads. Unset: 16 filesystem, 8 object storage
//   RUNAI_STREAMER_CHUNK_BYTESIZE : bytes per read call. Unset: 2 MiB filesystem, the S3 client default
//                                   for object storage. Minimums are enforced - 2 MiB and 5 MiB respectively
// Worker pools are created lazily, one per backend actually used.

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
// A submission is a list of files; each file carries a list of RANGES to read. A range is an
// arbitrary (source offset, size) within its file with its own destination - ranges need not be
// contiguous in the file, need not be contiguous in memory, and need not be ordered.
//
// num_files     : number of files to read
// paths         : list of file paths - one entry per file, however many ranges that file has
// num_ranges    : number of ranges for each file
// range_offsets : flat array of sum(num_ranges) source offsets, each within its owning file
// range_sizes   : flat array of sum(num_ranges) range sizes in bytes
// range_dsts    : flat array of sum(num_ranges) destination pointers
//
// The three flat arrays are indexed identically and grouped by file in the order of paths: file f's
// ranges occupy [sum(num_ranges[0..f)), sum(num_ranges[0..f])). Destinations must not overlap.
//
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
    unsigned * num_ranges,
    size_t * range_offsets,
    size_t * range_sizes,
    void ** range_dsts
);

// Multi-request response. Returns the next ready range from any in-flight submission.
//  out_submission_id : set to the owning submission's id (which submission this response is for).
//  file_index, index : the file, and the index of the range within that file, as submitted.
//  submission_done   : set to 1 iff this was the submission's last response (it is now complete).
//  timeout_ms        : max time to wait for a response; 0 blocks indefinitely.
// ret is the truthful per-range code (Success or a specific error), TimedOut on timeout, or
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
