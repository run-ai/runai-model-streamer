#include <fcntl.h>
#include <fnmatch.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <filesystem>
#include <vector>
#include "utils/fd/fd.h"
#include "utils/logging/logging.h"
#include "common/submission/submission_id.h"


namespace runai::llm::streamer
{
// One range of a submission, exactly as the caller described it: its own source offset, size and
// destination. The mock keeps all three per range rather than a file offset plus a running destination
// cursor, so a submission whose ranges are non-contiguous in the file or in memory is served correctly.
struct MockRange {
    size_t offset;
    size_t size;
    char * dst;
};

struct State {
    utils::Fd file;
    std::vector<MockRange> ranges;
    unsigned total_items = 0;
    unsigned current_item = 0;

    // Set when the file could not be opened. Every range still gets a response, carrying this code -
    // the real streamer fails a file's ranges individually rather than dropping them, and a dropped
    // response would hang the caller (runai_response blocks).
    int error = 0;
};

State __state;
std::vector<State> __multi_state;
unsigned __multi_file_count = 0;
unsigned __current_multi_file = 0;

// multi-request bookkeeping: the mock serves one submission at a time, so a single id/counter suffices.
SubmissionId __submission_id = 0;
unsigned __response_total = 0;   // total sub-range responses expected for the current submission
unsigned __response_given = 0;   // responses handed out so far


// Record one file's ranges. The seek is deliberately NOT done here: each range seeks to its own offset
// when it is served, since ranges need not be ordered or adjacent.
int request(void * streamer, const char * path, unsigned num_ranges, const size_t * range_offsets, const size_t * range_sizes, void ** range_dsts, State * state)
{
    // Record the ranges BEFORE touching the file. The submission owes exactly one response per range
    // whatever happens below, and runai_request does not consult this function's result - so returning
    // early would leave total_items at 0 while the response counter had already been raised by num_ranges,
    // and those responses would never be produced. The caller would then block forever in runai_response.
    state->ranges.reserve(num_ranges);
    for (unsigned j = 0; j < num_ranges; ++j) {
        state->ranges.push_back(MockRange{ range_offsets[j], range_sizes[j], reinterpret_cast<char*>(range_dsts[j]) });
    }
    state->total_items = num_ranges;

    // A file with NO ranges yields no transfer and no batch in the real streamer, so it is never opened.
    // All-zero-sized ranges are different: they do yield a batch, and Batch::execute opens the file before
    // looking at any size - so an unopenable file fails them (verified: it returns FileAccessError).
    if (num_ranges == 0) {
        return 0;
    }

    state->file = utils::Fd(::open(path, O_RDONLY));
    if (state->file.fd() == -1) {
        LOG(ERROR) << "Error opening file: " << path;
        state->error = 2;   // common::ResponseCode::FileAccessError, reported once per range
        return -1;
    }

    return 0;
}

// Read one sub-range. Returns 0 on success or a per-sub-range error code (matching common::ResponseCode:
// FileAccessError=2 on a read failure, EofError=3 on a short read). Even on an error this is a COMPLETED
// sub-range response: *index is set and the item is consumed, so runai_response still reports the owning
// submission id and submission_done - matching the real C API (which sets all out-params on an error response).
int response(void * streamer, unsigned * index, State * state)
{
    size_t result = 0;
    const auto & range = state->ranges[state->current_item];

    // Consume the item up front, so every exit below produces exactly one response for this range.
    *index = state->current_item;
    state->current_item++;

    // The file could not be opened: report the error rather than dropping the response. Checked BEFORE the
    // zero-sized shortcut, because the real streamer opens the file regardless of range size.
    if (state->error != 0)
    {
        return state->error;
    }

    // A zero-sized range reads nothing, so it is completed without touching the (successfully opened) file.
    if (range.size == 0)
    {
        return 0;
    }

    int ret = 0;
    try
    {
        // seek per range: the previous range may have been elsewhere in the file, or later in it
        state->file.seek(range.offset);
        result = state->file.read(range.size, range.dst, utils::Fd::Read::Eof);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Failed to read from file at offset " << range.offset;
        ret = 2;   // common::ResponseCode::FileAccessError
    }

    if (ret == 0 && result != range.size)
    {
        LOG(ERROR) << "Reached EOF";
        ret = 3;   // common::ResponseCode::EofError
    }

    return ret;
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

// Pull the next ready sub-range across the multi-file state (shared by runai_response). Returns -1 when
// every file is drained.
static int mock_next_response(void * streamer, unsigned * file_index, unsigned * index)
{
    if (__current_multi_file >= __multi_state.size()) {
        return -1; // All files processed
    }

    State& state = __multi_state[__current_multi_file];

    if (state.current_item >= state.total_items) {
        ++__current_multi_file;
        return mock_next_response(streamer, file_index, index); // recurse to next file
    }

    *file_index = __current_multi_file;
    return response(streamer, index, &state);
}

extern "C" int runai_set_credentials(
    void * streamer,
    const char ** param_keys,
    const char ** param_values,
    unsigned num_params)
{
    return 0;
}

extern "C" int runai_request(
    void * streamer,
    SubmissionId * out_submission_id,
    unsigned num_files,
    const char ** paths,
    unsigned * num_ranges,
    size_t * range_offsets,
    size_t * range_sizes,
    void ** range_dsts
)
{
    __multi_state.clear();
    __current_multi_file = 0;
    __multi_file_count = num_files;
    __response_total = 0;
    __response_given = 0;

    // The range arrays are flat and grouped by file in the order of paths; base walks that grouping.
    // Each file's ranges are handed over as they were submitted - every range keeps its own offset,
    // size and destination, so scattered submissions are served correctly rather than collapsed into
    // one contiguous span per file.
    size_t base = 0;
    for (unsigned i = 0; i < num_files; ++i) {
        const unsigned n = num_ranges[i];

        State state;
        request(streamer,
                paths[i],
                n,
                range_offsets + base,
                range_sizes + base,
                range_dsts + base,
                &state);

        __response_total += n;
        __multi_state.push_back(std::move(state));
        base += n;
    }

    __submission_id += 1;
    if (out_submission_id != nullptr) {
        *out_submission_id = __submission_id;
    }
    return 0;
}

extern "C" int runai_response(
    void * streamer,
    SubmissionId * out_submission_id,
    unsigned * file_index,
    unsigned * index,
    int * submission_done,
    unsigned timeout_ms
)
{
    int r = mock_next_response(streamer, file_index, index);
    if (r < 0) {
        return r;   // no more sub-ranges (drained) - not a real response
    }

    // r is a real sub-range result: 0 (Success) or a per-range error code. Set the out-params in both cases
    // (like the real C API), so a per-range error still carries its submission id and submission_done.
    if (out_submission_id != nullptr) {
        *out_submission_id = __submission_id;
    }
    __response_given += 1;
    if (submission_done != nullptr) {
        *submission_done = (__response_given >= __response_total) ? 1 : 0;
    }
    return r;
}

extern "C" const char * runai_response_str(int response_code)
{
    return 0;
}

extern "C" int runai_list_files(
    void *        streamer,
    const char *  prefix,
    int           is_recursive,
    const char ** allow_patterns,
    unsigned      num_allow_patterns,
    const char ** ignore_patterns,
    unsigned      num_ignore_patterns,
    void (*callback)(const char*, size_t, void*),
    void *        user_data)
{
    namespace fs = std::filesystem;

    if (streamer == nullptr || prefix == nullptr || callback == nullptr)
    {
        return -1;
    }

    // Object storage paths are not supported in the mock
    if (::strncmp(prefix, "s3://", 5) == 0 ||
        ::strncmp(prefix, "gs://", 5) == 0 ||
        ::strncmp(prefix, "az://", 5) == 0)
    {
        return 0;
    }

    const fs::path root(prefix);
    if (!fs::exists(root))
    {
        return 1;
    }

    auto fire = [&](const fs::path& p, size_t size)
    {
        const char* cpath = p.c_str();
        if (allow_patterns != nullptr && num_allow_patterns > 0)
        {
            bool matched = false;
            for (unsigned j = 0; j < num_allow_patterns; ++j)
            {
                if (::fnmatch(allow_patterns[j], cpath, 0) == 0) { matched = true; break; }
            }
            if (!matched) return;
        }
        for (unsigned j = 0; ignore_patterns != nullptr && j < num_ignore_patterns; ++j)
        {
            if (::fnmatch(ignore_patterns[j], cpath, 0) == 0) return;
        }
        callback(cpath, size, user_data);
    };

    auto visit = [&](const fs::directory_entry& entry)
    {
        if (entry.is_regular_file())
        {
            fire(entry.path(), static_cast<size_t>(entry.file_size()));
        }
    };

    if (is_recursive)
    {
        for (const auto& e : fs::recursive_directory_iterator(root)) visit(e);
    }
    else
    {
        for (const auto& e : fs::directory_iterator(root)) visit(e);
    }

    return 0;
}

} // namespace runai::llm::streamer
