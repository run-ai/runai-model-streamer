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
struct State {
    utils::Fd file;
    std::vector<size_t> read_item_sizes;
    unsigned total_items = 0;
    unsigned current_item = 0;
    char* destination = nullptr;
    unsigned current_dst_offset = 0;
};

State __state;
std::vector<State> __multi_state;
unsigned __multi_file_count = 0;
unsigned __current_multi_file = 0;

// multi-request bookkeeping: the mock serves one submission at a time, so a single id/counter suffices.
SubmissionId __submission_id = 0;
unsigned __response_total = 0;   // total sub-range responses expected for the current submission
unsigned __response_given = 0;   // responses handed out so far


int request(void * streamer, const char * path, size_t file_offset, size_t bytesize, char * dst, unsigned num_sizes, size_t * internal_sizes, State * state)
{
    state->file = utils::Fd(::open(path, O_RDONLY));
    if (state->file.fd() == -1) {
        LOG(ERROR) << "Error opening file: " << path;
        return -1;
    }

    try
    {
        state->file.seek(file_offset);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Error seek in file: " << path << " to: " << file_offset;
        return -1;
    }

    state->read_item_sizes.resize(num_sizes);
    std::memcpy(state->read_item_sizes.data(), internal_sizes, num_sizes * sizeof(size_t));

    state->total_items = num_sizes;
    state->destination = dst;
    return 0;
}

// Read one sub-range. Returns 0 on success or a per-sub-range error code (matching common::ResponseCode:
// FileAccessError=2 on a read failure, EofError=3 on a short read). Even on an error this is a COMPLETED
// sub-range response: *index is set and the item is consumed, so runai_response still reports the owning
// submission id and submission_done - matching the real C API (which sets all out-params on an error response).
int response(void * streamer, unsigned * index, State * state)
{
    size_t result = 0;
    auto to_read = state->read_item_sizes[state->current_item];
    auto to_dst = state->destination + state->current_dst_offset;
    int ret = 0;
    try
    {
        result = state->file.read(to_read, to_dst, utils::Fd::Read::Eof);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Failed to read from file";
        ret = 2;   // common::ResponseCode::FileAccessError
    }

    if (ret == 0 && result != to_read)
    {
        LOG(ERROR) << "Reached EOF";
        ret = 3;   // common::ResponseCode::EofError
    }

    state->current_dst_offset += result;
    *index = state->current_item;
    state->current_item++;
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
    //
    // The mock reads each file as ONE contiguous span, starting at its first range's offset and
    // destination. That matches every caller today - a file's ranges are laid out consecutively in both
    // the file and the destination - but a genuinely scattered request would need a seek per range here.
    size_t base = 0;
    for (unsigned i = 0; i < num_files; ++i) {
        const unsigned n = num_ranges[i];

        size_t bytesize = 0;
        for (unsigned j = 0; j < n; ++j) {
            bytesize += range_sizes[base + j];
        }

        State state;
        request(streamer,
                paths[i],
                n > 0 ? range_offsets[base] : 0,
                bytesize,
                n > 0 ? reinterpret_cast<char*>(range_dsts[base]) : nullptr,
                n,
                range_sizes + base,
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
