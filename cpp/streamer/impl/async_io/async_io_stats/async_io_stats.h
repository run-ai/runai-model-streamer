#pragma once

#include <cstddef>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include "common/posix_io/strategy/strategy.h"
#include "common/submission/submission_id.h"

namespace runai::llm::streamer::impl
{

// What one submission did, and which reader served it.
//
// A small struct with an accessor, NOT a metrics framework. The point is that a TEST can read these
// numbers. Checking them by reading log lines would tie the test to the wording of a message, and
// the wording changes.
struct SubmissionStats
{
    SubmissionId submission_id = 0;

    // Which reader served each file. A submission can now use two of them at once - the synchronous
    // pool for one file and an engine for another - so "why was this slow?" cannot be answered
    // without knowing what read what.
    //
    // One entry per file that produced a transfer. A file with no ranges reads nothing and appears
    // here, so that "this file was skipped" and "this file is missing" stay different answers.
    struct FileStrategy
    {
        std::string path;
        common::posix_io::Strategy strategy = common::posix_io::Strategy::SyncBuffered;
    };
    std::vector<FileStrategy> files;

    // Mounts that had to share an engine because the limit was reached. Zero unless
    // RUNAI_STREAMER_FS_MAX_ENGINES is too low for the number of mounts read.
    unsigned shared_engine_mounts = 0;

    // NOT HERE, on purpose:
    //
    //   achieved depth  belongs to the ENGINE, not to a submission. Submissions share one engine's
    //                   window, so "the highest number of reads during submission X" has no clear
    //                   meaning. It needs its own place, per engine.
    //
    //   short-read re-stages  the worker counts these, and one worker serves many submissions.
    //                   Giving each submission its own number needs the count to travel from the
    //                   worker back to here. Worth doing, but it is not free, so it is separate.
    //
    // Both are left out rather than added as fields that nothing fills. An empty counter reads as
    // "this did not happen".
};

std::ostream & operator<<(std::ostream &, const SubmissionStats &);

// Stats for the streamer: one entry per submission, plus a total.
//
// PER SUBMISSION, not only a total. The streamer lives for many submissions, so a total alone cannot
// answer "did THIS submission behave badly?" - which is the question a test asks. A total alone is
// also misleading rather than merely rough: a submission served half by the synchronous pool would
// report zero for counters that only the engine can raise.
//
// Old submissions are dropped once the list is full, so a long-lived streamer cannot grow without
// limit. The total keeps counting.
//
// Thread safe. Submissions can run at the same time, and the numbers arrive from their workers.
class AsyncIoStats
{
 public:
    // How many submissions to remember. Small on purpose: this is for the last few runs and for
    // tests, not a history.
    static constexpr size_t MaxSubmissions = 64;

    void record(const SubmissionStats & stats);

    // The stats for one submission, or nothing if it is unknown or has been dropped.
    bool find(SubmissionId submission_id, SubmissionStats & out) const;

    // Every submission still remembered, oldest first.
    std::vector<SubmissionStats> submissions() const;

    // Totals over every submission, including ones already dropped.
    SubmissionStats total() const;

    // How many submissions were dropped to keep the list small. Tells a reader that `submissions()`
    // is not the whole story.
    size_t dropped() const;

 private:
    mutable std::mutex _mutex;

    std::vector<SubmissionStats> _submissions;   // oldest first
    SubmissionStats _total;
    size_t _dropped = 0;
};

}; // namespace runai::llm::streamer::impl
