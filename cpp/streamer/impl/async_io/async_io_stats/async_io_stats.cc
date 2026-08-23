#include "streamer/impl/async_io/async_io_stats/async_io_stats.h"

#include <algorithm>

namespace runai::llm::streamer::impl
{

std::ostream & operator<<(std::ostream & os, const SubmissionStats & stats)
{
    os << "submission " << stats.submission_id;

    if (stats.shared_engine_mounts > 0)
    {
        os << ", mounts sharing an engine " << stats.shared_engine_mounts;
    }

    // Counted, not listed. A model has hundreds of files, and one line per file would bury the
    // numbers above. The list itself is for a test or for a reader who asks for it.
    unsigned async_files = 0;
    for (const auto & file : stats.files)
    {
        if (common::posix_io::is_async(file.strategy))
        {
            ++async_files;
        }
    }

    if (!stats.files.empty())
    {
        os << ", files " << stats.files.size() << " (" << async_files << " through an engine)";
    }

    return os;
}

void AsyncIoStats::record(const SubmissionStats & stats)
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);

    // The total counts every submission, including ones dropped below. Without that, a long run
    // would appear to have done less work than it did.
    //
    // Shared mounts is kept as the HIGHEST seen, not a sum. Two submissions usually read the same
    // mounts, so adding them would count the same sharing twice.
    _total.shared_engine_mounts = std::max(_total.shared_engine_mounts, stats.shared_engine_mounts);
    _total.files.clear();   // a list of every file ever read is not a total

    _submissions.push_back(stats);

    if (_submissions.size() > MaxSubmissions)
    {
        _submissions.erase(_submissions.begin());
        ++_dropped;
    }
}

bool AsyncIoStats::find(SubmissionId submission_id, SubmissionStats & out) const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);

    for (const auto & stats : _submissions)
    {
        if (stats.submission_id == submission_id)
        {
            out = stats;
            return true;
        }
    }

    return false;
}

std::vector<SubmissionStats> AsyncIoStats::submissions() const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    return _submissions;
}

SubmissionStats AsyncIoStats::total() const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    return _total;
}

size_t AsyncIoStats::dropped() const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    return _dropped;
}

}; // namespace runai::llm::streamer::impl
