#include "streamer/impl/strategy_resolver/strategy_resolver.h"

#include <sstream>
#include <utility>
#include <vector>

#include "common/exception/exception.h"
#include "common/posix_io/io_uring_probe/io_uring_probe.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

namespace
{

using common::posix_io::Strategy;

// Why this candidate cannot serve, or Success if it can.
//
// SyncBuffered is always available - it is the existing threadpool, which needs nothing probed. That
// is what makes it usable as the last entry of a list and what makes exhaustion mean something: a
// list that ends in sync_buffered cannot be exhausted, and one that does not was a deliberate choice
// to fail rather than fall back.
common::ResponseCode availability_of(Strategy strategy)
{
    switch (strategy)
    {
    case Strategy::SyncBuffered:
        return common::ResponseCode::Success;

    case Strategy::IoUringBuffered:
        return common::posix_io::IoUringProbe::instance().capability().error == common::ResponseCode::Success
             ? common::ResponseCode::Success
             : common::posix_io::IoUringProbe::instance().capability().error;

    case Strategy::IoUringDirect:
        // The ring is available but the direct path is not written yet (S7). Reported as unavailable
        // rather than served buffered under a direct name, which would look like it worked.
        return common::ResponseCode::UnsupportedBackendMix;

    case Strategy::LibaioDirect:
        return common::ResponseCode::UnsupportedBackendMix;   // no libaio engine yet (S8)
    }

    return common::ResponseCode::UnknownError;
}

} // namespace

StrategyResolver::StrategyResolver(std::string default_candidates) :
    _default_candidates(std::move(default_candidates))
{}

common::ResponseCode StrategyResolver::set_candidates(const std::string & candidates)
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);

    // Parse before deciding anything, so a typo is reported as a typo rather than as a conflict.
    try
    {
        (void)common::posix_io::parse_candidates(candidates);
    }
    catch (const common::Exception & e)
    {
        LOG(ERROR) << "Cannot set filesystem strategy to '" << candidates << "': " << e.error();
        return common::ResponseCode::InvalidParameterError;
    }

    // Already settled. Compare against what was actually walked, which may have come from the
    // environment rather than from an earlier call - otherwise setting a strategy after the first
    // request would report Success and do nothing.
    if (_resolved_from.has_value())
    {
        if (_resolved_from.value() == candidates)
        {
            return common::ResponseCode::Success;
        }

        LOG(ERROR) << "Filesystem strategy is already resolved from '" << _resolved_from.value()
                   << "' (using " << _resolved.value() << "); create a new streamer to use '"
                   << candidates << "'";
        return common::ResponseCode::UnsupportedBackendMix;
    }

    // Not resolved yet: first set wins, as for credentials.
    if (_candidates.has_value() && _candidates.value() != candidates)
    {
        LOG(ERROR) << "Filesystem strategy was already set to '" << _candidates.value()
                   << "'; create a new streamer to use '" << candidates << "'";
        return common::ResponseCode::UnsupportedBackendMix;
    }

    _candidates = candidates;
    return common::ResponseCode::Success;
}

common::ResponseCode StrategyResolver::resolve()
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    return resolve_locked();
}

common::ResponseCode StrategyResolver::resolve_locked()
{
    if (_resolved.has_value())
    {
        return common::ResponseCode::Success;   // idempotent; every submission after the first
    }

    const auto & candidates = _candidates.has_value() ? _candidates.value() : _default_candidates;

    std::vector<Strategy> list;
    try
    {
        list = common::posix_io::parse_candidates(candidates);
    }
    catch (const common::Exception & e)
    {
        LOG(ERROR) << "Cannot read filesystem strategy '" << candidates << "': " << e.error();
        return common::ResponseCode::InvalidParameterError;
    }

    // Recorded before the walk, so that a set_candidates arriving afterwards has something to compare
    // against even if no candidate could serve.
    _resolved_from = candidates;

    std::ostringstream rejections;

    for (const auto strategy : list)
    {
        const auto available = availability_of(strategy);
        if (available == common::ResponseCode::Success)
        {
            _resolved = strategy;
            LOG(INFO) << "Filesystem strategy: " << strategy << " (from '" << candidates << "')"
                      << rejections.str();
            return common::ResponseCode::Success;
        }

        // Collected rather than logged one by one: on success they explain why the winner won, and on
        // exhaustion they are the whole error.
        rejections << "; " << strategy << " rejected (" << available << ")";
    }

    LOG(ERROR) << "No filesystem strategy in '" << candidates << "' can be served here"
               << rejections.str()
               << ". Add sync_buffered to the list to allow the synchronous reader";
    return common::ResponseCode::UnsupportedBackendMix;
}

common::posix_io::Strategy StrategyResolver::resolved() const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    ASSERT(_resolved.has_value()) << "filesystem strategy read before it was resolved";
    return _resolved.value();
}

bool StrategyResolver::is_resolved() const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    return _resolved.has_value();
}

std::string StrategyResolver::resolved_from() const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    return _resolved_from.value_or(std::string());
}

}; // namespace runai::llm::streamer::impl
