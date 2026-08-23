#pragma once

#include <mutex>
#include <optional>
#include <string>

#include "common/posix_io/strategy/strategy.h"
#include "common/response_code/response_code.h"

namespace runai::llm::streamer::impl
{

// Which filesystem strategy this streamer uses: one answer, for the streamer's whole life.
//
// The strategy names an ENGINE, and the engine is streamer-scoped (5.7), so this is settled once and
// never revisited. What varies per file is the cache mode and the mount - a different question,
// answered elsewhere.
//
// Resolution walks the candidate list and takes the first the host can provide, logging each
// rejection with its reason. Exhausting the list is an error carrying those reasons, never a silent
// fall-through to the synchronous reader: an operator who asked for io_uring and quietly got
// something else has no way to find out.
//
// Availability comes from IoUringProbe, NOT from building an engine. An engine needs a depth, which
// needs RUNAI_STREAMER_PROCESS_GROUP_SIZE, and it belongs to the worker; the probe needs nothing.
//
// Thread safe - several submitters can race on the first request.
class StrategyResolver
{
 public:
    // `default_candidates` is what resolve() walks if nobody calls set_candidates. Normally
    // RUNAI_STREAMER_FS_STRATEGY, read into Config.
    explicit StrategyResolver(std::string default_candidates);

    // Record the caller's preference, before it is used. TWO rules, and both are needed:
    //
    //   - First set wins, exactly as credentials do (streamer.cc:117-137): the same value again is
    //     Success, a different value is rejected. Silently replacing a value the caller already acted
    //     on is worse than refusing.
    //
    //   - After resolution, any value differing from the one actually resolved from is rejected -
    //     INCLUDING the first. Without this, a caller who set nothing, let the first request resolve
    //     from the environment, and only then called this would pass the first-set-wins test and get
    //     Success with no effect at all. A setter that reports success and changes nothing is the
    //     failure this rule exists to prevent.
    //
    // So a caller who wants a different strategy needs a new streamer. That is the answer credentials
    // give, for the same reason: something has already been built from the first answer.
    //
    // Returns InvalidParameterError on an unparsable list - a typo must not become a silent fallback.
    common::ResponseCode set_candidates(const std::string & candidates);

    // Settle it. Idempotent: the first call decides and later calls repeat its result.
    //
    // Called on the FIRST SUBMISSION, not at construction, so every setter has had its chance first.
    // Resolving in the constructor would leave set_candidates unable to take effect and its rules
    // guarding nothing.
    common::ResponseCode resolve();

    // Valid only after resolve() returned Success.
    common::posix_io::Strategy resolved() const;

    bool is_resolved() const;

    // The list resolve() actually walked, for logging and for set_candidates' second rule.
    std::string resolved_from() const;

 private:
    common::ResponseCode resolve_locked();

    mutable std::mutex _mutex;

    const std::string _default_candidates;

    std::optional<std::string> _candidates;     // what the caller set, if anything
    std::optional<std::string> _resolved_from;  // the list resolve() walked - setter's or default
    std::optional<common::posix_io::Strategy> _resolved;
};

}; // namespace runai::llm::streamer::impl
