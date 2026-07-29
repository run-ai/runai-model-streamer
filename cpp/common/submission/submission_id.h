#pragma once

#include <cstdint>

namespace runai::llm::streamer
{

// Identifier for one runai_request submission: assigned by SubmissionsMgr (returned from runai_request),
// stamped on every response, and echoed by runai_response so a shared responder can be demuxed across
// concurrent submissions. Part of the streamer's public C API contract (hence the runai::llm::streamer
// namespace, not ::common).
//
// It lives here, in a dependency-free leaf under common/, rather than in the //streamer C-API package
// because the lower layers that carry it - common::Response, SubmissionsMgr, Batch - sit below the C API
// and must name it without depending on the streamer package (which would invert the layering; //common
// is also shared with the object-storage plugins). 64-bit so the id space is effectively unbounded;
// callers treat it as an opaque token. 0 is reserved as the "none" value (the Response default).
using SubmissionId = std::uint64_t;

} // namespace runai::llm::streamer
