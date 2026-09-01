#pragma once

#include <cstddef>
#include <string>

#include "posix_io/io_engine/io_engine.h"

namespace runai::llm::streamer::common::posix_io
{

// What a direct read was asked to do. Enough to say why the kernel refused it.
struct Requested
{
    size_t offset = 0;
    size_t bytesize = 0;
    const char * buffer = nullptr;
};

// The one number that matters for a direct read.
//
// The kernel has separate limits for the memory address and for the file offset, and they can differ.
// The fully general rule is harder than it is worth. We use the larger of the two for everything, so
// one number covers all three checks. Over-aligning is always correct. It only wastes a little.
size_t block_size(const Limits & limits);

// The block size we assume a direct read needs.
//
// 4096, assumed rather than measured. statx reports the real numbers through STATX_DIOALIGN, which
// arrived in kernel 6.1 while our floor is 5.15 - and it is missing from the build headers here even
// on a 6.8 kernel. 4096 is a safe superset of every block size in use (512 and 4096). Over-aligning
// can only waste a little; it can never fail.
//
// Read by two places that must agree:
//
//   the engines   IoUringEngine and LibaioEngine both report it as their Limits alignment
//   routing       streamer.cc file_groups decides congruence before any engine exists, so it cannot
//                 ask an engine and needs the number early
//
// They share the constant rather than each writing 4096, because a disagreement would be invisible.
// Routing and the worker would differ about which files can be read directly, nothing would fail, and
// the reads would quietly take a path nobody intended.
//
// If an engine ever measures the real value with statx, it will report something equal or NARROWER
// than this, and routing stays correct: it would refuse a few files that could have been read
// directly, sending them to the synchronous reader. Slower for those files, never wrong.
constexpr size_t DirectBlockSize = 4096;

// Can ANY part of this read be done directly?
//
// This is the rule that decides everything, and it is not "is the address aligned". A direct read
// puts file byte F+k at address B+k: the two move together. So it needs one k where both
// (F+k) and (B+k) are multiples of the block. That is possible only when:
//
//     (B - F) % block == 0
//
// If it fails, there is no such k anywhere in the range - not at the start, not in the middle. The
// range has no aligned part at all, so 100% of it must be copied. If it holds, only the first and
// last blocks need care.
//
// For an address and a file offset chosen without thought, this holds about once in `block` tries.
// That is why the streamer places its own buffers on purpose - see o_direct_alignment.md section 7.
bool is_congruent(size_t file_offset, const void * buffer, size_t block);

// A direct read that covers [offset, offset + bytesize), with everything aligned.
//
// The start moves DOWN to the block below, and the length moves UP. So the read covers more of the
// file than was asked for. The extra bytes are real file data, and they have to go somewhere:
//
//     head bytes  land at   buffer - head
//     tail bytes  land after the wanted data
//
// THE CALLER MUST OWN THAT SPACE. This function only does arithmetic. It cannot know whether there is
// room before `buffer`, and writing there is what an overrun looks like. The streamer's own pool
// reserves that space on purpose; memory from a caller does not have it, which is why a read into
// caller memory does not take this path.
//
// Only meaningful when is_congruent() holds. Without congruence there is no aligned read that lands
// the wanted bytes in the right place, whatever the rounding.
struct AlignedRead
{
    size_t offset = 0;     // rounded down to a block
    size_t bytesize = 0;   // rounded up to a block
    char * buffer = nullptr;   // moved down by the same amount as the offset
    size_t head = 0;       // unwanted bytes before the data, i.e. offset - original offset
};

AlignedRead expand(size_t offset, size_t bytesize, char * buffer, size_t block);

// Why a direct read was refused, naming the rule that failed.
//
// The kernel says only EINVAL, which is the same answer for four different mistakes. Reading that in
// a log tells nobody anything. This names each rule and says which one broke:
//
//   direct read rejected: offset=8000 (align 512 BAD), buffer=0x... (align 512 ok),
//                         length=100 (align 512 BAD), congruent=no
std::string alignment_diagnosis(const Requested & requested, const Limits & limits);

}; // namespace runai::llm::streamer::common::posix_io
