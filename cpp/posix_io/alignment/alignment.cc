#include "posix_io/alignment/alignment.h"

#include <algorithm>
#include <cstdint>
#include <sstream>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::posix_io
{

namespace
{

// A block of 0 or 1 means "no rule to follow". Treating it as 1 keeps every test below true without
// a special case, because everything is a multiple of 1.
size_t usable(size_t block)
{
    return block == 0 ? 1 : block;
}

} // namespace

size_t block_size(const Limits & limits)
{
    return std::max<size_t>({ usable(limits.offset_alignment), usable(limits.buffer_alignment), 1 });
}

bool is_congruent(size_t file_offset, const void * buffer, size_t block)
{
    const auto address = reinterpret_cast<uintptr_t>(buffer);
    const auto step = usable(block);

    // Written as two remainders rather than as (address - file_offset) % step.
    //
    // For our block sizes the two are the same. Both numbers are unsigned, so the subtraction wraps
    // when the file offset is larger - but 2^64 is itself a multiple of any power of two, so the wrap
    // does not change the remainder. Block sizes are always 512 or 4096.
    //
    // The remainder form is used anyway, because it needs no such argument: it is correct for any
    // block, and a reader does not have to work out whether the wrap is safe.
    return (address % step) == (file_offset % step);
}

AlignedRead expand(size_t offset, size_t bytesize, char * buffer, size_t block)
{
    const auto step = usable(block);

    AlignedRead out;
    out.offset = offset - (offset % step);
    out.head = offset - out.offset;

    // The end moves up to the next block. The length therefore covers the head bytes as well as the
    // wanted bytes, which is why head is added before rounding.
    const size_t wanted_end = out.head + bytesize;
    out.bytesize = wanted_end + ((step - (wanted_end % step)) % step);

    // The address moves down by exactly what the offset moved down by. That is what keeps the file
    // and the memory in step, and it is why this is only correct when they were congruent already.
    out.buffer = buffer - out.head;

    return out;
}

std::string alignment_diagnosis(const Requested & requested, const Limits & limits)
{
    const auto address = reinterpret_cast<uintptr_t>(requested.buffer);
    const auto block = block_size(limits);

    const bool offset_ok = limits.offset_alignment == 0 || requested.offset % limits.offset_alignment == 0;
    const bool length_ok = limits.offset_alignment == 0 || requested.bytesize % limits.offset_alignment == 0;
    const bool buffer_ok = limits.buffer_alignment == 0 || address % limits.buffer_alignment == 0;
    const bool congruent = is_congruent(requested.offset, requested.buffer, block);

    std::stringstream ss;
    ss << "direct read rejected:"
       << " offset=" << requested.offset << " (align " << limits.offset_alignment << (offset_ok ? " ok" : " BAD") << ")"
       << ", buffer=" << static_cast<const void *>(requested.buffer)
       << " (align " << limits.buffer_alignment << (buffer_ok ? " ok" : " BAD") << ")"
       << ", length=" << requested.bytesize
       << " (align " << limits.offset_alignment << (length_ok ? " ok" : " BAD") << ")"
       << ", congruent=" << (congruent ? "yes" : "no");

    // The length is checked against offset_alignment, not against buffer_alignment. The kernel's
    // offset rule covers the length as well. Checking it against the other number gives a message
    // where every field says "ok" and nothing explains the failure.
    //
    // congruence is last because it is the one that matters most: the three above can all say "ok"
    // while the read is still impossible.
    return ss.str();
}

}; // namespace runai::llm::streamer::common::posix_io
