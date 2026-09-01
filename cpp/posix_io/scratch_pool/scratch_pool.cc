#include "posix_io/scratch_pool/scratch_pool.h"

#include <cstdlib>
#include <algorithm>

#include "common/exception/exception.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::posix_io
{

ScratchPool::ScratchPool(size_t count, size_t block) :
    _block(block == 0 ? 1 : block),
    _memory(nullptr, &std::free)
{
    if (count == 0)
    {
        return;
    }

    // posix_memalign, not new[]: the kernel checks the ADDRESS of a direct read, and the default
    // allocator promises about 16 bytes. One allocation for all of them, so the base is aligned once
    // and every slice at a block multiple is aligned too.
    void * raw = nullptr;
    if (::posix_memalign(&raw, _block, count * _block) != 0)
    {
        LOG(ERROR) << "Cannot allocate " << count << " scratch buffers of " << _block << " bytes";
        throw common::Exception(common::ResponseCode::UnknownError);
    }

    _memory.reset(static_cast<char *>(raw));

    _free.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        _free.push_back(_memory.get() + i * _block);
    }
}

char * ScratchPool::take()
{
    if (_free.empty())
    {
        return nullptr;
    }

    char * buffer = _free.back();
    _free.pop_back();
    return buffer;
}

void ScratchPool::give(char * buffer)
{
    ASSERT(buffer != nullptr) << "returning a null scratch buffer";

    // Taken from the back and returned to the back, so a buffer is reused while it is still warm in
    // the cache rather than after every other one has been used.
    _free.push_back(buffer);
}

size_t ScratchPool::block() const
{
    return _block;
}

size_t ScratchPool::free_count() const
{
    return _free.size();
}

}; // namespace runai::llm::streamer::posix_io
