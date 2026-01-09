#include "streamer/impl/file/file.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <utility>

#include "common/exception/exception.h"
#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl
{

File::File(const std::string & path, const Config & config) :
    Reader(Reader::Mode::Sync),
    _fd(-1),
    _block_size(config.fs_block_bytesize)
{
    // Determine open flags based on RUNAI_STREAMER_DIRECTIO environment variable.
    // O_DIRECT enables Direct I/O, bypassing the kernel page cache for reads.
    // This is useful to avoid double-caching when using network filesystems or
    // when the application has its own caching layer.
    // Note: O_DIRECT requires aligned buffers and read sizes (typically to 512-byte
    // or filesystem block size boundaries). The existing code uses _block_size for
    // chunked reads, which should satisfy alignment requirements on most systems.
    int flags = O_RDONLY;
    std::string directio_env;
    if (utils::try_getenv("RUNAI_STREAMER_DIRECTIO", directio_env) && directio_env == "1")
    {
        flags |= O_DIRECT;
        LOG(INFO) << "Opening file " << path << " with O_DIRECT (DirectIO enabled)";
    }

    _fd = utils::Fd(::open(path.c_str(), flags));

    if (_fd.fd() == -1)
    {
        LOG(ERROR) << "Failed to access file " << path;
        throw common::Exception(common::ResponseCode::FileAccessError);
    }
}

void File::seek(size_t offset)
{
    try
    {
        _fd.seek(offset);
    }
    catch(const std::exception& e)
    {
        throw common::Exception(common::ResponseCode::EofError);
    }
}

void File::read(size_t bytesize, char * buffer)
{
    size_t result = 0;
    try
    {
        result = _fd.read(bytesize, buffer, utils::Fd::Read::Eof, _block_size);
    }
    catch(const std::exception& e)
    {
        throw common::Exception(common::ResponseCode::UnknownError);
    }

    if (result != bytesize)
    {
        LOG(ERROR) << "Read " << result << " bytes. Expected " << bytesize << " bytes with fd " << _fd.fd();
        throw common::Exception(common::ResponseCode::EofError);
    }
}

void File::async_read(const common::s3::S3ClientWrapper::Params & params, common::backend_api::ObjectRequestId_t request_handle, const common::Range & range, char * buffer)
{
    LOG(ERROR) << "Not implemented";
    throw common::Exception(common::ResponseCode::UnknownError);
}

common::ResponseCode File::async_response(std::vector<common::backend_api::Response> & responses, unsigned max_responses)
{
    LOG(ERROR) << "Not implemented";
    throw common::Exception(common::ResponseCode::UnknownError);
}

}; // namespace runai::llm::streamer::impl
