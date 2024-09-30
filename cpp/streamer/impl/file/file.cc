#include "streamer/impl/file/file.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <utility>

#include "common/exception/exception.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

File::File(const std::string & path, const Config & config) :
    Reader(Reader::Mode::Sync),
    _fd(::open(path.c_str(), O_RDONLY)),
    _block_size(config.fs_block_bytesize)
{
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

void File::async_read(std::vector<common::Range> & ranges, char * buffer)
{
    LOG(ERROR) << "Not implemented";
    throw common::Exception(common::ResponseCode::UnknownError);
}

common::Response File::async_response()
{
    LOG(ERROR) << "Not implemented";
    throw common::Exception(common::ResponseCode::UnknownError);
}

}; // namespace runai::llm::streamer::impl
