#include "utils/temp/file/file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <utility>

#include "utils/fd/fd.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::utils::temp
{

Path::Path(const std::string & dir, const std::string & name) :
    name(name),
    path(dir + (name.empty() ? "" : "/") + name)
{}

Path::Path(Path && other)
{
    *this = std::move(other);
}

Path & Path::operator=(Path && other)
{
    _delete();

    name = other.name;
    path = other.path;

    other.path = other.name = "";

    return *this;
}

Path::~Path()
{
    try
    {
        _delete();
    }
    catch (...)
    {}
}

bool Path::exists(const std::string & path)
{
    return ::access(path.c_str(), F_OK) != -1;
}

bool Path::is_directory(const std::string & path)
{
    struct stat stat = {};

    PASSERT(::stat(path.c_str(), &stat) == 0) << "Failed querying stats of '" << path << "'";

    return S_ISDIR(stat.st_mode);
}

void Path::_delete()
{
    if (path.empty() || !exists(path))
    {
        return;
    }

    ASSERT(!Path::is_directory(path)) << "Removing directory is not implemented";

    PASSERT(::unlink(path.c_str()) != -1) << "Failed removing file '" << path << "'";
}

File::File(const std::string & dir, const std::string & name, const std::vector<uint8_t> & data) :
    _path(dir, name),
    name(_path.name),
    path(_path.path)
{
    utils::Fd::write(path, data, O_WRONLY | O_CREAT, 0777);
}

File::File(const std::vector<uint8_t> & data) : File(".", random::string(), data)
{}

} // namespace runai::llm::streamer::utils::temp
