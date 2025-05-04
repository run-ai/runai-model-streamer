#include "common/path/path.h"

namespace runai::llm::streamer::common::s3
{

Path::Path(const StorageUri_C & path, unsigned index) :
    uri(path),
    index(index)
{}

}; //namespace runai::llm::streamer::common::s3
