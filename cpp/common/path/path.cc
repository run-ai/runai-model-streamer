#include "common/path/path.h"

namespace runai::llm::streamer::common::s3
{

Path::Path(const StorageUri_C & path) :
    uri(path)
{}

}; //namespace runai::llm::streamer::common::s3
