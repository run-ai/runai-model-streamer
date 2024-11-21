
#include <vector>
#include <string>
#include <chrono>

#include "streamer/impl/streamer/streamer.h"

#include "utils/logging/logging.h"
#include "utils/random/random.h"
#include "utils/fd/fd.h"
#include "utils/strings/strings.h"

int main(int argc, char *argv[])
{
    if (argc < 2) {
        LOG(ERROR) << "Usage: " << argv[0] << " <s3_uri or filesystem path>";
        return 1;
    }

    const std::string path(argv[1]);

    char** object_keys;
    size_t object_count;

    runai::llm::streamer::impl::Streamer streamer;

    auto start = std::chrono::steady_clock::now();
    runai::llm::streamer::common::ResponseCode ret = streamer.list_objects(path, &object_keys, &object_count);
    if (ret != runai::llm::streamer::common::ResponseCode::Success)
    {
        LOG(ERROR) << "Failed with response " << ret;
        return 1;
    }

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    LOG(INFO) << "Listing " << object_count << " files from path " << path;

    for (size_t i =0 ; i < object_count; ++i)
    {
        LOG(INFO) << object_keys[i];
    }

    // free memory
    LOG(INFO) << "Free memory";
    runai::llm::streamer::utils::Strings::free_cstring_list(object_keys, object_count);
    LOG(INFO) << "Free memory done";

    return 0;
}
