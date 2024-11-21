#pragma once

#include <string>
#include <vector>

namespace runai::llm::streamer::utils
{

struct Strings
{
    static void create_cstring_list(std::vector<std::string> & strings, char*** object_keys, size_t * object_count);
    static void free_cstring_list(char** object_keys, size_t object_count);
};

} // namespace runai::llm::streamer::utils
