#pragma once

#include <string>
#include <vector>

namespace runai::llm::streamer::utils
{

struct Strings
{
    static void create_cstring_list(std::vector<std::string> & strings, char*** list_ptr, size_t * count);
    static void free_cstring_list(char** list, size_t count);
};

} // namespace runai::llm::streamer::utils
