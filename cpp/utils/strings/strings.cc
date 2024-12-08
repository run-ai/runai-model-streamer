#include "utils/strings/strings.h"
#include <cstring>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::utils
{

void Strings::create_cstring_list(std::vector<std::string> & strings, char*** object_keys, size_t * object_count)
{
    *object_count = strings.size();
    if (strings.size())
    {
        // malloc and free are used here for ctypes (python integration layer)
        *object_keys = reinterpret_cast<char**>(malloc(strings.size() * sizeof(char*)));
        for (size_t i = 0; i < strings.size(); ++i)
        {
            auto length = strings[i].size() + 1;
            (*object_keys)[i] = reinterpret_cast<char*>(malloc(length)); // Allocate memory for each string
            std::strncpy((*object_keys)[i], strings[i].c_str(), length);
        }
    }
    else
    {
        *object_keys = nullptr;
    }
}

void Strings::free_cstring_list(char** object_keys, size_t object_count)
{
    ASSERT(((object_count == 0 && object_keys == nullptr) || (object_count && object_keys != nullptr))) << "invalid arguments - size is " << object_count;

    LOG(SPAM) << "will delete "<< object_keys;

    for (size_t i = 0; i < object_count; ++i)
    {
        if (object_keys[i])
        {
            free(reinterpret_cast<char*>(object_keys[i])); // Free each string
            object_keys[i] = nullptr;
        }
    }

    if (object_keys)
    {
        free(object_keys); // Free the array of pointers
    }
}

} // namespace runai::llm::streamer::utils
