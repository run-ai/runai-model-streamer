#include "utils/strings/strings.h"
#include <cstring>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::utils
{

void Strings::create_cstring_list(std::vector<std::string>& strings, char*** list_ptr, size_t* count)
{
    // Set object count
    *count = strings.size();

    if (*count == 0)
    {
        // Set to null for empty input
        *list_ptr = nullptr;
        return;
    }

    // Allocate memory for the array of C-string pointers
    *list_ptr = reinterpret_cast<char**>(malloc(*count * sizeof(char*)));
    if (*list_ptr == nullptr) // Check malloc success
    {
        throw std::bad_alloc();
    }

    try
    {
        for (size_t i = 0; i < *count; ++i)
        {
            // Allocate memory for each string (+1 for null terminator)
            size_t length = strings[i].size() + 1;
            (*list_ptr)[i] = reinterpret_cast<char*>(malloc(length));
            if ((*list_ptr)[i] == nullptr) // Check malloc success
            {
                throw std::bad_alloc();
            }

            // Copy string content
            std::memcpy((*list_ptr)[i], strings[i].c_str(), length);
        }
    }
    catch (...)
    {
        free_cstring_list(*list_ptr, *count);
        *list_ptr = nullptr;
        *count = 0;
        throw; // Re-throw the exception
    }
}

void Strings::free_cstring_list(char** list, size_t count)
{
    ASSERT(((count == 0 && list == nullptr) || (count && list != nullptr))) << "invalid arguments - size is " << count;

    for (size_t i = 0; i < count; ++i)
    {
        if (list[i])
        {
            free(reinterpret_cast<char*>(list[i])); // Free each string
            list[i] = nullptr;
        }
    }

    if (list)
    {
        free(list); // Free the array of pointers
    }
}

} // namespace runai::llm::streamer::utils
