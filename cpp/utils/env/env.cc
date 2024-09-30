#include "utils/env/env.h"

#include <sys/syscall.h>
#include <unistd.h>

#include "utils/logging/logging.h"


namespace runai::llm::streamer::utils
{

template <>
bool try_getenv<std::string>(const std::string & variable, /* out */ std::string & s)
{
    char const * const raw = ::getenv(variable.c_str());

    if (raw == nullptr)
    {
        return false;
    }

    /* out */ s = std::string(raw);
    return true;
}

template <>
bool try_getenv<int>(const std::string & variable, /* out */ int & i)
{
    std::string s;
    if (!try_getenv<std::string>(variable, /* out */ s))
    {
        return false;
    }

    std::string::size_type idx;
    i = std::stoi(s, /* out */ &idx);

    ASSERT(idx == s.size()) << "Failed parsing string '" << s << "' as an integer";

    return true;
}

template <>
bool try_getenv<unsigned long>(const std::string & variable, /* out */ unsigned long & i)
{
    std::string s;
    if (!try_getenv<std::string>(variable, /* out */ s))
    {
        return false;
    }

    std::string::size_type idx;
    i = std::stoul(s, /* out */ &idx);

    ASSERT(idx == s.size()) << "Failed parsing string '" << s << "' as an integer";

    return true;
}

template <>
bool try_getenv<bool>(const std::string & variable, /* out */ bool & b)
{
    int i;
    if (!try_getenv<int>(variable, /* out */ i))
    {
        return false;
    }

    /* out */ b = static_cast<bool>(i);
    return true;
}

bool env_exists(const std::string & variable)
{
    std::string value;
    return try_getenv(variable, /* out */ value);
}

template <>
std::string getenv<std::string>(const std::string & variable)
{
    std::string value;

    if (!try_getenv(variable, /* out */ value))
    {
        LOG(ERROR) << "Failed getting environment variable '" << variable << "'";
        throw std::exception();
    }

    return value;
}

template <>
int getenv<int>(const std::string & variable)
{
    std::string::size_type idx;
    const auto s = getenv<std::string>(variable);
    int i = std::stoi(s, /* out */ &idx);

    ASSERT(idx == s.size()) << "Failed parsing string '" << s << "' as an integer";

    return i;
}

template <>
unsigned long getenv<unsigned long>(const std::string & variable)
{
    std::string::size_type idx;
    const auto s = getenv<std::string>(variable);
    unsigned long i = std::stoul(s, /* out */ &idx);

    ASSERT(idx == s.size()) << "Failed parsing string '" << s << "' as an integer";

    return i;
}

template <>
bool getenv<bool>(const std::string & variable)
{
    return static_cast<bool>(getenv<int>(variable));
}

template <>
int getenv<int>(const std::string & variable, int def)
{
    std::string raw;

    if (!try_getenv(variable, /* out */ raw))
    {
        return def;
    }

    std::string::size_type idx;
    int i = std::stoi(raw, /* out */ &idx);

    ASSERT(idx == raw.size()) << "Failed parsing string '" << raw << "' as an integer";

    return i;
}

template <>
unsigned long getenv<unsigned long>(const std::string & variable, unsigned long def)
{
    std::string raw;

    if (!try_getenv(variable, /* out */ raw))
    {
        return def;
    }

    std::string::size_type idx;
    unsigned long i = std::stoul(raw, /* out */ &idx);

    ASSERT(idx == raw.size()) << "Failed parsing string '" << raw << "' as an integer";

    return i;
}

template <>
bool getenv<bool>(const std::string & variable, bool def)
{
    return static_cast<bool>(getenv<int>(variable, static_cast<int>(def)));
}

template <>
std::string getenv<std::string>(const std::string & variable, std::string def)
{
    std::string value;

    if (!try_getenv(variable, /* out */ value))
    {
        return def;
    }

    return value;
}

} // namespace runai::llm::streamer::utils
