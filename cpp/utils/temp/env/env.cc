#include "utils/temp/env/env.h"

#include <stdlib.h>

#include <chrono>
#include <thread>
#include <utility>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::utils::temp
{

static constexpr auto PRE_SETENV_DELAY = std::chrono::milliseconds{10};

Env::Env(const std::string & value) : Env(random::string(), value)
{}

Env::Env(const std::string & name, char const * const value) :
    name(name),
    value(value)
{
    // We are sleeping for 10ms here in order to avoid a getenv/setenv race that occurs
    // sometimes upon launching a thread (calling getenv()) and using this method to
    // set an environment variable in our UT.
    std::this_thread::sleep_for(PRE_SETENV_DELAY);
    PASSERT(::setenv(name.c_str(), value, 0 /* no overwrite */) != -1) << "Failed setting environment variable '" << name << "' to \"" << value << "\"";
}

Env::Env(const std::string & name, const std::string & value) :
    Env(name, value.c_str())
{}

Env::Env(const std::string & name, int value) :
    Env(name, std::to_string(value))
{}

Env::Env(const std::string & name, unsigned long value) :
    Env(name, std::to_string(value))
{}

Env::Env(const std::string & name, bool value) :
    Env(name, static_cast<int>(value))
{}

Env::Env(const std::string & name, float value) :
    Env(name, std::to_string(value))
{}

Env::~Env()
{
    if (!name.empty() && ::unsetenv(name.c_str()) == -1)
    {
        LOG(ERROR) << "Failed unsetting environment variable '" << name << "'";
    }
}

} // namespace runai::llm::streamer::utils::temp
