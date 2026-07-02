#pragma once

#include <string>

#include "utils/random/random.h"

namespace runai::llm::streamer::utils::temp
{

struct Env
{
    Env(const std::string & value = random::string());

    Env(
        const std::string & name,
        char const * const value);

    Env(
        const std::string & name,
        const std::string & value);

    Env(
        const std::string & name,
        int value);

    Env(
        const std::string & name,
        unsigned long value);

    Env(
        const std::string & name,
        bool value);

    Env(
        const std::string & name,
        float value);

    ~Env();

    Env(Env &&) = delete;
    Env(const Env &) = delete;

    Env & operator=(Env &&) = delete;
    Env & operator=(const Env &) = delete;

    std::string name;
    std::string value;
};

// Unsets an environment variable for the lifetime of the object and restores its
// previous value (or leaves it unset) on destruction
struct UnsetEnv
{
    explicit UnsetEnv(const std::string & name);
    ~UnsetEnv();

    UnsetEnv(UnsetEnv &&) = delete;
    UnsetEnv(const UnsetEnv &) = delete;

    UnsetEnv & operator=(UnsetEnv &&) = delete;
    UnsetEnv & operator=(const UnsetEnv &) = delete;

    std::string name;
    std::string previous_value;
    bool had_value = false;
};

} // namespace runai::llm::streamer::utils::temp
