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

} // namespace runai::llm::streamer::utils::temp
