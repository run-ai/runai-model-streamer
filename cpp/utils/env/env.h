#pragma once

#include <string>

namespace runai::llm::streamer::utils
{

template <typename T = std::string>
bool try_getenv(const std::string & variable, /* out */ T & value);

template <>
bool try_getenv(const std::string & variable, /* out */ std::string & value);

template <>
bool try_getenv(const std::string & variable, /* out */ int & value);

template <>
bool try_getenv(const std::string & variable, /* out */ bool & value);

template <>
bool try_getenv(const std::string & variable, /* out */ unsigned long & value);

bool env_exists(const std::string & variable);

void chmod(const std::string & path, mode_t mode);

template <typename T = std::string>
T getenv(const std::string & variable);

template <typename T = std::string>
T getenv(const std::string & variable, T def);

template <>
std::string getenv<std::string>(const std::string & variable);

template <>
int getenv<int>(const std::string & variable);

template <>
unsigned long getenv<unsigned long>(const std::string & variable);

template <>
bool getenv<bool>(const std::string & variable);

template <>
int getenv<int>(const std::string & variable, int def);

template <>
unsigned long getenv<unsigned long>(const std::string & variable, unsigned long def);

template <>
bool getenv<bool>(const std::string & variable, bool def);

template <>
std::string getenv<std::string>(const std::string & variable, std::string def);

} // namespace runai::llm::streamer::utils
