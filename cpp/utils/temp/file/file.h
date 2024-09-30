#pragma once

#include <string>
#include <vector>

#include "utils/random/random.h"

namespace runai::llm::streamer::utils::temp
{

struct Path
{
    Path(
        const std::string & dir = ".",
        const std::string & name = random::string());
    ~Path();

    Path(Path &&);
    Path(const Path &) = delete;

    Path & operator=(Path &&);
    Path & operator=(const Path &) = delete;

    std::string name;
    std::string path;

    static bool exists(const std::string & path);
    static bool is_directory(const std::string & path);

 private:
    void _delete();
};

struct File
{
    File(
        const std::string & dir = ".",
        const std::string & name = random::string(),
        const std::vector<uint8_t> & data = random::buffer());

    File(const std::vector<uint8_t> & data);

    ~File() = default;

    File(File &&) = default;
    File(const File &) = delete;

    File & operator=(File &&) = default;
    File & operator=(const File &) = delete;

    const std::string & location() const { return _path.path; }

 private:
    Path _path;

 public:
    std::string name;
    std::string path;
};

} // namespace runai::llm::streamer::utils::temp
