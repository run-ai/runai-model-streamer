#include "streamer/impl/config/fs_parallelism/fs_parallelism.h"

#include <algorithm>
#include <limits>
#include <cctype>
#include <ostream>
#include <set>
#include <stdexcept>

#include "common/exception/exception.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

namespace
{

std::string trimmed(const std::string & text)
{
    const auto first = text.find_first_not_of(" \t");
    if (first == std::string::npos)
    {
        return std::string();
    }

    const auto last = text.find_last_not_of(" \t");
    return text.substr(first, last - first + 1);
}

std::string lowered(std::string text)
{
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return text;
}

// A positive count, or throw.
//
// Rejected rather than repaired, like every other numeric variable here: a value the user meant to set
// and mistyped must not read as "unset". Zero is rejected too - it would mean a reader that admits
// nothing, which is not a configuration anyone wants and would divide by zero downstream.
unsigned positive_count(const std::string & text, const std::string & whole)
{
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
    {
        LOG(ERROR) << "RUNAI_STREAMER_FS_PARALLELISM=" << whole << " : '" << text
                   << "' is not a positive number";
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }

    try
    {
        const auto value = std::stoul(text);
        if (value == 0 || value > std::numeric_limits<unsigned>::max())
        {
            LOG(ERROR) << "RUNAI_STREAMER_FS_PARALLELISM=" << whole << " : " << text
                       << " is out of range - it must be at least 1";
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
        return static_cast<unsigned>(value);
    }
    catch (const std::out_of_range &)
    {
        LOG(ERROR) << "RUNAI_STREAMER_FS_PARALLELISM=" << whole << " : " << text << " is too large";
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }
}

} // namespace

FsParallelism::FsParallelism(unsigned value) :
    _default(value)
{}

FsParallelism FsParallelism::parse(const std::string & value)
{
    FsParallelism out;

    const auto whole = trimmed(value);
    if (whole.empty())
    {
        LOG(ERROR) << "RUNAI_STREAMER_FS_PARALLELISM is empty; it must start with a number of reads";
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }

    std::set<std::string> seen;
    size_t begin = 0;
    bool first = true;

    while (begin <= whole.size())
    {
        const auto comma = whole.find(',', begin);
        const auto element = trimmed(whole.substr(begin, comma == std::string::npos
                                                        ? std::string::npos : comma - begin));

        // The DEFAULT comes first, and is mandatory. Without it a mount matching no entry would have
        // no answer, and the variable would have to invent one - which is the kind of silent decision
        // this whole setting exists to remove.
        if (first)
        {
            if (element.find('=') != std::string::npos)
            {
                LOG(ERROR) << "RUNAI_STREAMER_FS_PARALLELISM=" << whole
                           << " must begin with a number, before any <type>=<value> entries";
                throw common::Exception(common::ResponseCode::InvalidParameterError);
            }

            out._default = positive_count(element, whole);
            first = false;
        }
        else
        {
            const auto equals = element.find('=');
            if (equals == std::string::npos)
            {
                LOG(ERROR) << "RUNAI_STREAMER_FS_PARALLELISM=" << whole << " : '" << element
                           << "' is not <type>=<value>";
                throw common::Exception(common::ResponseCode::InvalidParameterError);
            }

            const auto type = lowered(trimmed(element.substr(0, equals)));
            if (type.empty())
            {
                LOG(ERROR) << "RUNAI_STREAMER_FS_PARALLELISM=" << whole << " : '" << element
                           << "' has no filesystem type before the '='";
                throw common::Exception(common::ResponseCode::InvalidParameterError);
            }

            // A repeat is REJECTED, not resolved by order. Both numbers were written on purpose, and
            // picking one silently would leave the user reading a value that never applied. The
            // strategy list already refuses a repeated candidate for the same reason.
            if (!seen.insert(type).second)
            {
                LOG(ERROR) << "RUNAI_STREAMER_FS_PARALLELISM=" << whole << " : '" << type
                           << "' appears more than once";
                throw common::Exception(common::ResponseCode::InvalidParameterError);
            }

            out._entries.push_back(Entry{ type, positive_count(trimmed(element.substr(equals + 1)), whole) });
        }

        if (comma == std::string::npos)
        {
            break;
        }
        begin = comma + 1;
    }

    return out;
}

unsigned FsParallelism::for_type(const std::string & fs_type, size_t & out_matched) const
{
    const auto type = lowered(fs_type);

    for (size_t i = 0; i < _entries.size(); ++i)
    {
        // PREFIX, so `nfs=` covers `nfs` and `nfs4` without the user having to know which one the
        // kernel reports for their mount.
        if (type.compare(0, _entries[i].type.size(), _entries[i].type) == 0)
        {
            out_matched = i;
            return _entries[i].value;
        }
    }

    out_matched = _entries.size();
    return _default;
}

unsigned FsParallelism::for_type(const std::string & fs_type) const
{
    size_t matched = 0;
    return for_type(fs_type, matched);
}

unsigned FsParallelism::default_value() const
{
    return _default;
}

const std::vector<FsParallelism::Entry> & FsParallelism::entries() const
{
    return _entries;
}

std::ostream & operator<<(std::ostream & os, const FsParallelism & parallelism)
{
    os << parallelism.default_value();

    for (const auto & entry : parallelism.entries())
    {
        os << ", " << entry.type << "=" << entry.value;
    }

    return os;
}

}; // namespace runai::llm::streamer::impl
