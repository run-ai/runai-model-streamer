#pragma once

#include <limits>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

namespace runai::llm::streamer::impl
{

// RUNAI_STREAMER_FS_PARALLELISM: how many filesystem reads may be in flight, per mount.
//
//     <default> [ "," <type> "=" <value> ]*
//
//     512                        every mount
//     512,nfs=64                 64 on NFS, 512 elsewhere
//     512,nfs=64,virtiofs=256    and 256 on virtiofs
//
// The leading default is mandatory, so every mount has an answer even when it matches no entry. A
// plain integer is therefore always valid, and stays the whole meaning of the variable for anyone who
// never needs per-type values.
//
// Types are keyed rather than paths because a path is a per-deployment detail - the same model sits at
// different paths on different clusters - while "NFS is the slow one" travels with the storage. The
// type is what /proc/self/mountinfo reports, so `nfs`, `nfs4`, `virtiofs`, `ext4`, `overlay`.
class FsParallelism
{
 public:
    // A type entry, in the order it was written. Order matters when one key is a prefix of another.
    struct Entry
    {
        std::string type;
        unsigned    value = 0;
    };

    // Parse, or THROW InvalidParameterError naming the variable.
    //
    // Rejected rather than repaired, like every other numeric variable here: a value the user meant to
    // set and mistyped must not read as "unset". Rejected are a missing or non-numeric default, a
    // non-numeric or zero value, an entry with no "=", an empty type, and a REPEATED type - the last
    // one because resolving it by order would silently pick one of two numbers the user wrote.
    static FsParallelism parse(const std::string & value);

    // A plain value, for the callers that have no mount to ask about.
    explicit FsParallelism(unsigned value);

    // The value for a mount of this type: the FIRST entry whose type is a prefix of it, else the
    // default. Prefix, so `nfs=` covers `nfs` and `nfs4` without the user knowing which the kernel
    // reports.
    //
    // First match, not longest match: with `nfs=64,nfs4=32` an nfs4 mount gets 64. The caller logs
    // which entry matched, so the effect is visible rather than surprising, and writing the longer key
    // first gives the other answer.
    //
    // `out_matched` is the index of the entry that answered, or entries().size() when the default did.
    unsigned for_type(const std::string & fs_type, size_t & out_matched) const;
    unsigned for_type(const std::string & fs_type) const;

    unsigned default_value() const;
    const std::vector<Entry> & entries() const;

 private:
    FsParallelism() = default;

    unsigned _default = 0;
    std::vector<Entry> _entries;
};

std::ostream & operator<<(std::ostream &, const FsParallelism &);

}; // namespace runai::llm::streamer::impl
