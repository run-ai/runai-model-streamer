#pragma once

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/BucketLocationConstraint.h>


#include <cstddef>

#include "common/storage_uri/storage_uri.h"

namespace runai::llm::streamer::impl::s3
{

// In-flight window (bytes) for object-storage submission: a bandwidth-delay product from
// the target throughput (target_gbps, i.e. throughputTargetGbps), floored at a couple of
// chunk_bytesize (the client's default_storage_chunk_size). RUNAI_STREAMER_S3_MAX_INFLIGHT_MIB
// (in MiB) overrides it directly.
size_t inflight_window_bytes(size_t chunk_bytesize, double target_gbps);

struct ClientConfiguration
{
    // concurrent_readers multiplies the per-reader throughput target, so one client asks for the whole
    // capacity and the CRT opens the connections in one pool.
    //
    // part_size is the size of the ranged reads this client will be given. Setting it keeps one read
    // to one CRT part; leaving it 0 keeps the SDK default, and the CRT then splits a larger read into
    // parts of its own. The CRT raises anything below 5 MiB.
    explicit ClientConfiguration(unsigned concurrent_readers = 1, size_t part_size = 0);

    Aws::S3Crt::ClientConfiguration config;
};

}; //namespace runai::llm::streamer::impl::s3
