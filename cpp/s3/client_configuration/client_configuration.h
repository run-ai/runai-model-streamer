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
    // Neither has a default: the caller states both. concurrent_readers multiplies the per-reader
    // throughput target, so one client asks for the whole capacity and the CRT opens the connections
    // in one pool.
    //
    // chunk_bytesize is the size of the ranged reads this client will be given, and becomes the CRT
    // part size so one read is one part. 0 keeps the SDK default, and the CRT then splits a larger
    // read into parts of its own. The CRT raises anything below 5 MiB.
    ClientConfiguration(unsigned concurrent_readers, size_t chunk_bytesize);

    Aws::S3Crt::ClientConfiguration config;
};

}; //namespace runai::llm::streamer::impl::s3
