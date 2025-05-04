
#pragma once

#include <atomic>
#include <vector>

#include "streamer/impl/batch/batch.h"

namespace runai::llm::streamer::impl
{

struct Workload
{
    Workload() = default;
    Workload(Workload &&) = default;
    Workload & operator=(Workload &&) = default;

    void execute(std::atomic<bool> & stopped);

    std::vector<Batch> batches;
};

}; // namespace runai::llm::streamer::impl
