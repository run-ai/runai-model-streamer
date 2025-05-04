
#include "streamer/impl/workload/workload.h"

#include "utils/logging/logging.h";

namespace runai::llm::streamer::impl
{

void Workload::execute(std::atomic<bool> & stopped)
{
    for (auto & batch : batches)
    {
        batch.execute(stopped);
        LOG(DEBUG) << "Finished batch " << batch;
    }
}

}; // namespace runai::llm::streamer::impl
