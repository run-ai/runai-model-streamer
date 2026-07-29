
#include "streamer/impl/workload/workload.h"

#include <utility>

#include "common/response_code/response_code.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

size_t Workload::size() const
{
    return _batches_by_file_index.size();
}

std::map<unsigned, Batch> & Workload::batches()
{
    return _batches_by_file_index;
}

const std::map<unsigned, Batch> & Workload::batches() const
{
    return _batches_by_file_index;
}

common::ResponseCode Workload::add_batch(Batch && batch)
{
    const auto file_index = batch.file_index;
    ASSERT(_batches_by_file_index.find(file_index) == _batches_by_file_index.end()) << "Batch for file index " << file_index << " already exists";

    if (size() == 0)
    {
        _is_object_storage = batch.is_object_storage();
    }
    else if  (auto res = verify_batch(batch); res != common::ResponseCode::Success)
    {
        return res;
    }

    _batches_by_file_index.emplace(file_index, std::move(batch));

    return common::ResponseCode::Success;
}

bool Workload::is_object_storage() const
{
    return _is_object_storage;
}

void Workload::fail(common::ResponseCode code)
{
    for (auto & [file_index, batch] : _batches_by_file_index)
    {
        batch.handle_error(code);
    }
}

common::ResponseCode Workload::verify_batch(const Batch & batch)
{
    if (batch.is_object_storage() != is_object_storage())
    {
         LOG(ERROR) << "Workload contains paths of different storage backends";

        return common::ResponseCode::InvalidParameterError;
    }

    return common::ResponseCode::Success;
}

void Workload::execute(std::atomic<bool> & stopped)
{
    if (size() == 0)
    {
        return;
    }

    // Object-storage workloads are read asynchronously by the ObjectStorageWorker pool, not here.
    ASSERT(!is_object_storage()) << "object-storage workload must be executed by ObjectStorageWorker";

    for (auto & [file_index, batch] : _batches_by_file_index)
    {
        batch.execute(stopped);
        LOG(DEBUG) << "Finished batch " << batch;
    }
}

}; // namespace runai::llm::streamer::impl
