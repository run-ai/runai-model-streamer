#include "streamer/impl/assigner/assigner.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/exception/exception.h"
#include "common/storage_uri/storage_uri.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

Assigner::Assigner(const std::vector<FileRanges> & request, std::shared_ptr<const Config> config) :
_config(config),
_is_object_storage(check_object_storage(request)),
_num_workers(_is_object_storage ? _config->s3_concurrency : _config->concurrency),
_num_workloads(0)
{
    LOG(DEBUG) << "Assigning " << request.size() << " files to " << _num_workers << " workers";

    if (request.empty())
    {
        LOG(WARNING) << "Assigner: No files provided.";
        return; // Nothing to assign
    }

    const size_t total_bytes_to_read = coalesce(request);

    if (_transfers.empty())
    {
        LOG(WARNING) << "Assigner: no ranges to read.";
        return;
    }

    if (total_bytes_to_read == 0)
    {
        LOG(WARNING) << "Total bytes to read is zero.";
    }

    assign(total_bytes_to_read);
}

// Merge each file's ranges into ContiguousTransfers. Single pass over the ranges, which also
// accumulates the byte total and detects unordered input - none of these needs its own scan.
size_t Assigner::coalesce(const std::vector<FileRanges> & request)
{
    size_t total_bytes = 0;

    // The best case is one transfer per file. Growing past that is cheap: ContiguousTransfer's move is
    // noexcept (two vector moves plus PODs), so reallocation moves the range vectors rather than
    // copying them.
    _transfers.reserve(request.size());

    for (unsigned file_index = 0; file_index < request.size(); ++file_index)
    {
        const auto & ranges = request[file_index].ranges;

        // Index of the transfer currently open for extension; _transfers.size() means "none open".
        // Reset per file, which is also what stops a transfer from ever spanning two files.
        size_t open = _transfers.size();
        bool ordered = true;

        for (unsigned i = 0; i < ranges.size(); ++i)
        {
            const auto & range = ranges[i];
            char * const destination = static_cast<char *>(range.dst);

            if (total_bytes > std::numeric_limits<size_t>::max() - range.size)
            {
                LOG(ERROR) << "Total byte size calculation overflow";
                throw common::Exception(common::ResponseCode::InvalidParameterError);
            }
            total_bytes += range.size;

            if (open < _transfers.size())
            {
                auto & current = _transfers[open];

                // Extend only when adjacent in BOTH the file and the destination: a single read fills a
                // single contiguous buffer, so either gap alone breaks the merge.
                if (current.offset + current.size == range.offset &&
                    current.destination + current.size == destination)
                {
                    current.size += range.size;
                    current.range_sizes.push_back(range.size);
                    continue;
                }

                // Noticed here for free rather than in a separate ordering pass
                ordered = ordered && range.offset >= current.offset;
            }

            // NOTE: never hold a reference into _transfers across this push - it may reallocate.
            // The open transfer is tracked by index for exactly that reason.
            open = _transfers.size();
            _transfers.emplace_back();

            auto & opened = _transfers.back();
            opened.file_index = file_index;
            opened.offset = range.offset;
            opened.size = range.size;
            opened.destination = destination;
            opened.first_range_index = i;
            opened.range_sizes.push_back(range.size);
        }

        if (!ordered)
        {
            LOG(DEBUG) << "Ranges of " << request[file_index].path << " are not ordered by ascending"
                       << " offset; coalescing is limited to ranges adjacent in the order given";
        }
    }

    LOG(DEBUG) << "Coalesced the request into " << _transfers.size() << " contiguous transfers";

    return total_bytes;
}

// Divide the transfers between the workers by total bytes. A transfer larger than a worker's remaining
// target is split, with offset and destination advancing together so every slice stays contiguous.
void Assigner::assign(size_t total_bytes_to_read)
{
    size_t base_bytes_remainder = 0;
    const size_t base_bytes_per_worker = bytes_per_worker(total_bytes_to_read, base_bytes_remainder);

    _worker_assignments.resize(_num_workers);

    size_t index = 0;     // the transfer being assigned
    size_t consumed = 0;  // how many of its bytes are already assigned

    for (unsigned workload_idx = 0; workload_idx < _num_workers && index < _transfers.size(); ++workload_idx)
    {
        const size_t target = (workload_idx == 0 ? base_bytes_per_worker + base_bytes_remainder : base_bytes_per_worker);
        size_t assigned = 0;

        LOG(DEBUG) << "Assigning work to worker " << workload_idx << ", target bytes: " << target;

        while (index < _transfers.size())
        {
            auto & transfer = _transfers[index];

            // A zero sized transfer is still given a task: its ranges are zero sized, and every range
            // owes exactly one response whatever its size.
            if (transfer.size > 0 && assigned >= target)
            {
                break;
            }

            const size_t remaining = transfer.size - consumed;
            const size_t to_assign = std::min(remaining, target - assigned);

            transfer.tasks.emplace_back(workload_idx,
                                        transfer.file_index,
                                        transfer.offset + consumed,
                                        to_assign,
                                        transfer.destination + consumed);

            LOG(SPAM) << "  Worker " << workload_idx << ": file " << transfer.file_index
                      << " offset " << transfer.offset + consumed << " size " << to_assign;

            assigned += to_assign;
            consumed += to_assign;
            _worker_assignments[workload_idx].total_bytes += to_assign;

            if (consumed == transfer.size)
            {
                ++index;
                consumed = 0;
            }
        }

        LOG(DEBUG) << "Finished assignment for worker " << workload_idx << ", total bytes assigned: " << assigned;
    }

    // Verification - every byte assigned exactly once, and every transfer fully covered by its tasks
    size_t assigned_total = 0;
    for (const auto & assignment : _worker_assignments)
    {
        assigned_total += assignment.total_bytes;
    }

    ASSERT(assigned_total == total_bytes_to_read) << "Verification failed: Total bytes assigned (" << assigned_total
        << ") does not match total bytes requested (" << total_bytes_to_read << ")";

    for (size_t i = 0; i < _transfers.size(); ++i)
    {
        size_t transfer_total = 0;
        for (const auto & task : _transfers[i].tasks)
        {
            transfer_total += task.size;
        }
        ASSERT(transfer_total == _transfers[i].size) << "Transfer " << i << " of file " << _transfers[i].file_index
            << " assigned " << transfer_total << " bytes but covers " << _transfers[i].size;
    }

    LOG(DEBUG) << "Workload assignment verification successful. Total bytes assigned: " << assigned_total;
}

bool Assigner::check_object_storage(const std::vector<FileRanges> & request) const
{
    if (request.empty())
    {
        return false;
    }

    try
    {
        common::s3::StorageUri uri(request[0].path);
        return true;
    }
    catch(const std::exception& e)
    {
    }

    return false;
}

const std::vector<ContiguousTransfer> & Assigner::transfers() const
{
    return _transfers;
}

unsigned Assigner::get_num_workers() const
{
    return _num_workers;
}

size_t Assigner::bytes_per_worker(size_t total_bytes_to_read, size_t & remainder_bytesize)
{
    size_t block_bytesize = _is_object_storage ? _config->s3_block_bytesize : _config->fs_block_bytesize;
    size_t num_blocks = total_bytes_to_read / block_bytesize;

    // zero size files are assigned to one worker
    // this is because zero size files may contain zero size tensors, which we still need to send response for
    _num_workloads = std::max(std::min(num_blocks, static_cast<size_t>(_num_workers)), 1UL);
    size_t base_bytes_per_worker = num_blocks / _num_workloads * block_bytesize;

    remainder_bytesize = total_bytes_to_read - _num_workloads * base_bytes_per_worker;

    LOG(DEBUG) << "Total bytes: " << total_bytes_to_read
                << ", Block bytesize: " << block_bytesize
                << ", Num blocks: " << num_blocks
                << ", Num workers: " << _num_workloads << " out of " << _num_workers
                << ", Base bytes/worker: " << base_bytes_per_worker
                << ", Remainder bytesize: " << remainder_bytesize;

    return base_bytes_per_worker;
}

unsigned Assigner::num_workloads() const
{
    return _num_workloads;
}

} // namespace runai::llm::streamer::impl
