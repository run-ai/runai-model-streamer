#include "streamer/impl/assigner/assigner.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>
#include <numeric> // For std::accumulate
#include <cmath>   // For std::ceil
#include <stdexcept> // For exceptions
#include <limits>  // For numeric_limits
#include <map>
#include <utility>

#include "common/exception/exception.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

MultiFileWorkloadAssigner::MultiFileWorkloadAssigner(
        const std::vector<std::string> & paths,
        const std::vector<size_t> & file_offsets,
        const std::vector<size_t>&  bytesizes,
        const std::vector<void*> & dsts,
        std::shared_ptr<const Config> config) :
_config(config),
_num_workers(_config->concurrency)
{
    const size_t num_files = paths.size();
    if (num_files == 0)
    {
        LOG(WARNING) << "MultiFileWorkloadAssigner: No files provided.";
        return; // Nothing to assign
    }

    if (!(num_files == file_offsets.size() && num_files == bytesizes.size() && num_files == dsts.size()))
    {
        LOG(ERROR) <<  "Input vector sizes mismatch";
        throw common::ResponseCode::InvalidParameterError;
    }

    // Calculate Total Workload
    size_t total_bytes_to_read = 0;
    for (size_t size : bytesizes) {
        // Check for potential overflow if sizes are huge
        if (total_bytes_to_read > std::numeric_limits<size_t>::max() - size)
        {
            LOG(ERROR) << "Total byte size calculation overflow";
                throw common::ResponseCode::InvalidParameterError;
        }
        total_bytes_to_read += size;
    }

    if (total_bytes_to_read == 0)
    {
        LOG(WARNING) << "Total bytes to read is zero.";
        return; // Nothing to assign
    }

    // Determine Workload per Worker
    const size_t base_bytes_per_worker = total_bytes_to_read / _num_workers;
    const size_t remainder_bytes = total_bytes_to_read % _num_workers;
    LOG(DEBUG) << "Total bytes: " << total_bytes_to_read
                << ", Workers: " << _num_workers
                << ", Base bytes/worker: " << base_bytes_per_worker
                << ", Remainder: " << remainder_bytes;


    // --- Assign Workload Iteratively ---
    _worker_assignments.resize(_num_workers);

    // ASSUMES dsts[0] is base of one large buffer
    char * global_dst_buffer = static_cast<char*>(dsts[0]);

    size_t current_global_dst_offset = 0; // Tracks position in the conceptual global buffer
    size_t current_file_index = 0;
    size_t current_offset_within_file = file_offsets[0]; // Start at the beginning of the first file's range

    for (unsigned worker_idx = 0; worker_idx < _num_workers; ++worker_idx)
    {
        // Assign slightly more work to the first 'remainder_bytes' workers
        size_t target_bytes_for_this_worker = base_bytes_per_worker + (worker_idx < remainder_bytes ? 1 : 0);
        size_t bytes_assigned_to_this_worker = 0;

        LOG(DEBUG) << "Assigning work to worker " << worker_idx << ", target bytes: " << target_bytes_for_this_worker;

        while (bytes_assigned_to_this_worker < target_bytes_for_this_worker && current_file_index < num_files)
        {
            const std::string& file_path = paths[current_file_index];
            const size_t file_start_offset = file_offsets[current_file_index];
            const size_t file_total_requested_size = bytesizes[current_file_index];
            char* current_global_dst_ptr = global_dst_buffer + current_global_dst_offset;

            // Sanity check: current offset should be within the requested range for the file
            if (current_offset_within_file < file_start_offset || current_offset_within_file >= file_start_offset + file_total_requested_size) {
                // This case should ideally not happen if logic is correct, but indicates a problem if it does.
                // It might happen if a file has bytesize 0.
                    if (file_total_requested_size == 0) {
                        // Skip zero-sized file request
                        current_file_index++;
                        if (current_file_index < num_files) {
                            current_offset_within_file = file_offsets[current_file_index];
                        }
                        continue; // Move to next file
                    } else {
                    LOG(ERROR) << "Logic error: current_offset_within_file (" << current_offset_within_file
                                << ") is outside the requested range [" << file_start_offset << ", "
                                << file_start_offset + file_total_requested_size << ") for file " << current_file_index;
                    throw std::logic_error("Internal error during workload assignment: Invalid file offset.");
                    }
            }

            const size_t bytes_remaining_in_current_file = (file_start_offset + file_total_requested_size) - current_offset_within_file;
            const size_t bytes_still_needed_by_worker = target_bytes_for_this_worker - bytes_assigned_to_this_worker;

            const size_t bytes_to_assign_now = std::min(bytes_remaining_in_current_file, bytes_still_needed_by_worker);

            if (bytes_to_assign_now > 0)
            {
                 // Create Task

                 LOG(DEBUG) << "Assigned read file task to worker " << worker_idx << " file index: " << current_file_index << " file offset: " << current_offset_within_file << " bytesize: " << bytes_to_assign_now;
                _worker_assignments[worker_idx].tasks.emplace_back(
                    worker_idx,
                    current_file_index,
                    file_path,
                    current_offset_within_file,
                    bytes_to_assign_now,
                    current_global_dst_ptr);

                // --- Update State ---
                bytes_assigned_to_this_worker += bytes_to_assign_now;
                _worker_assignments[worker_idx].total_bytes += bytes_to_assign_now;
                current_offset_within_file += bytes_to_assign_now;
                current_global_dst_offset += bytes_to_assign_now; // Advance global destination offset

                LOG(SPAM) << "  Worker " << worker_idx << ": Assigned task - File " << current_file_index
                            << " ('" << file_path << "'), Offset " << current_offset_within_file - bytes_to_assign_now
                            << ", Size " << bytes_to_assign_now;
            }

            // Check if we finished the current file
            if (current_offset_within_file == file_start_offset + file_total_requested_size)
            {
                current_file_index++;
                if (current_file_index < num_files)
                {
                    // Reset offset for the new file to its starting requested offset
                    current_offset_within_file = file_offsets[current_file_index];
                }
            }
        } // End while loop for assigning work to current worker

        LOG(DEBUG) << "Finished assignment for worker " << worker_idx << ", total bytes assigned: " << bytes_assigned_to_this_worker;
    } // End for loop iterating through workers

    // Verification
    size_t assigned_total = 0;
    for (const auto & assignment : _worker_assignments)
    {
        assigned_total += assignment.total_bytes;
    }

    ASSERT(assigned_total == total_bytes_to_read) << "Verification failed: Total bytes assigned (" << assigned_total
        << ") does not match total bytes requested (" << total_bytes_to_read << ")";

    LOG(DEBUG) << "Workload assignment verification successful. Total bytes assigned: " << assigned_total;

    unsigned i = 0;
    for (auto & worker : _worker_assignments)
    {
        for (auto & read_task : worker.tasks)
        {
            const auto file_index = read_task.original_file_index;
            _assignments[file_index].push_back(std::move(read_task));
        }
        ++i;
    }

    // Verification
    for (unsigned i = 0; i < paths.size(); ++i)
    {
        size_t file_assigned_total = 0;
        for (auto & task : _assignments[i])
        {
            file_assigned_total += task.size;
        }
        ASSERT(file_assigned_total == bytesizes[i]) << "File index " << i << " total assigned " << file_assigned_total << " not equal to file size " << bytesizes[i];
    }
}

// Access the assignments of a given file by the original file index
const std::vector<FileReadTask> & MultiFileWorkloadAssigner::file_assignments(unsigned file_index)
{
    return _assignments[file_index];
}

unsigned MultiFileWorkloadAssigner::get_num_workers() const
{
    return _num_workers;
}

} // namespace runai::llm::streamer::impl
