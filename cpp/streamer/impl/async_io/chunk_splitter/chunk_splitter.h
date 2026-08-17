#pragma once

#include <cstddef>
#include <ostream>
#include <vector>

#include "streamer/impl/task/task.h"

namespace runai::llm::streamer::impl
{

// One read request to an IoEngine, and the tasks it covers.
//
// A batch is read as a sequence of these: contiguous in the file and in the destination, since a
// batch covers one contiguous transfer.
struct Chunk
{
    size_t offset = 0;       // absolute file offset
    size_t bytesize = 0;
    char * buffer = nullptr;

    // Tasks [first_task, first_task + task_count) of the batch. When this chunk lands, every one of
    // them has all of its bytes, so each can be accounted at once.
    unsigned first_task = 0;
    unsigned task_count = 0;
};

std::ostream & operator<<(std::ostream &, const Chunk &);

// Group a batch's tasks into chunks.
//
// This does NOT re-apply the boundary rule. Batches::handle_request already cuts tasks so that none
// crosses a multiple of chunk_bytesize, so the chunks are simply the tasks grouped by which chunk
// they fall in. Deriving them instead of computing them again is what stops the two from drifting
// apart - and if they did drift, a chunk would cover part of a task and its completion would report
// bytes that had not arrived.
//
// Every task belongs to exactly one chunk, so the spans tile [0, tasks.size()). Zero-sized tasks -
// which a zero-sized range produces - carry no bytes and so belong to no chunk of their own; they are
// absorbed into the neighbouring chunk and accounted when it lands.
//
// A batch with no bytes at all yields NO chunks. Its tasks still owe one response each, so the caller
// must complete them itself rather than waiting for I/O that will never be issued.
//
// Takes std::vector<Task> rather than the Tasks alias, which lives in batch.h - the splitter has no
// business pulling in Batch, the responder and the object-storage wrapper just to name a vector.
std::vector<Chunk> split_into_chunks(const std::vector<Task> & tasks, size_t chunk_bytesize);

}; // namespace runai::llm::streamer::impl
