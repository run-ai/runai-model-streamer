
#pragma once

#include <memory>
#include <string>
#include <vector>
#include <ostream>

#include "common/responder/responder.h"
#include "common/storage_uri/storage_uri.h"
#include "common/s3_wrapper/s3_wrapper.h"
#include "common/response_code/response_code.h"
#include "common/shared_queue/shared_queue.h"
//#include "common/range/range.h"

#include "streamer/impl/config/config.h"
#include "streamer/impl/cuda/cuda_loader.h"
#include "streamer/impl/task/task.h"
#include "streamer/impl/reader/reader.h"

namespace runai::llm::streamer::impl
{

// Batch represents a single range in the file, and is used by a single worker thread
// The range may contain several sub ranges (requests) which can be whole or partial
// Each sub range (or part of it) is represented by a task
//
//         [                Batch range                     ]
//   [ ... request 1  ][request 2][   request 3   ][ request 4 ....]
//         [task 1    ][  task 2 ][    task 3     ][ task 4 ]

using Tasks = std::vector<Task>;


struct Batch
{
  struct Range : common::Range
  {
      Range() = default;
      Range(size_t start_offset, size_t end_offset);
      Range(const Tasks & tasks);

      size_t end;

   private:
      static size_t calculate_start(const Tasks & tasks);
      static size_t calculate_end(const Tasks & tasks);
  };

  Batch() = default;
  Batch(Batch &&) = default;
  Batch & operator=(Batch &&) = default;

  Batch(unsigned worker_index,
        unsigned file_index,
        const std::string & path,
        const common::s3::S3ClientWrapper::Params & params,
        const Tasks && tasks,
        std::shared_ptr<common::Responder> responder,
        std::shared_ptr<const Config> config,
        bool cuda = false);

  // total number of requested bytes
  size_t total_bytes() const;

  // end offset of the batch
  size_t end_offset() const;

  // read the batch synchronously
  void execute(std::atomic<bool> & stopped);

  // request the batch asynchronously
  void request(std::shared_ptr<Reader> reader, std::atomic<bool> & stopped);

  // handle response from the reader
  void handle_response(const common::backend_api::Response & response, const Task * task_ptr);

  // handle error
  void handle_error(common::ResponseCode response_code);

  // notify tasks until file offset
  void finished_until(size_t file_offset, common::ResponseCode ret = common::ResponseCode::Success);
  unsigned finished_until() const;

  bool is_object_storage() const;

  unsigned worker_index;

  // source file
  unsigned file_index;
  std::string path;

  // s3 parameters
  const common::s3::S3ClientWrapper::Params object_storage_params;

  const Tasks tasks;

  // range in file
  Range range;

  std::shared_ptr<common::Responder> responder;

  std::shared_ptr<const Config> config;

 private:
  void read(const Config & config, std::atomic<bool> & stopped);
  void read_cuda(const Config & config, std::atomic<bool> & stopped, const cuda::CudaDriver & drv);

  void async_wait(Reader * reader, std::atomic<bool> & stopped);

  void request_async_read(Reader * reader, std::atomic<bool> & stopped);

  // handle response from a single task
  void handle_task_response(const common::ResponseCode response_code, const Task * task_ptr);

 private:
  // index of first unfinished task
  unsigned _unfinished = 0;

  bool _cuda = false;

  std::unique_ptr<Reader> _reader;
};

std::ostream & operator<<(std::ostream & os, const Batch::Range & r);
std::ostream & operator<<(std::ostream & os, const Batch & r);

}; // namespace runai::llm::streamer::impl
