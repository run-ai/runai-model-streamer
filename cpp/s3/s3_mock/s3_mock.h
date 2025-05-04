#pragma once

#include "common/range/range.h"
#include "common/path/path.h"
#include "common/s3_credentials/s3_credentials.h"
#include "common/response_code/response_code.h"

namespace runai::llm::streamer::common::s3
{

extern "C" common::ResponseCode runai_create_s3_client(const common::s3::Path & path,  const common::s3::Credentials_C & credentials, void ** client);
extern "C" void runai_remove_s3_client(void * client);
extern "C" common::ResponseCode  runai_async_read_s3_client(void * client, unsigned num_ranges, common::Range * ranges, size_t chunk_bytesize, char * buffer);
extern "C" common::ResponseCode  runai_async_response_s3_client(void * client, unsigned * index /* output parameter */);
extern "C" void runai_stop_s3_clients();
extern "C" void runai_release_s3_clients();

extern "C" void runai_mock_s3_set_response_time_ms(unsigned milliseconds);
extern "C" int runai_mock_s3_clients();
extern "C" void runai_mock_s3_cleanup();

}; //namespace runai::llm::streamer::common::s3
