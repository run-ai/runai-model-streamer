#pragma once

#include "common/backend_api/object_storage/object_storage.h"
#include "common/path/path.h"
#include "common/s3_credentials/s3_credentials.h"
#include "common/response/response.h"
#include "common/range/range.h"

namespace runai::llm::streamer::impl::gcs
{

// create client
extern "C" common::ResponseCode runai_create_s3_client(const common::s3::Path * path,
                                                       const common::s3::Credentials_C * credentials,
                                                       void ** client);
// destroy client
extern "C" void runai_remove_s3_client(void * client);
// asynchronous read
extern "C" common::ResponseCode  runai_async_read_s3_client(void * client,
                                                            common::backend_api::ObjectRequestId_t request_id,
                                                            const common::s3::Path * path,
                                                            common::Range * range,
                                                            size_t chunk_bytesize,
                                                            char * buffer);
// wait for asynchronous read response
extern "C" common::ResponseCode  runai_async_response_s3_client(void * client,
                                                                common::backend_api::ObjectCompletionEvent_t* event_buffer,
                                                                unsigned int max_events_to_retrieve,
                                                                unsigned int* out_num_events_retrieved);
// stop clients
// Stops the responder of each client, in order to notify callers which sent a request and are waiting for a response
extern "C" void runai_stop_s3_clients();
// release clients
extern "C" void runai_release_s3_clients();

}; //namespace runai::llm::streamer::impl::gcs
