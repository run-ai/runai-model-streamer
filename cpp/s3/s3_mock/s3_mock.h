#pragma once

#include "common/range/range.h"
#include "common/s3_credentials/s3_credentials.h"
#include "common/response_code/response_code.h"
#include "common/backend_api/response/response.h"
#include "common/backend_api/object_storage/object_storage.h"

namespace runai::llm::streamer::common::s3
{

// --- Backend API ---

extern "C" common::backend_api::ResponseCode_t obj_open_backend(common::backend_api::ObjectBackendHandle_t* out_backend_handle);
extern "C" common::backend_api::ResponseCode_t obj_close_backend(common::backend_api::ObjectBackendHandle_t backend_handle);
extern "C" common::backend_api::ObjectShutdownPolicy_t obj_get_backend_shutdown_policy();
// --- Client API ---

extern "C" common::backend_api::ResponseCode_t obj_create_client(
    common::backend_api::ObjectBackendHandle_t backend_handle,
    const common::backend_api::ObjectClientConfig_t* client_initial_config,
    common::backend_api::ObjectClientHandle_t* out_client_handle
);

extern "C" common::backend_api::ResponseCode_t obj_remove_client(
    common::backend_api::ObjectClientHandle_t client_handle
);

extern "C" common::backend_api::ResponseCode_t obj_request_read(
    common::backend_api::ObjectClientHandle_t client_handle,
    const char* path,
    common::backend_api::ObjectRange_t range,
    char* destination_buffer,
    common::backend_api::ObjectRequestId_t request_id
);

extern "C" common::backend_api::ResponseCode_t obj_wait_for_completions(
    common::backend_api::ObjectClientHandle_t client_handle,
    common::backend_api::ObjectCompletionEvent_t* event_buffer,
    unsigned int max_events_to_retrieve,
    unsigned int* out_num_events_retrieved,
    common::backend_api::ObjectWaitMode_t wait_mode
);

extern "C" common::backend_api::ResponseCode_t obj_cancel_all_reads();
extern "C" common::backend_api::ResponseCode_t obj_remove_all_clients();

extern "C" common::backend_api::ResponseCode_t obj_list_files(
    common::backend_api::ObjectClientHandle_t client_handle,
    const char* prefix,
    int is_recursive,
    common::backend_api::ObjectFileEntry_t** out_entries,
    unsigned* out_num_entries
);

extern "C" void obj_free_file_list(common::backend_api::ObjectFileEntry_t* entries, unsigned num_entries);

// Test hooks for obj_list_files:
//   set the files that the next obj_list_files call returns
extern "C" void runai_mock_s3_set_files(const char** paths, const size_t* sizes, unsigned count);
//   force obj_list_files to return the given response code (Success restores normal behavior)
extern "C" void runai_mock_s3_set_list_files_response(common::backend_api::ResponseCode_t response);
//   the is_recursive argument observed by the last obj_list_files call (-1 if never called)
extern "C" int runai_mock_s3_last_list_files_is_recursive();

extern "C" void runai_mock_s3_set_response_time_ms(unsigned milliseconds);
extern "C" void runai_s3_mock_set_backend_shutdown_policy(common::backend_api::ObjectShutdownPolicy_t policy);
extern "C" int runai_mock_s3_clients();
extern "C" void runai_mock_s3_cleanup();
extern "C" bool runai_mock_s3_is_shutdown();

}; //namespace runai::llm::streamer::common::s3
