/*
 * SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <streamer/streamer.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

/* Read a byte range from a local file or an object-storage URI to stdout.
 * Usage: hello_streamer PATH BYTE_COUNT
 * Configure object-storage credentials through the backend's environment variables.
 */
int main(int argc, char ** argv)
{
    char * end = NULL;
    if (argc != 3 || argv[2][0] < '0' || argv[2][0] > '9')
    {
        fprintf(stderr, "Usage: %s PATH BYTE_COUNT\n", argv[0]);
        return 1;
    }
    errno = 0;
    unsigned long long count = strtoull(argv[2], &end, 10);
    if (errno || *end || count == 0 || count > SIZE_MAX)
    {
        fprintf(stderr, "BYTE_COUNT must be a positive size_t value\n");
        return 1;
    }
    size_t size = (size_t)count;
    void * buffer = malloc(size);
    if (buffer == NULL)
    {
        fprintf(stderr, "Cannot allocate destination\n");
        return 1;
    }
    void * streamer = NULL;
    int rc = runai_file_streamer_start(&streamer);
    if (rc == RUNAI_FILE_STREAMER_RESPONSE_SUCCESS)
    {
        /* Ordinary malloc buffers need no direct-I/O alignment with this strategy. */
        rc = runai_file_streamer_set_fs_strategy(streamer, "sync_buffered");
    }
    RunaiFileStreamerSubmissionId submitted = 0;
    const char * path = argv[1];
    unsigned ranges = 1;
    size_t offset = 0;
    RunaiFileStreamerDevice device = {RUNAI_FILE_STREAMER_DEVICE_CPU, 0};
    if (rc == RUNAI_FILE_STREAMER_RESPONSE_SUCCESS)
    {
        rc = runai_file_streamer_request(streamer, &submitted, 1, &path, &ranges,
                                         &offset, &size, &buffer, device);
    }
    if (rc == RUNAI_FILE_STREAMER_RESPONSE_SUCCESS)
    {
        RunaiFileStreamerSubmissionId answered = 0;
        unsigned file_index = 0;
        unsigned range_index = 0;
        int done = 0;
        rc = runai_file_streamer_response(streamer, &answered, &file_index, &range_index, &done, 30000);
        if (rc == RUNAI_FILE_STREAMER_RESPONSE_SUCCESS &&
            (answered != submitted || file_index != 0 || range_index != 0 || !done))
        {
            fprintf(stderr, "Unexpected response identity\n");
            rc = RUNAI_FILE_STREAMER_RESPONSE_UNKNOWN_ERROR;
        }
    }
    /* Teardown drains outstanding reads, including on timeout, before freeing the destination. */
    if (streamer != NULL)
        runai_file_streamer_end(streamer);
    int result = 1;
    if (rc == RUNAI_FILE_STREAMER_RESPONSE_SUCCESS)
        result = fwrite(buffer, 1, size, stdout) == size && fflush(stdout) == 0 ? 0 : 1;
    else
        fprintf(stderr, "Streamer: %s (%d)\n", runai_file_streamer_response_str(rc), rc);
    free(buffer);
    return result;
}
