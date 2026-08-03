from typing import Dict, List, Iterator, Optional, Tuple
from runai_model_streamer.libstreamer.libstreamer import (
    SUCCESS_ERROR_CODE,
    runai_start,
    runai_end,
    runai_set_credentials,
    runai_request,
    runai_response,
    runai_response_str,
    runai_list_files,
)
from runai_model_streamer.file_streamer.requests_iterator import (
    FilesRequestsIteratorWithBuffer,
    FileChunks,
)

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
    is_s3_path,
    is_gs_path,
    is_azure_path,
    get_s3_credentials_module,
)

import torch

import logging

logger = logging.getLogger(__name__)

s3_credentials_module = get_s3_credentials_module()

class RunaiStreamerInvalidInputException(Exception):
    pass

def homogeneous_paths(paths: List[str]) -> bool:
    if not paths:
        return True  # Empty list is homogeneous by default

    def path_type_fn(path: str):
        if is_s3_path(path):
            return is_s3_path
        elif is_gs_path(path):
            return is_gs_path
        elif is_azure_path(path):
            return is_azure_path
        else:
            return None

    first_type = path_type_fn(paths[0])
    for path in paths[1:]:
        if path_type_fn(path) != first_type:
            return False
    return True

class FileStreamer:
    def __init__(self) -> None:
        # Initialized here (not only in __enter__) so methods such as list_files
        # can be used without entering the context manager
        self.streamer = None
        self.s3_session = None
        self.s3_credentials = None    # resolved credentials (from handle_object_store)
        self._credentialed_streamer = None   # the C++ handle we already applied credentials to
        self.submission_id = None     # id of the submission currently being drained (runai_request)

    def __enter__(self) -> "FileStreamer":
        self.streamer = runai_start()
        self.device_str = None
        self.s3_session = None
        self.s3_credentials = None
        self._credentialed_streamer = None
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        # Overall throughput is logged by SafetensorsStreamer (the user-facing session), not here: one session
        # spans multiple FileStreamer requests (already so under a memory limit, and more so with the ring
        # buffer's concurrent submissions), so it is not a property of any single request. __exit__ only does
        # the C++ handle cleanup.
        #
        # End AND clear the handle: list_files() now reuses self.streamer when set, so a call after the
        # context must not pass the freed C++ pointer to runai_list_files (a dangling handle -> crash/UAF).
        # Clearing makes post-context listing start a fresh temporary streamer (the owns_streamer path).
        if self.streamer:
            runai_end(self.streamer)
            self.streamer = None
            self._credentialed_streamer = None

    def handle_object_store(self, path: str, streamer, credentials: Optional[S3Credentials] = None) -> str:
        # Two independent concerns, deliberately gated separately so they can't desync:
        #   1. Resolve the credentials ONCE (self.s3_session is None). boto3 resolution is AWS-only, so it runs
        #      only for S3 paths (is_s3_path) - the backend type is only known from the path.
        #   2. Apply them ONCE PER C++ handle (streamer is not self._credentialed_streamer). Credentials are
        #      streamer-scoped state in the C++ layer, so a new handle (e.g. a temporary streamer created by an
        #      out-of-context list_files) needs its own set - the resolution flag must NOT suppress that.
        # Nothing is applied when there is nothing to apply (no credentials -> the C++ default/ambient provider
        # chain is used, and the streamer's set-once credentials stay free).
        if s3_credentials_module and is_s3_path(path):
            if self.s3_session is None:
                self.s3_session, self.s3_credentials = s3_credentials_module.get_credentials(credentials)
            if streamer is not None and self._has_credentials() and streamer is not self._credentialed_streamer:
                runai_set_credentials(streamer, self.s3_credentials)
                self._credentialed_streamer = streamer
        return path

    def _has_credentials(self) -> bool:
        # True when the resolved credentials carry at least one value (else there is nothing to set)
        c = self.s3_credentials
        return c is not None and any(
            v is not None for v in (c.access_key_id, c.secret_access_key, c.session_token, c.region_name, c.endpoint)
        )


    def list_files(
        self,
        prefix: str,
        is_recursive: bool = True,
        allow_patterns: Optional[List[str]] = None,
        ignore_patterns: Optional[List[str]] = None,
    ) -> List[Tuple[str, int]]:
        # Listing reuses the streamer (its object-storage clients / backend handle / credentials).
        # When called outside the context manager (self.streamer is None), start a temporary
        # streamer just for this call and end it afterwards.
        owns_streamer = self.streamer is None
        streamer = runai_start() if owns_streamer else self.streamer

        results: List[Tuple[str, int]] = []
        try:
            # first object-storage path (the prefix): under RUNAI_STREAMER_NO_BOTO3_SESSION=0 this resolves S3
            # credentials from the environment via boto3 and applies them once; else the C++ default chain is
            # used. Inside the try so a temp streamer is still ended if credential resolution/apply throws.
            self.handle_object_store(prefix, streamer)
            runai_list_files(
                streamer,
                prefix,
                lambda path, size: results.append((path, size)),
                is_recursive=is_recursive,
                allow_patterns=allow_patterns,
                ignore_patterns=ignore_patterns,
            )
        finally:
            if owns_streamer:
                runai_end(streamer)
        return results

    def stream_files(
            self,
            file_stream_requests: List[FileChunks],
            credentials: Optional[S3Credentials] = None,
            device: Optional[str] = "cpu",
) -> None:
        if not homogeneous_paths([file_stream_request.path for file_stream_request in file_stream_requests]):
            raise RunaiStreamerInvalidInputException("Cannot stream files from multiple source types in parallel") 

        self.device_str = device

        for file_stream_request in file_stream_requests:
            # first object-storage path resolves + applies the credentials to the streamer, once
            file_stream_request.path = self.handle_object_store(file_stream_request.path, self.streamer, credentials)

        self.requests_iterator: FilesRequestsIteratorWithBuffer = FilesRequestsIteratorWithBuffer.with_memory_mode(file_stream_requests)

        self.active_request = self.requests_iterator.next_request()
        if self.active_request is None:
            return

        self.submit_active_request()

    def submit_active_request(self) -> None:
        # The three range arrays are flat and grouped by file, in the order of paths - which is the order
        # the request's files are in, and the order range_dsts was built in, so it passes through as is.
        #
        # range_dsts are RAW ADDRESSES into the requests iterator's buffer: they keep nothing alive. The
        # buffer must outlive the submission, until its last response has been consumed. That holds here
        # because self.requests_iterator owns the buffer and is only replaced by the next stream_files,
        # which cannot start before get_chunks has drained this one.
        request = self.active_request
        self.submission_id = runai_request(
            self.streamer,
            [file_request.path for file_request in request.files],
            [len(file_request.sizes) for file_request in request.files],
            [offset for file_request in request.files for offset in file_request.offsets],
            [size for file_request in request.files for size in file_request.sizes],
            request.range_dsts,
        )

    def get_chunks(self) -> Iterator:
        if not self.streamer:
            raise ValueError("Streamer not initialized")
        
        if self.active_request is None:
            return 
        
        
        while True:
            yield from self.request_ready_chunks()

            self.active_request = self.requests_iterator.next_request()
            if self.active_request is None:
                break

            self.submit_active_request()

    # This function iterates over indexes of ready chunks.
    # The indexes are relative to the last request that sent
    # And need to be translated to global index in the chunks list
    def request_ready_chunks(self) -> Iterator:
        # Only one submission is in flight at a time (get_chunks fully drains the active request before
        # submitting the next), so every response here belongs to self.submission_id and the count loop is
        # exact. runai_response blocks indefinitely (timeout 0) and returns None only on teardown.
        num_ranges = self.active_request.num_ranges
        consumed = 0
        try:
            for i in range(num_ranges):
                response = runai_response(self.streamer)
                if response is None:
                    return
                consumed += 1
                ret, submission_id, file_relative_index, range_relative_index, _submission_done = response
                # Single-submission invariant: FileStreamer drains one submission fully before starting the next,
                # so every response must belong to self.submission_id. This catches a stale response from a prior
                # (e.g. failed) submission being misattributed to this one and returning wrong data. Use if/raise
                # (not assert) so this data-integrity check is not stripped under `python -O`.
                # REMOVE THIS for the ring buffer: FileStreamer will then manage CONCURRENT submissions and
                # request_ready_chunks must demux responses by submission_id instead of enforcing a single one.
                if submission_id != self.submission_id:
                    raise ValueError(
                        f"response for submission {submission_id} but expected {self.submission_id}"
                    )
                # single-submission streaming: any per-sub-range error fails the whole stream (fail-fast)
                if ret != SUCCESS_ERROR_CODE:
                    raise ValueError(
                        f"Could not receive response from libstreamer due to: {runai_response_str(ret)}"
                    )

                file_id, range_index, range_buffer = self.requests_iterator.get_global_file_and_range(
                    self.active_request, file_relative_index, range_relative_index
                )
                # create one dimensional tensor from the range buffer
                # we return a tensor of shape (1, range_buffer.size)
                # the data type of the original range_buffer, as created by the requests_iterator, is preserved (uint8)
                tensor = torch.from_numpy(range_buffer).view(1, -1)

                # currently file streamer is always reading a cpu buffer
                # so we don't need to move the tensor to the device
                # for future GDS/CUDA support we will need to move the tensor to the device (cpu or different device)
                if self.device_str == "cpu":
                    yield file_id, range_index, tensor
                else:
                    device_tensor = tensor.to(self.device_str)
                    yield file_id, range_index, device_tensor
        finally:
            # The submission must be drained however this generator ends, not only when it runs to
            # completion. range_dsts are raw addresses into the requests iterator's buffer, so a range still
            # in flight writes into memory the next stream_files has already freed and handed to the next
            # submission - and its late response is then delivered to that submission's drain loop, which
            # rejects it and abandons ITS responses in turn, so the streamer never recovers.
            # A finally covers both exits: the fail-fast raises above, and a caller abandoning the generator
            # (break, or raising inside its for loop - which safetensors_pytorch does), since closing a
            # generator resumes it here.
            self.drain_active_submission(num_ranges - consumed)

    def drain_active_submission(self, remaining: int) -> None:
        # self.streamer is already cleared if the generator is collected after __exit__; runai_end has joined
        # the workers by then, so there is nothing left to wait for and the handle must not be touched.
        if not self.streamer:
            return
        for _ in range(remaining):
            if runai_response(self.streamer) is None:
                return   # teardown: no further responses are coming

