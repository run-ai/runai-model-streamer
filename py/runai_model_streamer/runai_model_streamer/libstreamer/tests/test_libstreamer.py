import unittest
import tempfile
import shutil
import os
import mmap
import ctypes
from runai_model_streamer.libstreamer.libstreamer import (
    SUCCESS_ERROR_CODE,
    runai_start,
    runai_set_credentials,
    runai_request,
    runai_response,
)
from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
)


def buffer_address(buffer) -> int:
    # range_dsts are absolute addresses, so the destination buffer's base has to be taken explicitly -
    # the binding no longer accepts a Python buffer object and derives offsets from it
    return ctypes.addressof(ctypes.c_char.from_buffer(buffer))


class TestBindings(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def test_runai_library(self):
        # Multi-request API: submit returns a submission id, and each response reports its submission id,
        # file/range and whether the submission is complete. Also exercises runai_set_credentials (streamer-
        # scoped, unused for this filesystem read) on the second pass.
        file_path_1 = os.path.join(self.temp_dir, "test_file_1.txt")
        with open(file_path_1, "w") as file:
            file.write("XTest Text1TestText2Test-Text3\n")

        file_path_2 = os.path.join(self.temp_dir, "test_file_2.txt")
        with open(file_path_2, "w") as file:
            file.write("YTest Text44TestText")

        size = 60
        for use_credentials in (False, True):
            self._run_read(file_path_1, file_path_2, size, use_credentials)

    def _run_read(self, file_path_1, file_path_2, size, use_credentials):
        buffer = mmap.mmap(-1, size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
        buffer_ptr = id(buffer)

        streamer = runai_start()
        self.assertNotEqual(streamer, 0)

        if use_credentials:
            # credentials are streamer-scoped now: set them once on the streamer (unused for this
            # filesystem read, but exercises the set-credentials path)
            runai_set_credentials(streamer, S3Credentials(
                access_key_id="your_access_key",
                secret_access_key="your_secret_key",
                session_token="your_session_token",
                region_name="us-west-2",
                endpoint="optional_endpoint"))

        # Two files, two ranges each. Every range carries its own source offset and its own destination
        # address, so the layout below is stated outright rather than derived from a base offset plus
        # consecutive sizes: file 1 reads 10 bytes at 1 and 9 at 11, file 2 reads 11 at 1 and 8 at 12.
        base = buffer_address(buffer)
        submission_id = runai_request(
            streamer,
            [file_path_1, file_path_2],
            [2, 2],                                 # num_ranges per file
            [1, 11, 1, 12],                         # range_offsets, flat and grouped by file
            [10, 9, 11, 8],                         # range_sizes
            [base, base + 10, base + 19, base + 30] # range_dsts, absolute addresses
        )

        # collect all four sub-range responses (order across files is not guaranteed)
        expected = {(0, 0), (0, 1), (1, 0), (1, 1)}
        done_count = 0
        for _ in range(len(expected)):
            result = runai_response(streamer)
            self.assertIsNotNone(result)
            ret, sub_id, file_index, range_index, submission_done = result
            self.assertEqual(ret, SUCCESS_ERROR_CODE)
            self.assertEqual(sub_id, submission_id)
            self.assertIn((file_index, range_index), expected)
            expected.discard((file_index, range_index))
            if submission_done:
                done_count += 1
        self.assertEqual(expected, set())
        # submission_done is set exactly once - on the submission's last response
        self.assertEqual(done_count, 1)

        # verify the data landed in the shared buffer, copyless
        self.assertEqual(bytes(buffer[0:10]), b"Test Text1")
        self.assertEqual(bytes(buffer[10:19]), b"TestText2")
        self.assertEqual(bytes(buffer[19:30]), b"Test Text44")
        self.assertEqual(bytes(buffer[30:38]), b"TestText")
        self.assertEqual(id(buffer), buffer_ptr)

    def test_zero_sized_range_is_answered(self):
        # A zero-sized range is a range: it gets its own response, like any other. The caller's indexing
        # counts on exactly one response per range, so swallowing it would shift every later index.
        file_path = os.path.join(self.temp_dir, "zeros.txt")
        with open(file_path, "w") as file:
            file.write("abcde")

        buffer = mmap.mmap(-1, 64, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
        base = buffer_address(buffer)

        streamer = runai_start()
        submission_id = runai_request(
            streamer, [file_path], [2], [0, 0], [0, 5], [base, base]
        )

        seen = []
        for _ in range(2):
            result = runai_response(streamer)
            self.assertIsNotNone(result)
            ret, sub_id, file_index, range_index, submission_done = result
            self.assertEqual(ret, SUCCESS_ERROR_CODE)
            self.assertEqual(sub_id, submission_id)
            seen.append(range_index)

        self.assertEqual(sorted(seen), [0, 1])
        self.assertEqual(bytes(buffer[0:5]), b"abcde")

    def test_file_without_ranges_produces_no_response(self):
        # A file may carry no ranges at all. It is accepted and simply contributes nothing - the
        # submission completes on the responses of the files that do have ranges.
        empty_path = os.path.join(self.temp_dir, "no_ranges.txt")
        open(empty_path, "w").close()
        data_path = os.path.join(self.temp_dir, "data.txt")
        with open(data_path, "w") as file:
            file.write("payload")

        buffer = mmap.mmap(-1, 64, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)

        streamer = runai_start()
        submission_id = runai_request(
            streamer, [empty_path, data_path], [0, 1], [0], [7], [buffer_address(buffer)]
        )

        result = runai_response(streamer)
        self.assertIsNotNone(result)
        ret, sub_id, file_index, range_index, submission_done = result
        self.assertEqual(ret, SUCCESS_ERROR_CODE)
        self.assertEqual(sub_id, submission_id)
        self.assertEqual(file_index, 1)
        self.assertTrue(submission_done)
        self.assertEqual(bytes(buffer[0:7]), b"payload")

    def test_response_error_is_returned_not_raised(self):
        # A per-sub-range error (here EOF: asking for more bytes than the file holds) must be SURFACED by the
        # binding as data - a tuple with a non-Success response_code - NOT raised and NOT returned as None.
        # And since it is the submission's last (only) response, submission_done must be True, so a
        # multi-submission caller can attribute the failure to its submission and still learn it completed.
        file_path = os.path.join(self.temp_dir, "small.txt")
        with open(file_path, "w") as file:
            file.write("short")   # 5 bytes

        buffer = mmap.mmap(-1, 64, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)

        streamer = runai_start()
        self.assertNotEqual(streamer, 0)

        # one range asking for 20 bytes from a 5-byte file -> the read falls short (EOF error)
        submission_id = runai_request(
            streamer, [file_path], [1], [0], [20], [buffer_address(buffer)]
        )

        result = runai_response(streamer)
        # not raised, and not the teardown FinishedError (which would return None)
        self.assertIsNotNone(result)
        response_code, sub_id, file_index, range_index, submission_done = result
        self.assertNotEqual(response_code, SUCCESS_ERROR_CODE)   # error surfaced as data, not raised
        self.assertEqual(sub_id, submission_id)                  # attributable to its submission
        self.assertTrue(submission_done)                         # error was the submission's last response

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
