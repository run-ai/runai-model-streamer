import unittest
import tempfile
import shutil
import os
import mmap
from runai_model_streamer.libstreamer.libstreamer import (
    runai_start,
    runai_set_credentials,
    runai_request,
    runai_response,
    runai_request_ex,
    runai_response_ex,
)
from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
)


class TestBindings(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def test_runai_library(self):
        # Prepare files with content
        file_path_1 = os.path.join(self.temp_dir, "test_file_1.txt")
        with open(file_path_1, "w") as file:
            file.write("XTest Text1TestText2Test-Text3\n")

        file_path_2 = os.path.join(self.temp_dir, "test_file_2.txt")
        with open(file_path_2, "w") as file:
            file.write("YTest Text44TestText")

        size = 60

        for use_credentials in (False, True) :
            buffer = mmap.mmap(-1, size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
            buffer_ptr = id(buffer)

            streamer = runai_start()
            self.assertNotEqual(streamer, 0)

            # Chunks of text sizes in file content
            items = [[10, 9], [11, 8]]
            if use_credentials:
                # credentials are streamer-scoped now: set them once on the streamer (unused for this
                # filesystem read, but exercises the set-credentials path)
                credentials = S3Credentials(
                    access_key_id="your_access_key",
                    secret_access_key="your_secret_key",
                    session_token="your_session_token",
                    region_name="us-west-2",
                    endpoint="optional_endpoint")
                runai_set_credentials(streamer, credentials)
            runai_request(streamer, [file_path_1, file_path_2], [1, 1], [19, 19], [buffer], items)

            # Read both file contents
            result_file, result = runai_response(streamer)
            self.assertEqual(result_file, 0)
            self.assertEqual(result, 0)
            self.assertEqual(bytes(buffer[:10]), b"Test Text1")

            result_file, result = runai_response(streamer)
            self.assertEqual(result_file, 0)
            self.assertEqual(result, 1)
            self.assertEqual(bytes(buffer[10:19]), b"TestText2")

            result_file, result = runai_response(streamer)
            self.assertEqual(result_file, 1)
            self.assertEqual(result, 0)
            self.assertEqual(bytes(buffer[19:30]), b"Test Text44")

            result_file, result = runai_response(streamer)
            self.assertEqual(result_file, 1)
            self.assertEqual(result, 1)
            self.assertEqual(bytes(buffer[30:38]), b"TestText")

            # Assert buffer filled copyless
            self.assertEqual(id(buffer), buffer_ptr)

    def test_runai_library_ex(self):
        # Same read as test_runai_library, but via the multi-request _ex API: submit returns a submission
        # id, and each response reports its submission id, file/range and whether the submission is complete.
        file_path_1 = os.path.join(self.temp_dir, "test_file_1.txt")
        with open(file_path_1, "w") as file:
            file.write("XTest Text1TestText2Test-Text3\n")

        file_path_2 = os.path.join(self.temp_dir, "test_file_2.txt")
        with open(file_path_2, "w") as file:
            file.write("YTest Text44TestText")

        size = 60
        buffer = mmap.mmap(-1, size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
        buffer_ptr = id(buffer)

        streamer = runai_start()
        self.assertNotEqual(streamer, 0)

        items = [[10, 9], [11, 8]]
        submission_id = runai_request_ex(
            streamer, [file_path_1, file_path_2], [1, 1], [19, 19], [buffer], items
        )

        # collect all four sub-range responses (order across files is not guaranteed)
        expected = {(0, 0), (0, 1), (1, 0), (1, 1)}
        done_count = 0
        for _ in range(len(expected)):
            result = runai_response_ex(streamer)
            self.assertIsNotNone(result)
            sub_id, file_index, range_index, submission_done = result
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

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
