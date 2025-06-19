import unittest
import tempfile
import shutil
import os
import mmap
from runai_model_streamer.libstreamer.libstreamer import (
    runai_start,
    runai_request,
    runai_response,
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
                credentials = S3Credentials(
                    access_key_id="your_access_key",
                    secret_access_key="your_secret_key",
                    session_token="your_session_token",
                    region_name="us-west-2",
                    endpoint="optional_endpoint")
                runai_request(streamer, [file_path_1, file_path_2], [1, 1], [19, 19], [buffer], items, credentials)
            else:
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

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
