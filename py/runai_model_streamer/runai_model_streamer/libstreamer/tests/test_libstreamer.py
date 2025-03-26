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
        # Prepare file with content
        file_path = os.path.join(self.temp_dir, "test_file.txt")
        with open(file_path, "w") as file:
            file.write("XTest Text1TestText2Test-Text3\n")

        size = 30

        for use_credentials in (False, True) :
            buffer = mmap.mmap(-1, size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
            buffer_ptr = id(buffer)

            streamer = runai_start()
            self.assertNotEqual(streamer, 0)

            # Chunks of text sizes in file content
            items = [10, 9]
            if use_credentials:
                credentials = S3Credentials(
                    access_key_id="your_access_key",
                    secret_access_key="your_secret_key",
                    session_token="your_session_token",
                    region_name="us-west-2",
                    endpoint="optional_endpoint")
                runai_request(streamer, file_path, 1, 30, buffer, items, credentials)
            else:
                runai_request(streamer, file_path, 1, 30, buffer, items)

            # Read both file contents
            result = runai_response(streamer)
            self.assertEqual(result, 0)
            self.assertEqual(bytes(buffer[:10]), b"Test Text1")

            result = runai_response(streamer)
            self.assertEqual(result, 1)
            self.assertEqual(bytes(buffer[10:19]), b"TestText2")

            # Assert buffer filled copyless
            self.assertEqual(id(buffer), buffer_ptr)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
