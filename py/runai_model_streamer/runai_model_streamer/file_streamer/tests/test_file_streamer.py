import unittest
import tempfile
import shutil
import os
from unittest.mock import patch
from runai_model_streamer.file_streamer.file_streamer import FileStreamer
from runai_model_streamer.file_streamer.requests_iterator import (MemoryCapMode, FileChunks)


class TestBindings(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def test_runai_library(self):
        # Prepare file with content
        file_content = "XTest Text1TestText2Test-Text3\n"
        file_id = 17
        file_path = os.path.join(self.temp_dir, "test_file.txt")
        with open(file_path, "w") as file:
            file.write(file_content)

        request_sizes = [10, 0, 9, 10]
        id_to_results = {
            0: {"expected_text": "Test Text1"},
            1: {"expected_text": ""},
            2: {"expected_text": "TestText2"},
            3: {"expected_text": "Test-Text3"},
        }
        with FileStreamer() as fs:
            fs.stream_files([FileChunks(file_id, file_path, 1, request_sizes)])
            for res_file_id, id, dst in fs.get_chunks():
                self.assertEqual(res_file_id, file_id)
                self.assertEqual(
                    dst.numpy().tobytes().decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )

    @patch("runai_model_streamer.file_streamer.requests_iterator._get_memory_mode")
    def test_min_memory_cap(self, mock_get_memory_mode):
        mock_get_memory_mode.return_value = MemoryCapMode.largest_chunk
        file_content = "XTest Text1TestText2Test-Text3\n"
        file_id = 17
        file_path = os.path.join(self.temp_dir, "min_test_file.txt")
        with open(file_path, "w") as file:
            file.write(file_content)

        request_sizes = [10, 9, 10]
        id_to_results = {
            0: {"expected_text": "Test Text1"},
            1: {"expected_text": "TestText2"},
            2: {"expected_text": "Test-Text3"},
        }
        with FileStreamer() as fs:
            fs.stream_files([FileChunks(file_id, file_path, 1, request_sizes)])
            for res_file_id, id, dst in fs.get_chunks():
                self.assertEqual(res_file_id, file_id)
                self.assertEqual(
                    dst.numpy().tobytes().decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )

    @patch("os.getenv")
    @patch("runai_model_streamer.file_streamer.requests_iterator._get_memory_mode")
    def test_limited_memory_cap(self, mock_get_memory_mode, mock_getenv):
        mock_get_memory_mode.return_value = MemoryCapMode.limited
        mock_getenv.return_value = 6

        file_content = "XABBCCCDDDDEEEEEFFFFGGGHHI"
        file_id = 17
        file_path = os.path.join(self.temp_dir, "limited_test_file.txt")
        with open(file_path, "w") as file:
            file.write(file_content)

        request_sizes = [1, 2, 3, 4, 5, 4, 3, 2, 1]
        id_to_results = {
            0: {"expected_text": "A"},
            1: {"expected_text": "BB"},
            2: {"expected_text": "CCC"},
            3: {"expected_text": "DDDD"},
            4: {"expected_text": "EEEEE"},
            5: {"expected_text": "FFFF"},
            6: {"expected_text": "GGG"},
            7: {"expected_text": "HH"},
            8: {"expected_text": "I"},
        }
        with FileStreamer() as fs:
            fs.stream_files([FileChunks(file_id, file_path, 1, request_sizes)])
            for res_file_id, id, dst, in fs.get_chunks():
                self.assertEqual(res_file_id, file_id)
                self.assertEqual(
                    dst.numpy().tobytes().decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
