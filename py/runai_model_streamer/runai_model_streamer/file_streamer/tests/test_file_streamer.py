import unittest
import tempfile
import shutil
import os
from unittest.mock import patch
from runai_model_streamer.file_streamer.file_streamer import FileStreamer
from runai_model_streamer.file_streamer.requests_iterator import MemoryCapMode


class TestBindings(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def test_runai_library(self):
        # Prepare file with content
        file_content = "XTest Text1TestText2Test-Text3\n"
        file_path = os.path.join(self.temp_dir, "test_file.txt")
        with open(file_path, "w") as file:
            file.write(file_content)
        size = len(file_content) - 2

        request_sizes = [10, 9, 10]
        id_to_results = {
            0: {"expected_offset": 0, "expected_text": "Test Text1"},
            1: {"expected_offset": 10, "expected_text": "TestText2"},
            2: {"expected_offset": 19, "expected_text": "Test-Text3"},
        }
        with FileStreamer() as fs:
            fs.stream_file(file_path, 1, request_sizes)
            for id, dst, offset in fs.get_chunks():
                self.assertEqual(offset, id_to_results[id]["expected_offset"])
                self.assertEqual(
                    dst[offset : offset + request_sizes[id]].tobytes().decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )

    def test_runai_read(self):
        file_content = "MyTensorHelloTest"
        file_path = os.path.join(self.temp_dir, "runai_read_test.txt")
        with open(file_path, "w") as file:
            file.write(file_content)

        with FileStreamer() as fs:
            dst = fs.read_file(file_path, 2, len(file_content) - 2)
            self.assertEqual(bytearray(dst).decode("utf-8"), "TensorHelloTest")

        with FileStreamer() as fs:
            list_files = fs.list(self.temp_dir)
            self.assertEqual(len(list_files), 1)
            self.assertEqual(list_files[0], file_path)

    @patch("runai_model_streamer.file_streamer.requests_iterator._get_memory_mode")
    def test_min_memory_cap(self, mock_get_memory_mode):
        mock_get_memory_mode.return_value = MemoryCapMode.largest_chunk
        file_content = "XTest Text1TestText2Test-Text3\n"
        file_path = os.path.join(self.temp_dir, "min_test_file.txt")
        with open(file_path, "w") as file:
            file.write(file_content)

        request_sizes = [10, 9, 10]
        id_to_results = {
            0: {"expected_offset": 0, "expected_text": "Test Text1"},
            1: {"expected_offset": 0, "expected_text": "TestText2"},
            2: {"expected_offset": 0, "expected_text": "Test-Text3"},
        }
        with FileStreamer() as fs:
            fs.stream_file(file_path, 1, request_sizes)
            for id, dst, offset in fs.get_chunks():
                self.assertEqual(offset, id_to_results[id]["expected_offset"])
                self.assertEqual(
                    dst[offset : offset + request_sizes[id]].tobytes().decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )

    @patch("os.getenv")
    @patch("runai_model_streamer.file_streamer.requests_iterator._get_memory_mode")
    def test_limited_memory_cap(self, mock_get_memory_mode, mock_getenv):
        mock_get_memory_mode.return_value = MemoryCapMode.limited
        mock_getenv.return_value = 6

        file_content = "XABBCCCDDDDEEEEEFFFFGGGHHI"
        file_path = os.path.join(self.temp_dir, "limited_test_file.txt")
        with open(file_path, "w") as file:
            file.write(file_content)

        request_sizes = [1, 2, 3, 4, 5, 4, 3, 2, 1]
        id_to_results = {
            0: {"expected_offset": 0, "expected_text": "A"},
            1: {"expected_offset": 1, "expected_text": "BB"},
            2: {"expected_offset": 3, "expected_text": "CCC"},
            3: {"expected_offset": 0, "expected_text": "DDDD"},
            4: {"expected_offset": 0, "expected_text": "EEEEE"},
            5: {"expected_offset": 0, "expected_text": "FFFF"},
            6: {"expected_offset": 0, "expected_text": "GGG"},
            7: {"expected_offset": 3, "expected_text": "HH"},
            8: {"expected_offset": 5, "expected_text": "I"},
        }
        with FileStreamer() as fs:
            fs.stream_file(file_path, 1, request_sizes)
            for id, dst, offset in fs.get_chunks():
                self.assertEqual(offset, id_to_results[id]["expected_offset"])
                self.assertEqual(
                    dst[offset : offset + request_sizes[id]].tobytes().decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
