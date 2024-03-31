import unittest
import tempfile
import shutil
import os
from runai_streamer.file_streamer.file_streamer import FileStreamer


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
        dst = bytearray(size)

        request_sizes = [10, 9, 10]
        id_to_results = {
            0: {"expected_offset": 0, "expected_text": "Test Text1"},
            1: {"expected_offset": 10, "expected_text": "TestText2"},
            2: {"expected_offset": 19, "expected_text": "Test-Text3"},
        }
        with FileStreamer() as fs:
            fs.stream_file(file_path, 1, dst, request_sizes)
            for id, offset in fs.get_chunks():
                self.assertEqual(offset, id_to_results[id]["expected_offset"])
                self.assertEqual(
                    dst[offset : offset + request_sizes[id]].decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )

    def test_runai_read(self):
        file_content = "MyTensorHelloTest"
        file_path = os.path.join(self.temp_dir, "runai_read_test.txt")
        with open(file_path, "w") as file:
            file.write(file_content)
        dst = bytearray(len(file_content) - 2)

        with FileStreamer() as fs:
            fs.read_file(file_path, 2, dst)
            self.assertEqual(dst.decode("utf-8"), "TensorHelloTest")

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
