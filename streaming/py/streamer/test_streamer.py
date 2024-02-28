import unittest
import tempfile
import shutil
import ctypes
import os
import mmap
from streamer.streamer import runai_request


class TestRunaiRequest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def test_runai_request(self):
        file_path = os.path.join(self.temp_dir, "test_file.txt")
        with open(file_path, "w") as file:
            file.write("Test Text\n")
        size = 9
        fd = mmap.mmap(-1, size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
        buffer = (ctypes.c_ubyte * size).from_buffer(fd)
        buffer_ptr = id(buffer)

        result = runai_request(file_path, 1, 5, buffer, 0, [])

        self.assertEqual(buffer[0], 101)
        self.assertEqual(id(buffer), buffer_ptr)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
