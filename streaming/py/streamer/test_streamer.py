import unittest
import tempfile
import shutil
import ctypes
import os
import mmap
from streamer.streamer import runai_start, runai_request, runai_response
from streamer import (t_streamer)


class TestBindings(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()


    def test_runai_library(self):
        # Prepare file with content
        file_path = os.path.join(self.temp_dir, "test_file.txt")
        with open(file_path, "w") as file:
            file.write("XTest Text1TestText2Test-Text3\n")
        
        size = 30
        fd = mmap.mmap(-1, size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
        buffer = (ctypes.c_ubyte * size).from_buffer(fd)
        buffer_ptr = id(buffer)

        streamer = t_streamer(0)
        result = runai_start(ctypes.byref(streamer))
        self.assertNotEqual(streamer, 0)

        # Chunks of text sizes in file content
        items = [10, 9]
        result = runai_request(streamer, file_path, 1, 30, buffer, 2, items)
        self.assertEqual(result, 0)

        # Read both file contents
        index = [0]
        result = runai_response(streamer, index)
        self.assertEqual(result, 0)
        self.assertEqual(index[0], 0)
        self.assertEqual(bytes(buffer[:10]), b'Test Text1')

        result = runai_response(streamer, index)
        self.assertEqual(result, 0)
        self.assertEqual(index[0], 1)
        self.assertEqual(bytes(buffer[10:19]), b'TestText2')

        # Assert buffer filled copyless
        self.assertEqual(id(buffer), buffer_ptr)


    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
