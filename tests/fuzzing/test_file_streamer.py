import unittest
import tempfile
import filecmp
import shutil
import os
import random
from runai_model_streamer.file_streamer.file_streamer import FileStreamer
from runai_model_streamer.file_streamer.requests_iterator import (
    RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME,
)

MIN_CHUNK_NUM = 1
MAX_CHUNK_NUM = 500
MIN_CHUNK_SIZE = 16
MAX_CHUNK_SIZE = 2048


def random_chunks():
    chunk_sizes = []
    content = b""
    for i in range(random.randint(MIN_CHUNK_NUM, MAX_CHUNK_NUM)):
        random_binary_content = os.urandom(
            random.randint(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE)
        )
        chunk_sizes.append(len(random_binary_content))
        content = content + random_binary_content
    return content, chunk_sizes


def random_memory_mode(chunks):
    memory_mode = random.choice([-1])
    os.environ[RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME] = str(memory_mode)
    if memory_mode == 1:
        memory_limit = random.randint(max(chunks), sum(chunks))
        os.environ[RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME] = str(memory_limit)


class TestFuzzing(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def test_file_streamer(self):
        file_content, chunk_sizes = random_chunks()
        random_memory_mode(chunk_sizes)
        file_path = os.path.join(self.temp_dir, "test_file.txt")
        with open(file_path, "wb") as file:
            file.write(file_content)

        with FileStreamer() as fs:
            list_files = fs.list(self.temp_dir)
            self.assertEqual(len(list_files), 1)
            self.assertEqual(list_files[0], file_path)

        copy_file_path = file_path + ".copy"
        with FileStreamer() as fs:
            fs.copy(file_path, copy_file_path)
        are_identical = filecmp.cmp(file_path, copy_file_path, shallow=False)
        self.assertTrue(are_identical)

        initial_offset = chunk_sizes[0]
        request_sizes = chunk_sizes[1:]
        expected_id_to_results = {}
        for i in range(len(request_sizes)):
            if i == len(request_sizes):
                expected_content = (
                    file_content[sum(request_sizes[0:i]) :] + initial_offset
                )
            else:
                expected_content = file_content[
                    sum(request_sizes[0:i])
                    + initial_offset : sum(request_sizes[0 : i + 1])
                    + initial_offset
                ]
            expected_id_to_results[i] = {
                "expected_offset": sum(request_sizes[0:i]),
                "expected_content": expected_content,
            }

        with FileStreamer() as fs:
            fs.stream_file(file_path, initial_offset, request_sizes)
            for id, dst, offset in fs.get_chunks():
                self.assertEqual(offset, expected_id_to_results[id]["expected_offset"])
                self.assertEqual(
                    dst[offset : offset + request_sizes[id]].tobytes(),
                    expected_id_to_results[id]["expected_content"],
                )

    def tearDown(self):
        os.environ.pop(RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME, None)
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
