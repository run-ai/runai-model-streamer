import unittest
import tempfile
import shutil
import os
import random
from runai_model_streamer.file_streamer import (FileStreamer, FileChunks)
from runai_model_streamer.file_streamer.requests_iterator import (
    RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME,
)

MIN_NUM_FILES = 1
MAX_NUM_FILES = 20
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

def random_file_chunks(i, dir):
    file_content, chunk_sizes = random_chunks()
    file_path = os.path.join(dir, f"test_file-{i}.txt")
    with open(file_path, "wb") as file:
        file.write(file_content)
        
    initial_offset = chunk_sizes[0]
    request_sizes = chunk_sizes[1:]

    expected_id_to_results = {}
    for j in range(len(request_sizes)):
        if j == len(request_sizes):
            expected_content = (
                file_content[sum(request_sizes[0:j]) :] + initial_offset
            )
        else:
            expected_content = file_content[
                sum(request_sizes[0:j])
                + initial_offset : sum(request_sizes[0 : j + 1])
                + initial_offset
            ]
        expected_id_to_results[i] = {
            "expected_content": expected_content,
        }
    return expected_id_to_results, FileChunks(i, file_path, initial_offset, request_sizes)

class TestFuzzing(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def test_file_streamer(self):
        expected_file_to_id_to_results = {}
        file_to_file_chunks = {}
        files_chunks = []
        
        for i in range(random.randint(MIN_NUM_FILES, MAX_NUM_FILES)):
            expected_id_to_results, file_chunks = random_file_chunks(i, self.temp_dir)
            expected_file_to_id_to_results[file_chunks.path] = expected_id_to_results
            file_to_file_chunks[file_chunks.path] = file_chunks
            files_chunks.append(file_chunks)

        random_memory_mode([chunk for chunk in file_chunks.chunks for file_chunks in files_chunks])

        with FileStreamer() as fs:
            fs.stream_files(files_chunks)
            for file, id, dst in fs.get_chunks():
                self.assertIn(file, [file_chunks.path for file_chunks in files_chunks])

                file_chunks = file_to_file_chunks[file]
                expected_id_to_results = expected_file_to_id_to_results[file]
                self.assertEqual(
                    dst.numpy().tobytes(),
                    expected_id_to_results[id]["expected_content"],
                )

    def tearDown(self):
        os.environ.pop(RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME, None)
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
