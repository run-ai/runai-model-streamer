import unittest
import tempfile
import shutil
import os
from unittest.mock import patch
from runai_model_streamer.file_streamer.file_streamer import FileStreamer
from runai_model_streamer.file_streamer.requests_iterator import (
    MemoryCapMode,
    FileChunks,
    RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME,
    RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME,
)


class TestRingConcurrency(unittest.TestCase):
    """Several submissions in flight at once. Each range carries a distinct, position-encoding payload,
    so a response attributed to the wrong submission or the wrong buffer produces the wrong text rather
    than passing by luck."""

    RANGE_SIZE = 8

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def write_ranges(self, name: str, count: int):
        """A file of `count` ranges, range i holding exactly "rng{i:05d}"."""
        expected = {i: f"rng{i:05d}" for i in range(count)}
        path = os.path.join(self.temp_dir, name)
        with open(path, "w") as handle:
            handle.write("".join(expected[i] for i in range(count)))
        return path, expected

    def stream(self, path: str, count: int):
        """Stream the file one range at a time, returning (results, max concurrently live submissions)."""
        results = {}
        max_live = 0
        with FileStreamer() as fs:
            fs.stream_files([FileChunks.contiguous(17, path, 0, [self.RANGE_SIZE] * count)])
            for file_id, range_index, buffer in fs.get_chunks():
                self.assertEqual(file_id, 17)
                # read the payload BEFORE resuming the generator: once we resume, the submission may
                # complete and its buffer be recycled under us
                results[range_index] = buffer.numpy().tobytes().decode("utf-8")
                max_live = max(max_live, len(fs.live_requests))
        return results, max_live

    @patch.dict(os.environ, {RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME: "64",
                             RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "8"})
    def test_many_submissions_in_flight_deliver_every_range(self):
        # 8 ranges of 8 bytes and a 64 byte budget over 8 buffers: 8 bytes each, so all 8 requests are
        # submitted up front and every response has to be attributed to its own submission.
        path, expected = self.write_ranges("concurrent.txt", 8)
        results, max_live = self.stream(path, 8)
        self.assertEqual(results, expected)
        self.assertGreater(max_live, 1, "expected several submissions in flight at once")

    @patch.dict(os.environ, {RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME: "16",
                             RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "2"})
    def test_ring_recycles_buffers_across_many_requests(self):
        # a ring of 2 serving 10 ranges: buffers must be released and refilled 5 times over, and a range
        # must never be read out of a buffer that a later request has already overwritten
        path, expected = self.write_ranges("recycled.txt", 10)
        results, max_live = self.stream(path, 10)
        self.assertEqual(results, expected)
        self.assertEqual(max_live, 2)

    @patch.dict(os.environ, {RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME: "32",
                             RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "4"})
    def test_deep_ring_recycles_while_other_submissions_are_still_in_flight(self):
        # The steady state of a real load, which neither of the tests above reaches: a ring of 4 serving
        # 20 ranges, so buffers are handed back and immediately re-submitted while three other
        # submissions are still filling. Covers depth AND recycling at once - a refill that stops
        # keeping the ring full stays correct, so only the max_live assertion catches it.
        path, expected = self.write_ranges("deep.txt", 20)
        results, max_live = self.stream(path, 20)
        self.assertEqual(results, expected)
        self.assertEqual(max_live, 4)

    @patch.dict(os.environ, {RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME: "64",
                             RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "8"})
    def test_abandoning_the_generator_drains_every_live_submission(self):
        # the consumer walks away with several submissions still in flight. Every one of them must be
        # drained before the streamer is torn down, or a late write lands in freed memory.
        path, _ = self.write_ranges("abandoned.txt", 8)
        with FileStreamer() as fs:
            fs.stream_files([FileChunks.contiguous(17, path, 0, [self.RANGE_SIZE] * 8)])
            chunks = fs.get_chunks()
            next(chunks)
            self.assertGreater(len(fs.live_requests), 1)
            chunks.close()     # resumes the generator at its yield, running the finally
            self.assertEqual(fs.outstanding, 0)
            self.assertEqual(fs.live_requests, {})


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
            fs.stream_files([FileChunks.contiguous(file_id, file_path, 1, request_sizes)])
            seen = set()
            for res_file_id, id, dst in fs.get_chunks():
                seen.add(id)
                self.assertEqual(res_file_id, file_id)
                self.assertEqual(
                    dst.numpy().tobytes().decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )
            # every range produced exactly one response (full drain, incl. the zero-size range)
            self.assertEqual(seen, set(id_to_results))

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
            fs.stream_files([FileChunks.contiguous(file_id, file_path, 1, request_sizes)])
            seen = set()
            for res_file_id, id, dst in fs.get_chunks():
                seen.add(id)
                self.assertEqual(res_file_id, file_id)
                self.assertEqual(
                    dst.numpy().tobytes().decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )
            # every range produced exactly one response (full drain)
            self.assertEqual(seen, set(id_to_results))

    # Patch the ENVIRONMENT, not os.getenv itself. Patching the lookup function makes every variable
    # answer the same value, so this test used to set the ring's buffer size and buffer-count floor to 6
    # as well as the memory limit - silently, and only because requests_iterator grew more variables.
    # The real _get_memory_mode derives `limited` from 6 on its own, so it needs no patch either.
    @patch.dict(os.environ, {RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME: "6"})
    def test_limited_memory_cap(self):
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
            fs.stream_files([FileChunks.contiguous(file_id, file_path, 1, request_sizes)])
            seen = set()
            for res_file_id, id, dst, in fs.get_chunks():
                seen.add(id)
                self.assertEqual(res_file_id, file_id)
                self.assertEqual(
                    dst.numpy().tobytes().decode("utf-8"),
                    id_to_results[id]["expected_text"],
                )
            # every range produced exactly one response (full drain across multiple buffer requests)
            self.assertEqual(seen, set(id_to_results))

    def test_empty_file_first_does_not_drop_the_rest(self):
        # end-to-end regression for issue #157: an empty shard ahead of a data shard used to close the
        # request, which get_chunks reads as end of stream, silently dropping every remaining file
        empty_path = os.path.join(self.temp_dir, "empty.txt")
        open(empty_path, "w").close()
        data_path = os.path.join(self.temp_dir, "data.txt")
        with open(data_path, "w") as file:
            file.write("HelloWorld")

        with FileStreamer() as fs:
            fs.stream_files([
                FileChunks(17, empty_path, [], []),
                FileChunks.contiguous(18, data_path, 0, [5, 5]),
            ])
            results = [(file_id, index, dst.numpy().tobytes().decode("utf-8"))
                       for file_id, index, dst in fs.get_chunks()]

        self.assertEqual(sorted(results), [(18, 0, "Hello"), (18, 1, "World")])

    def test_file_of_only_zero_sized_ranges_is_answered(self):
        # A file whose tensors are all zero sized is not an empty shard - it has header entries, and
        # safetensors yields those tensors. Each zero-sized range must still get its own response, or
        # the caller's tensor indexing drifts. This used to be skipped in Python because the C++ layer
        # could not answer a zero-sized range (an empty batch range never completed its tasks).
        file_path = os.path.join(self.temp_dir, "zeros.txt")
        with open(file_path, "w") as file:
            file.write("content")

        with FileStreamer() as fs:
            fs.stream_files([FileChunks.contiguous(17, file_path, 0, [0, 0, 0])])
            results = [(file_id, index, len(dst.numpy().tobytes()))
                       for file_id, index, dst in fs.get_chunks()]

        self.assertEqual(sorted(results), [(17, 0, 0), (17, 1, 0), (17, 2, 0)])

    def test_get_chunks_raises_on_read_error(self):
        # FileStreamer is single-submission and fail-fast: a per-sub-range error from the streamer must raise
        # out of get_chunks (the low-level binding no longer raises, so FileStreamer itself enforces this).
        # Requesting more bytes than the file holds makes the read fall short -> a per-sub-range error.
        file_id = 17
        file_path = os.path.join(self.temp_dir, "small.txt")
        with open(file_path, "w") as file:
            file.write("short")   # 5 bytes

        with FileStreamer() as fs:
            fs.stream_files([FileChunks.contiguous(file_id, file_path, 0, [20])])   # one 20-byte range from a 5-byte file
            with self.assertRaises(ValueError):
                list(fs.get_chunks())

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
