import os
import unittest
from unittest.mock import patch
from runai_model_streamer.file_streamer.requests_iterator import (
    align_up,
    get_cuda_alignment,
    RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR,
    ChunksIterator,
    FileChunksIterator,
    FilesRequestsIterator,
    FilesRequestsIteratorWithBuffer,
    FileChunks,
    MemoryCapMode
)

class TestChunksIterator(unittest.TestCase):
    def test_next_request_exact_limit(self):
        chunks_iterator = ChunksIterator([1, 2, 3, 4])

        chunks, _ = chunks_iterator.next_chunks(6)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [1, 2, 3])

        chunks, _ = chunks_iterator.next_chunks(4)
        self.assertTrue(chunks_iterator.is_finished())
        self.assertEqual(chunks, [4])

    def test_next_request_high_limit(self):
        chunks_iterator = ChunksIterator([1, 2, 3, 4])

        chunks, _ = chunks_iterator.next_chunks(6)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [1, 2, 3])

        chunks, _ = chunks_iterator.next_chunks(6)
        self.assertTrue(chunks_iterator.is_finished())
        self.assertEqual(chunks, [4])

    def test_next_request_low_limit(self):
        chunks_iterator = ChunksIterator([1, 2, 3, 4])

        chunks, _ = chunks_iterator.next_chunks(6)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [1, 2, 3])

        chunks, _ = chunks_iterator.next_chunks(2)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [])

    def test_next_request_with_item_zero(self):
        chunks_iterator = ChunksIterator([1, 0, 3, 4])

        chunks, _ = chunks_iterator.next_chunks(4)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [1, 0, 3])

        chunks, _ = chunks_iterator.next_chunks(4)
        self.assertTrue(chunks_iterator.is_finished())
        self.assertEqual(chunks, [4])


class TestFileChunksIterator(unittest.TestCase):
    def test_next_request_exact_limit(self):
        file_chunks_iterator = FileChunksIterator(FileChunks(17, "a.txt", 10, [1, 2, 3, 4]))

        file_chunks = file_chunks_iterator.next_chunks(6)
        self.assertFalse(file_chunks_iterator.is_finished())
        self.assertIsNotNone(file_chunks)
        self.assertEqual(file_chunks.id, 17)
        self.assertEqual(file_chunks.path, "a.txt")
        self.assertEqual(file_chunks.offset, 10)
        self.assertEqual(file_chunks.chunks, [1, 2, 3])

        file_chunks = file_chunks_iterator.next_chunks(6)
        self.assertTrue(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.offset, 16)
        self.assertEqual(file_chunks.chunks, [4])

    def test_next_request_offset_for_non_exact_memory_limit(self):
        file_chunks_iterator = FileChunksIterator(FileChunks(17, "a.txt", 10, [1, 2, 3, 4]))

        file_chunks_iterator.next_chunks(5)
        file_chunks = file_chunks_iterator.next_chunks(6)
        self.assertFalse(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.offset, 13)

class TestFilesRequestsIterator(unittest.TestCase):
    def test_next_request_single_file(self):
        files_requests_iterator = FilesRequestsIterator(5, [FileChunks(17, "a.txt", 10, [1, 2, 3, 4])])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].path, "a.txt")
        self.assertEqual(files_requests.files[0].offset, 10)
        self.assertEqual(files_requests.files[0].chunks, [1, 2])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].path, "a.txt")
        self.assertEqual(files_requests.files[0].offset, 13)
        self.assertEqual(files_requests.files[0].chunks, [3])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].path, "a.txt")
        self.assertEqual(files_requests.files[0].offset, 16)
        self.assertEqual(files_requests.files[0].chunks, [4])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNone(files_requests)

    def test_next_request_multi_file(self):
        files_requests_iterator = FilesRequestsIterator(7, [FileChunks(17, "a.txt", 10, [1, 2, 3, 4]), FileChunks(18, "b.txt", 10, [1, 2, 3, 4])])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].path, "a.txt")
        self.assertEqual(files_requests.files[0].offset, 10)
        self.assertEqual(files_requests.files[0].chunks, [1, 2, 3])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 2)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].path, "a.txt")
        self.assertEqual(files_requests.files[0].offset, 16)
        self.assertEqual(files_requests.files[0].chunks, [4])
        self.assertEqual(files_requests.files[1].id, 18)
        self.assertEqual(files_requests.files[1].path, "b.txt")
        self.assertEqual(files_requests.files[1].offset, 10)
        self.assertEqual(files_requests.files[1].chunks, [1, 2])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 18)
        self.assertEqual(files_requests.files[0].path, "b.txt")
        self.assertEqual(files_requests.files[0].offset, 13)
        self.assertEqual(files_requests.files[0].chunks, [3, 4])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNone(files_requests)
    
    def test_next_request_multi_file_block_on_file(self):
        files_requests_iterator = FilesRequestsIterator(5, [FileChunks(17, "a.txt", 10, [1, 2, 3, 4]), FileChunks(18, "b.txt", 10, [1, 2, 3, 4])])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].path, "a.txt")
        self.assertEqual(files_requests.files[0].offset, 10)
        self.assertEqual(files_requests.files[0].chunks, [1, 2])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].path, "a.txt")
        self.assertEqual(files_requests.files[0].offset, 13)
        self.assertEqual(files_requests.files[0].chunks, [3])

    def test_get_global_file_and_chunk(self):
        files_requests_iterator = FilesRequestsIterator(3, [FileChunks(17, "a.txt", 10, [1, 2, 3, 4])])

        files_requests_iterator.next_request()

        file_id, chunk_index = files_requests_iterator.get_global_file_and_chunk(0, 0)
        self.assertEqual(file_id, 17)
        self.assertEqual(chunk_index, 0)

        file_id, chunk_index = files_requests_iterator.get_global_file_and_chunk(0, 1)
        self.assertEqual(file_id, 17)
        self.assertEqual(chunk_index, 1)

        files_requests_iterator.next_request()

        file_id, chunk_index = files_requests_iterator.get_global_file_and_chunk(0, 0)
        self.assertEqual(file_id, 17)
        self.assertEqual(chunk_index, 2)
    
    def test_file_chunks_zero_chunks(self):
        requests_iterator = FilesRequestsIterator(10, [FileChunks(17, "a.txt", 10, [])])

        res = requests_iterator.next_request()
        self.assertIsNone(res)

class TestFilesRequestsIteratorWithBuffer(unittest.TestCase):
    def test_memory_cap_unlimited(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [FileChunks(17, "a.txt", 10, [1, 2, 3, 4])], 100
        )
        self.assertEqual(len(requests_iterator.buffer), 10)

    def test_memory_cap_limited(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited, [FileChunks(17, "a.txt", 10, [1, 2, 3, 4])], 5
        )
        self.assertEqual(len(requests_iterator.buffer), 5)
    
    def test_memory_cap_largest_chunk(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [FileChunks(17, "a.txt", 10, [1, 2, 3, 4])], 5
        )
        self.assertEqual(len(requests_iterator.buffer), 4)
    
    def test_memory_cap_largest_chunk_multi_file(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [FileChunks(17, "a.txt", 10, [1, 2, 3, 4]), FileChunks(18, "b.txt", 10, [1, 2, 7, 4])], 5
        )
        self.assertEqual(len(requests_iterator.buffer), 7)
    
    def test_files_buffers(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [FileChunks(17, "a.txt", 10, [1, 2, 3, 4]), FileChunks(18, "b.txt", 10, [1, 2, 7, 4])], 5
        )
        self.assertEqual(len(requests_iterator.buffer), 7)

        requests_iterator.next_request()
        self.assertEqual(len(requests_iterator.file_buffers), 1)
        self.assertEqual(len(requests_iterator.file_buffers[0]), 6)

        requests_iterator.next_request()
        self.assertEqual(len(requests_iterator.file_buffers), 2)
        self.assertEqual(len(requests_iterator.file_buffers[0]), 4)
        self.assertEqual(len(requests_iterator.file_buffers[1]), 3)

    def test_limited_memory_cap_and_smaller_chunks(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited, [FileChunks(17, "a.txt", 10, [1, 2]), FileChunks(18, "b.txt", 10, [3, 4])], 50
        )
        self.assertEqual(len(requests_iterator.buffer), 10)

    
    def test_files_buffers_as_big_buffer(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [FileChunks(17, "a.txt", 10, [1, 2]), FileChunks(18, "b.txt", 10, [3, 4])], 5
        )
        self.assertEqual(len(requests_iterator.buffer), 10)

        requests_iterator.next_request()
        self.assertEqual(len(requests_iterator.file_buffers), 2)
        requests_iterator.buffer[0] = 9
        requests_iterator.buffer[3] = 8
        self.assertEqual(requests_iterator.file_buffers[0][0], 9)
        self.assertEqual(requests_iterator.file_buffers[1][0], 8)

class TestAlignment(unittest.TestCase):
    """Test CPU buffer allocation and chunk slicing when buffer_strides differ from chunks."""

    def _make_file_chunks(self, chunks, alignment):
        strides = [align_up(c, alignment) for c in chunks]
        buffer_strides = strides if strides != chunks else None
        return FileChunks(0, "test.bin", 0, chunks, buffer_strides=buffer_strides)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '256'})
    def test_get_cuda_alignment_256(self):
        self.assertEqual(get_cuda_alignment(), 256)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '16'})
    def test_get_cuda_alignment_16(self):
        self.assertEqual(get_cuda_alignment(), 16)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '256'})
    def test_buffer_size_alignment_256(self):
        # align_up(100,256)=256, align_up(200,256)=256, align_up(300,256)=512 → total 1024
        fc = self._make_file_chunks([100, 200, 300], get_cuda_alignment())
        iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [fc], device="cpu"
        )
        self.assertEqual(len(iterator.buffer), 256 + 256 + 512)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '16'})
    def test_buffer_size_alignment_16(self):
        # align_up(10,16)=16, align_up(20,16)=32, align_up(30,16)=32 → total 80
        fc = self._make_file_chunks([10, 20, 30], get_cuda_alignment())
        iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [fc], device="cpu"
        )
        self.assertEqual(len(iterator.buffer), 16 + 32 + 32)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '256'})
    def test_file_buffer_covers_padded_region_alignment_256(self):
        fc = self._make_file_chunks([100, 200, 300], get_cuda_alignment())
        iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [fc], device="cpu"
        )
        iterator.next_request()
        self.assertEqual(len(iterator.file_buffers), 1)
        self.assertEqual(len(iterator.file_buffers[0]), 256 + 256 + 512)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '256'})
    def test_chunk_slices_return_actual_size_alignment_256(self):
        # get_global_file_and_chunk must return the actual (unpadded) bytes per tensor
        chunks = [100, 200, 300]
        fc = self._make_file_chunks(chunks, get_cuda_alignment())
        iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [fc], device="cpu"
        )
        iterator.next_request()
        for i, expected_size in enumerate(chunks):
            _, _, buf = iterator.get_global_file_and_chunk(0, i)
            self.assertEqual(len(buf), expected_size)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '16'})
    def test_chunk_slices_return_actual_size_alignment_16(self):
        chunks = [10, 20, 30]
        fc = self._make_file_chunks(chunks, get_cuda_alignment())
        iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [fc], device="cpu"
        )
        iterator.next_request()
        for i, expected_size in enumerate(chunks):
            _, _, buf = iterator.get_global_file_and_chunk(0, i)
            self.assertEqual(len(buf), expected_size)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '256'})
    def test_memory_limit_enforced_against_padded_strides(self):
        # chunks [100, 200, 300] → strides [256, 256, 512]
        # memory_limit=512: first two tensors fit (256+256=512), third does not
        chunks = [100, 200, 300]
        fc = self._make_file_chunks(chunks, get_cuda_alignment())
        iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited, [fc], user_memory_limit=512, device="cpu"
        )
        req = iterator.next_request()
        self.assertIsNotNone(req)
        self.assertEqual(req.files[0].chunks, [100, 200])

        req = iterator.next_request()
        self.assertIsNotNone(req)
        self.assertEqual(req.files[0].chunks, [300])

        req = iterator.next_request()
        self.assertIsNone(req)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '256'})
    def test_cpu_buffer_base_alignment_256(self):
        # For CPU buffers the base address is not controlled, but the buffer object
        # must be sized to sum(effective_strides) so per-tensor slices are correct.
        # This test confirms the buffer length is right regardless of actual base addr.
        chunks = [100, 200, 300]
        fc = self._make_file_chunks(chunks, get_cuda_alignment())
        iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [fc], device="cpu"
        )
        self.assertEqual(len(iterator.buffer), 256 + 256 + 512)

    @patch.dict(os.environ, {RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: '16'})
    def test_memory_limit_enforced_against_padded_strides_alignment_16(self):
        # chunks [10, 20, 30] → strides [16, 32, 32]
        # memory_limit=48: first two tensors fit (16+32=48), third does not
        chunks = [10, 20, 30]
        fc = self._make_file_chunks(chunks, get_cuda_alignment())
        iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited, [fc], user_memory_limit=48, device="cpu"
        )
        req = iterator.next_request()
        self.assertIsNotNone(req)
        self.assertEqual(req.files[0].chunks, [10, 20])

        req = iterator.next_request()
        self.assertIsNotNone(req)
        self.assertEqual(req.files[0].chunks, [30])

        req = iterator.next_request()
        self.assertIsNone(req)


if __name__ == "__main__":
    unittest.main()
