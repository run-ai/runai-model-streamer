import unittest
from runai_model_streamer.file_streamer.requests_iterator import (
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

        chunks = chunks_iterator.next_chunks(6)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [1, 2, 3])

        chunks = chunks_iterator.next_chunks(4)
        self.assertTrue(chunks_iterator.is_finished())
        self.assertEqual(chunks, [4])

    def test_next_request_high_limit(self):
        chunks_iterator = ChunksIterator([1, 2, 3, 4])

        chunks = chunks_iterator.next_chunks(6)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [1, 2, 3])
        
        chunks = chunks_iterator.next_chunks(6)
        self.assertTrue(chunks_iterator.is_finished())
        self.assertEqual(chunks, [4])

    def test_next_request_low_limit(self):
        chunks_iterator = ChunksIterator([1, 2, 3, 4])

        chunks = chunks_iterator.next_chunks(6)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [1, 2, 3])
        
        chunks = chunks_iterator.next_chunks(2)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [])
    
    def test_next_request_with_item_zero(self):
        chunks_iterator = ChunksIterator([1, 0, 3, 4])

        chunks = chunks_iterator.next_chunks(4)
        self.assertFalse(chunks_iterator.is_finished())
        self.assertEqual(chunks, [1, 0, 3])
        
        chunks = chunks_iterator.next_chunks(4)
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

if __name__ == "__main__":
    unittest.main()
