import unittest
from runai_model_streamer.file_streamer.requests_iterator import (
    FileChunksIterator,
    FilesRequestsIterator,
    FilesRequestsIteratorWithBuffer,
    FileChunks,
    MemoryCapMode
)


class TestFileChunks(unittest.TestCase):
    def test_contiguous_computes_offsets(self):
        file_chunks = FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])
        self.assertEqual(file_chunks.offsets, [10, 11, 13, 16])
        self.assertEqual(file_chunks.sizes, [1, 2, 3, 4])
        self.assertEqual(file_chunks.total_size(), 10)
        self.assertEqual(file_chunks.max_chunk_size(), 4)

    def test_contiguous_with_zero_sized_range(self):
        # a zero sized range does not advance the offset, so it shares the next range's offset
        file_chunks = FileChunks.contiguous(17, "a.txt", 10, [5, 0, 3])
        self.assertEqual(file_chunks.offsets, [10, 15, 15])

    def test_scattered_ranges_are_kept_as_given(self):
        # ranges need not be contiguous, ordered, or even distinct - nothing is derived from them
        file_chunks = FileChunks(17, "a.txt", [100, 5, 60], [4, 4, 4])
        self.assertEqual(file_chunks.offsets, [100, 5, 60])
        self.assertEqual(file_chunks.total_size(), 12)

    def test_mismatched_lengths_rejected(self):
        with self.assertRaises(ValueError):
            FileChunks(17, "a.txt", [0, 10], [4])


class TestFileChunksIterator(unittest.TestCase):
    # The budget tests below covered ChunksIterator before it was folded into FileChunksIterator:
    # ranges carry their own offsets now, so there is no separate size-only iterator to drive.
    def test_next_request_exact_limit(self):
        file_chunks_iterator = FileChunksIterator(FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]))

        file_chunks = file_chunks_iterator.next_chunks(6)
        self.assertFalse(file_chunks_iterator.is_finished())
        self.assertIsNotNone(file_chunks)
        self.assertEqual(file_chunks.id, 17)
        self.assertEqual(file_chunks.path, "a.txt")
        self.assertEqual(file_chunks.offsets, [10, 11, 13])
        self.assertEqual(file_chunks.sizes, [1, 2, 3])

        file_chunks = file_chunks_iterator.next_chunks(4)
        self.assertTrue(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.offsets, [16])
        self.assertEqual(file_chunks.sizes, [4])

    def test_next_request_high_limit(self):
        file_chunks_iterator = FileChunksIterator(FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]))

        file_chunks = file_chunks_iterator.next_chunks(6)
        self.assertFalse(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.sizes, [1, 2, 3])

        file_chunks = file_chunks_iterator.next_chunks(6)
        self.assertTrue(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.sizes, [4])

    def test_next_request_low_limit(self):
        file_chunks_iterator = FileChunksIterator(FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]))

        file_chunks = file_chunks_iterator.next_chunks(6)
        self.assertFalse(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.sizes, [1, 2, 3])

        # the next range does not fit the remaining budget: nothing is handed out, and the file is
        # not finished - the caller must offer it again with a fresh budget
        file_chunks = file_chunks_iterator.next_chunks(2)
        self.assertFalse(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.sizes, [])
        self.assertEqual(file_chunks.offsets, [])

    def test_next_request_with_item_zero(self):
        file_chunks_iterator = FileChunksIterator(FileChunks.contiguous(17, "a.txt", 10, [1, 0, 3, 4]))

        file_chunks = file_chunks_iterator.next_chunks(4)
        self.assertFalse(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.sizes, [1, 0, 3])
        self.assertEqual(file_chunks.offsets, [10, 11, 11])

        file_chunks = file_chunks_iterator.next_chunks(4)
        self.assertTrue(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.sizes, [4])

    def test_next_request_offset_for_non_exact_memory_limit(self):
        file_chunks_iterator = FileChunksIterator(FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]))

        file_chunks_iterator.next_chunks(5)
        file_chunks = file_chunks_iterator.next_chunks(6)
        self.assertFalse(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.offsets, [13])

    def test_scattered_ranges_survive_slicing(self):
        file_chunks_iterator = FileChunksIterator(FileChunks(17, "a.txt", [100, 5, 60], [4, 4, 4]))

        file_chunks = file_chunks_iterator.next_chunks(8)
        self.assertEqual(file_chunks.offsets, [100, 5])
        self.assertEqual(file_chunks.sizes, [4, 4])

        file_chunks = file_chunks_iterator.next_chunks(8)
        self.assertTrue(file_chunks_iterator.is_finished())
        self.assertEqual(file_chunks.offsets, [60])


class TestFilesRequestsIterator(unittest.TestCase):
    def test_next_request_single_file(self):
        files_requests_iterator = FilesRequestsIterator(5, [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].path, "a.txt")
        self.assertEqual(files_requests.files[0].offsets, [10, 11])
        self.assertEqual(files_requests.files[0].sizes, [1, 2])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].offsets, [13])
        self.assertEqual(files_requests.files[0].sizes, [3])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].offsets, [16])
        self.assertEqual(files_requests.files[0].sizes, [4])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNone(files_requests)

    def test_next_request_multi_file(self):
        files_requests_iterator = FilesRequestsIterator(
            7,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]),
             FileChunks.contiguous(18, "b.txt", 10, [1, 2, 3, 4])],
        )

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].offsets, [10, 11, 13])
        self.assertEqual(files_requests.files[0].sizes, [1, 2, 3])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 2)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].offsets, [16])
        self.assertEqual(files_requests.files[0].sizes, [4])
        self.assertEqual(files_requests.files[1].id, 18)
        self.assertEqual(files_requests.files[1].path, "b.txt")
        self.assertEqual(files_requests.files[1].offsets, [10, 11])
        self.assertEqual(files_requests.files[1].sizes, [1, 2])
        # file_base makes (file index, range index) -> flat position O(1)
        self.assertEqual(files_requests.file_base, [0, 1])
        self.assertEqual(files_requests.num_ranges, 3)
        self.assertEqual(files_requests.flat_index(1, 1), 2)

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 18)
        self.assertEqual(files_requests.files[0].offsets, [13, 16])
        self.assertEqual(files_requests.files[0].sizes, [3, 4])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNone(files_requests)

    def test_next_request_multi_file_block_on_file(self):
        files_requests_iterator = FilesRequestsIterator(
            5,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]),
             FileChunks.contiguous(18, "b.txt", 10, [1, 2, 3, 4])],
        )

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].sizes, [1, 2])

        files_requests = files_requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(len(files_requests.files), 1)
        self.assertEqual(files_requests.files[0].id, 17)
        self.assertEqual(files_requests.files[0].sizes, [3])

    def test_get_global_file_and_chunk(self):
        files_requests_iterator = FilesRequestsIterator(3, [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])])

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
        requests_iterator = FilesRequestsIterator(10, [FileChunks(17, "a.txt", [], [])])

        res = requests_iterator.next_request()
        self.assertIsNone(res)

    def test_next_request_empty_file_first_does_not_terminate(self):
        # regression for issue #157: an empty shard at the head of the queue used to close the request,
        # which get_chunks reads as end of stream, silently dropping every remaining file
        requests_iterator = FilesRequestsIterator(
            100, [FileChunks(17, "empty.txt", [], []), FileChunks.contiguous(18, "a.txt", 10, [5])]
        )

        files_requests = requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual([f.id for f in files_requests.files], [18])
        self.assertEqual(files_requests.files[0].sizes, [5])

        self.assertIsNone(requests_iterator.next_request())

    def test_next_request_empty_files_between_data_files(self):
        requests_iterator = FilesRequestsIterator(
            100,
            [
                FileChunks.contiguous(17, "a.txt", 10, [1, 2]),
                FileChunks(18, "empty1.txt", [], []),
                FileChunks(19, "empty2.txt", [], []),
                FileChunks.contiguous(20, "b.txt", 10, [3]),
            ],
        )

        files_requests = requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual([f.id for f in files_requests.files], [17, 20])

        self.assertIsNone(requests_iterator.next_request())

    def test_file_of_only_zero_sized_ranges_is_streamed(self):
        # A file whose tensors are all zero sized is NOT an empty shard: it has header entries, and
        # safetensors yields those tensors (shape preserved, data_offsets [x, x]). They are submitted
        # so the C++ layer answers one response per range and the caller's tensor indexing lines up.
        requests_iterator = FilesRequestsIterator(
            100,
            [FileChunks.contiguous(17, "zeros.txt", 10, [0, 0]),
             FileChunks.contiguous(18, "a.txt", 10, [5])],
        )

        files_requests = requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual([f.id for f in files_requests.files], [17, 18])
        self.assertEqual(files_requests.files[0].sizes, [0, 0])
        self.assertEqual(files_requests.num_ranges, 3)

        self.assertIsNone(requests_iterator.next_request())

    def test_zero_sized_range_among_non_zero(self):
        requests_iterator = FilesRequestsIterator(100, [FileChunks.contiguous(17, "a.txt", 10, [5, 0, 3])])

        files_requests = requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(files_requests.files[0].sizes, [5, 0, 3])
        self.assertEqual(files_requests.files[0].offsets, [10, 15, 15])
        self.assertEqual(files_requests.num_ranges, 3)

    def test_only_zero_sized_ranges_in_the_whole_request(self):
        # every range is zero sized, so the memory limit is 0 - the request must still be produced,
        # with one range (and so one response) each
        requests_iterator = FilesRequestsIterator(0, [FileChunks.contiguous(17, "zeros.txt", 10, [0, 0, 0])])

        files_requests = requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(files_requests.num_ranges, 3)

        self.assertIsNone(requests_iterator.next_request())


class TestFilesRequestsIteratorWithBuffer(unittest.TestCase):
    def test_memory_cap_unlimited(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])], 100
        )
        self.assertEqual(len(requests_iterator.buffer), 10)

    def test_memory_cap_limited(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited, [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])], 5
        )
        self.assertEqual(len(requests_iterator.buffer), 5)

    def test_memory_cap_largest_chunk(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])], 5
        )
        self.assertEqual(len(requests_iterator.buffer), 4)

    def test_memory_cap_largest_chunk_multi_file(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]),
             FileChunks.contiguous(18, "b.txt", 10, [1, 2, 7, 4])],
            5,
        )
        self.assertEqual(len(requests_iterator.buffer), 7)

    def test_memory_cap_largest_chunk_no_files(self):
        # the largest_chunk branch used to call a bare max() and raise on an empty sequence, while the
        # limited branch guarded with default=0. Both go through _largest_range now.
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [], None
        )
        self.assertEqual(len(requests_iterator.buffer), 0)
        self.assertIsNone(requests_iterator.next_request())

    def test_memory_cap_largest_chunk_empty_shard(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk,
            [FileChunks(17, "empty.txt", [], []), FileChunks.contiguous(18, "a.txt", 10, [4])],
            None,
        )
        self.assertEqual(len(requests_iterator.buffer), 4)

    def test_memory_cap_largest_chunk_only_zero_sized_ranges(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [FileChunks.contiguous(17, "zeros.txt", 10, [0, 0])], None
        )
        self.assertEqual(len(requests_iterator.buffer), 0)

        files_requests = requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(files_requests.num_ranges, 2)

    def test_limited_memory_cap_and_smaller_chunks(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2]), FileChunks.contiguous(18, "b.txt", 10, [3, 4])],
            50,
        )
        self.assertEqual(len(requests_iterator.buffer), 10)

    def test_range_dsts_pack_the_request(self):
        # replaces the old per-file file_buffers: destinations are per range now, packed back to back
        # across the whole request, and handed to runai_request as absolute addresses.
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]),
             FileChunks.contiguous(18, "b.txt", 10, [1, 2, 7, 4])],
            5,
        )
        self.assertEqual(len(requests_iterator.buffer), 7)
        base = requests_iterator.buffer_address

        files_requests = requests_iterator.next_request()
        self.assertEqual([f.id for f in files_requests.files], [17])
        self.assertEqual(files_requests.range_dsts, [base, base + 1, base + 3])

        files_requests = requests_iterator.next_request()
        self.assertEqual([f.id for f in files_requests.files], [17, 18])
        # a.txt's remaining 4 bytes, then b.txt's 1 and 2 - one destination per range, and the buffer
        # is reused from its start for every request
        self.assertEqual(files_requests.range_dsts, [base, base + 4, base + 5])
        self.assertEqual(files_requests.file_base, [0, 1])

    def test_get_global_file_and_chunk_aliases_the_buffer(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2]), FileChunks.contiguous(18, "b.txt", 10, [3, 4])],
            5,
        )
        self.assertEqual(len(requests_iterator.buffer), 10)

        requests_iterator.next_request()
        requests_iterator.buffer[0] = 9
        requests_iterator.buffer[3] = 8

        file_id, chunk_index, view = requests_iterator.get_global_file_and_chunk(0, 0)
        self.assertEqual((file_id, chunk_index), (17, 0))
        self.assertEqual(len(view), 1)
        self.assertEqual(view[0], 9)

        file_id, chunk_index, view = requests_iterator.get_global_file_and_chunk(1, 0)
        self.assertEqual((file_id, chunk_index), (18, 0))
        self.assertEqual(len(view), 3)
        self.assertEqual(view[0], 8)

    def test_get_global_file_and_chunk_zero_sized_range(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [FileChunks.contiguous(17, "a.txt", 10, [5, 0, 3])], None
        )

        requests_iterator.next_request()

        file_id, chunk_index, view = requests_iterator.get_global_file_and_chunk(0, 1)
        self.assertEqual((file_id, chunk_index), (17, 1))
        self.assertEqual(len(view), 0)

        # the zero sized range consumes no buffer, so the range after it starts where it did
        _, _, view = requests_iterator.get_global_file_and_chunk(0, 2)
        self.assertEqual(len(view), 3)
