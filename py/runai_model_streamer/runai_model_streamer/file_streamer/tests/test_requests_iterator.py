import logging
import os
import unittest
from unittest.mock import patch
from runai_model_streamer.file_streamer.requests_iterator import (
    FileChunksIterator,
    FilesRequestsIterator,
    FilesRequestsIteratorWithBuffer,
    FileChunks,
    MemoryCapMode,
    RunaiStreamerMemoryLimitException,
    RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME,
    _ring_sizing,
)


class TestRingSizing(unittest.TestCase):
    """The ring's (buffer_size, num_buffers). The DEPTH is configured and the buffer size follows from
    it: B = max(largest_range, budget // N), then N is clamped by what the budget affords and by how
    many requests there will be. Ranges are sized in bytes and the depth is overridden per test so the
    fixtures stay small and readable."""

    def files(self, *sizes_per_file):
        return [
            FileChunks.contiguous(i, f"{i}.txt", 0, list(sizes))
            for i, sizes in enumerate(sizes_per_file)
        ]

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "4"})
    def test_depth_is_the_target_and_the_buffer_follows(self):
        # budget 400 over 4 buffers -> 100 each, and 500 bytes of data means all 4 get used
        self.assertEqual(_ring_sizing(MemoryCapMode.limited, self.files([100] * 5), 400), (100, 4))

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "2"})
    def test_the_depth_target_is_honoured(self):
        # same data and budget as above, but asking for 2 buffers gives 2 twice the size - the whole
        # point of the knob is that it decides the shape while the limit decides the memory
        self.assertEqual(_ring_sizing(MemoryCapMode.limited, self.files([100] * 5), 400), (200, 2))

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "4"})
    def test_largest_range_floors_the_buffer_and_so_caps_the_depth(self):
        # a range carries one destination and cannot span two buffers, so a 250 byte range forces a 250
        # byte buffer even though budget // 4 is 87 - and a 350 byte budget then pays for only one.
        # This is the mechanism that caps reachable depth at budget // largest_range.
        self.assertEqual(
            _ring_sizing(MemoryCapMode.limited, self.files([250], [10] * 10), 1000), (250, 1)
        )

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "4"})
    def test_depth_never_exceeds_the_number_of_requests(self):
        # 150 bytes in 50 byte buffers is 3 requests, so the 4th buffer could never hold one
        self.assertEqual(_ring_sizing(MemoryCapMode.limited, self.files([50] * 3), 10_000), (50, 3))

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "4"})
    def test_a_single_request_stream_gets_a_single_buffer(self):
        # one range, so one request: a ring here would be buffers with nothing to pipeline against.
        # This is what the safetensors metadata reads look like - a handful of bytes, twice per load.
        self.assertEqual(_ring_sizing(MemoryCapMode.limited, self.files([8]), 10_000_000_000), (8, 1))

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "4"})
    def test_unlimited_budgets_the_whole_stream(self):
        # -1 keeps its documented meaning: the budget is the data, so the ring spans it at the target
        # depth - 700 bytes over 4 buffers of 175
        self.assertEqual(_ring_sizing(MemoryCapMode.unlimited, self.files([100] * 7), None), (175, 4))

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "4"})
    def test_largest_chunk_mode_stays_a_single_buffer(self):
        # mode 0 exists to mean minimal memory; a ring would defeat the only reason to ask for it
        self.assertEqual(_ring_sizing(MemoryCapMode.largest_chunk, self.files([10, 40], [25]), None), (40, 1))

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "4"})
    def test_nothing_to_read_gives_one_empty_buffer(self):
        self.assertEqual(_ring_sizing(MemoryCapMode.unlimited, self.files([], []), None), (0, 1))
        self.assertEqual(_ring_sizing(MemoryCapMode.unlimited, self.files([0, 0]), None), (0, 1))

    def test_limit_below_largest_range_is_rejected(self):
        with self.assertRaises(RunaiStreamerMemoryLimitException):
            _ring_sizing(MemoryCapMode.limited, self.files([250]), 100)

    def test_limited_without_a_limit_is_rejected(self):
        with self.assertRaises(RunaiStreamerMemoryLimitException):
            _ring_sizing(MemoryCapMode.limited, self.files([10]), None)

    def test_the_ring_never_exceeds_the_limit(self):
        """The property that replaced the floor-exceeded warning.

        The old rule had MIN_RING_BUFFERS as a floor that could push N x B past what the caller asked
        for, so it warned. Depth is now clamped by budget // buffer_size instead of floored, which
        makes overrunning the limit unrepresentable - asserted here across the shapes that used to
        produce it (a limit far below the data, a limit equal to one range, awkward divisors)."""
        for depth in ("1", "2", "3", "4", "16"):
            for sizes, limit in (
                ([10] * 20, 60),        # limit far below the data - the old warning case
                ([10] * 20, 10),        # limit exactly one range: one buffer, no more
                ([7] * 13, 100),        # nothing divides evenly
                ([250], 250),           # a single range exactly filling the limit
                ([1] * 100, 999_999),   # limit far above the data
            ):
                with patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: depth}):
                    buffer_size, num_buffers = _ring_sizing(
                        MemoryCapMode.limited, self.files(sizes), limit
                    )
                self.assertLessEqual(
                    buffer_size * num_buffers, limit,
                    f"depth={depth} sizes={sizes[:3]}... limit={limit} -> "
                    f"{num_buffers} x {buffer_size}",
                )
                self.assertGreaterEqual(num_buffers, 1)

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "0"})
    def test_zero_ring_buffers_still_yields_a_ring(self):
        # a hostile 0 must not produce a pool with no buffers to hand out
        _, num_buffers = _ring_sizing(MemoryCapMode.largest_chunk, self.files([10]), None)
        self.assertEqual(num_buffers, 1)
        _, num_buffers = _ring_sizing(MemoryCapMode.unlimited, self.files([10] * 5), None)
        self.assertGreaterEqual(num_buffers, 1)


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

    def test_get_global_file_and_range(self):
        files_requests_iterator = FilesRequestsIterator(3, [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])])

        first = files_requests_iterator.next_request()

        file_id, range_index = files_requests_iterator.get_global_file_and_range(first, 0, 0)
        self.assertEqual(file_id, 17)
        self.assertEqual(range_index, 0)

        file_id, range_index = files_requests_iterator.get_global_file_and_range(first, 0, 1)
        self.assertEqual(file_id, 17)
        self.assertEqual(range_index, 1)

        second = files_requests_iterator.next_request()

        file_id, range_index = files_requests_iterator.get_global_file_and_range(second, 0, 0)
        self.assertEqual(file_id, 17)
        self.assertEqual(range_index, 2)

        # The earlier request is still resolvable now that a later one has been built - each request
        # carries its own range_base rather than reading iterator state that has moved on. This is what
        # allows a response to arrive while a subsequent submission is already in flight.
        self.assertEqual(files_requests_iterator.get_global_file_and_range(first, 0, 1), (17, 1))

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
    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "1"})
    def test_memory_cap_unlimited(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])], 100
        )
        self.assertEqual(requests_iterator.buffer_size, 10)

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "1"})
    def test_memory_cap_limited(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited, [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])], 5
        )
        self.assertEqual(requests_iterator.buffer_size, 5)

    def test_memory_cap_largest_chunk(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4])], 5
        )
        self.assertEqual(requests_iterator.buffer_size, 4)

    def test_memory_cap_largest_chunk_multi_file(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]),
             FileChunks.contiguous(18, "b.txt", 10, [1, 2, 7, 4])],
            5,
        )
        self.assertEqual(requests_iterator.buffer_size, 7)

    def test_memory_cap_largest_chunk_no_files(self):
        # the largest_chunk branch used to call a bare max() and raise on an empty sequence, while the
        # limited branch guarded with default=0. Both go through _largest_range now.
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [], None
        )
        self.assertEqual(requests_iterator.buffer_size, 0)
        self.assertIsNone(requests_iterator.next_request())

    def test_memory_cap_largest_chunk_empty_shard(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk,
            [FileChunks(17, "empty.txt", [], []), FileChunks.contiguous(18, "a.txt", 10, [4])],
            None,
        )
        self.assertEqual(requests_iterator.buffer_size, 4)

    def test_memory_cap_largest_chunk_only_zero_sized_ranges(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [FileChunks.contiguous(17, "zeros.txt", 10, [0, 0])], None
        )
        self.assertEqual(requests_iterator.buffer_size, 0)

        files_requests = requests_iterator.next_request()
        self.assertIsNotNone(files_requests)
        self.assertEqual(files_requests.num_ranges, 2)

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "1"})
    def test_limited_memory_cap_and_smaller_chunks(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2]), FileChunks.contiguous(18, "b.txt", 10, [3, 4])],
            50,
        )
        self.assertEqual(requests_iterator.buffer_size, 10)

    def test_range_dsts_pack_the_request(self):
        # replaces the old per-file file_buffers: destinations are per range now, packed back to back
        # across the whole request, and handed to runai_request as absolute addresses.
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2, 3, 4]),
             FileChunks.contiguous(18, "b.txt", 10, [1, 2, 7, 4])],
            5,
        )
        self.assertEqual(requests_iterator.buffer_size, 7)
        base = requests_iterator.buffer_addresses[0]

        files_requests = requests_iterator.next_request()
        self.assertEqual([f.id for f in files_requests.files], [17])
        self.assertEqual(files_requests.range_dsts, [base, base + 1, base + 3])

        # largest_chunk mode is a ring of one, so the single buffer has to come back before the next
        # request can be built - and the next request then packs from that same base.
        requests_iterator.release(files_requests)

        files_requests = requests_iterator.next_request()
        self.assertEqual([f.id for f in files_requests.files], [17, 18])
        # a.txt's remaining 4 bytes, then b.txt's 1 and 2 - one destination per range, packed from the
        # start of the buffer this request was given
        self.assertEqual(files_requests.range_dsts, [base, base + 4, base + 5])
        self.assertEqual(files_requests.file_base, [0, 1])

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "1"})
    def test_get_global_file_and_range_aliases_the_buffer(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited,
            [FileChunks.contiguous(17, "a.txt", 10, [1, 2]), FileChunks.contiguous(18, "b.txt", 10, [3, 4])],
            5,
        )
        self.assertEqual(requests_iterator.buffer_size, 10)

        request = requests_iterator.next_request()
        requests_iterator.buffers[0][0] = 9
        requests_iterator.buffers[0][3] = 8

        file_id, range_index, view = requests_iterator.get_global_file_and_range(request, 0, 0)
        self.assertEqual((file_id, range_index), (17, 0))
        self.assertEqual(len(view), 1)
        self.assertEqual(view[0], 9)

        file_id, range_index, view = requests_iterator.get_global_file_and_range(request, 1, 0)
        self.assertEqual((file_id, range_index), (18, 0))
        self.assertEqual(len(view), 3)
        self.assertEqual(view[0], 8)

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "2"})
    def test_consecutive_requests_get_distinct_buffers(self):
        # 3 ranges of 4 bytes and an 8 byte budget over 2 buffers: 4 bytes each, one range per request,
        # and the second request must NOT land in the first one's buffer - that is the whole point of
        # the pool.
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited, [FileChunks.contiguous(17, "a.txt", 10, [4, 4, 4])], 8
        )
        self.assertEqual((requests_iterator.buffer_size, requests_iterator.num_buffers), (4, 2))

        first = requests_iterator.next_request()
        first_index = first.buffer_index
        second = requests_iterator.next_request()
        self.assertNotEqual(first_index, second.buffer_index)
        self.assertNotEqual(first.range_dsts, second.range_dsts)

        # exhausted: a third request has nowhere to go until one comes back
        self.assertFalse(requests_iterator.has_free_buffer())
        with self.assertRaises(RuntimeError):
            requests_iterator.next_request()

        # released, so the third request reuses the first one's buffer and packs from its base
        requests_iterator.release(first)
        third = requests_iterator.next_request()
        self.assertEqual(third.buffer_index, first_index)
        self.assertEqual(third.range_dsts, [requests_iterator.buffer_addresses[first_index]])

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "2"})
    def test_releasing_twice_is_rejected(self):
        # a double release would put the same buffer in the free list twice, so two live requests would
        # silently share it and overwrite each other's data
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited, [FileChunks.contiguous(17, "a.txt", 10, [4, 4])], 8
        )
        request = requests_iterator.next_request()
        requests_iterator.release(request)
        with self.assertRaises(ValueError):
            requests_iterator.release(request)

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "3"})
    def test_buffers_are_distinct_slices_of_one_allocation(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.limited, [FileChunks.contiguous(17, "a.txt", 10, [4, 4, 4])], 12
        )
        self.assertEqual(len(requests_iterator.buffers), 3)
        self.assertEqual(len(requests_iterator.pool), 12)

        # writing through one buffer must not disturb another, and each view must alias the pool
        for index, buffer in enumerate(requests_iterator.buffers):
            buffer[:] = index
        self.assertEqual(list(requests_iterator.pool), [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2])
        self.assertEqual(
            requests_iterator.buffer_addresses,
            [requests_iterator.pool.ctypes.data + 4 * i for i in range(3)],
        )

    def test_end_of_stream_does_not_consume_a_buffer(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.largest_chunk, [], None
        )
        self.assertIsNone(requests_iterator.next_request())
        self.assertTrue(requests_iterator.has_free_buffer())

    @patch.dict(os.environ, {RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "1"})
    def test_get_global_file_and_range_zero_sized_range(self):
        requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_cap(
            MemoryCapMode.unlimited, [FileChunks.contiguous(17, "a.txt", 10, [5, 0, 3])], None
        )

        request = requests_iterator.next_request()

        file_id, range_index, view = requests_iterator.get_global_file_and_range(request, 0, 1)
        self.assertEqual((file_id, range_index), (17, 1))
        self.assertEqual(len(view), 0)

        # the zero sized range consumes no buffer, so the range after it starts where it did
        _, _, view = requests_iterator.get_global_file_and_range(request, 0, 2)
        self.assertEqual(len(view), 3)
