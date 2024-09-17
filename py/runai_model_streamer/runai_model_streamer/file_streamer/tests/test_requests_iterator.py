import unittest
from runai_model_streamer.file_streamer.requests_iterator import (
    RequestsIterator,
    MemoryCapMode,
)


class TestRequestsIterator(unittest.TestCase):
    def test_unlimited_memory_cap(self):
        requests_iterator, memory_size = RequestsIterator.with_memory_cap(
            MemoryCapMode.unlimited, 100, [1, 2, 3, 4]
        )

        self.assertEqual(memory_size, 10)

        request = requests_iterator.next_request()
        self.assertIsNotNone(request)
        self.assertEqual(request.file_offset, 100)
        self.assertEqual(request.chunks, [1, 2, 3, 4])

        request = requests_iterator.next_request()
        self.assertIsNone(request)

    def test_largest_chunk_memory_cap(self):
        requests_iterator, memory_size = RequestsIterator.with_memory_cap(
            MemoryCapMode.largest_chunk, 100, [1, 2, 3, 4]
        )

        self.assertEqual(memory_size, 4)

        request = requests_iterator.next_request()
        self.assertIsNotNone(request)
        self.assertEqual(request.file_offset, 100)
        self.assertEqual(request.chunks, [1, 2])

        request = requests_iterator.next_request()
        self.assertIsNotNone(request)
        self.assertEqual(request.file_offset, 103)
        self.assertEqual(request.chunks, [3])

        request = requests_iterator.next_request()
        self.assertIsNotNone(request)
        self.assertEqual(request.file_offset, 106)
        self.assertEqual(request.chunks, [4])

        request = requests_iterator.next_request()
        self.assertIsNone(request)

    def test_limited_memory_cap(self):
        requests_iterator, memory_size = RequestsIterator.with_memory_cap(
            MemoryCapMode.limited, 100, [1, 2, 3, 4], 6
        )

        self.assertEqual(memory_size, 6)

        request = requests_iterator.next_request()
        self.assertIsNotNone(request)
        self.assertEqual(request.file_offset, 100)
        self.assertEqual(request.chunks, [1, 2, 3])

        request = requests_iterator.next_request()
        self.assertIsNotNone(request)
        self.assertEqual(request.file_offset, 106)
        self.assertEqual(request.chunks, [4])

        request = requests_iterator.next_request()
        self.assertIsNone(request)


if __name__ == "__main__":
    unittest.main()
