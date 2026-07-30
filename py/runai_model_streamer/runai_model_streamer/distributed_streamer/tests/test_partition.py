import unittest
from collections import Counter
from typing import List, Dict, Tuple

# Assuming the classes and functions are in these locations for the test.
# You may need to adjust the import paths based on your project structure.
from runai_model_streamer.distributed_streamer.partition import (
    partition_by_chunks,
    partition_by_files,
    get_total_number_of_chunks,
)
from runai_model_streamer.file_streamer import FileChunks

class TestPartitioning(unittest.TestCase):
    def setUp(self):
        """Set up a standard set of requests for use in multiple tests."""
        self.requests: List[FileChunks] = [
            FileChunks.contiguous(0, path="file_A.dat", offset=1000, sizes=[100, 50, 200]), # Total: 350
            FileChunks.contiguous(1, path="file_B.dat", offset=0, sizes=[400]),             # Total: 400
            FileChunks.contiguous(2, path="file_A.dat", offset=5000, sizes=[80, 20]),       # Total: 100
            FileChunks.contiguous(3, path="file_C.dat", offset=800, sizes=[300, 150]),      # Total: 450
        ]
        self.total_size = sum(r.total_size() for r in self.requests) # 1300

    def _get_partition_total_size(self, partition: List[Tuple[FileChunks, dict]]) -> int:
        """Helper to calculate the total byte size of a single partition."""
        return sum(fc.total_size() for fc, _ in partition)

    def _verify_all_chunks_present(self, original_requests: List[FileChunks], partitions: List[List[Tuple[FileChunks, dict]]]):
        """
        Verifies that every individual range from the original requests is present in the output
        partitions, preserving path, offset, and size.

        Counted as a multiset, not a set: zero-sized ranges share an offset with the range that
        follows them, so two of them at the same offset are indistinguishable as tuples and a set
        would silently collapse them.
        """
        def census(requests):
            counter = Counter()
            for req in requests:
                for offset, size in zip(req.offsets, req.sizes):
                    counter[(req.path, offset, size)] += 1
            return counter

        original = census(original_requests)
        output = census([fc for partition in partitions for fc, _ in partition])

        self.assertEqual(original, output,
                         "Every original range must be present in the output partitions with correct path, offset, and size")

    def _verify_chunk_maps(self, original_requests: List[FileChunks], partitions: List[List[Tuple[FileChunks, dict]]]):
        """Verifies the correctness of the source maps for partitioned ranges."""
        total_mapped_chunks = 0
        original_chunk_data = {}
        for req in original_requests:
            for chunk_idx, (offset, size) in enumerate(zip(req.offsets, req.sizes)):
                original_chunk_data[(req.id, chunk_idx)] = (req.path, offset, size)

        for partition in partitions:
            for new_fc, source_map in partition:
                total_mapped_chunks += len(source_map)
                for new_chunk_idx, (orig_req_idx, orig_chunk_idx, map_chunk_size) in source_map.items():
                    original_pos_tuple = (orig_req_idx, orig_chunk_idx)
                    self.assertIn(original_pos_tuple, original_chunk_data)

                    orig_path, orig_offset, orig_size = original_chunk_data[original_pos_tuple]

                    self.assertEqual(new_fc.sizes[new_chunk_idx], orig_size, "Range size in new object must match original")
                    self.assertEqual(map_chunk_size, orig_size, "Range size in map must match original")
                    # the offset is carried, not derived from a prefix sum over the preceding sizes
                    self.assertEqual(new_fc.offsets[new_chunk_idx], orig_offset, "Offset must match original")
                    self.assertEqual(new_fc.path, orig_path, "Path must match original")

        self.assertEqual(len(original_chunk_data), total_mapped_chunks, "Total number of mapped ranges must equal total original ranges")

    def test_partition_by_files(self):
        """Tests for the partition_by_files function."""
        # Test case 1: Basic partitioning into 3 parts
        n = 3
        partitions = partition_by_files(self.requests, n)

        self.assertEqual(len(partitions), n, "Should return the correct number of partitions")

        partition_sizes = [self._get_partition_total_size(p) for p in partitions]
        self.assertEqual(sum(partition_sizes), self.total_size, "Total size should be conserved")
        self.assertCountEqual(partition_sizes, [450, 400, 450])

        self._verify_all_chunks_present(self.requests, partitions)
        self._verify_chunk_maps(self.requests, partitions)

        # Test case 2: Partitioning into 1 part
        n = 1
        partitions = partition_by_files(self.requests, n)
        self.assertEqual(len(partitions), n)
        self.assertEqual(self._get_partition_total_size(partitions[0]), self.total_size)

        self._verify_all_chunks_present(self.requests, partitions)
        self._verify_chunk_maps(self.requests, partitions)

        # Test case 3: Empty input list
        partitions = partition_by_files([], n)
        self.assertEqual(len(partitions), n)
        self.assertTrue(all(len(p) == 0 for p in partitions))

        # Test case 4: More partitions than requests
        n = 5
        partitions = partition_by_files(self.requests, n)
        self.assertEqual(len(partitions), n)
        self.assertEqual(sum(self._get_partition_total_size(p) for p in partitions), self.total_size)
        self.assertTrue(any(len(p) == 0 for p in partitions))

        self._verify_all_chunks_present(self.requests, partitions)
        self._verify_chunk_maps(self.requests, partitions)

    def test_partition_by_chunks(self):
        """Tests for the partition_by_chunks function."""
        # Test case 1: Basic partitioning into 3 parts
        n = 3
        partitions = partition_by_chunks(self.requests, n)

        self.assertEqual(len(partitions), n, "Should return the correct number of partitions")

        partition_sizes = [self._get_partition_total_size(p) for p in partitions]
        self.assertEqual(sum(partition_sizes), self.total_size, "Total size should be conserved")
        self.assertCountEqual(partition_sizes, [450, 420, 430])

        self._verify_all_chunks_present(self.requests, partitions)
        self._verify_chunk_maps(self.requests, partitions)

        # Test case 2: Partitioning into 1 part
        n = 1
        partitions = partition_by_chunks(self.requests, n)
        self.assertEqual(len(partitions), n)
        self.assertEqual(self._get_partition_total_size(partitions[0]), self.total_size)

        self._verify_all_chunks_present(self.requests, partitions)
        self._verify_chunk_maps(self.requests, partitions)

    def test_one_file_chunks_per_path_per_rank(self):
        """A rank's ranges for a path are emitted as a single FileChunks, whether or not they are
        contiguous. Previously the reconstruction split them into one FileChunks per contiguous run,
        which on a 66-shard model produced 16308 entries with a mean run length of 1.14."""
        contiguous_requests = [
            FileChunks.contiguous(0, path="A", offset=0, sizes=[10, 20]),
            FileChunks.contiguous(1, path="A", offset=30, sizes=[5]),
        ]
        partitions = partition_by_chunks(contiguous_requests, 1)
        self.assertEqual(len(partitions[0]), 1)

        new_fc, _ = partitions[0][0]
        self.assertListEqual(new_fc.offsets, [0, 10, 30])
        self.assertListEqual(new_fc.sizes, [10, 20, 5])

        self._verify_all_chunks_present(contiguous_requests, partitions)
        self._verify_chunk_maps(contiguous_requests, partitions)

    def test_non_contiguous_ranges_share_one_file_chunks(self):
        """Ranges carry their own offsets, so a gap no longer forces a second FileChunks."""
        gapped_requests = [
            FileChunks.contiguous(0, path="A", offset=0, sizes=[10]),
            FileChunks.contiguous(1, path="A", offset=9000, sizes=[10]),
        ]
        partitions = partition_by_chunks(gapped_requests, 1)
        self.assertEqual(len(partitions[0]), 1)

        new_fc, _ = partitions[0][0]
        self.assertListEqual(new_fc.offsets, [0, 9000])
        self.assertListEqual(new_fc.sizes, [10, 10])

        self._verify_all_chunks_present(gapped_requests, partitions)
        self._verify_chunk_maps(gapped_requests, partitions)

    def test_ranges_are_sorted_by_offset(self):
        """Sorting is what lets the C++ assigner coalesce a rank's ranges back into contiguous
        transfers - it only merges ranges that arrive in ascending file order."""
        requests = [FileChunks(0, path="A", offsets=[500, 0, 250], sizes=[10, 10, 10])]
        partitions = partition_by_chunks(requests, 1)

        new_fc, _ = partitions[0][0]
        self.assertListEqual(new_fc.offsets, [0, 250, 500])

    def test_partition_by_chunks_with_zero_size_chunks(self):
        """Zero-sized ranges are kept, not dropped.

        A zero-element tensor is a real safetensors entry that the reference implementation yields
        with its shape intact, and the C++ layer answers one response per range including this one.
        Dropping them here would make the distributed path yield fewer tensors than the
        single-process path.
        """
        requests_with_zero = [
            FileChunks.contiguous(0, path="Z.dat", offset=0, sizes=[10, 50, 0, 100]),
            FileChunks.contiguous(1, path="Y.dat", offset=10, sizes=[0, 0, 25]),
        ]
        n = 2
        partitions = partition_by_chunks(requests_with_zero, n)

        self.assertEqual(len(partitions), n)
        total_size = sum(self._get_partition_total_size(p) for p in partitions)
        expected_size = (10 + 50 + 100) + 25
        self.assertEqual(total_size, expected_size)

        # every range is accounted for, the zero-sized ones included
        self.assertEqual(get_total_number_of_chunks(partitions), 7)

        self._verify_all_chunks_present(requests_with_zero, partitions)
        self._verify_chunk_maps(requests_with_zero, partitions)

    def test_zero_sized_range_sorts_before_a_range_at_the_same_offset(self):
        """A zero-sized range shares the offset of whatever follows it. Sorted after that range it
        would land at the far end of it and break the contiguous transfer twice; sorted before, both
        the file offsets and the destinations stay adjacent and the transfer survives."""
        requests = [FileChunks.contiguous(0, path="A", offset=0, sizes=[10, 0, 50])]
        partitions = partition_by_chunks(requests, 1)

        new_fc, _ = partitions[0][0]
        self.assertListEqual(new_fc.offsets, [0, 10, 10])
        self.assertListEqual(new_fc.sizes, [10, 0, 50])

    def tearDown(self):
        """No cleanup needed for these tests."""
        pass

if __name__ == "__main__":
    unittest.main()
