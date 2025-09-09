import unittest
from typing import List, Dict, Tuple

# Assuming the classes and functions are in these locations for the test.
# You may need to adjust the import paths based on your project structure.
from runai_model_streamer.distributed_streamer.partition import (
    partition_by_chunks,
    partition_by_files,
)
from runai_model_streamer.file_streamer import FileChunks

class TestPartitioning(unittest.TestCase):
    def setUp(self):
        """Set up a standard set of requests for use in multiple tests."""
        self.requests: List[FileChunks] = [
            FileChunks(0, path="file_A.dat", offset=1000, chunks=[100, 50, 200]), # Total: 350
            FileChunks(1, path="file_B.dat", offset=0, chunks=[400]),             # Total: 400
            FileChunks(2, path="file_A.dat", offset=5000, chunks=[80, 20]),       # Total: 100
            FileChunks(3, path="file_C.dat", offset=800, chunks=[300, 150]),      # Total: 450
        ]
        self.total_size = sum(sum(r.chunks) for r in self.requests) # 1300

    def _get_partition_total_size(self, partition: List[Tuple[FileChunks, dict]]) -> int:
        """Helper to calculate the total byte size of a single partition."""
        return sum(sum(fc.chunks) for fc, _ in partition)

    def _verify_all_chunks_present(self, original_requests: List[FileChunks], partitions: List[List[Tuple[FileChunks, dict]]]):
        """
        Verifies that every individual chunk from the original requests is present
        in the output partitions, preserving path, offset, and size.
        """
        original_chunks_set = set()
        for req in original_requests:
            current_offset = req.offset
            for chunk_size in req.chunks:
                # The partition function intentionally ignores zero-sized chunks,
                # so we must ignore them in our verification set as well.
                if chunk_size > 0:
                    original_chunks_set.add((req.path, current_offset, chunk_size))
                current_offset += chunk_size

        output_chunks_set = set()
        for partition in partitions:
            for fc, _ in partition:
                current_offset = fc.offset
                for chunk_size in fc.chunks:
                    output_chunks_set.add((fc.path, current_offset, chunk_size))
                    current_offset += chunk_size
        
        self.assertEqual(original_chunks_set, output_chunks_set, 
                         "All original non-zero chunks must be present in the output partitions with correct path, offset, and size")

    def _verify_chunk_maps(self, original_requests: List[FileChunks], partitions: List[List[Tuple[FileChunks, dict]]]):
        """Verifies the correctness of the source maps for partitioned chunks."""
        total_mapped_chunks = 0
        original_chunk_data = {}
        for req_idx, req in enumerate(original_requests):
            current_offset = req.offset
            for chunk_idx, chunk_size in enumerate(req.chunks):
                if chunk_size > 0:
                    original_chunk_data[(req_idx, chunk_idx)] = (req.path, current_offset, chunk_size)
                current_offset += chunk_size
        
        for partition in partitions:
            for new_fc, source_map in partition:
                total_mapped_chunks += len(source_map)
                new_fc_offset = new_fc.offset
                for new_chunk_idx, (orig_req_idx, orig_chunk_idx, map_chunk_size) in source_map.items():
                    original_pos_tuple = (orig_req_idx, orig_chunk_idx)
                    self.assertIn(original_pos_tuple, original_chunk_data)
                    
                    orig_path, orig_offset, orig_size = original_chunk_data[original_pos_tuple]
                    new_chunk_size = new_fc.chunks[new_chunk_idx]

                    self.assertEqual(new_chunk_size, orig_size, "Chunk size in new object must match original")
                    self.assertEqual(map_chunk_size, orig_size, "Chunk size in map must match original")
                    
                    new_chunk_internal_offset = sum(new_fc.chunks[:new_chunk_idx])
                    new_chunk_abs_offset = new_fc_offset + new_chunk_internal_offset
                    self.assertEqual(new_chunk_abs_offset, orig_offset, "Absolute offset must match original")
                    self.assertEqual(new_fc.path, orig_path, "Path must match original")

        self.assertEqual(len(original_chunk_data), total_mapped_chunks, "Total number of mapped chunks must equal total original non-zero chunks")

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

        # Test case 3: Test re-merging of contiguous chunks
        contiguous_requests = [
            FileChunks(0, path="A", offset=0, chunks=[10, 20]),
            FileChunks(1, path="A", offset=30, chunks=[5])
        ]
        partitions = partition_by_chunks(contiguous_requests, 1)
        self.assertEqual(len(partitions[0]), 1, "All contiguous chunks should be merged")
        
        new_fc, source_map = partitions[0][0]
        self.assertEqual(new_fc.offset, 0)
        self.assertListEqual(new_fc.chunks, [10, 20, 5])
        
        self._verify_all_chunks_present(contiguous_requests, partitions)
        self._verify_chunk_maps(contiguous_requests, partitions)

    def test_partition_by_chunks_with_zero_size_chunks(self):
        """Tests that zero-sized chunks are correctly handled (ignored)."""
        requests_with_zero = [
            FileChunks(0, path="Z.dat", offset=0, chunks=[10, 50, 0, 100]),
            FileChunks(1, path="Y.dat", offset=10, chunks=[0, 0, 25])
        ]
        n = 2
        partitions = partition_by_chunks(requests_with_zero, n)

        self.assertEqual(len(partitions), n)
        total_size = sum(self._get_partition_total_size(p) for p in partitions)
        expected_size = (10 + 50 + 100) + 25
        self.assertEqual(total_size, expected_size)

        self._verify_all_chunks_present(requests_with_zero, partitions)
        self._verify_chunk_maps(requests_with_zero, partitions)

    def tearDown(self):
        """No cleanup needed for these tests."""
        pass

if __name__ == "__main__":
    unittest.main()

