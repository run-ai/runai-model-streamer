import unittest
from typing import List

# Assuming the classes and functions are in these locations for the test.
# You may need to adjust the import paths based on your project structure.
from runai_model_streamer.distributed_streamer.partition import (
    partition_by_chunks,
    partition_by_files,
    create_broadcast_plan
)
from runai_model_streamer.file_streamer import FileChunks

class TestPartitioning(unittest.TestCase):
    def setUp(self):
        """Set up a standard set of requests for use in multiple tests."""
        self.requests: List[FileChunks] = [
            FileChunks(path="file_A.dat", offset=1000, chunks=[100, 50, 200]), # Total: 350
            FileChunks(path="file_B.dat", offset=0, chunks=[400]),             # Total: 400
            FileChunks(path="file_A.dat", offset=5000, chunks=[80, 20]),       # Total: 100
            FileChunks(path="file_C.dat", offset=800, chunks=[300, 150]),      # Total: 450
        ]
        self.total_size = sum(sum(r.chunks) for r in self.requests) # 1300

    def _get_partition_total_size(self, partition: List[FileChunks]) -> int:
        """Helper to calculate the total byte size of a single partition."""
        return sum(sum(fc.chunks) for fc in partition)

    def _verify_all_chunks_present(self, original_requests: List[FileChunks], partitions: List[List[FileChunks]]):
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
            for fc in partition:
                current_offset = fc.offset
                for chunk_size in fc.chunks:
                    output_chunks_set.add((fc.path, current_offset, chunk_size))
                    current_offset += chunk_size
        
        self.assertEqual(original_chunks_set, output_chunks_set, 
                         "All original non-zero chunks must be present in the output partitions with correct path, offset, and size")

    def test_partition_by_files(self):
        """Tests for the partition_by_files function."""
        # Test case 1: Basic partitioning into 3 parts
        n = 3
        partitions = partition_by_files(self.requests, n)
        
        self.assertEqual(len(partitions), n, "Should return the correct number of partitions")
        
        partition_sizes = [self._get_partition_total_size(p) for p in partitions]
        self.assertEqual(sum(partition_sizes), self.total_size, "Total size should be conserved")

        # Based on the deterministic algorithm (largest first):
        # Sizes: 450, 400, 350, 100
        # P1 gets 450
        # P2 gets 400
        # P3 gets 350
        # P3 gets 100 (since it's the smallest with 350) -> P3=450
        # Expected final sizes: [450, 400, 450]
        self.assertCountEqual(partition_sizes, [450, 400, 450])

        # Verification: Check that all original FileChunks objects are present
        output_requests = [fc for p in partitions for fc in p]
        self.assertCountEqual(
            [repr(r) for r in self.requests],
            [repr(r) for r in output_requests],
            "All original FileChunks objects must be present in the output"
        )

        # Test case 2: Partitioning into 1 part
        n = 1
        partitions = partition_by_files(self.requests, n)
        self.assertEqual(len(partitions), n)
        self.assertEqual(self._get_partition_total_size(partitions[0]), self.total_size)
        self.assertEqual(len(partitions[0]), len(self.requests))

        # Test case 3: Empty input list
        partitions = partition_by_files([], n)
        self.assertEqual(len(partitions), n)
        self.assertTrue(all(len(p) == 0 for p in partitions))

        # Test case 4: More partitions than requests
        n = 5
        partitions = partition_by_files(self.requests, n)
        self.assertEqual(len(partitions), n)
        self.assertEqual(sum(self._get_partition_total_size(p) for p in partitions), self.total_size)
        # We expect one empty partition
        self.assertTrue(any(len(p) == 0 for p in partitions))


    def test_partition_by_chunks(self):
        """Tests for the partition_by_chunks function."""
        # Test case 1: Basic partitioning into 3 parts
        n = 3
        partitions = partition_by_chunks(self.requests, n)
        
        self.assertEqual(len(partitions), n, "Should return the correct number of partitions")

        partition_sizes = [self._get_partition_total_size(p) for p in partitions]
        self.assertEqual(sum(partition_sizes), self.total_size, "Total size should be conserved")

        # Based on the deterministic algorithm (largest individual chunks first):
        # Chunks: 400, 300, 200, 150, 100, 80, 50, 20 -> Expected sizes: [450, 420, 430]
        self.assertCountEqual(partition_sizes, [450, 420, 430])

        # Verification for this test case
        self._verify_all_chunks_present(self.requests, partitions)

        # Test case 2: Partitioning into 1 part
        n = 1
        partitions = partition_by_chunks(self.requests, n)
        self.assertEqual(len(partitions), n)
        self.assertEqual(self._get_partition_total_size(partitions[0]), self.total_size)
        
        # Verification for this test case
        self._verify_all_chunks_present(self.requests, partitions)

        # Test case 3: Test re-merging of contiguous chunks
        contiguous_requests = [
            FileChunks(path="A", offset=0, chunks=[10, 20]), # These are contiguous
            FileChunks(path="A", offset=30, chunks=[5])      # This is also contiguous
        ]
        partitions = partition_by_chunks(contiguous_requests, 1)
        partition = partitions[0]
        self.assertEqual(len(partition), 1, "All contiguous chunks should be merged")
        
        self.assertEqual(partition[0].offset, 0)
        self.assertListEqual(partition[0].chunks, [10, 20, 5])
        
        # Verification for this test case
        self._verify_all_chunks_present(contiguous_requests, partitions)

    def test_partition_by_chunks_with_zero_size_chunks(self):
        """Tests that zero-sized chunks are correctly handled (ignored)."""
        requests_with_zero = [
            FileChunks(path="Z.dat", offset=0, chunks=[10, 50, 0, 100]),
            FileChunks(path="Y.dat", offset=10, chunks=[0, 0, 25])
        ]
        n = 2
        partitions = partition_by_chunks(requests_with_zero, n)

        self.assertEqual(len(partitions), n)

        # Verify total size is correct (zero chunks should not contribute)
        total_size = sum(self._get_partition_total_size(p) for p in partitions)
        expected_size = (10 + 50 + 100) + 25
        self.assertEqual(total_size, expected_size)

        # Verify that only the non-zero chunks are present in the output
        self._verify_all_chunks_present(requests_with_zero, partitions)

    def test_create_broadcast_plan(self):
        """Tests the round-robin broadcast plan creation."""
        # Create a sample partition structure.
        # Partition 0: 3 chunks
        # Partition 1: 1 chunk
        # Partition 2: 4 chunks
        partitions = [
            [FileChunks("A", 0, [10, 20, 30])],
            [FileChunks("B", 0, [40])],
            [FileChunks("C", 0, [50, 60]), FileChunks("D", 0, [70, 80])]
        ]
        
        plan = create_broadcast_plan(partitions)
        
        # Total chunks = 3 + 1 + 4 = 8. Plan length should be 8.
        self.assertEqual(len(plan), 8)
        
        # Check the number of broadcasts for each process.
        self.assertEqual(plan.count(0), 3)
        self.assertEqual(plan.count(1), 1)
        self.assertEqual(plan.count(2), 4)
        
        # Check the round-robin order.
        # P0, P1, P2
        # P0, P2 (P1 is done)
        # P0, P2
        # P2
        expected_plan = [0, 1, 2, 0, 2, 0, 2, 2]
        self.assertListEqual(plan, expected_plan)

        # Test with empty partitions
        plan_empty = create_broadcast_plan([])
        self.assertListEqual(plan_empty, [])

    def tearDown(self):
        """No cleanup needed for these tests."""
        pass

if __name__ == "__main__":
    unittest.main()
