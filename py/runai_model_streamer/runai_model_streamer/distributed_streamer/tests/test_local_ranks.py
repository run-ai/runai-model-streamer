# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from runai_model_streamer.distributed_streamer import distributed_streamer
from runai_model_streamer.distributed_streamer.distributed_streamer import (
    DistributedStreamer,
    _distributedStreamerParams,
)
from runai_model_streamer.safetensors_streamer.safetensors_streamer import SafetensorsStreamer
from runai_model_streamer.safetensors_streamer.tests.test_metadata_ordering import write_safetensors

PROCESS_GROUP_SIZE = "RUNAI_STREAMER_PROCESS_GROUP_SIZE"


class TestDiscoverRanks(unittest.TestCase):
    """find_local_ranks() is a collective over all the ranks of the process group, which a read with
    discover_ranks=False must not enter."""

    def setUp(self):
        self.dist = dist = MagicMock()
        dist.is_initialized.return_value = True
        dist.get_world_size.return_value = 8
        dist.get_rank.return_value = 3
        patches = [
            patch.object(distributed_streamer, "dist", dist),
            patch.object(distributed_streamer, "_estimated_group_size", None),
            patch.object(_distributedStreamerParams, "find_local_ranks", return_value=(8, 3, [list(range(8))])),
            patch.dict(os.environ, {}, clear=True),
        ]
        self.find_local_ranks = [p.start() for p in patches][2]
        for p in patches:
            self.addCleanup(p.stop)

    def test_a_read_without_discovery_counts_the_processes_without_the_collective(self):
        streamer = DistributedStreamer()
        streamer.file_streamer = MagicMock()
        streamer.stream_files([], None, "cpu", False, discover_ranks=False)

        self.find_local_ranks.assert_not_called()
        self.assertEqual(streamer.params.my_global_rank, 3)
        self.assertEqual(os.environ[PROCESS_GROUP_SIZE], "8")

    def test_a_discovery_replaces_an_estimate(self):
        # on 2 nodes without LOCAL_WORLD_SIZE the estimate is the world size, twice the processes on each node
        self.dist.get_world_size.return_value = 16
        _distributedStreamerParams().set_params([], discover_ranks=False)
        self.assertEqual(os.environ[PROCESS_GROUP_SIZE], "16")

        _distributedStreamerParams().set_params([], discover_ranks=True)
        self.assertEqual(os.environ[PROCESS_GROUP_SIZE], "8")

    @patch.dict(os.environ, {PROCESS_GROUP_SIZE: "4"})
    def test_a_discovery_keeps_a_value_set_by_the_user(self):
        _distributedStreamerParams().set_params([], discover_ranks=True)
        self.assertEqual(os.environ[PROCESS_GROUP_SIZE], "4")

    def test_safetensors_streamer_discovers_only_for_a_distributed_load(self):
        # a non distributed load may run on a single rank. A distributed load discovers in its first metadata read,
        # which fixes the queue depth of the CPP layer
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, temp_dir, ignore_errors=True)
        path = os.path.join(temp_dir, "model.safetensors")
        write_safetensors(path, [("t", {"dtype": "U8", "shape": [4], "data_offsets": [0, 4]})], b"\x00\x01\x02\x03")

        for is_distributed, discoveries in ((False, 0), (True, 1)):
            with self.subTest(is_distributed=is_distributed):
                self.find_local_ranks.reset_mock()
                with SafetensorsStreamer() as streamer:
                    streamer.stream_files([path], is_distributed=is_distributed)
                    self.assertEqual(len(list(streamer.get_tensors())), 1)

                self.assertEqual(self.find_local_ranks.call_count, discoveries)


if __name__ == "__main__":
    unittest.main()
