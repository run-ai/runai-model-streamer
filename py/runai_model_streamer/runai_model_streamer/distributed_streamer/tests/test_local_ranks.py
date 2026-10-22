# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import unittest
from unittest.mock import MagicMock, patch

from runai_model_streamer.distributed_streamer import distributed_streamer
from runai_model_streamer.distributed_streamer.distributed_streamer import (
    DistributedStreamer,
    _distributedStreamerParams,
)
from runai_model_streamer.safetensors_streamer import safetensors_streamer
from runai_model_streamer.safetensors_streamer.safetensors_streamer import SafetensorsStreamer

PROCESS_GROUP_SIZE = "RUNAI_STREAMER_PROCESS_GROUP_SIZE"


class TestLocalRanksDiscovery(unittest.TestCase):
    """find_local_ranks() is a collective over all the ranks of the process group, so only a distributed load,
    which all the ranks join, discovers the local ranks."""

    def setUp(self):
        dist = MagicMock()
        dist.is_initialized.return_value = True
        dist.get_world_size.return_value = 8
        dist.get_rank.return_value = 3
        patches = [
            patch.object(distributed_streamer, "dist", dist),
            patch.object(_distributedStreamerParams, "find_local_ranks", return_value=(8, 3, [list(range(8))])),
            patch.dict(os.environ, {}, clear=True),
        ]
        self.find_local_ranks = [p.start() for p in patches][1]
        for p in patches:
            self.addCleanup(p.stop)

    def test_a_non_distributed_read_counts_the_processes_without_discovery(self):
        # it may run on a single rank, and its idle peers would never join the discovery
        streamer = DistributedStreamer()
        streamer.file_streamer = MagicMock()
        streamer.stream_files([], None, "cpu", False)

        self.find_local_ranks.assert_not_called()
        self.assertEqual(streamer.params.my_global_rank, 3)
        self.assertEqual(os.environ[PROCESS_GROUP_SIZE], "8")

    def test_a_distributed_load_discovers_before_the_metadata_reads(self):
        # the first read fixes the queue depth of the CPP layer, and the metadata reads are not distributed
        def prepare_request(*_args, **_kwargs):
            self.find_local_ranks.assert_called_once()
            self.assertEqual(os.environ[PROCESS_GROUP_SIZE], "8")
            raise StopIteration

        with patch.object(safetensors_streamer.safetensors_pytorch, "prepare_request", side_effect=prepare_request):
            with self.assertRaises(StopIteration):
                SafetensorsStreamer().stream_files(["model.safetensors"], is_distributed=True)


if __name__ == "__main__":
    unittest.main()
