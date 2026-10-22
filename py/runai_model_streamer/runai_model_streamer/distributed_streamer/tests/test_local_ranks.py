# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import unittest
from unittest.mock import MagicMock, patch

from runai_model_streamer.distributed_streamer import distributed_streamer
from runai_model_streamer.distributed_streamer.distributed_streamer import _distributedStreamerParams

PROCESS_GROUP_SIZE = "RUNAI_STREAMER_PROCESS_GROUP_SIZE"


def fake_dist(initialized: bool = True, world_size: int = 8, rank: int = 3) -> MagicMock:
    dist = MagicMock()
    dist.is_initialized.return_value = initialized
    dist.get_world_size.return_value = world_size
    dist.get_rank.return_value = rank
    return dist


class TestSetLocalRanks(unittest.TestCase):
    """find_local_ranks() is a collective over all ranks, skipped for non distributed streaming once
    RUNAI_STREAMER_PROCESS_GROUP_SIZE is set. These cover when the count is written."""

    def set_local_ranks(self, is_distributed: bool, dist: MagicMock, discovered=(8, 3, [list(range(8))])):
        params = _distributedStreamerParams()
        with patch.object(distributed_streamer, "dist", dist), \
             patch.object(params, "find_local_ranks", return_value=discovered) as find_local_ranks:
            params.set_local_ranks(is_distributed)
        return params, find_local_ranks

    @patch.dict(os.environ, {}, clear=True)
    def test_non_distributed_discovers_when_the_count_is_unset(self):
        # the first request of a load is a non distributed metadata read, and it fixes the queue depth
        # of the handle, so the count has to be written before it
        params, find_local_ranks = self.set_local_ranks(False, fake_dist())

        find_local_ranks.assert_called_once()
        self.assertEqual(params.num_processes_on_node, 8)
        self.assertEqual(os.environ[PROCESS_GROUP_SIZE], "8")

    @patch.dict(os.environ, {}, clear=True)
    def test_the_count_is_not_written_without_a_collective(self):
        # a rank streaming alone before the process group exists must not set the count, or it would
        # skip a later discovery that its peers enter
        for dist in (fake_dist(initialized=False), fake_dist(world_size=1, rank=0)):
            with self.subTest(initialized=dist.is_initialized.return_value):
                self.set_local_ranks(False, dist, discovered=(1, 0, [[0]]))

                self.assertNotIn(PROCESS_GROUP_SIZE, os.environ)


if __name__ == "__main__":
    unittest.main()
