# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
import tempfile
import unittest
from datetime import timedelta
from unittest.mock import patch

import torch
import torch.distributed as dist
import torch.multiprocessing as mp
from safetensors.torch import save_file

from runai_model_streamer.distributed_streamer.distributed_streamer import DistributedStreamer
from runai_model_streamer import SafetensorsStreamer
from runai_model_streamer.file_streamer.requests_iterator import (
    FileChunks,
    RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME,
    RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME,
    RUNAI_STREAMER_MAX_PADS_PER_BUFFER_ENV_VAR_NAME,
)

RANGE_SIZE = 8

# Two buffers of one range each against six ranges, so the ring has to recycle.
#
# The pad budget has to go. A slot is buffer_size + direct_block * pads, so under the DEFAULT budget
# a slot would be 512 KiB whatever the limit says, and a recycled slot would write each range at a
# fresh offset inside it. The ring would still recycle, but nothing would ever be overwritten, and
# both checks below would then pass with or without the bug.
TIGHT_RING = {
    RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME: "16",
    RUNAI_STREAMER_RING_BUFFERS_ENV_VAR_NAME: "2",
    RUNAI_STREAMER_MAX_PADS_PER_BUFFER_ENV_VAR_NAME: "0",
}


def _read_on_one_rank(rank, rendezvous, path):
    dist.init_process_group("gloo", init_method=rendezvous, rank=rank, world_size=2,
                            timeout=timedelta(seconds=20))
    try:
        if rank == 1:
            with SafetensorsStreamer() as streamer:
                streamer.stream_file(path, device="cpu", is_distributed=False)
                weights = dict(streamer.get_tensors())
            torch.testing.assert_close(weights["weight"], torch.arange(6))
            assert streamer.file_streamer.ring_info()[0] == rank
        dist.barrier()
    finally:
        dist.destroy_process_group()


class TestOwnedFollowsTheDestination(unittest.TestCase):
    """Only a host destination hands its buffers over, and only "cpu" equals "cpu".

    This class covers the entry point where that matters. stream_files decides whether the ring is
    handed over from `owned and device == "cpu"`, so a host written any other way turned `owned=True`
    into a recycled ring under tensors the caller had been promised.

    torch.device("cpu") is the dangerous one: .to() returns the tensor unchanged, so nothing copies
    and the caller is left holding the buffer. The indexed forms copy by accident - torch keeps an
    index the CPU has no use for - which hides the fault rather than fixing it."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def write_ranges(self, count: int):
        """A file of `count` ranges, range i holding exactly "rng{i:05d}"."""
        expected = {i: f"rng{i:05d}" for i in range(count)}
        path = os.path.join(self.temp_dir, "none_device.txt")
        with open(path, "w") as handle:
            handle.write("".join(expected[i] for i in range(count)))
        return path, expected

    @patch.dict(os.environ, {"RUNAI_STREAMER_DIST_TIMEOUT": "10"})
    def test_non_distributed_safetensors_read_does_not_require_peers(self):
        path = os.path.join(self.temp_dir, "rank_local.safetensors")
        save_file({"weight": torch.arange(6)}, path)
        rendezvous = "file://" + os.path.join(self.temp_dir, "process_group")
        mp.spawn(_read_on_one_rank, args=(rendezvous, path), nprocs=2, join=True)

    @patch.dict(os.environ, TIGHT_RING)
    def test_every_host_spelling_hands_the_ring_over(self):
        for device in [None, "cpu", "cpu:0", torch.device("cpu"), torch.device("cpu", 0)]:
            with self.subTest(device=repr(device)):
                path, expected = self.write_ranges(6)
                chunks = [FileChunks.contiguous(17, path, 0, [RANGE_SIZE] * 6)]

                with DistributedStreamer() as streamer:
                    streamer.stream_files(chunks, None, device=device, is_distributed=False, owned=True)
                    inner = streamer.file_streamer
                    self.assertEqual(inner.device_str, "cpu")
                    self.assertTrue(inner.requests_iterator._pool._owned)
                    held = {index: buffer for _, index, buffer in streamer.get_chunks()}

                # Every range got memory of its own. Under a recycled ring these six collapse to
                # two addresses holding the last two ranges twice over.
                self.assertEqual(len({b.data_ptr() for b in held.values()}), 6)

                self.assertEqual(
                    {i: b.numpy().tobytes().decode("utf-8") for i, b in held.items()},
                    expected,
                )

    @patch.dict(os.environ, TIGHT_RING)
    def test_a_device_destination_keeps_the_ring(self):
        """The other half of the mapping. A device destination yields tensor.to(device), a fresh
        allocation the caller already owns, so the ring stays ours to reuse however owned is set.

        Asserted on the ring, which needs no GPU: stream_files builds it, and nothing reaches CUDA
        until the first .to() in get_chunks."""
        for owned in (True, False):
            with self.subTest(owned=owned):
                path, _ = self.write_ranges(6)
                chunks = [FileChunks.contiguous(17, path, 0, [RANGE_SIZE] * 6)]

                with DistributedStreamer() as streamer:
                    streamer.stream_files(chunks, None, device="cuda", is_distributed=False,
                                          owned=owned)
                    inner = streamer.file_streamer

                    self.assertEqual(inner.device_str, "cuda")
                    self.assertFalse(inner.requests_iterator._pool._owned)

                    # Nothing consumed the submission - get_chunks would need a GPU - and the C layer
                    # must not be left holding destinations.
                    inner.drain_live_submissions()


if __name__ == "__main__":
    unittest.main()
