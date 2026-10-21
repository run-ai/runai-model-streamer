#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Test registry index selection without pulling or publishing container images."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("resolve-devcontainer-image.sh")
IMAGE = "ghcr.io/dsx-ai-factory/model-streamer/devcontainer:release-2.19"
DIGEST = "sha256:" + "a" * 64


class ResolveDevcontainerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.trace = self.directory / "calls"
        docker = self.directory / "docker"
        docker.write_text(
            '#!/bin/sh\n'
            'printf "%s\\n" "$@" >> "$DOCKER_TRACE"\n'
            'printf "%s\\n" "$TEST_MANIFEST"\n'
            'exit "${DOCKER_EXIT_CODE:-0}"\n'
        )
        docker.chmod(0o755)

    def resolve(self, manifest, *args, docker_exit=0):
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            env={
                **os.environ,
                "PATH": str(self.directory) + os.pathsep + os.environ["PATH"],
                "DOCKER_TRACE": str(self.trace),
                "TEST_MANIFEST": json.dumps(manifest),
                "DOCKER_EXIT_CODE": str(docker_exit),
            },
            capture_output=True,
            text=True,
        )

    def index(self, architectures=("amd64", "arm64"), **overrides):
        return {
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "digest": DIGEST,
            "manifests": [
                {"platform": {"os": "linux", "architecture": arch}}
                for arch in architectures
            ],
            **overrides,
        }

    def test_resolves_oci_and_docker_indexes_without_pulling(self):
        for media_type in (
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
        ):
            with self.subTest(media_type=media_type):
                result = self.resolve(self.index(mediaType=media_type), IMAGE)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, IMAGE.rsplit(":", 1)[0] + "@" + DIGEST + "\n")
        self.assertEqual(
            self.trace.read_text().splitlines(),
            ["buildx", "imagetools", "inspect", IMAGE, "--format", "{{json .Manifest}}"] * 2,
        )

    def test_allows_attestation_descriptors(self):
        manifest = self.index()
        manifest["manifests"].append({"platform": {"os": "unknown", "architecture": "unknown"}})
        self.assertEqual(self.resolve(manifest, IMAGE).returncode, 0)

    def test_rejects_incomplete_or_invalid_indexes(self):
        for manifest in (
            self.index(("amd64",)),
            self.index(("arm64",)),
            self.index(()),
            self.index(digest="sha256:invalid"),
            self.index(digest=None),
            self.index(mediaType="application/vnd.oci.image.manifest.v1+json"),
            self.index(manifests=[{"platform": {"os": "windows", "architecture": "amd64"}}]),
            {},
        ):
            with self.subTest(manifest=manifest):
                result = self.resolve(manifest, IMAGE)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn("Run Build and Push DevContainer", result.stderr)

    def test_registry_failure_does_not_emit_a_reference(self):
        result = self.resolve(self.index(), IMAGE, docker_exit=1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("Cannot access devcontainer", result.stderr)

    def test_invalid_arguments_fail_before_registry_access(self):
        for args in ((), (IMAGE, IMAGE), ("ubuntu:latest",), (IMAGE + "@" + DIGEST,)):
            with self.subTest(args=args):
                self.assertEqual(self.resolve({}, *args).returncode, 2)
        self.assertFalse(self.trace.exists())


if __name__ == "__main__":
    unittest.main()
