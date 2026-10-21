#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Test registry index selection without pulling or publishing container images."""

import itertools
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
IMAGE = "ghcr.io/dsx-ai-factory/model-streamer/devcontainer:release-2.19"
DIGEST = "sha256:" + "a" * 64


class ImageResolutionChecks:
    """Verify registry failures and platform selection without a Docker daemon."""

    def setUp(self):
        """Create a Docker stub that records calls and returns a registry fixture."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.trace = self.directory / "calls"
        self.output = self.directory / "output"
        workflow = (ROOT / ".github/workflows" / self.workflow).read_text()
        step = workflow.split("      id: " + self.step_id + "\n", 1)[1]
        step = step.split("\n    - name:", 1)[0]
        lines = step.split("      run: |\n", 1)[1].splitlines(keepends=True)
        # Execute the actual indented shell block, without requiring a YAML dependency.
        self.script = textwrap.dedent("".join(itertools.takewhile(
            lambda line: not line.strip() or line.startswith("        "), lines
        )))
        docker = self.directory / "docker"
        docker.write_text(
            '#!/bin/sh\n'
            'printf "%s\\n" "$@" >> "$DOCKER_TRACE"\n'
            'printf "%s\\n" "$TEST_MANIFEST"\n'
            'exit "${DOCKER_EXIT_CODE:-0}"\n'
        )
        docker.chmod(0o755)

    def resolve(self, manifest, docker_exit=0):
        """Execute the workflow step with a JSON fixture or raw registry response."""
        self.output.write_text("")
        return subprocess.run(
            ["bash", "-euo", "pipefail", "-c", self.script],
            cwd=ROOT,
            env={
                **os.environ,
                "PATH": str(self.directory) + os.pathsep + os.environ["PATH"],
                "DOCKER_TRACE": str(self.trace),
                "BASELINE_IMAGE": IMAGE,
                "BASE_BRANCH": "release/2.19",
                "VERSION": "2.19.1",
                "GITHUB_REPOSITORY": "DSX-AI-Factory/Model-Streamer",
                "GITHUB_OUTPUT": str(self.output),
                "GITHUB_STEP_SUMMARY": str(self.directory / "summary"),
                "TEST_MANIFEST": manifest if isinstance(manifest, str) else json.dumps(manifest),
                "DOCKER_EXIT_CODE": str(docker_exit),
            },
            capture_output=True,
            text=True,
        )

    def index(self, architectures=("amd64", "arm64"), **overrides):
        """Build an index fixture with the requested Linux platform descriptors."""
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
        """Return the index digest, retaining native selection for both runners."""
        for media_type in (
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
        ):
            with self.subTest(media_type=media_type):
                result = self.resolve(self.index(mediaType=media_type))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.output.read_text(), "image=" + IMAGE.rsplit(":", 1)[0] + "@" + DIGEST + "\n")
        self.assertEqual(
            self.trace.read_text().splitlines(),
            ["buildx", "imagetools", "inspect", IMAGE, "--format", "{{json .Manifest}}"] * 2,
        )

    def test_allows_attestation_descriptors(self):
        """Ignore non-Linux attestation entries when checking required platforms."""
        manifest = self.index()
        manifest["manifests"].append({"platform": {"os": "unknown", "architecture": "unknown"}})
        self.assertEqual(self.resolve(manifest).returncode, 0)

    def test_allows_additional_linux_platforms(self):
        """Require AMD64 and ARM64 without rejecting otherwise usable indexes."""
        result = self.resolve(self.index(("amd64", "arm64", "s390x")))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.read_text(), "image=" + IMAGE.rsplit(":", 1)[0] + "@" + DIGEST + "\n")

    def test_rejects_malformed_registry_responses(self):
        """Never export an image reference from empty or malformed JSON."""
        for manifest in ("", "not json", "{"):
            with self.subTest(manifest=manifest):
                result = self.resolve(manifest)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.output.read_text(), "")

    def test_rejects_incomplete_or_invalid_indexes(self):
        """Reject legacy images, missing Linux platforms, and invalid digests."""
        for manifest in (
            self.index(("amd64",)),
            self.index(("arm64",)),
            self.index(()),
            self.index(("not-amd64", "not-arm64")),
            self.index(digest="sha256:invalid"),
            self.index(digest=None),
            self.index(digest=DIGEST + "\n"),
            self.index(mediaType="application/vnd.oci.image.manifest.v1+json"),
            self.index(manifests=[{"platform": {"os": "windows", "architecture": "amd64"}}]),
            self.index(manifests=[
                {"platform": {"os": "linux", "architecture": "amd64"}},
                {"platform": {"os": "windows", "architecture": "arm64"}},
            ]),
            {},
        ):
            with self.subTest(manifest=manifest):
                result = self.resolve(manifest)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.output.read_text(), "")
                self.assertIn("Run Build and Push DevContainer", result.stderr)

    def test_registry_failure_does_not_emit_a_reference(self):
        """Propagate registry failures even if the command emitted valid JSON."""
        result = self.resolve(self.index(), docker_exit=1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_text(), "")
        self.assertIn("Cannot use devcontainer", result.stderr)


class PRImageResolutionTests(ImageResolutionChecks, unittest.TestCase):
    """Exercise the PR setup shell block."""

    workflow = "on-pr.yaml"
    step_id = "published_image"


class ReleaseImageResolutionTests(ImageResolutionChecks, unittest.TestCase):
    """Exercise the release setup shell block."""

    workflow = "on-release.yaml"
    step_id = "image"


if __name__ == "__main__":
    unittest.main()
