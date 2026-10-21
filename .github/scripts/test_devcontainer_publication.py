#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the reusable publication workflow without writing to a registry."""

import itertools
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "ghcr.io/dsx-ai-factory/model-streamer/devcontainer"
AMD64 = REPOSITORY + "@sha256:" + "a" * 64
ARM64 = REPOSITORY + "@sha256:" + "b" * 64
DIGEST = "sha256:" + "c" * 64


class PublicationTests(unittest.TestCase):
    """Validate publication inputs, registry failures, and verified outputs."""

    def setUp(self):
        """Extract the workflow shell block and create a recording Docker stub."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.output = self.directory / "output"
        self.trace = self.directory / "trace"
        workflow = (ROOT / ".github/workflows/publish-devcontainer.yml").read_text()
        step = workflow.split("      id: publish\n", 1)[1]
        lines = step.split("      run: |\n", 1)[1].splitlines(keepends=True)
        self.script = textwrap.dedent("".join(itertools.takewhile(
            lambda line: not line.strip() or line.startswith("        "), lines
        )))
        docker = self.directory / "docker"
        docker.write_text(
            '#!/bin/sh\n'
            'printf "%s\\n" "$*" >> "$DOCKER_TRACE"\n'
            'case "$1 $2 $3" in\n'
            '  "buildx imagetools create") exit "$CREATE_EXIT" ;;\n'
            '  "buildx imagetools inspect") printf "%s\\n" "$MANIFEST"; exit "$INSPECT_EXIT" ;;\n'
            '  *) exit 99 ;;\n'
            'esac\n'
        )
        docker.chmod(0o755)

    def publish(self, **overrides):
        """Execute the actual workflow shell with controlled registry responses."""
        self.output.write_text("")
        self.trace.write_text("")
        manifest = {
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "digest": DIGEST,
            "manifests": [{"platform": {"os": "linux", "architecture": arch}}
                          for arch in ("amd64", "arm64")],
        }
        return subprocess.run(
            ["bash", "-euo", "pipefail", "-c", self.script],
            cwd=ROOT,
            env={
                **os.environ,
                "PATH": str(self.directory) + os.pathsep + os.environ["PATH"],
                "DOCKER_TRACE": str(self.trace),
                "GITHUB_OUTPUT": str(self.output),
                "GITHUB_REPOSITORY": "DSX-AI-Factory/Model-Streamer",
                "BASELINE_TAG": "latest", "AMD64_IMAGE": AMD64, "ARM64_IMAGE": ARM64,
                "CREATE_EXIT": "0", "INSPECT_EXIT": "0", "MANIFEST": json.dumps(manifest),
                **overrides,
            },
            capture_output=True, text=True,
        )

    def test_publishes_saved_digests_and_returns_verified_index(self):
        """Publish latest and release tags using only the saved native references."""
        for tag in ("latest", "release-2.19"):
            with self.subTest(tag=tag):
                result = self.publish(BASELINE_TAG=tag)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.trace.read_text().splitlines(), [
                    f"buildx imagetools create --tag {REPOSITORY}:{tag} {AMD64} {ARM64}",
                    f"buildx imagetools inspect {REPOSITORY}:{tag} --format {{{{json .Manifest}}}}",
                ])
                self.assertEqual(self.output.read_text(),
                                 f"image={REPOSITORY}@{DIGEST}\ndigest={DIGEST}\n")

    def test_rejects_mutable_or_invalid_native_references(self):
        """Reject unsafe inputs before creating or inspecting any manifest."""
        for key, ref in itertools.product(("AMD64_IMAGE", "ARM64_IMAGE"), (
            "", REPOSITORY + ":latest-amd64", REPOSITORY + "@sha256:invalid",
            "ghcr.io/other/image@sha256:" + "a" * 64, AMD64 + "\n",
        )):
            with self.subTest(key=key, ref=ref):
                result = self.publish(**{key: ref})
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.output.read_text(), "")
                self.assertEqual(self.trace.read_text(), "")

    def test_rejects_unsupported_tags(self):
        """Only allow the existing generic branch tag contracts."""
        for tag in ("", "release/2.19", "release-2.19.1", "latest-amd64", "latest\nx=y"):
            with self.subTest(tag=tag):
                result = self.publish(BASELINE_TAG=tag)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.trace.read_text(), "")
                self.assertEqual(self.output.read_text(), "")

    def test_creation_failure_stops_before_verification(self):
        """Do not inspect or return outputs after a failed manifest push."""
        result = self.publish(CREATE_EXIT="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.trace.read_text().splitlines()), 1)
        self.assertEqual(self.output.read_text(), "")

    def test_inspection_failure_does_not_return_outputs(self):
        """Require successful registry verification even if it emits valid JSON."""
        result = self.publish(INSPECT_EXIT="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_text(), "")

    def test_invalid_or_incomplete_index_does_not_return_outputs(self):
        """Reject malformed responses, invalid digests, and missing platforms."""
        for manifest in ("not json", "{}", *(
            json.dumps({"mediaType": "application/vnd.oci.image.index.v1+json",
                        "digest": digest,
                        "manifests": [{"platform": {"os": "linux", "architecture": arch}}
                                      for arch in arches]})
            for arches, digest in ((["amd64"], DIGEST), (["arm64"], DIGEST),
                                   (["amd64", "arm64"], "invalid"))
        )):
            with self.subTest(manifest=manifest):
                result = self.publish(MANIFEST=manifest)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.output.read_text(), "")


if __name__ == "__main__":
    unittest.main()
