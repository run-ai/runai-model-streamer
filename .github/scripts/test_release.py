# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the release workflow's shell steps and tag validation locally."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

VERSION = "0.17.2"
WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "on-release.yaml"


def workflow_script(name):
    lines = WORKFLOW.read_text().splitlines()
    start = lines.index("    - name: " + name)
    run = next(i for i in range(start, len(lines)) if lines[i] == "      run: |")
    script = []
    for line in lines[run + 1:]:
        if line.strip() and not line.startswith("        "):
            break
        script.append(line[8:])
    return "\n".join(script)


class ReleaseBundleTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="release-bundle-test-"))
        self.paths = []
        for package in ("runai_model_streamer", "runai_model_streamer_s3",
                        "runai_model_streamer_gcs", "runai_model_streamer_azure"):
            directory = self.root / "packages" / package / "dist"
            directory.mkdir(parents=True)
            for arch in ("x86_64", "aarch64"):
                path = directory / f"{package}-{VERSION}-py3-none-manylinux_2_30_{arch}.whl"
                path.write_bytes(b"fixture wheel content")
                self.paths.append(path)

    def collect(self):
        return subprocess.run(["bash", "-c", workflow_script("Collect wheels and generate checksums")],
                              cwd=self.root, capture_output=True, text=True,
                              env={**os.environ, "PACKAGE_VERSION": VERSION,
                                   "GITHUB_STEP_SUMMARY": str(self.root / "summary")})

    def verify(self):
        return subprocess.run(["sha256sum", "--check", "../SHA256SUMS"],
                              cwd=self.root / "release-bundle" / "wheels",
                              capture_output=True, text=True)

    def test_complete_bundle_and_checksums(self):
        result = self.collect()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(list((self.root / "release-bundle" / "wheels").glob("*.whl"))), 8)
        self.assertEqual(self.verify().returncode, 0)
        for path in self.paths:
            self.assertEqual(path.read_bytes(),
                             (self.root / "release-bundle" / "wheels" / path.name).read_bytes())

    def test_missing_architecture_rejected(self):
        # Keep the fixture file, but make it unavailable to the wheel glob.
        self.paths[0].rename(self.paths[0].with_suffix(".unavailable"))
        self.assertNotEqual(self.collect().returncode, 0)

    def test_wrong_version_rejected(self):
        path = self.paths[0]
        path.rename(path.with_name(path.name.replace(VERSION, "0.17.1")))
        self.assertNotEqual(self.collect().returncode, 0)

    def test_duplicate_platform_wheel_rejected(self):
        path = self.paths[0]
        path.with_name(path.name.replace("manylinux_2_30", "manylinux2014")).write_bytes(b"duplicate")
        self.assertNotEqual(self.collect().returncode, 0)

    def test_checksum_detects_modified_wheel(self):
        self.assertEqual(self.collect().returncode, 0)
        (self.root / "release-bundle" / "wheels" / self.paths[0].name).write_bytes(b"changed")
        self.assertNotEqual(self.verify().returncode, 0)


class ReleaseTagTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="release-tag-test-"))
        self.git("init", "-b", "master")
        self.git("config", "user.name", "Release test")
        self.git("config", "user.email", "release@example.com")
        self.git("commit", "--allow-empty", "-m", "Initial")
        self.git("branch", "release/0.17")
        self.git("remote", "add", "origin", str(self.root))
        self.git("tag", "-a", VERSION, "-m", "Release")

    def git(self, *args):
        return subprocess.run(["git", *args], cwd=self.root, check=True, capture_output=True, text=True)

    def validate(self, version=VERSION):
        output = self.root / "output"
        return subprocess.run(["bash", str(Path(__file__).with_name("validate-release.sh").resolve())],
                              cwd=self.root, capture_output=True, text=True,
                              env={**os.environ, "GITHUB_REF_NAME": version, "GITHUB_REF_TYPE": "tag",
                                   "GITHUB_OUTPUT": str(output)})

    def test_annotated_tag_on_release_branch(self):
        result = self.validate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("branch=release/0.17", (self.root / "output").read_text())

    def test_commit_outside_release_branch_rejected(self):
        self.git("commit", "--allow-empty", "-m", "Master only")
        self.git("tag", "0.17.3")
        result = self.validate("0.17.3")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must point", result.stdout)

    def test_missing_release_branch_rejected(self):
        self.git("tag", "0.18.0")
        self.assertNotEqual(self.validate("0.18.0").returncode, 0)

    def test_lightweight_tag_on_release_branch(self):
        self.git("tag", "0.17.3")
        self.assertEqual(self.validate("0.17.3").returncode, 0)

    def test_tag_different_from_checked_out_commit_rejected(self):
        self.git("commit", "--allow-empty", "-m", "Later commit")
        self.assertNotEqual(self.validate().returncode, 0)

    def test_invalid_tag_rejected_before_fetch(self):
        for version in ("v0.17.2", "0.17.2rc1", "0.17.02", "0.17.2; false"):
            with self.subTest(version=version):
                result = self.validate(version)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("form X.Y.Z", result.stdout)


if __name__ == "__main__":
    unittest.main()
