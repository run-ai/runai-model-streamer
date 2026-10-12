"""Release validation tests; no registry writes or publication."""

import importlib.util
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
import zipfile

spec = importlib.util.spec_from_file_location(
    "release_artifacts", Path(__file__).with_name("release_artifacts.py"))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
VERSION = "0.17.2"


class ReleaseArtifactsTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="release-artifacts-test-"))
        self.source = self.root / "packages"
        self.source.mkdir()
        self.bundle = self.root / "bundle"
        for package in release.PACKAGES:
            for arch in release.ARCHES:
                self.wheel(package, arch)

    def wheel(self, package, arch, metadata_version=VERSION):
        platform = f"manylinux_2_30_{arch}"
        path = self.source / f"{package}-{VERSION}-py3-none-{platform}.whl"
        with zipfile.ZipFile(path, "w") as wheel:
            prefix = f"{package}-{VERSION}.dist-info/"
            wheel.writestr(prefix + "METADATA",
                           f"Name: {package.replace('_', '-')}\nVersion: {metadata_version}\n")
            wheel.writestr(prefix + "WHEEL", f"Tag: py3-none-{platform}\n")
        return path

    def pack(self):
        release.bundle(self.source, self.bundle, VERSION)
        return release.verify(self.bundle, VERSION)

    def test_eight_wheels_round_trip_and_standard_checksum_format(self):
        paths = self.pack()
        self.assertEqual(len(paths), 8)
        for line in (self.bundle / "SHA256SUMS").read_text().splitlines():
            checksum, name = line.split("  ")
            self.assertEqual(checksum, release.digest(self.bundle / "wheels" / name))
            self.assertEqual((self.source / Path(name).name).read_bytes(),
                             (self.bundle / "wheels" / name).read_bytes())

    def test_incomplete_or_duplicate_matrix_rejected(self):
        paths = list(self.source.iterdir())
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            release.validate_wheels(paths[:-1], VERSION)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            release.validate_wheels(paths + paths[:1], VERSION)

    def test_wrong_metadata_version_rejected(self):
        self.wheel(release.PACKAGES[0], release.ARCHES[0], "0.17.1")
        with self.assertRaisesRegex(ValueError, "metadata"):
            self.pack()

    def test_wrong_tag_version_rejected(self):
        for version in ("v0.17.2", "0.17.2rc1", "00.17.2", "0.17"):
            with self.subTest(version=version), self.assertRaises(ValueError):
                release.validate_wheels(list(self.source.iterdir()), version)

    def test_modified_wheel_or_manifest_rejected(self):
        paths = self.pack()
        with paths[0].open("ab") as wheel:
            wheel.write(b"changed after testing")
        with self.assertRaisesRegex(ValueError, "SHA256SUMS"):
            release.verify(self.bundle, VERSION)

    def test_missing_manifest_entry_rejected(self):
        self.pack()
        manifest = self.bundle / "SHA256SUMS"
        manifest.write_text("\n".join(manifest.read_text().splitlines()[1:]) + "\n")
        with self.assertRaisesRegex(ValueError, "SHA256SUMS"):
            release.verify(self.bundle, VERSION)

    def test_retry_stages_only_unpublished_files(self):
        paths = self.pack()
        existing = {"urls": [{"filename": paths[0].name,
                              "digests": {"sha256": release.digest(paths[0])}}]}
        with patch.object(release, "read_json", return_value=existing):
            pending = self.root / "pending"
            self.assertEqual(release.stage_unpublished(paths, VERSION, pending), 7)
            self.assertNotIn(paths[0].name, [p.name for p in pending.iterdir()])

    def test_retry_rejects_different_pypi_bytes(self):
        paths = self.pack()
        existing = {"urls": [{"filename": paths[0].name, "digests": {"sha256": "bad"}}]}
        with patch.object(release, "read_json", return_value=existing):
            with self.assertRaisesRegex(ValueError, "different bytes"):
                release.stage_unpublished(paths, VERSION, self.root / "pending")

    def test_completed_pypi_publication_stages_nothing(self):
        paths = self.pack()
        existing = {"urls": [{"filename": p.name, "digests": {"sha256": release.digest(p)}}
                              for p in paths]}
        with patch.object(release, "read_json", return_value=existing):
            self.assertEqual(release.stage_unpublished(paths, VERSION, self.root / "pending"), 0)

    def test_unexpected_pypi_wheel_rejected(self):
        paths = self.pack()
        existing = {"urls": [{"filename": "unexpected.whl", "digests": {"sha256": "bad"}}]}
        with patch.object(release, "read_json", return_value=existing):
            with self.assertRaisesRegex(ValueError, "unexpected wheels"):
                release.stage_unpublished(paths, VERSION, self.root / "pending")

    def test_http_errors_fail_closed_except_not_found(self):
        for status in (403, 429, 500):
            with self.subTest(status=status), patch.object(
                    release, "urlopen", side_effect=HTTPError("https://example.com", status, "", {}, io.BytesIO())):
                with self.assertRaises(HTTPError):
                    release.read_json("https://example.com")
        with patch.object(release, "urlopen", side_effect=HTTPError("https://example.com", 404, "", {}, io.BytesIO())):
            self.assertIsNone(release.read_json("https://example.com"))

    def test_github_retry_checks_digests_and_uploads_only_missing_assets(self):
        paths = self.pack() + [self.bundle / "SHA256SUMS"]
        existing = {"draft": True, "assets": [
            {"name": paths[0].name, "digest": "sha256:" + release.digest(paths[0])}]}
        with patch.object(release, "github_release", return_value=existing), patch.object(
                release.subprocess, "run") as run:
            release.publish_github("owner/repo", VERSION, paths)
        self.assertEqual(run.call_count, 2)
        upload = run.call_args_list[0].args[0]
        self.assertNotIn(str(paths[0]), upload)
        self.assertIn(str(paths[-1]), upload)
        self.assertNotIn("--clobber", upload)
        existing["assets"][0]["digest"] = "sha256:wrong"
        with self.assertRaisesRegex(ValueError, "GitHub asset differs"):
            release.check_github_assets(existing, paths)

    def test_new_release_is_draft_until_all_assets_uploaded(self):
        paths = self.pack() + [self.bundle / "SHA256SUMS"]
        with patch.object(release, "github_release", return_value=None), patch.object(
                release.subprocess, "run") as run:
            release.publish_github("owner/repo", VERSION, paths)
        self.assertEqual(run.call_count, 3)
        self.assertIn("--verify-tag", run.call_args_list[0].args[0])
        self.assertIn("--draft", run.call_args_list[0].args[0])
        self.assertIn("--draft=false", run.call_args_list[2].args[0])

    def test_failed_github_upload_leaves_draft(self):
        paths = self.pack()
        with patch.object(release, "github_release", return_value=None), patch.object(
                release.subprocess, "run", side_effect=[None, subprocess.CalledProcessError(1, "upload")]) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                release.publish_github("owner/repo", VERSION, paths)
        self.assertEqual(run.call_count, 2)

    def test_completed_github_release_is_not_modified(self):
        paths = self.pack() + [self.bundle / "SHA256SUMS"]
        existing = {"draft": False, "assets": [
            {"name": path.name, "digest": "sha256:" + release.digest(path)} for path in paths]}
        with patch.object(release, "github_release", return_value=existing), patch.object(
                release.subprocess, "run") as run:
            release.publish_github("owner/repo", VERSION, paths)
        run.assert_not_called()


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
