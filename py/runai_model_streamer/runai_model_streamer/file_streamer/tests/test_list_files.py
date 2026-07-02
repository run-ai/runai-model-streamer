import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from runai_model_streamer.file_streamer.file_streamer import FileStreamer
from runai_model_streamer.libstreamer.libstreamer import runai_list_files
from runai_model_streamer.s3_utils.s3_utils import S3Credentials


def _list_files_stub(entries):
    """Returns a side_effect for mock runai_list_files that fires the callback with given (path, size) pairs."""
    def stub(prefix, callback, is_recursive=True, allow_patterns=None, ignore_patterns=None, params=None):
        for path, size in entries:
            callback(path, size)
    return stub


# ---------------------------------------------------------------------------
# Filesystem
# ---------------------------------------------------------------------------

class TestListFilesFilesystem(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def _write(self, rel_path, content=b"x"):
        full = os.path.join(self.temp_dir, rel_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(content)
        return full

    def test_basic_listing(self):
        self._write("a.txt", b"hello")
        self._write("b.safetensors", b"world!")
        results = FileStreamer().list_files(self.temp_dir)
        paths = {r[0] for r in results}
        self.assertIn(os.path.join(self.temp_dir, "a.txt"), paths)
        self.assertIn(os.path.join(self.temp_dir, "b.safetensors"), paths)

    def test_returns_correct_sizes(self):
        self._write("sized.bin", b"12345")
        results = FileStreamer().list_files(self.temp_dir)
        by_path = {r[0]: r[1] for r in results}
        self.assertEqual(by_path[os.path.join(self.temp_dir, "sized.bin")], 5)

    def test_recursive(self):
        self._write("root.txt", b"r")
        self._write("sub/nested.txt", b"n")
        nested = os.path.join(self.temp_dir, "sub", "nested.txt")

        recursive_paths = {r[0] for r in FileStreamer().list_files(self.temp_dir, is_recursive=True)}
        non_recursive_paths = {r[0] for r in FileStreamer().list_files(self.temp_dir, is_recursive=False)}

        self.assertIn(nested, recursive_paths)
        self.assertNotIn(nested, non_recursive_paths)

    def test_allow_pattern(self):
        self._write("model.safetensors", b"m")
        self._write("config.json", b"c")
        results = FileStreamer().list_files(self.temp_dir, allow_patterns=["*.safetensors"])
        paths = {r[0] for r in results}
        self.assertTrue(all(p.endswith(".safetensors") for p in paths))
        self.assertFalse(any(p.endswith(".json") for p in paths))

    def test_ignore_pattern(self):
        self._write("model.safetensors", b"m")
        self._write("config.json", b"c")
        results = FileStreamer().list_files(self.temp_dir, ignore_patterns=["*.json"])
        paths = {r[0] for r in results}
        self.assertFalse(any(p.endswith(".json") for p in paths))
        self.assertTrue(any(p.endswith(".safetensors") for p in paths))

    def test_empty_directory(self):
        results = FileStreamer().list_files(self.temp_dir)
        self.assertEqual(results, [])

    def test_nonexistent_path_raises(self):
        with self.assertRaises(ValueError):
            FileStreamer().list_files(os.path.join(self.temp_dir, "does_not_exist"))

    def test_callback_exception_is_reraised(self):
        # an exception raised inside the callback must propagate out of
        # runai_list_files rather than be swallowed by the ctypes boundary
        self._write("a.txt", b"hello")

        class BoomError(Exception):
            pass

        def raising_callback(path, size):
            raise BoomError("callback failed")

        with self.assertRaises(BoomError):
            runai_list_files(self.temp_dir, raising_callback, is_recursive=True)


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

class TestListFilesS3(unittest.TestCase):
    PREFIX = "s3://my-bucket/models/"
    ENTRIES = [
        ("s3://my-bucket/models/model.safetensors", 1024),
        ("s3://my-bucket/models/config.json", 256),
    ]

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_basic_listing(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub(self.ENTRIES)
        results = FileStreamer().list_files(self.PREFIX)
        self.assertEqual(sorted(results), sorted(self.ENTRIES))

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_is_recursive_passed(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub([])
        FileStreamer().list_files(self.PREFIX, is_recursive=False)
        self.assertEqual(mock_rlf.call_args.kwargs["is_recursive"], False)

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_allow_patterns_passed(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub([])
        FileStreamer().list_files(self.PREFIX, allow_patterns=["*.safetensors"])
        self.assertEqual(mock_rlf.call_args.kwargs["allow_patterns"], ["*.safetensors"])

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_ignore_patterns_passed(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub([])
        FileStreamer().list_files(self.PREFIX, ignore_patterns=["*.json"])
        self.assertEqual(mock_rlf.call_args.kwargs["ignore_patterns"], ["*.json"])

    @patch("runai_model_streamer.file_streamer.file_streamer.s3_credentials_module")
    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_credentials_mapped_to_params(self, mock_rlf, mock_s3_mod):
        mock_rlf.side_effect = _list_files_stub([])
        creds = S3Credentials(
            access_key_id="AKID",
            secret_access_key="SECRET",
            session_token="TOKEN",
            region_name="us-east-1",
            endpoint="https://s3.example.com",
        )
        mock_s3_mod.get_credentials.return_value = (MagicMock(), creds)
        FileStreamer().list_files(self.PREFIX, credentials=creds)
        params = mock_rlf.call_args.kwargs["params"]
        self.assertEqual(params["key"], "AKID")
        self.assertEqual(params["secret"], "SECRET")
        self.assertEqual(params["token"], "TOKEN")
        self.assertEqual(params["region"], "us-east-1")
        self.assertEqual(params["endpoint"], "https://s3.example.com")

    @patch("runai_model_streamer.file_streamer.file_streamer.s3_credentials_module", None)
    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_no_params_when_no_credentials_module(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub(self.ENTRIES)
        results = FileStreamer().list_files(self.PREFIX)
        self.assertIsNone(mock_rlf.call_args.kwargs["params"])
        self.assertEqual(sorted(results), sorted(self.ENTRIES))


# ---------------------------------------------------------------------------
# GCS
# ---------------------------------------------------------------------------

class TestListFilesGCS(unittest.TestCase):
    PREFIX = "gs://my-bucket/models/"
    ENTRIES = [
        ("gs://my-bucket/models/model.safetensors", 2048),
        ("gs://my-bucket/models/config.json", 512),
    ]

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_basic_listing(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub(self.ENTRIES)
        results = FileStreamer().list_files(self.PREFIX)
        self.assertEqual(sorted(results), sorted(self.ENTRIES))

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_is_recursive_passed(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub([])
        FileStreamer().list_files(self.PREFIX, is_recursive=False)
        self.assertEqual(mock_rlf.call_args.kwargs["is_recursive"], False)

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_allow_patterns_passed(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub([])
        FileStreamer().list_files(self.PREFIX, allow_patterns=["*.safetensors"])
        self.assertEqual(mock_rlf.call_args.kwargs["allow_patterns"], ["*.safetensors"])

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_no_credentials_for_gcs(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub([])
        FileStreamer().list_files(self.PREFIX)
        self.assertIsNone(mock_rlf.call_args.kwargs["params"])


# ---------------------------------------------------------------------------
# Azure
# ---------------------------------------------------------------------------

class TestListFilesAzure(unittest.TestCase):
    PREFIX = "az://my-container/models/"
    ENTRIES = [
        ("az://my-container/models/model.safetensors", 4096),
        ("az://my-container/models/config.json", 128),
    ]

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_basic_listing(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub(self.ENTRIES)
        results = FileStreamer().list_files(self.PREFIX)
        self.assertEqual(sorted(results), sorted(self.ENTRIES))

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_is_recursive_passed(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub([])
        FileStreamer().list_files(self.PREFIX, is_recursive=False)
        self.assertEqual(mock_rlf.call_args.kwargs["is_recursive"], False)

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_ignore_patterns_passed(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub([])
        FileStreamer().list_files(self.PREFIX, ignore_patterns=["*.json"])
        self.assertEqual(mock_rlf.call_args.kwargs["ignore_patterns"], ["*.json"])

    @patch("runai_model_streamer.file_streamer.file_streamer.runai_list_files")
    def test_no_credentials_for_azure(self, mock_rlf):
        mock_rlf.side_effect = _list_files_stub([])
        FileStreamer().list_files(self.PREFIX)
        self.assertIsNone(mock_rlf.call_args.kwargs["params"])


if __name__ == "__main__":
    unittest.main()
