import os
import unittest
from unittest.mock import patch, MagicMock

from botocore import UNSIGNED

import runai_model_streamer_s3.files.files as files


class TestFiles(unittest.TestCase):
    def test_filter_allow(self):
        res = files._filter_ignore(
            ["test_file1.txt1", "test_file2.txt2", "test_file3.txt3"],
            allow_pattern=["*.txt2"]
        )
        self.assertEqual(res, ["test_file2.txt2"])

    def test_filter_allow_full_path(self):
        res = files._filter_ignore(
            ["test_file1.txt1", "dir/test_file2.txt2", "test_file3.txt3"],
            allow_pattern=["*.txt2"]
        )
        self.assertEqual(res, ["dir/test_file2.txt2"])

    def test_filter_ignore(self):
        res = files._filter_ignore(
            ["test_file1.txt1", "test_file2.txt2", "test_file3.txt3"],
            ignore_pattern=["*.txt2"]
        )
        self.assertEqual(res, ["test_file1.txt1", "test_file3.txt3"])

    def test_removeprefix(self):
        res = files.removeprefix("test_prefix_string", "test_prefix_")
        self.assertEqual(res, "string")

    def test_removeprefix_no(self):
        res = files.removeprefix("test_prefix_string", "test_suffix_")
        self.assertEqual(res, "test_prefix_string")


def _env_without_unsigned():
    env = os.environ.copy()
    env.pop(files.RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR, None)
    return env


class TestBuildClientConfig(unittest.TestCase):
    def test_unsigned_enabled_when_one(self):
        with patch.dict(os.environ, {files.RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR: "1"}):
            config = files._build_client_config()
        self.assertIsNotNone(config)
        self.assertEqual(config.signature_version, UNSIGNED)

    def test_unsigned_disabled_when_zero(self):
        with patch.dict(os.environ, {files.RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR: "0"}):
            config = files._build_client_config()
        sig = config.signature_version if config else None
        self.assertNotEqual(sig, UNSIGNED)

    def test_unsigned_disabled_when_absent(self):
        with patch.dict(os.environ, _env_without_unsigned(), clear=True):
            config = files._build_client_config()
        sig = config.signature_version if config else None
        self.assertNotEqual(sig, UNSIGNED)


class TestBuildS3Client(unittest.TestCase):
    @patch("runai_model_streamer_s3.files.files.boto3")
    @patch("runai_model_streamer_s3.files.files.get_credentials")
    def test_credentials_used_when_unsigned_disabled(self, mock_get_credentials, mock_boto3):
        mock_session = MagicMock()
        mock_get_credentials.return_value = (mock_session, MagicMock())
        with patch.dict(os.environ, {files.RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR: "0"}):
            files._build_s3_client(None)
        mock_get_credentials.assert_called_once()
        mock_session.client.assert_called_once()
        mock_boto3.client.assert_not_called()

    @patch("runai_model_streamer_s3.files.files.boto3")
    @patch("runai_model_streamer_s3.files.files.get_credentials")
    def test_credentials_used_when_unsigned_absent(self, mock_get_credentials, mock_boto3):
        mock_session = MagicMock()
        mock_get_credentials.return_value = (mock_session, MagicMock())
        with patch.dict(os.environ, _env_without_unsigned(), clear=True):
            files._build_s3_client(None)
        mock_get_credentials.assert_called_once()
        mock_session.client.assert_called_once()
        mock_boto3.client.assert_not_called()

    @patch("runai_model_streamer_s3.files.files.boto3")
    @patch("runai_model_streamer_s3.files.files.get_credentials")
    def test_credentials_not_used_when_unsigned_enabled(self, mock_get_credentials, mock_boto3):
        with patch.dict(os.environ, {files.RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR: "1"}):
            files._build_s3_client(None)
        mock_get_credentials.assert_not_called()
        mock_boto3.client.assert_called_once()


if __name__ == "__main__":
    unittest.main()
