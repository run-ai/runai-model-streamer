import os
import unittest
from unittest.mock import patch, MagicMock

from runai_model_streamer_s3.credentials.credentials import (
    get_credentials,
    AWS_CA_BUNDLE_ENV,
    RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR,
)


def _env_without_unsigned():
    env = os.environ.copy()
    env.pop(RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR, None)
    env.pop(AWS_CA_BUNDLE_ENV, None)
    env.pop("RUNAI_STREAMER_NO_BOTO3_SESSION", None)
    return env


class TestGetCredentialsUnsigned(unittest.TestCase):
    @patch("runai_model_streamer_s3.credentials.credentials.boto3")
    def test_unsigned_returns_no_session(self, mock_boto3):
        mock_boto3.Session.return_value._session.get_config_variable.return_value = None
        with patch.dict(os.environ, {RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR: "1"}, clear=False):
            session, _ = get_credentials(None)
        self.assertIsNone(session)

    @patch("runai_model_streamer_s3.credentials.credentials.boto3")
    def test_unsigned_sets_ca_bundle(self, mock_boto3):
        mock_boto3.Session.return_value._session.get_config_variable.return_value = "/etc/ssl/custom.pem"
        env = _env_without_unsigned()
        env[RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR] = "1"
        with patch.dict(os.environ, env, clear=True):
            get_credentials(None)
        self.assertEqual(os.environ.get(AWS_CA_BUNDLE_ENV), "/etc/ssl/custom.pem")

    @patch("runai_model_streamer_s3.credentials.credentials.boto3")
    def test_unsigned_disabled_resolves_credentials(self, mock_boto3):
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = None
        mock_session._session.get_config_variable.return_value = None
        mock_boto3.Session.return_value = mock_session
        with patch.dict(os.environ, {RUNAI_STREAMER_S3_UNSIGNED_ENV_VAR: "0"}, clear=False):
            session, _ = get_credentials(None)
        mock_session.get_credentials.assert_called_once()
        self.assertIsNotNone(session)

    @patch("runai_model_streamer_s3.credentials.credentials.boto3")
    def test_unsigned_absent_resolves_credentials(self, mock_boto3):
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = None
        mock_session._session.get_config_variable.return_value = None
        mock_boto3.Session.return_value = mock_session
        with patch.dict(os.environ, _env_without_unsigned(), clear=True):
            session, _ = get_credentials(None)
        mock_session.get_credentials.assert_called_once()
        self.assertIsNotNone(session)


if __name__ == "__main__":
    unittest.main()
