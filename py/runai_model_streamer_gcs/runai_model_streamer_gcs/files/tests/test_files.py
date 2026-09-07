import os
import shutil
import tempfile
import unittest
import runai_model_streamer_gcs.files.files as files


class TestFiles(unittest.TestCase):
    def test_filter_allow(self):
        res = files._filter_allow(
            ["test_file1.txt1", "test_file2.txt2", "test_file3.txt3"],
            patterns=["*.txt2"]
        )
        self.assertEqual(res, ["test_file2.txt2"])

    def test_filter_allow_full_path(self):
        res = files._filter_allow(
            ["test_file1.txt1", "dir/test_file2.txt2", "test_file3.txt3"],
            patterns=["*.txt2"]
        )
        self.assertEqual(res, ["dir/test_file2.txt2"])

    def test_filter_ignore(self):
        res = files._filter_ignore(
            ["test_file1.txt1", "test_file2.txt2", "test_file3.txt3"],
            patterns=["*.txt2"]
        )
        self.assertEqual(res, ["test_file1.txt1", "test_file3.txt3"])

    def test_removeprefix(self):
        res = files.removeprefix("test_prefix_string", "test_prefix_")
        self.assertEqual(res, "string")

    def test_removeprefix_no(self):
        res = files.removeprefix("test_prefix_string", "test_suffix_")
        self.assertEqual(res, "test_prefix_string")


class TestSafeDestinationPath(unittest.TestCase):
    def setUp(self):
        self.dst = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dst, ignore_errors=True)

    def test_allows_valid_nested_path(self):
        result = files._safe_destination_path(
            self.dst, "models/llama/", "models/llama/subdir/config.json")
        self.assertEqual(
            result, os.path.realpath(os.path.join(self.dst, "subdir/config.json")))

    def test_allows_valid_top_level_path(self):
        result = files._safe_destination_path(
            self.dst, "models/llama/", "models/llama/config.json")
        self.assertEqual(
            result, os.path.realpath(os.path.join(self.dst, "config.json")))

    def test_rejects_traversal_object_name(self):
        with self.assertRaises(ValueError):
            files._safe_destination_path(
                self.dst, "models/llama/",
                "models/llama/../../../../etc/cron.d/malicious")

    def test_rejects_traversal_object_name_not_matching_base_dir(self):
        with self.assertRaises(ValueError):
            files._safe_destination_path(
                self.dst, "models/llama/", "../../etc/passwd")


if __name__ == "__main__":
    unittest.main()
