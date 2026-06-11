import unittest
from unittest.mock import MagicMock
import runai_model_streamer_azure.files.files as files
from azure.storage.blob import BlobProperties, BlobServiceClient, ContainerClient


class TestFiles(unittest.TestCase):
    def test_filter_allow(self):
        res = files._filter_allow(
            ["test_file1.txt1", "test_file2.txt2", "test_file3.txt3"],
            ["*.txt2"]
        )
        self.assertEqual(res, ["test_file2.txt2"])

    def test_filter_allow_full_path(self):
        res = files._filter_allow(
            ["test_file1.txt1", "dir/test_file2.txt2", "test_file3.txt3"],
            ["*.txt2"]
        )
        self.assertEqual(res, ["dir/test_file2.txt2"])

    def test_filter_ignore(self):
        res = files._filter_ignore(
            ["test_file1.txt1", "test_file2.txt2", "test_file3.txt3"],
            ["*.txt2"]
        )
        self.assertEqual(res, ["test_file1.txt1", "test_file3.txt3"])

    def test_removeprefix(self):
        res = files.removeprefix("test_prefix_string", "test_prefix_")
        self.assertEqual(res, "string")

    def test_removeprefix_no(self):
        res = files.removeprefix("test_prefix_string", "test_suffix_")
        self.assertEqual(res, "test_prefix_string")


class TestListFiles(unittest.TestCase):

    def make_blob(self, name, size=10, metadata=None):
        blob = MagicMock(spec=BlobProperties)
        blob.name = name
        blob.size = size
        blob.metadata = metadata
        return blob
    
    def setUp(self):
        self.mock_container_client = MagicMock(spec=ContainerClient)
        self.mock_blob_client = MagicMock(spec=BlobServiceClient)
        self.mock_blob_client.get_container_client.return_value = (
            self.mock_container_client
        )

    def test_listfiles_recursive(self):
        # list_blobs and walk_blobs return an Iterable so list is fine
        test_blobs = [
            self.make_blob("file1.txt"),
            self.make_blob("dir1/test.txt"),
            self.make_blob("adls-dir1", size=0, metadata={"hdi_isfolder": "true"}),  # ADLS directory stub
            self.make_blob("dir2/", size=0, metadata=None),
            self.make_blob("empty-blob", size=0),
            self.make_blob("adls-caps", size=0, metadata={"Hdi_isfolder": "true"}),  
            self.make_blob("empty-file", size=0, metadata={"Hdi_isfolder": "false"}),
        
        ]

        self.mock_container_client.list_blobs.return_value = test_blobs
        _, _, result = files.list_files(self.mock_blob_client, "az://container/", recursive=True)
        self.assertEqual(result, ["file1.txt", "dir1/test.txt", "empty-blob", "empty-file"])
    
    def test_listfiles_non_recursive(self):
        test_blobs = [
            self.make_blob("file1.txt"),
            self.make_blob("adls-dir1", size=0, metadata={"hdi_isfolder": "true"}), 
            self.make_blob("dir2/"),
            self.make_blob("empty-blob", size=0),
            self.make_blob("adls-caps", size=0, metadata={"Hdi_isfolder": "true"}),  
            self.make_blob("empty-file", size=0, metadata={"Hdi_isfolder": "false"}),
        ]

        self.mock_container_client.walk_blobs.return_value = test_blobs
        _, _, result = files.list_files(self.mock_blob_client, "az://container/", )
        self.assertEqual(result, ["file1.txt", "empty-blob", "empty-file"])

    def test_listfiles_with_allow_pattern(self):
        blobs = [
            self.make_blob("models/weights/config.json"),
            self.make_blob("models/weights/model.safetensors"),
            self.make_blob("models/README")
        ]
       
        self.mock_container_client.list_blobs.return_value = blobs
        _, _, result = files.list_files(
            self.mock_blob_client, "az://container/", allow_pattern=["*.safetensors"], recursive=True
        )
        self.assertEqual(result,["models/weights/model.safetensors"])

    def test_listfiles_with_ignore_pattern(self):
        blobs = [
            self.make_blob("models/weights/config.json"),
            self.make_blob("models/weights/model.safetensors"),
            self.make_blob("models/README")
        ]
        self.mock_container_client.list_blobs.return_value = blobs
        _, _, result = files.list_files(
            self.mock_blob_client, "az://container/", ignore_pattern=["*.safetensors"], recursive=True
        )
        self.assertEqual(result, ["models/weights/config.json", "models/README"])

if __name__ == "__main__":
    unittest.main()
