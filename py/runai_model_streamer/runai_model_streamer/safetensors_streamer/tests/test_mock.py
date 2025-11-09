import unittest
import torch
import os
import tempfile
import shutil
from safetensors import safe_open
from unittest.mock import patch

# Import the class to be tested
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
    pull_files,
    list_safetensors,
)

from runai_model_streamer.safetensors_streamer.streamer_mock import StreamerPatcher

class TestSafetensorsStreamerMock(unittest.TestCase):

    def setUp(self):
        """Find the test file path once for all tests."""
        self.file_name = "test.safetensors"
        self.file_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_file_path = os.path.join(self.file_dir, self.file_name)
        
        # Make sure the test file exists before running any test
        if not os.path.exists(self.local_file_path):
            raise FileNotFoundError(
                f"Test file 'test.safetensors' not found in {self.file_dir}. "
                "Please add it to run the tests."
            )

    # We patch `SafetensorsStreamer` *where it is imported*, which is
    # in this local test module. `__name__` resolves to the module name.
    @patch(__name__ + '.SafetensorsStreamer')
    def test_safetensors_streamer_S3_MOCK(self, mock_streamer_class):
        """
        Mocked test: Verifies streaming from a FAKE S3 path
        is correctly patched and reads the local file.
        """
        
        # 1. Create a fake S3 path
        fake_s3_path = f"s3://my-fake-bucket/models/{self.file_name}"

        # 2. Initialize the Patcher, pointing to the *directory*
        patcher = StreamerPatcher(local_model_path=self.file_dir)
        
        # 3. Connect the patch to the patcher
        # When `SafetensorsStreamer()` is called, use the patcher's factory
        mock_streamer_class.side_effect = patcher.create_mock_streamer

        # 4. Run streamer logic, passing the FAKE S3 PATH
        our = {}
        with SafetensorsStreamer() as run_sf: # This call is now mocked
            # Pass the *fake* path here. The patcher will rewrite it.
            run_sf.stream_file(fake_s3_path, None, "cpu")
            for name, tensor in run_sf.get_tensors():
                our[name] = tensor

        # 5. Load baseline from the REAL LOCAL PATH
        their = {}
        with safe_open(self.local_file_path, framework="pt", device="cpu") as f:
            for name in f.keys():
                their[name] = f.get_tensor(name)

        # 6. Assertions 
        self.assertEqual(len(our.items()), len(their.items()))
        self.assertGreater(len(our.items()), 0, "No tensors loaded via mock")
        for name, our_tensor in our.items():
            self.assertTrue(our_tensor.is_contiguous())
            self.assertEqual(our_tensor.dtype, their[name].dtype)
            self.assertEqual(our_tensor.shape, their[name].shape)
            res = torch.all(our_tensor.eq(their[name]))
            self.assertTrue(res)

    @patch(__name__ + '.SafetensorsStreamer')
    def test_safetensors_streamer_GS_MOCK(self, mock_streamer_class):
        """
        Mocked test: Verifies streaming from a FAKE GS path
        is correctly patched and reads the local file.
        """
        
        # 1. Create a fake GS path
        fake_gs_path = f"gs://my-fake-bucket/models/{self.file_name}"

        # 2. Initialize the Patcher
        patcher = StreamerPatcher(local_model_path=self.file_dir)
        
        # 3. Connect the patch to the patcher
        mock_streamer_class.side_effect = patcher.create_mock_streamer

        # 4. Run streamer logic, passing the FAKE GS PATH
        our = {}
        with SafetensorsStreamer() as run_sf:
            run_sf.stream_file(fake_gs_path, None, "cpu")
            for name, tensor in run_sf.get_tensors():
                our[name] = tensor

        # 5. Load baseline from the REAL LOCAL PATH
        their = {}
        with safe_open(self.local_file_path, framework="pt", device="cpu") as f:
            for name in f.keys():
                their[name] = f.get_tensor(name)

        # 6. Assertions
        self.assertEqual(len(our.items()), len(their.items()))
        self.assertGreater(len(our.items()), 0, "No tensors loaded via mock")
        for name, their_tensor in their.items():
            self.assertTrue(name in our, f"Tensor {name} missing from mocked run")
            self.assertTrue(torch.all(our[name].eq(their_tensor)))

    @patch(__name__ + '.list_safetensors')
    def test_list_safetensors_S3_MOCK(self, mock_list_safetensors):
        """
        Mocked test: Verifies list_safetensors shim logic.
        """

        fake_s3_path = "s3://my-fake-bucket/models/"
        patcher = StreamerPatcher(local_model_path=self.file_dir)
        
        # Connect the patch to the patcher's shim method
        mock_list_safetensors.side_effect = patcher.shim_list_safetensors
        
        # Call the (now-mocked) function
        listed_files = list_safetensors(fake_s3_path)
        
        # Assertions
        self.assertIsInstance(listed_files, list)
        self.assertEqual(len(listed_files), 2, "No files listed by shim")
        self.assertTrue(
            any(f.endswith(self.file_name) for f in listed_files),
            f"{self.file_name} not found in listed files: {listed_files}"
        )

    @patch(__name__ + '.list_safetensors')
    def test_list_safetensors_GS_MOCK(self, mock_list_safetensors):
        """
        Mocked test: Verifies list_safetensors shim logic.
        """
        
        fake_gs_path = "gs://my-fake-bucket/models/"
        patcher = StreamerPatcher(local_model_path=self.file_dir)
        
        # Connect the patch to the patcher's shim method
        mock_list_safetensors.side_effect = patcher.shim_list_safetensors
        
        # Call the (now-mocked) function
        listed_files = list_safetensors(fake_gs_path)
        
        # Assertions
        self.assertIsInstance(listed_files, list)
        self.assertGreater(len(listed_files), 0, "No files listed by shim")
        self.assertTrue(
            any(f.endswith(self.file_name) for f in listed_files),
            f"{self.file_name} not found in listed files: {listed_files}"
        )

    # Patch the function *where it is imported*, which is this local module.
    @patch(__name__ + '.pull_files')
    def test_pull_files_S3_MOCK(self, mock_pull_files):
        """
        Mocked test: Verifies pull_files shim logic.
        """
        
        fake_s3_path = "s3://my-fake-bucket/models/"
        dest_dir = tempfile.mkdtemp()

        try:
            patcher = StreamerPatcher(local_model_path=self.file_dir)
            
            # Connect the patch to the patcher's shim method
            mock_pull_files.side_effect = patcher.shim_pull_files
            
            # Call the (now-mocked) function
            pull_files(
                model_path=fake_s3_path,
                dst=dest_dir,
                allow_pattern=["*.safetensors"], # Test the filtering
                ignore_pattern=None
            )
            
            # Assertions
            expected_file_path = os.path.join(dest_dir, self.file_name)
            self.assertTrue(
                os.path.exists(expected_file_path),
                f"File was not 'pulled' to {expected_file_path}"
            )
            
        finally:
            shutil.rmtree(dest_dir)

    @patch(__name__ + '.pull_files')
    def test_pull_files_GS_MOCK(self, mock_pull_files):
        """
        Mocked test: Verifies pull_files shim logic with a GS path.
        """
        
        fake_gs_path = "gs://my-fake-bucket/models/"
        dest_dir = tempfile.mkdtemp()

        try:
            patcher = StreamerPatcher(local_model_path=self.file_dir)
            mock_pull_files.side_effect = patcher.shim_pull_files
            
            pull_files(
                model_path=fake_gs_path,
                dst=dest_dir,
                allow_pattern=["*.safetensors"],
                ignore_pattern=None
            )
            
            expected_file_path = os.path.join(dest_dir, self.file_name)
            self.assertTrue(
                os.path.exists(expected_file_path),
                f"File was not 'pulled' to {expected_file_path}"
            )
            
        finally:
            shutil.rmtree(dest_dir)



if __name__ == "__main__":
    unittest.main()