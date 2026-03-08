import unittest
import os
import shutil
import tempfile
import multiprocessing
import time
import fcntl

import runai_model_streamer.safetensors_streamer.safetensors_streamer as _ss_module
from runai_model_streamer.safetensors_streamer.safetensors_streamer import ObjectStorageModel

SOURCE_FILES = {
    "model.safetensors": b"fake-weights",
    "config.json": b'{"model_type": "test"}',
    "tokenizer.json": b'{"version": "1.0"}',
}

# Fake object storage URL used wherever ObjectStorageModel requires a valid scheme.
# The patched pull_files ignores it and copies from self.src_dir instead.
FAKE_MODEL_PATH = "s3://fake-bucket/fake-model"


class TestObjectStorageModel(unittest.TestCase):

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _create_source_dir():
        src = tempfile.mkdtemp()
        for name, content in SOURCE_FILES.items():
            with open(os.path.join(src, name), "wb") as f:
                f.write(content)
        return src

    def _fake_pull_files(
        self, model_path, dst, allow_pattern=None, ignore_pattern=None, s3_credentials=None
    ):
        """Simulates a download by copying source files into dst."""
        for fname in os.listdir(self.src_dir):
            shutil.copy2(os.path.join(self.src_dir, fname), os.path.join(dst, fname))

    def _slow_fake_pull_files(
        self, model_path, dst, allow_pattern=None, ignore_pattern=None, s3_credentials=None
    ):
        """Slow variant so concurrent processes actually overlap."""
        time.sleep(0.15)
        for fname in os.listdir(self.src_dir):
            shutil.copy2(os.path.join(self.src_dir, fname), os.path.join(dst, fname))

    @staticmethod
    def _worker(dst_dir, result_queue):
        """Entry point for each worker process (fork-safe static method)."""
        try:
            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=dst_dir) as obj:
                skipped = obj._skip
                obj.pull_files()
            result_queue.put({"status": "ok", "skipped": skipped})
        except Exception as exc:
            result_queue.put({"status": "error", "error": str(exc)})

    def _run_concurrent(self, num_processes):
        """
        Patch pull_files in the parent then fork workers. With fork, children
        inherit the patched module attribute and therefore call _slow_fake_pull_files.
        _slow_fake_pull_files is an instance method bound to self, so children
        inherit self.src_dir via the forked address space.
        """
        original = _ss_module.pull_files
        _ss_module.pull_files = self._slow_fake_pull_files

        try:
            ctx = multiprocessing.get_context("fork")
            result_queue = ctx.Queue()
            processes = [
                ctx.Process(
                    target=self._worker,
                    args=(self.dst_dir, result_queue),
                )
                for _ in range(num_processes)
            ]
            for p in processes:
                p.start()
            for p in processes:
                p.join(timeout=30)
            timed_out = [p for p in processes if p.is_alive()]
            for p in timed_out:
                p.terminate()
                p.join()
            if timed_out:
                self.fail(f"{len(timed_out)} worker process(es) timed out and were terminated")

            results = [result_queue.get_nowait() for _ in range(num_processes)]
        finally:
            _ss_module.pull_files = original

        return results

    # -----------------------------------------------------------------------
    # setUp / tearDown
    # -----------------------------------------------------------------------

    def setUp(self):
        self.src_dir = self._create_source_dir()
        self.dst_dir = tempfile.mkdtemp()
        shutil.rmtree(self.dst_dir)  # ObjectStorageModel will recreate it

    def tearDown(self):
        shutil.rmtree(self.src_dir, ignore_errors=True)
        shutil.rmtree(self.dst_dir, ignore_errors=True)
        lock_path = self.dst_dir + ".lock"
        if os.path.exists(lock_path):
            os.remove(lock_path)

    # -----------------------------------------------------------------------
    # Single-process: basic download
    # -----------------------------------------------------------------------

    def test_files_present_after_done(self):
        original = _ss_module.pull_files
        _ss_module.pull_files = self._fake_pull_files
        try:
            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj:
                obj.pull_files()
        finally:
            _ss_module.pull_files = original

        for name in SOURCE_FILES:
            self.assertTrue(
                os.path.exists(os.path.join(self.dst_dir, name)),
                f"{name} missing from dst",
            )

    def test_sentinel_written_after_done(self):
        original = _ss_module.pull_files
        _ss_module.pull_files = self._fake_pull_files
        try:
            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj:
                obj.pull_files()
        finally:
            _ss_module.pull_files = original

        self.assertTrue(
            os.path.exists(os.path.join(self.dst_dir, ObjectStorageModel.SENTINEL_NAME))
        )

    def test_sentinel_not_written_before_exit(self):
        original = _ss_module.pull_files
        _ss_module.pull_files = self._fake_pull_files
        try:
            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj:
                obj.pull_files()
                self.assertFalse(
                    os.path.exists(os.path.join(self.dst_dir, ObjectStorageModel.SENTINEL_NAME))
                )
        finally:
            _ss_module.pull_files = original

    # -----------------------------------------------------------------------
    # Single-process: idempotency
    # -----------------------------------------------------------------------

    def test_second_call_skips_download(self):
        call_count = [0]
        fake = self._fake_pull_files

        def counting(model_path, dst, allow_pattern=None, ignore_pattern=None, s3_credentials=None):
            call_count[0] += 1
            fake(model_path, dst, allow_pattern, ignore_pattern, s3_credentials)

        original = _ss_module.pull_files
        _ss_module.pull_files = counting
        try:
            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj1:
                obj1.pull_files()

            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj2:
                obj2.pull_files()
        finally:
            _ss_module.pull_files = original

        self.assertEqual(call_count[0], 1, "pull_files should be called exactly once")

    def test_second_call_files_still_accessible(self):
        original = _ss_module.pull_files
        _ss_module.pull_files = self._fake_pull_files
        try:
            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj1:
                obj1.pull_files()

            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj2:
                obj2.pull_files()
        finally:
            _ss_module.pull_files = original

        for name in SOURCE_FILES:
            self.assertTrue(os.path.exists(os.path.join(self.dst_dir, name)))

    # -----------------------------------------------------------------------
    # Single-process: two pull_files calls before done (vLLM pattern)
    # -----------------------------------------------------------------------

    def test_two_pull_files_calls_one_done(self):
        """Matches the vLLM pattern: pull model files then tokenizer files."""
        call_count = [0]
        fake = self._fake_pull_files

        def counting(model_path, dst, allow_pattern=None, ignore_pattern=None, s3_credentials=None):
            call_count[0] += 1
            fake(model_path, dst, allow_pattern, ignore_pattern, s3_credentials)

        original = _ss_module.pull_files
        _ss_module.pull_files = counting
        try:
            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj:
                obj.pull_files(allow_pattern=["*.safetensors"])
                obj.pull_files(ignore_pattern=["*.safetensors"])
        finally:
            _ss_module.pull_files = original

        self.assertEqual(call_count[0], 2)
        self.assertTrue(
            os.path.exists(os.path.join(self.dst_dir, ObjectStorageModel.SENTINEL_NAME))
        )

    # -----------------------------------------------------------------------
    # Single-process: stale dst cleanup
    # -----------------------------------------------------------------------

    def test_stale_dst_cleaned_before_download(self):
        os.makedirs(self.dst_dir)
        stale = os.path.join(self.dst_dir, "stale.bin")
        with open(stale, "wb") as f:
            f.write(b"old data")

        original = _ss_module.pull_files
        _ss_module.pull_files = self._fake_pull_files
        try:
            with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj:
                obj.pull_files()
        finally:
            _ss_module.pull_files = original

        self.assertFalse(os.path.exists(stale), "stale file should have been removed")
        for name in SOURCE_FILES:
            self.assertTrue(os.path.exists(os.path.join(self.dst_dir, name)))

    # -----------------------------------------------------------------------
    # Single-process: invalid model_path rejected before any side effects
    # -----------------------------------------------------------------------

    def test_invalid_model_path_raises_before_lock(self):
        with self.assertRaises(ValueError):
            ObjectStorageModel(model_path="/local/path/model", dst=self.dst_dir)

        self.assertFalse(
            os.path.exists(self.dst_dir),
            "dst must not be created for an invalid model_path",
        )
        self.assertFalse(
            os.path.exists(self.dst_dir + ".lock"),
            "lock file must not be created for an invalid model_path",
        )

    # -----------------------------------------------------------------------
    # Single-process: empty download raises and releases lock
    # -----------------------------------------------------------------------

    def test_empty_download_raises_runtime_error(self):
        def empty_pull_files(model_path, dst, allow_pattern=None, ignore_pattern=None, s3_credentials=None):
            pass  # downloads nothing

        original = _ss_module.pull_files
        _ss_module.pull_files = empty_pull_files
        try:
            with self.assertRaises(RuntimeError):
                with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj:
                    obj.pull_files()
        finally:
            _ss_module.pull_files = original

        self.assertFalse(
            os.path.exists(os.path.join(self.dst_dir, ObjectStorageModel.SENTINEL_NAME)),
            "sentinel must not be written after an empty download",
        )

    def test_empty_download_releases_lock(self):
        def empty_pull_files(model_path, dst, allow_pattern=None, ignore_pattern=None, s3_credentials=None):
            pass  # downloads nothing

        original = _ss_module.pull_files
        _ss_module.pull_files = empty_pull_files
        try:
            with self.assertRaises(RuntimeError):
                with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj:
                    obj.pull_files()
        finally:
            _ss_module.pull_files = original

        # Verify the lock file is free by acquiring it non-blocking
        lock_path = self.dst_dir + ".lock"
        with open(lock_path, "w") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(f, fcntl.LOCK_UN)
            except BlockingIOError:
                self.fail("lock was not released after empty download")

    # -----------------------------------------------------------------------
    # Single-process: exception during pull suppresses sentinel
    # -----------------------------------------------------------------------

    def test_sentinel_not_written_when_pull_raises(self):
        def raising_pull_files(model_path, dst, allow_pattern=None, ignore_pattern=None, s3_credentials=None):
            raise RuntimeError("simulated download failure")

        original = _ss_module.pull_files
        _ss_module.pull_files = raising_pull_files
        try:
            with self.assertRaises(RuntimeError):
                with ObjectStorageModel(model_path=FAKE_MODEL_PATH, dst=self.dst_dir) as obj:
                    obj.pull_files()
        finally:
            _ss_module.pull_files = original

        self.assertFalse(
            os.path.exists(os.path.join(self.dst_dir, ObjectStorageModel.SENTINEL_NAME)),
            "sentinel must not be written after a failed download",
        )

    # -----------------------------------------------------------------------
    # Single-process: trailing slash normalisation
    # -----------------------------------------------------------------------

    def test_model_path_without_trailing_slash(self):
        original = _ss_module.pull_files
        _ss_module.pull_files = self._fake_pull_files
        try:
            with ObjectStorageModel(model_path="s3://fake-bucket/fake-model", dst=self.dst_dir) as obj:
                obj.pull_files()
        finally:
            _ss_module.pull_files = original

        for name in SOURCE_FILES:
            self.assertTrue(os.path.exists(os.path.join(self.dst_dir, name)))

    def test_model_path_with_trailing_slash(self):
        original = _ss_module.pull_files
        _ss_module.pull_files = self._fake_pull_files
        try:
            with ObjectStorageModel(model_path="s3://fake-bucket/fake-model/", dst=self.dst_dir) as obj:
                obj.pull_files()
        finally:
            _ss_module.pull_files = original

        for name in SOURCE_FILES:
            self.assertTrue(os.path.exists(os.path.join(self.dst_dir, name)))

    # -----------------------------------------------------------------------
    # Multi-process tests
    # -----------------------------------------------------------------------

    def test_concurrent_all_processes_succeed(self):
        results = self._run_concurrent(4)
        errors = [r for r in results if r["status"] != "ok"]
        self.assertEqual(errors, [], f"Some processes failed: {errors}")

    def test_concurrent_only_one_process_downloads(self):
        results = self._run_concurrent(4)
        downloaders = [r for r in results if r["status"] == "ok" and not r["skipped"]]
        self.assertEqual(
            len(downloaders),
            1,
            f"Expected exactly 1 downloading process, got {len(downloaders)}",
        )

    def test_concurrent_files_present_after_download(self):
        self._run_concurrent(4)
        for name in SOURCE_FILES:
            self.assertTrue(
                os.path.exists(os.path.join(self.dst_dir, name)),
                f"{name} missing after concurrent download",
            )

    def test_concurrent_sentinel_present_after_download(self):
        self._run_concurrent(4)
        self.assertTrue(
            os.path.exists(os.path.join(self.dst_dir, ObjectStorageModel.SENTINEL_NAME))
        )

    def test_two_concurrent_processes(self):
        results = self._run_concurrent(2)
        errors = [r for r in results if r["status"] != "ok"]
        self.assertEqual(errors, [])
        downloaders = [r for r in results if not r["skipped"]]
        self.assertEqual(len(downloaders), 1)


if __name__ == "__main__":
    unittest.main()
