import unittest
import subprocess
import sys
import os
from typing import Set

def discover_and_sort_tests(start_dir: str):
    """
    Discovers all tests and sorts them into serial and distributed suites.
    """
    loader = unittest.TestLoader()
    # Discover all tests matching the pattern 'test*.py'
    full_suite = loader.discover(start_dir, pattern="test*.py", top_level_dir=".")

    serial_tests = unittest.TestSuite()
    distributed_test_modules = set()

    def sort_suite(suite):
        """Recursively iterate through the test suite to sort tests."""
        for item in suite:
            if isinstance(item, unittest.TestSuite):
                sort_suite(item)
            else: # It's a TestCase
                module_name = item.__module__
                if 'distributed' in module_name:
                    # We don't add the test case directly, but record its module
                    # because we need to run the whole file with torchrun.
                    distributed_test_modules.add(module_name)
                else:
                    serial_tests.addTest(item)
    
    sort_suite(full_suite)
    return serial_tests, list(distributed_test_modules)

def run_serial_tests(suite: unittest.TestSuite) -> bool:
    """Runs the suite of serial tests."""
    if suite.countTestCases() == 0:
        print("▶️  No serial tests found.")
        return True
        
    print(f"▶️  Running {suite.countTestCases()} serial tests...")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    if result.wasSuccessful():
        print("✅ Serial tests passed!")
        return True
    else:
        print("❌ Serial tests failed.")
        return False

def run_distributed_tests(modules: Set[str], nproc: int = 2) -> bool:
    """Runs each distributed test module using torchrun."""
    if not modules:
        print("▶️  No distributed tests found.")
        return True

    print(f"\n▶️  Running {len(modules)} distributed test module(s) with torchrun (nproc={nproc})...")
    all_passed = True
    for module in modules:
        print(f"\n--- Running module: {module} ---")
        command = [
            sys.executable, "-m", "torch.distributed.run",
            "--nproc_per_node", str(nproc),
            "-m", "unittest", module
        ]
        
        # Pass environment variables from the parent process (like STREAMER_LIBRARY)
        try:
            subprocess.run(command, check=True, env=os.environ)
            print(f"✅ Distributed test module {module} passed!")
        except subprocess.CalledProcessError:
            print(f"❌ Distributed test module {module} failed.")
            all_passed = False
            # Optional: stop on first failure
            # return False
    
    return all_passed

def main():
    start_directory = "runai_model_streamer"
    print(f"--- Discovering tests in '{start_directory}' ---")
    
    serial_suite, distributed_modules = discover_and_sort_tests(start_directory)
    
    # Run serial tests first
    serial_ok = run_serial_tests(serial_suite)
    
    # Run distributed tests only if serial tests pass
    distributed_ok = False
    if serial_ok:
        distributed_ok = run_distributed_tests(distributed_modules)
    
    print("\n--- Test Summary ---")
    if serial_ok and distributed_ok:
        print("✅ All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()