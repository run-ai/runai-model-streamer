import importlib
import pkgutil
import typing
import unittest

import runai_model_streamer


class TestAnnotationsResolve(unittest.TestCase):
    """Every annotation in the package must actually resolve.

    These modules use `from __future__ import annotations`, which stores annotations as STRINGS and never
    evaluates them. That hides two classes of bug from the test suite and from import itself:

      - a type used in an annotation but never imported (`Tuple` without `from typing import Tuple`);
      - syntax the target interpreter cannot evaluate, e.g. PEP 604 `str | None` on Python 3.8.

    Neither shows up when the code runs. They surface only when something resolves the annotations - a type
    checker, `typing.get_type_hints`, a docs generator, pydantic - or the moment someone deletes the
    `from __future__ import annotations` line, at which point the module fails at IMPORT.

    Both bugs were present in this package and invisible until a reviewer read them. This test resolves
    every annotation so the next one fails here instead.
    """

    def _import_package_modules(self):
        """Import every module in the package. Returns (modules, skipped)."""
        modules = []
        skipped = []
        for module_info in pkgutil.walk_packages(
            runai_model_streamer.__path__, prefix=f"{runai_model_streamer.__name__}."
        ):
            name = module_info.name
            if ".tests" in name:
                continue
            try:
                modules.append(importlib.import_module(name))
            except Exception as error:
                # Optional backends (boto3, torch.distributed, a plugin .so) may be absent in this
                # environment. An import failure is not this test's subject and would fail loudly in the
                # tests that actually use the module.
                skipped.append(f"{name}: {type(error).__name__}: {error}")
        return modules, skipped

    def _annotated_targets(self, module):
        """Functions and classes DEFINED in this module, plus the classes' own methods."""
        for attr_name in dir(module):
            obj = getattr(module, attr_name, None)
            # __module__ filters out names merely imported into this module - those belong to whichever
            # module defines them, and are checked when that module is swept.
            if getattr(obj, "__module__", None) != module.__name__:
                continue

            if callable(obj) or isinstance(obj, type):
                yield attr_name, obj

            if isinstance(obj, type):
                for member_name in vars(obj):
                    member = getattr(obj, member_name, None)
                    if callable(member):
                        yield f"{attr_name}.{member_name}", member

    def test_every_annotation_resolves(self):
        modules, skipped = self._import_package_modules()
        self.assertTrue(modules, f"no modules could be imported; skipped: {skipped}")

        failures = []
        for module in modules:
            for label, target in self._annotated_targets(module):
                try:
                    typing.get_type_hints(target)
                except Exception as error:
                    failures.append(f"{module.__name__}.{label}: {type(error).__name__}: {error}")

        self.assertEqual(
            failures,
            [],
            "annotations that do not resolve - a missing import, or syntax this interpreter cannot "
            "evaluate. They are silent today only because of `from __future__ import annotations`, and "
            "would become import errors without it:\n  " + "\n  ".join(failures),
        )


if __name__ == "__main__":
    unittest.main()
