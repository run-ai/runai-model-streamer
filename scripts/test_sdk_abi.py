# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path
import tempfile
import unittest

from check_sdk_abi import ROOT, check_source


class AbiTests(unittest.TestCase):
    def test_published_node_cannot_gain_lose_or_move_symbols(self):
        source = (ROOT / "cpp/streamer/streamer.ldscript").read_text()
        baseline = json.loads((ROOT / "sdk/abi.json").read_text())
        symbol = "        runai_file_streamer_start;"
        mutations = [source.replace(symbol, ""),
                     source.replace(symbol, symbol + "\n        runai_new;"),
                     source.replace("RUNAI_FILE_STREAMER_1.0 {", "RUNAI_FILE_STREAMER_1.1 {"),
                     source.replace(symbol, "") + "\nRUNAI_FILE_STREAMER_1.1 { global: runai_file_streamer_start; };\n"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "streamer.ldscript"
            path.write_text(source)
            check_source(path, baseline)
            for mutation in mutations:
                with self.subTest(mutation=mutation[-200:]):
                    path.write_text(mutation)
                    with self.assertRaises(ValueError):
                        check_source(path, baseline)


if __name__ == "__main__":
    unittest.main()
