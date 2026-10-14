# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check the frozen ELF export contract and the SDK's runtime requirements."""

import argparse
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def version_nodes(source):
    source = re.sub(r"/\*.*?\*/|#[^\n]*", "", source, flags=re.S)
    nodes = {}
    for match in re.finditer(r"([\w.]+)\s*\{([^}]+)\}\s*([\w.]*)\s*;", source):
        name, body, parent = match.groups()
        if name in nodes:
            raise ValueError(f"Duplicate version node: {name}")
        exports = re.search(r"global:\s*(.*?)(?:local:|$)", body, re.S)
        symbols = sorted(s.strip() for s in exports.group(1).split(";") if s.strip()) if exports else []
        if any(not re.fullmatch(r"\w+", symbol) for symbol in symbols):
            raise ValueError("Exported symbols must be explicit names")
        nodes[name] = {"parent": parent, "symbols": symbols}
    if not nodes:
        raise ValueError("No version nodes found")
    return nodes


def check_source(script, baseline):
    if version_nodes(script.read_text()) != baseline:
        raise ValueError("ELF version nodes differ from sdk/abi.json. Existing nodes are frozen; "
                         "new symbols require a new node and an explicit baseline update.")


def readelf(*args):
    return subprocess.check_output(["readelf", *map(str, args)], text=True)


def check_libraries(directory, baseline):
    library = directory / "libstreamer.so"
    dynamic = readelf("-dW", library)
    paths = re.findall(r"\((?:RUNPATH|RPATH)\).*?\[([^]]*)\]", dynamic)
    if not any("$ORIGIN" in path.split(":") for path in paths):
        raise ValueError("libstreamer.so must carry $ORIGIN in DT_RUNPATH or DT_RPATH")
    expected = {f"{symbol}@@{node}" for node, entry in baseline.items() for symbol in entry["symbols"]}
    exported = set()
    for line in readelf("--dyn-syms", "--wide", library).splitlines():
        fields = line.split()
        if len(fields) >= 8 and fields[4] in ("GLOBAL", "WEAK") and fields[6] != "UND":
            name = fields[7]
            if name not in baseline:
                exported.add(name)
    if exported != expected:
        raise ValueError(f"ELF exports differ: missing={expected - exported}, extra={exported - expected}")
    for name in ("libstreamer.so", "libstreamers3.so", "libstreamergcs.so", "libstreamerazure.so"):
        symbols = readelf("--dyn-syms", "--wide", directory / name)
        versions = {tuple(map(int, version.split(".")))
                    for version in re.findall(r"@GLIBC_([0-9.]+)", symbols)}
        if not versions or max(versions) > (2, 30):
            raise ValueError(f"{name} violates the glibc 2.30 floor: {sorted(versions)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib-dir", type=Path)
    args = parser.parse_args()
    baseline = json.loads((ROOT / "sdk/abi.json").read_text())
    check_source(ROOT / "cpp/streamer/streamer.ldscript", baseline)
    if args.lib_dir:
        check_libraries(args.lib_dir, baseline)
    print("SDK ABI checks passed")


if __name__ == "__main__":
    main()
