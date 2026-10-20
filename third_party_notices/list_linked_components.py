# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""List the third-party components that a linker map shows were linked into a shared library.

    python3 list_linked_components.py libstreamers3.map [more.map ...]

Build the map with:  bazel build <target> --linkopt=-Wl,-Map=/path/lib.map
Prints, per map, the Bazel external repositories, the /opt prebuilt archives and the toolchain
archives (libstdc++, libgcc, glibc stubs) that supplied at least one object file, with the number
of object files taken from each. Code of the project itself is not listed.
"""

import collections
import re
import sys


def components(path):
    found = collections.Counter()
    started = False
    with open(path, errors="replace") as lines:
        for line in lines:
            if line.startswith("Archive member included"):
                started = True
                continue
            if not started:
                continue
            if line.startswith(("Discarded input", "Allocating common")):
                break
            if line[:1] in " \t\n":
                continue
            match = re.match(r"(.+?)\((.+)\)\s*$", line.strip())
            if not match:
                continue
            archive = match.group(1)
            external = re.search(r"external/([^/]+)/", archive)
            if external:
                found["bazel external: " + external.group(1)] += 1
            elif archive.startswith("/opt/"):
                found["prebuilt: " + archive] += 1
            elif archive.startswith("/usr/"):
                found["toolchain/system: " + archive] += 1
    return found


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    for path in sys.argv[1:]:
        found = components(path)
        print("== %s (%d components)" % (path, len(found)))
        for name, count in sorted(found.items()):
            print("  %5d  %s" % (count, name))


if __name__ == "__main__":
    main()
