# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Find what the collector would miss, without the network or Docker. Two checks:

* a tracked file that downloads or installs something but is not read by the collector;
* a file the collector reads in which a download, `git clone` or install command is not parsed
  (the collector found fewer than the file contains).

Both fail loudly so that a new script, or a command in a new shape, is noticed when it is added,
not when a list turns out to be incomplete. Python 3.8 compatible."""

import fnmatch
import re

import dockerfile
import inputs
from util import run

# The files the collector reads, as globs from the repository root (`*` also matches `/`).
READ_BY_THE_COLLECTOR = [
    ".devcontainer/Dockerfile",
    ".devcontainer/scripts/*.sh",
    ".devcontainer/devcontainer.json",
    ".github/workflows/*",
    "py/*/setup.py",
    "py/*/pyproject.toml",
    "py/*/requirements*",
    "py/*/Makefile",
]

# What makes a file an input of the build: a command that downloads or installs something.
DOWNLOADS_SOMETHING = re.compile(
    r"\b(?:curl|wget)\b[^\n#]*https?://|\bgit clone\b|\bpip3? install\b|\bnpm (?:install|i)\b|\bapt(?:-get)? install\b"
    r"|^\s*FROM\s|\bhttp_archive\b|\bgit_repository\b|^[\s-]*uses:", re.M)

MAX_BYTES = 2 * 1024 * 1024


def _matches(path, patterns):
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def unread_files(repo):
    """Tracked files that download or install something and that nothing accounts for."""
    config = inputs.config()["coverage"]
    accounted = READ_BY_THE_COLLECTOR + config["covered_by_bazel"] + config["not_an_input"]
    problems = []
    for path in sorted(run(["git", "ls-files"], cwd=repo).splitlines()):
        if _matches(path, accounted):
            continue
        try:
            with open("%s/%s" % (repo, path), "rb") as f:
                data = f.read(MAX_BYTES)
            text = data.decode("utf-8")
        except (OSError, UnicodeDecodeError):
            continue  # not a text file
        if DOWNLOADS_SOMETHING.search(text):
            problems.append("%s downloads or installs something, but the collector does not read it. Teach the collector "
                            "about it, or add it to `coverage` in categories.json if Bazel covers it or it is not an input "
                            "of the build" % path)
    return problems


def unparsed_commands(repo):
    """Commands in the build files that the parsers did not turn into a dependency."""
    problems = []
    for path in inputs.build_files(repo):
        text = inputs.read(repo, path)
        commands = "\n".join(command for _, command in dockerfile.runs(text, path))
        checks = [
            ("curl with a URL", r"\bcurl\b[^|;&\n]*?https?://", len(dockerfile.downloads(text, path))),
            ("git clone", r"\bgit clone\b", len(dockerfile.git_clones(text, path))),
            ("npm install", r"\bnpm install\b", len(dockerfile.npm_packages(text, path))),
            ("pip install", r"\bpip3? install\b", len(dockerfile.pip_packages(text, path))),
            ("apt-get install", r"\bapt-get install\b", len(dockerfile.apt_packages(text, path))),
        ]
        for label, pattern, parsed in checks:
            found = len(re.findall(pattern, commands))
            if found != parsed:
                problems.append("%s has %d `%s` commands and the collector read %d. Teach dockerfile.py the new shape"
                                % (path, found, label, parsed))
    return problems


def problems(repo):
    return unread_files(repo) + unparsed_commands(repo)
