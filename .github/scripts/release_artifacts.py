#!/usr/bin/env python3
"""Validate, checksum, and publish the exact wheels produced by release CI."""

import argparse
from email.parser import Parser
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import zipfile


PACKAGES = ("runai_model_streamer", "runai_model_streamer_s3",
            "runai_model_streamer_gcs", "runai_model_streamer_azure")
ARCHES = ("x86_64", "aarch64")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def validate_wheels(paths, version):
    if not re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version):
        raise ValueError("Expected a stable X.Y.Z version")
    expected = {(package, arch) for package in PACKAGES for arch in ARCHES}
    seen = set()
    for path in paths:
        match = re.fullmatch(
            r"(runai_model_streamer(?:_s3|_gcs|_azure)?)-" + re.escape(version)
            + r"-py3-none-(manylinux[0-9_]+_(x86_64|aarch64))\.whl", path.name)
        if path.is_symlink() or not match:
            raise ValueError(f"Unexpected wheel: {path}")
        package, platform, arch = match.groups()
        pair = (package, arch)
        if pair in seen:
            raise ValueError(f"Duplicate package/architecture: {pair}")
        seen.add(pair)
        with zipfile.ZipFile(path) as wheel:
            prefix = f"{package}-{version}.dist-info/"
            metadata = Parser().parsestr(wheel.read(prefix + "METADATA").decode())
            wheel_info = Parser().parsestr(wheel.read(prefix + "WHEEL").decode())
            if (re.sub(r"[-_.]+", "_", metadata.get("Name", "")).lower() != package
                    or metadata.get("Version") != version
                    or wheel_info.get_all("Tag") != [f"py3-none-{platform}"]):
                raise ValueError(f"Wheel metadata does not match its filename: {path.name}")
    if seen != expected:
        raise ValueError(f"Incomplete wheel set; missing: {sorted(expected - seen)}")


def checksum_text(paths):
    return "".join(f"{digest(path)}  {path.name}\n" for path in sorted(paths))


def bundle(source, destination, version):
    paths = sorted(source.rglob("*.whl"))
    validate_wheels(paths, version)
    destination.mkdir(parents=True, exist_ok=False)
    wheels = destination / "wheels"
    wheels.mkdir()
    for path in paths:
        shutil.copyfile(path, wheels / path.name)
    (destination / "SHA256SUMS").write_text(checksum_text(list(wheels.iterdir())))


def verify(destination, version):
    paths = sorted((destination / "wheels").iterdir())
    validate_wheels(paths, version)
    if (destination / "SHA256SUMS").read_text() != checksum_text(paths):
        raise ValueError("SHA256SUMS does not match the release wheels")
    return paths


def read_json(url, token=None):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        with urlopen(Request(url, headers=headers), timeout=60) as response:
            return json.load(response)
    except HTTPError as error:
        if error.code == 404:
            return None
        raise


def github_release(repository, version):
    return read_json(f"https://api.github.com/repos/{repository}/releases/tags/{version}",
                     os.environ["GH_TOKEN"])


def check_github_assets(release, paths):
    assets = {asset["name"]: asset for asset in (release or {}).get("assets", [])}
    for path in paths:
        existing = assets.get(path.name)
        if existing and existing.get("digest") != "sha256:" + digest(path):
            raise ValueError(f"GitHub asset differs (or has no digest): {path.name}")
    expected = {path.name for path in paths}
    if any(name.endswith(".whl") and name not in expected for name in assets):
        raise ValueError("GitHub release contains unexpected wheels")
    return assets


def stage_unpublished(paths, version, destination):
    """Skip only files whose PyPI digest matches; never silently replace a version."""
    published = {}
    for package in PACKAGES:
        release = read_json(f"https://pypi.org/pypi/{package}/{version}/json")
        for item in (release or {}).get("urls", []):
            published[item["filename"]] = item["digests"]["sha256"]
    expected = {path.name for path in paths}
    if any(name.endswith(".whl") and name not in expected for name in published):
        raise ValueError("PyPI release contains unexpected wheels")
    pending = []
    for path in paths:
        if path.name in published:
            if published[path.name] != digest(path):
                raise ValueError(f"PyPI already has different bytes for {path.name}")
        else:
            pending.append(path)
    destination.mkdir(parents=True, exist_ok=False)
    for path in pending:
        shutil.copyfile(path, destination / path.name)
    return len(pending)


def publish_github(repository, version, paths):
    release = github_release(repository, version)
    assets = check_github_assets(release, paths)
    command = ["gh", "release"]
    if release is None:
        subprocess.run(command + ["create", version, "--repo", repository, "--verify-tag",
                                  "--draft", "--title", version,
                                  "--notes", f"Release {version}"], check=True)
    missing = [str(path) for path in paths if path.name not in assets]
    if missing:
        subprocess.run(command + ["upload", version, "--repo", repository, *missing], check=True)
    if release is None or release.get("draft"):
        # A maintenance patch must not make an older release line the latest release.
        subprocess.run(command + ["edit", version, "--repo", repository,
                                  "--draft=false", "--latest=false"], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("bundle", "verify", "prepare-publish", "publish-github"))
    parser.add_argument("--version", required=True)
    parser.add_argument("--directory", type=Path, default=Path("release-bundle"))
    parser.add_argument("--source", type=Path, default=Path("packages"))
    parser.add_argument("--pending", type=Path, default=Path("pypi-upload"))
    args = parser.parse_args()
    if args.command == "bundle":
        bundle(args.source, args.directory, args.version)
        return
    paths = verify(args.directory, args.version)
    if args.command == "verify":
        return
    repository = os.environ["GITHUB_REPOSITORY"]
    assets = paths + [args.directory / "SHA256SUMS"]
    if args.command == "prepare-publish":
        check_github_assets(github_release(repository, args.version), assets)
        count = stage_unpublished(paths, args.version, args.pending)
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(f"pending={count}\n")
    else:
        publish_github(repository, args.version, assets)


if __name__ == "__main__":
    main()
