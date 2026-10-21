# Contributing to Model Streamer

Thank you for your interest in contributing to Model Streamer! This document provides guidelines and instructions to help you get started with contributing to our project.

## Getting Started
### New Contributors
We're excited to help you make your first contribution! Whether you're looking to file issues, develop features, fix bugs, or improve documentation, we're here to support you through the process.

Browse issues labeled `good first issue` or `help wanted` on GitHub for an easy introduction.

### Developers
The main building blocks of Model Streamer are documented in the [docs](docs/README.md) folder. Here are the key components:
- `cpp/streamer` - Core C++ streaming engine
- `cpp/s3`, `cpp/gcs`, `cpp/azure` - Object storage backend clients
- `cpp/cc` - C API exposed to Python via ctypes
- `py/runai_model_streamer` - Python SDK
- `py/runai_model_streamer_s3`, `py/runai_model_streamer_gcs`, `py/runai_model_streamer_azure` - Storage-specific Python packages

We recommend reviewing the [README](README.md) to understand the system architecture and build process before making significant contributions.

## How to Contribute
### Signing Off Your Work

We require that all contributors "sign-off" on their commits. This certifies
that the contribution is your original work, or you have rights to submit it
under the same license, or a compatible license.

Any contribution which contains commits that are not Signed-Off will not be
accepted. Read the full [Developer Certificate of Origin 1.1](DCO) before
signing off. The upstream text is at [developercertificate.org](https://developercertificate.org/).

Use the `--signoff` (or `-s`) option when committing your changes:

```bash
git commit -s -m "Add cool feature."
```

This appends a line using your configured Git identity:

```text
Signed-off-by: Your Name <your@email.com>
```

Only add your own sign-off when you can make the DCO certification. Preserve
the sign-offs and attribution of other contributors.

### Full Text of the DCO

```text
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

### NVIDIA IP Review

NVIDIA contributors and maintainers must follow the
[NVIDIA IP review process](https://nv/ip_review_process) for ongoing changes
and third-party contributions before accepting or distributing them.

### Source File Headers

Every source file needs this two-line NVIDIA SPDX header, written in the comment style of the file type:

```text
SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
```

Documentation, configuration and data files (for example `.md`, `.yaml`, `.json`) do not need one.
A pull request check fails when a file is missing the header. To add it, run
`python3 scripts/spdx_headers.py --fix` from the repository root.

To add the header automatically on every commit, install [pre-commit](https://pre-commit.com) and run
`pre-commit install` once.

### Reporting Issues
Open an issue with a clear description, steps to reproduce, and relevant environment details.

### Improving Documentation
Help us keep the docs clear and useful by fixing typos, updating outdated information, or adding examples.

### Contributing Changes
- Open or Find an Issue – Non-trivial pull requests should be associated with a GitHub issue. Search existing issues first; if none fits, open one before you start work. See [Linking Pull Requests to Issues](#linking-pull-requests-to-issues).
- Fork and Clone – Begin by forking the repository and cloning it to your local machine.
- Create a Branch – Use a descriptive branch name, such as `feature/add-cool-feature` or `bugfix/fix-issue123`.
- Make Changes – Keep your commits small, focused, and well-documented. For build and test instructions, refer to the [Development section](README.md#development) of the README.
- Submit a PR – For non-trivial changes, link the pull request to its issue (for example `Fixes #123` or `Relates to #123` in the description).
- Coverage - Please look at the coverage change details and create unit tests, integration tests or end-to-end tests to cover new functionality or changes.

### Linking Pull Requests to Issues

We ask that non-trivial pull requests reference an issue. The issue is the single place where
the problem, the scope, and the design discussion live. A non-trivial pull request without
an issue is hard to track, especially when:

- Changes are unified or split across several pull requests.
- The scope of a change shifts during review.
- Maintainers need to see why a change was made, or what else is related to it.

Use `Fixes #<issue>` or `Closes #<issue>` when the pull request completes the
issue, and `Relates to #<issue>` when it is only a part of it. If review changes
the scope, update the issue, or open follow-up issues for the work that moves out.

Trivial changes, such as typo fixes or small documentation updates, don't need
an issue. Maintainers may ask for one if a change turns out to be larger than it
first appeared.

### PR Title Guidelines

We recommend following the [Conventional Commits](https://www.conventionalcommits.org/) title specification. The format is:

```
<type>[optional scope]: <description>
```

#### Types

- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation only changes
- **style**: Changes that don't affect code meaning (formatting, whitespace)
- **refactor**: Code changes that neither fix a bug nor add a feature
- **perf**: Performance improvements
- **test**: Adding or updating tests
- **build**: Changes to build system or dependencies
- **ci**: Changes to CI/CD configuration
- **chore**: Other changes that don't modify src or test files
- **revert**: Reverts a previous commit

#### Scopes (Optional)

Common scopes for Model Streamer:
- `streamer`
- `s3`
- `gcs`
- `azure`
- `py`
- `cpp`
- `ci`
- `docs`

#### Breaking Changes

Breaking changes MUST be indicated by adding `!` after the type/scope: `feat(s3)!: remove deprecated field`

#### Examples

```
feat(s3): add retry with bounded deadline for transient chunk failures
fix(azure): resolve race condition in credential refresh
docs: update installation guide
refactor(streamer): simplify ring buffer allocation
feat(gcs)!: remove deprecated field from client config
```

#### Tips

- Use the imperative mood: "add feature" not "added feature"
- Don't end with a period

### Pull Request Checklist

Before introducing major changes, we strongly recommend opening a PR that outlines your proposed design.
Each pull request should meet the following requirements:
- All tests pass – Run the full test suite locally with: `make test`
- Test coverage – Add or update tests for any affected code.
- Documentation – Update relevant documentation to reflect your changes.
- PR description – Clearly describe what changed and why.
- Linked issue – For non-trivial changes, the description references the associated issue (`Fixes #123`).
- DCO – Every commit has the contributor's `Signed-off-by` line.

## Pull Request CI Environment

The setup job checks whether `.devcontainer/**` or the PR/devcontainer workflows
changed. If they did, the preparation job builds the checked-out devcontainer
using the reusable `devcontainer.yml` workflow and the published image as a build cache. With
`publish: false`, it saves the image as a temporary Actions artifact, retained for
one day. The artifact is named `ci-devcontainer-<run>-<attempt>`. The artifact
and image tags include the run ID and attempt to isolate
retries. This is a per-run image handoff, not a cache restored across PR runs.

All eight build jobs and the integration job download and load that same image.
Each build job starts one Docker container and uses `docker exec` for separate
C++ build, Python package build, C++ test, and Python test steps. The container
shares build outputs and installed dependencies across steps and is stopped even
if a build or test fails. The integration job runs through `devcontainers/ci`
with an image-only configuration. Neither path rebuilds the Dockerfile or
reinstalls Features. When those files are unchanged, the preparation jobs are
skipped and jobs use the published
`ghcr.io/dsx-ai-factory/model-streamer/devcontainer:latest` image directly. Both
paths preserve the seccomp setting required by the io_uring tests.

The **Build and Push DevContainer** workflow calls the same reusable workflow
with `publish: true` on master. It pushes the image to GHCR instead of uploading
an image artifact. PRs never push images to GHCR. The final required check fails
if preparation of a changed image fails; unchanged-image PRs still run all
application jobs.

Two additional preparation jobs build candidate devcontainers in parallel with
the existing image: AMD64 on `ubuntu-24.04` and ARM64 on `ubuntu-24.04-arm`.
Each uses the same Dockerfile and Features, but builds only its matching
architecture's basic libraries, IO libraries, AWS SDK, and Azure SDK. Both compiler
toolchains remain installed during this transition. The ARM64 candidate skips the
x86-specific TSAN workaround. Candidate images are built only; runtime validation
is deferred to the integration phase when CI switches to these images.

The reusable image workflow accepts an optional `architecture` input (`amd64` or
`arm64`); omitting it preserves the existing dual-architecture dependency build.
It sets `DEVCONTAINER_DEPENDENCY_ARCHES`, which the devcontainer configuration
passes as the `DEPENDENCY_ARCHES` Docker build argument. Local builds default to
`amd64 arm64` when that environment variable is unset.

Candidate PR image tags append `-amd64` or `-arm64` to the existing run/attempt
names. Candidates skip image export and artifact upload because no application
job consumes them yet. Only the existing image is uploaded for downstream jobs,
with one-day artifact retention. Application builds wait only for the existing
image; the final **Test, Build & Push** check additionally requires both candidate
jobs to succeed when image-related files changed, and accepts their skips otherwise.

On master and supported release branches, the publication workflow builds all
three images independently. Candidates use `latest-amd64` / `latest-arm64` or
`release-X.Y-amd64` / `release-X.Y-arm64` in the same GHCR image repository. Each
candidate imports only its matching branch-and-architecture image as a cache;
missing caches allow a clean build. Existing tags and CI consumers remain unchanged.
This temporarily increases CI compute while preparing the later consumer switch.

### Parallel PR checks

A setup job computes `PACKAGE_VERSION` once. Separate `build_x86` and `build_arm`
jobs in `on-pr.yaml` each call `build.yml` for four components: core (`streamer`),
S3, GCS, and Azure. They pass `x86_64` and `aarch64`, respectively, for eight builds
in total. The reusable workflow handles one architecture/component pair,
accepting the package version, selected image tag, and optional image artifact.
It contains no matrix, so other workflows can call a single build or define their
own matrix. Each invocation runs
`make ci-build-cpp` and `make ci-build-python` inside the selected devcontainer, using the same
native build command and packaging target as the existing full build. Both
commands take `COMPONENT` and `ARCH`; `make ci-build` runs them in order locally.
Each x86_64 job then runs
`make ci-test-cpp COMPONENT=... ARCH=x86_64` before uploading its wheel. The test
command uses the same architecture and backend defines as the preceding build,
allowing Bazel to reuse compatible compiled outputs on that runner.

S3, GCS, and Azure run their respective C++ test trees; core runs everything
outside those three trees, including common, POSIX I/O, and utility tests.
The core x86_64 job also runs `make ci-test-python`, installing only its core wheel
and Python test dependencies before running unit and distributed tests. Python
feedback therefore does not wait for the cloud backend builds. ARM64 jobs build
packages only.

After all four x86_64 builds succeed, the integration job downloads their wheels
and runs `make ci-test-integration`: integration suites followed by the filesystem
strategy sweep, without waiting for ARM64 builds. It uses `make ci-install` to install all four wheels via the
existing Python package Makefiles. Python distributed tests load the installed
real library; the mock unit tests retain their existing override. The integration
job also reuses the installed core library for the Python filesystem strategy
tests through `test-unit-real-installed`, avoiding another full native build.
The same tests still run for all four strategies. The integration job still builds
the two C++ strategy test targets and the Azure testing variant. `make test` remains
the full sequential local entry point and builds from source as before.

The build jobs upload eight intermediate `ci-packages-<component>-<arch>`
artifacts, retained for seven days for use by later CI jobs. The **Assemble packages**
job runs alongside integration tests after both architectures finish building.
It calls the reusable `assemble-bundle.yml` workflow to create one final download,
`release-bundle-pr-<run>-<attempt>`, with the same layout as the release bundle:
eight wheels under `wheels/`, two complete C++ SDK
archives under `cpp/`, and `SHA256SUMS`. SDK archives contain the libraries,
public headers, license, and third-party notices. The bundler checks completeness,
ELF architectures, matching headers/licenses, and checksums before upload.
PR versions use `0.0.0+dev.<commit>`; release publication still requires `X.Y.Z`.
Download the bundle for Python wheels and complete C++ SDKs. Bundle assembly
runs independently of integration tests; require the final **Test, Build & Push**
check to pass before using a PR build.
Intermediate wheel and C++ artifacts are retained for seven days in both PR and
release builds to support retries. Final PR bundles are retained for seven days;
tested `release-bundle-<version>-<run>-<attempt>` bundles are retained for 30 days
for approval and recovery.
Release CI calls the same assembly workflow after integration tests succeed,
then passes its bundle artifact ID to the approval-gated publication job.
The shared workflow downloads components, tests the packaging code, assembles
and verifies the bundle, and uploads it. Callers select the version, artifact
name, final-bundle retention, and development-version opt-in.
Each build job also uploads an intermediate `ci-cpp-<component>-<arch>`
artifact containing its shared library under `lib/` and the repository license.
All C++ artifacts include public C/C++ headers under
`include/streamer/`. Backend artifacts contain their plugin library; use them
alongside the core library for the same architecture. Libraries are copied out
of the container before it stops so the downloads contain real files rather
than Bazel symlinks. The `Test, Build & Push` required check succeeds
only when all eight builds (including their C++ and Python tests), integration
tests, package assembly, and any triggered candidate image builds succeed. The
existing devcontainer is built once before the matrix starts when its inputs
change; the eight component runners can overlap with the two candidate image
builders during this transition.

To verify an extracted PR bundle locally, substitute its development version:

```sh
python3 .github/scripts/release_bundle.py verify \
  --version 0.0.0+dev.abc1234 --allow-development-version --bundle release-bundle
```

## Getting Help
Need support or have a question? We're here to help:
- Report issues or ask questions by [opening an issue on GitHub](https://github.com/dsx-ai-factory/model-streamer/issues).

## License
By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

Thank you for your interest and happy coding!
