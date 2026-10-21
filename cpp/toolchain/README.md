# Toolchain

This directory contains configuration for generating toolchain targets
from a standard GCC/G++ toolchain.

The toolchain is installed at the repository specified by the `configure_toolchain()` rule.
It registers only the running container's architecture (`aarch64` or `x86_64`),
using native GCC/G++ and binutils from `/usr/bin`. Cross-compilation is not supported.

See .devcontainer/Dockerfile for toolchain Ubuntu package installation.

## Using the toolchain

Place the following in your `WORKSPACE` file:

```
load("//toolchain:configure.bzl", "configure_toolchain")
configure_toolchain(name = "my_awesome_toolchain")
```

This will allow bazel to reference generated targets using the name of the repository:

```
bazel build --platforms=@my_awesome_toolchain//:aarch64 //mytarget
```

The example targets an ARM64 container. Use `x86_64` in an AMD64 container.
In this repository, `make build` builds and packages only the native architecture.
Build the other architecture in its matching devcontainer on a native runner.
