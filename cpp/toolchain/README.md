# Toolchain

This directory contains configuration for generating toolchain targets
from a standard GCC/G++ toolchain.

The toolchain is installed at the repository specified by the `configure_toolchain()` rule.
It instals toolchain configuration for `aarch64-linux-gnu` and `x86_64-linux-gnu`

See .devcontainer/Dockerfile for toolchain Ubuntu package installation.

## Using the toolchain

Place the following in your `WORKSPACE` file:

```
load("//toolchain:configure.bzl", "configure_toolchain")
configure_toolchain(name = "my_awesome_toolchain")
```

This will allow bazel to reference generated targets using the name of the repository:

```
bazel build --config=@my_awesome_toolchain//:aarch64 //mytarget
```
