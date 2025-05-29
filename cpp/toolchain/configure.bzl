"""Configures the C++ toolchain."""

load("//toolchain:rules.bzl", "get_target_triplet", "runai_crosstool_tools")

# This should map to the devcontainer Dockerfile
# We install gcc-x86-64-linux-gnu and gcc-aarch64-linux-gnu toolchains
ARCHITECTURES = ["x86_64", "aarch64"]
HOST_ARCH = "x86_64"
OS = "linux-gnu"

def _get_gcc_version(repository_ctx, arch):
    gcc_tool = runai_crosstool_tools(get_target_triplet(OS, arch))["gcc"]
    return repository_ctx.execute([gcc_tool, "-dumpversion"]).stdout.strip()

def _cc_autoconf_toolchain_impl(repository_ctx):
    define_statements = []
    for arch in ARCHITECTURES:
        gcc_version = _get_gcc_version(repository_ctx, arch)
        toolchain_name = arch
        define_statements.append('define_toolchain(name = "%s", os = "%s", host_arch = "%s", arch = "%s", gcc_version = "%s")' % (toolchain_name, OS, HOST_ARCH, arch, gcc_version))

    repository_ctx.template(
        "BUILD",
        repository_ctx.path(Label("//toolchain/template:BUILD.tpl")),
        {
            "%{DEFINE_STATEMENTS}": "\n".join(define_statements),
        },
    )
    repository_ctx.file("toolchain.bzl", repository_ctx.read(Label("//toolchain/template:toolchain.bzl")))
    repository_ctx.file("rules.bzl", repository_ctx.read(Label("//toolchain:rules.bzl")))

_cc_autoconf_toolchain = repository_rule(
    implementation = _cc_autoconf_toolchain_impl,
    # Indicates that the repository inspects the system for configuration purpose
    configure = True,
)

def configure_toolchain(name):
    """configure_toolchain creates and registers toolchain targets for ARCHITECTURES in a repository.

    Toolchains can be referenced by "@<name>//:<arch>"

    Args:
      name: The name of the toolchain repository.
    """
    _cc_autoconf_toolchain(name = name)
    native.register_toolchains(
        "@%s//:all" % name,
    )
