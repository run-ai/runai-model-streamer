"""Rules used to setup the C++ toolchain."""

load("@rules_cc//cc:cc_toolchain_config_lib.bzl", "tool_path")  # buildifier: disable=deprecated-function
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/toolchains:cc_toolchain.bzl", "cc_toolchain")
load(":rules.bzl", "get_target_triplet", "runai_crosstool_tools")


def _builtin_include_directories(target_triplet, gcc_version):
    return [
        "/usr/include/c++/%s" % gcc_version,
        "/usr/include/%s/c++/%s" % (target_triplet, gcc_version),
        "/usr/include/c++/%s/backward" % gcc_version,
        "/usr/lib/gcc/%s/%s/include" % (target_triplet, gcc_version),
        "/usr/local/include",
        "/usr/lib/gcc/%s/%s/include-fixed" % (target_triplet, gcc_version),
    ]


def _cross_include_directories(target_triplet, gcc_version):
    return [
        "/usr/lib/gcc-cross/%s/%s/include" % (target_triplet, gcc_version),
        "/usr/%s/include" % target_triplet,
    ]


def _get_include_directories(target_triplet, gcc_version, use_cross):
    return (_cross_include_directories(target_triplet, gcc_version) if use_cross else _builtin_include_directories(target_triplet, gcc_version)) + [
        "/usr/include/%s" % target_triplet,
        "/usr/include",
    ]


def _toolchain_identifier(name):
    return "%s-toolchain" % name


def _impl(ctx):
    target_triplet = get_target_triplet(ctx.attr.os, ctx.attr.arch)
    tool_paths = [tool_path(name = k, path = v) for k, v in runai_crosstool_tools(target_triplet).items()]

    # Documented at
    # https://docs.bazel.build/versions/main/skylark/lib/cc_common.html#create_cc_toolchain_config_info.
    #
    # create_cc_toolchain_config_info is the public interface for registering
    # C++ toolchain behavior.
    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        toolchain_identifier = _toolchain_identifier(ctx.attr.name),
        host_system_name = "local",
        target_system_name = "local",
        target_cpu = ctx.attr.arch,
        target_libc = "unknown",
        compiler = "gcc",
        abi_version = "unknown",
        abi_libc_version = "unknown",
        tool_paths = tool_paths,
        cxx_builtin_include_directories = _get_include_directories(target_triplet, ctx.attr.gcc_version, ctx.attr.use_cross)
    )


_toolchain_config = rule(
    implementation = _impl,
    # You can alternatively define attributes here that make it possible to
    # instantiate different cc_toolchain_config targets with different behavior.
    attrs = {
        "os": attr.string(
            mandatory = True,
            doc = "The operating system (eg: linux-gnu)",
        ),
        "arch": attr.string(
            mandatory = True,
            doc = "The architecture (eg: x86_64 / aarch64)",
        ),
        "gcc_version": attr.string(
            mandatory = True,
            doc = "GCC major version to use (eg: 9)",
        ),
        "use_cross": attr.bool(
            mandatory = False,
            default = False,
            doc = "Uses crosstool include paths if True",
        ),
    },
    provides = [CcToolchainConfigInfo],
)


def define_toolchain(name, os, host_arch, arch, gcc_version):
    """"define_toolchain creates rules for a cc_toolchain.

    This expects toolchain tools to exist at a canonical path.
     * Tools are expected to in /usr/bin/<target_triplet>-<tool-name>
     * Include paths are expected to be at canonical locations

    Args:
      name: The name of the toolchain
      os: The operating system target for this toolchain
      host_arch: The host architecture for this toolchain
      arch: The target architecture for this toochain
      gcc_version: The GCC version of this toolchain
    """

    native.platform(
        name = name,
        constraint_values = [
            "@platforms//cpu:%s" % arch,
            "@platforms//os:linux",
        ],
    )

    toolchain_config_name = "%s_toolchain_config" % name
    _toolchain_config(name = toolchain_config_name, os = os, arch = arch, gcc_version = gcc_version, use_cross = host_arch != arch)

    empty_target_name = "%s_empty" % name
    empty_target_label = ":%s" % empty_target_name
    native.filegroup(name = empty_target_name)

    cc_toolchain(
        name = "%s_toolchain" % name,
        toolchain_identifier = _toolchain_identifier(name),
        toolchain_config = ":%s" % toolchain_config_name,
        all_files = empty_target_label,
        compiler_files = empty_target_label,
        dwp_files = empty_target_label,
        linker_files = empty_target_label,
        objcopy_files = empty_target_label,
        strip_files = empty_target_label,
        supports_param_files = 0,
    )

    native.toolchain(
        name = "%s_linux_toolchain" % name,
        exec_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % host_arch,
        ],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % arch,
        ],
        toolchain = ":%s_toolchain" % name,
        toolchain_type = "@bazel_tools//tools/cpp:toolchain_type",
    )
