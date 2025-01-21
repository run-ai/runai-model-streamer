load(
    "@bazel_tools//tools/cpp:cc_configure.bzl",
    "cc_autoconf_impl",
    "cc_autoconf_toolchains",
)

def __tool_path(toolchain_name, tool_name):
    return "/usr/bin/" + toolchain_name  + tool_name

def runai_crosstool_tools(toolchain_name):
    return {
        "ld":           __tool_path(toolchain_name, "ld"),
        "gcc":          __tool_path(toolchain_name, "gcc"),
        "g++":          __tool_path(toolchain_name, "g++"),
        "ar":           __tool_path(toolchain_name, "ar"),
        "cpp":          __tool_path(toolchain_name, "cpp"),
        "gcov":         __tool_path(toolchain_name, "gcov"),
        "nm":           __tool_path(toolchain_name, "nm"),
        "objdump":      __tool_path(toolchain_name, "objdump"),
        "strip":        __tool_path(toolchain_name, "strip"),
        "cc1plus":      __tool_path(toolchain_name, "cc1plus")
    }

def runai_cc_autoconf_configurator(impl):
    # This configuration was heavily inspired (copied) for bazel native cc toolchain
    # configuration.
    # when updating bazel version, we need to check if this was changed and update accordignly
    # https://github.com/bazelbuild/bazel/blob/539f0de204793bb13579ef280338beb2d8835a89/tools/cpp/cc_configure.bzl#L93-L154
    MSVC_ENVVARS = [
        "BAZEL_VC",
        "BAZEL_VC_FULL_VERSION",
        "BAZEL_VS",
        "BAZEL_WINSDK_FULL_VERSION",
        "VS90COMNTOOLS",
        "VS100COMNTOOLS",
        "VS110COMNTOOLS",
        "VS120COMNTOOLS",
        "VS140COMNTOOLS",
        "VS150COMNTOOLS",
        "VS160COMNTOOLS",
        "TMP",
        "TEMP",
    ]
    return repository_rule(
        environ = [
            "ABI_LIBC_VERSION",
            "ABI_VERSION",
            "BAZEL_COMPILER",
            "BAZEL_HOST_SYSTEM",
            "BAZEL_CXXOPTS",
            "BAZEL_LINKOPTS",
            "BAZEL_LINKLIBS",
            "BAZEL_LLVM_COV",
            "BAZEL_PYTHON",
            "BAZEL_SH",
            "BAZEL_TARGET_CPU",
            "BAZEL_TARGET_LIBC",
            "BAZEL_TARGET_SYSTEM",
            "BAZEL_USE_CPP_ONLY_TOOLCHAIN",
            "BAZEL_USE_XCODE_TOOLCHAIN",
            "BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN",
            "BAZEL_USE_LLVM_NATIVE_COVERAGE",
            "BAZEL_LLVM",
            "BAZEL_IGNORE_SYSTEM_HEADERS_VERSIONS",
            "USE_CLANG_CL",
            "CC",
            "CC_CONFIGURE_DEBUG",
            "CC_TOOLCHAIN_NAME",
            "CPLUS_INCLUDE_PATH",
            "DEVELOPER_DIR",
            "GCOV",
            "HOMEBREW_RUBY_PATH",
            "SYSTEMROOT",
            "USER",
        ] + MSVC_ENVVARS,
        implementation = impl,
        configure = True,
    )

def runai_cc_autoconf_default_impl(ctx):
    tools = runai_crosstool_tools("")
    return cc_autoconf_impl(ctx, overriden_tools = tools)

def runai_cc_configure_toolchain(toolchain_name, impl = cc_autoconf_impl):
    """A C++ configuration rules that generate the crosstool file."""

    cc_autoconf_toolchains(name = toolchain_name + "_toolchains")

    runai_cc_autoconf = runai_cc_autoconf_configurator(impl)
    runai_cc_autoconf(name = toolchain_name)

    native.bind(name = "cc_toolchain", actual = "@" + toolchain_name + "//:toolchain")
    native.register_toolchains(
        # Use register_toolchain's target pattern expansion to register all toolchains in the package.
        "@" + toolchain_name + "_toolchains//:all",
    )

def runai_cc_autoconf_impl_x86(ctx):
    toolchain_name = "x86_64-linux-gnu-"
    tools = runai_crosstool_tools(toolchain_name)
    return cc_autoconf_impl(ctx, overriden_tools = tools)

def runai_cc_autoconf_impl_arm64(ctx):
    toolchain_name = "aarch64-linux-gnu-"
    tools = runai_crosstool_tools(toolchain_name)
    return cc_autoconf_impl(ctx, overriden_tools = tools) 
