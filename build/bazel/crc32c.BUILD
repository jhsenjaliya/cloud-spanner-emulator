load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

# Generate crc32c_config.h with platform-appropriate settings.
# ARM64 uses hardware CRC32 instructions; x86_64 uses SSE4.2.
genrule(
    name = "crc32c_config_h",
    outs = ["crc32c/crc32c_config.h"],
    cmd = select({
        "@platforms//cpu:aarch64": """cat > $@ << 'EOF'
#ifndef CRC32C_CRC32C_CONFIG_H_
#define CRC32C_CRC32C_CONFIG_H_
#define HAVE_BUILTIN_PREFETCH 1
#define HAVE_MM_PREFETCH 0
#define HAVE_SSE42 0
#define HAVE_ARM64_CRC32C 1
#define CRC32C_TESTS_BUILT_WITH_GLOG 0
#define HAVE_STRONG_GETAUXVAL 1
#endif  // CRC32C_CRC32C_CONFIG_H_
EOF""",
        "@platforms//cpu:x86_64": """cat > $@ << 'EOF'
#ifndef CRC32C_CRC32C_CONFIG_H_
#define CRC32C_CRC32C_CONFIG_H_
#define HAVE_BUILTIN_PREFETCH 1
#define HAVE_MM_PREFETCH 1
#define HAVE_SSE42 1
#define HAVE_ARM64_CRC32C 0
#define CRC32C_TESTS_BUILT_WITH_GLOG 0
#define HAVE_STRONG_GETAUXVAL 0
#endif  // CRC32C_CRC32C_CONFIG_H_
EOF""",
        "//conditions:default": """cat > $@ << 'EOF'
#ifndef CRC32C_CRC32C_CONFIG_H_
#define CRC32C_CRC32C_CONFIG_H_
#define HAVE_BUILTIN_PREFETCH 1
#define HAVE_MM_PREFETCH 0
#define HAVE_SSE42 0
#define HAVE_ARM64_CRC32C 1
#define CRC32C_TESTS_BUILT_WITH_GLOG 0
#define HAVE_STRONG_GETAUXVAL 1
#endif  // CRC32C_CRC32C_CONFIG_H_
EOF""",
    }),
)

cc_library(
    name = "crc32c",
    srcs = [
        "src/crc32c.cc",
        "src/crc32c_portable.cc",
    ] + select({
        "@platforms//cpu:aarch64": ["src/crc32c_arm64.cc"],
        "@platforms//cpu:x86_64": ["src/crc32c_sse42.cc"],
        "//conditions:default": ["src/crc32c_arm64.cc"],
    }),
    hdrs = [
        "include/crc32c/crc32c.h",
        ":crc32c_config_h",
    ],
    copts = ["-DHAVE_BUILTIN_PREFETCH=1", "-DCRC32C_TESTS_BUILT_WITH_GLOG=0"] + select({
        "@platforms//cpu:aarch64": ["-march=armv8-a+crc+crypto"],
        "@platforms//cpu:x86_64": ["-msse4.2"],
        "//conditions:default": [],
    }),
    includes = ["include"],
    textual_hdrs = [
        "src/crc32c_arm64.h",
        "src/crc32c_arm64_check.h",
        "src/crc32c_internal.h",
        "src/crc32c_prefetch.h",
        "src/crc32c_read_le.h",
        "src/crc32c_round_up.h",
        "src/crc32c_sse42.h",
        "src/crc32c_sse42_check.h",
    ],
)
