load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

# Generate crc32c_config.h (normally done by CMake)
genrule(
    name = "crc32c_config_h",
    outs = ["crc32c/crc32c_config.h"],
    cmd = """cat > $@ << 'EOF'
#ifndef CRC32C_CRC32C_CONFIG_H_
#define CRC32C_CRC32C_CONFIG_H_

// Defined if the compiler has the __builtin_prefetch intrinsic.
#define HAVE_BUILTIN_PREFETCH 1

// Defined if the compiler supports __attribute__((target("...")))
#define HAVE_MM_PREFETCH 0

// Defined if the compiler supports _mm_crc32_u8 and friends (SSE4.2)
#define HAVE_SSE42 0

// Defined if the compiler supports __crc32cb and friends (ARM CRC32)
#define HAVE_ARM64_CRC32C 1

// Defined if the CRC32C tests are built with GLOG
#define CRC32C_TESTS_BUILT_WITH_GLOG 0

// Defined if the compiler supports __builtin_prefetch
#define HAVE_STRONG_GETAUXVAL 1

#endif  // CRC32C_CRC32C_CONFIG_H_
EOF""",
)

cc_library(
    name = "crc32c",
    srcs = [
        "src/crc32c.cc",
        "src/crc32c_portable.cc",
    ],
    hdrs = [
        "include/crc32c/crc32c.h",
        ":crc32c_config_h",
    ],
    copts = ["-DHAVE_BUILTIN_PREFETCH=1", "-DCRC32C_TESTS_BUILT_WITH_GLOG=0"],
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
