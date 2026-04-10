load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "leveldb",
    srcs = glob(
        [
            "db/*.cc",
            "table/*.cc",
            "util/*.cc",
        ],
        exclude = [
            "**/*_test.cc",
            "**/*_bench.cc",
            "db/leveldbutil.cc",
            "db/db_bench.cc",
            "util/env_windows.cc",
            "util/testutil.cc",
        ],
    ),
    hdrs = glob([
        "db/*.h",
        "table/*.h",
        "util/*.h",
        "include/leveldb/*.h",
        "port/*.h",
        "helpers/memenv/*.h",
    ]),
    copts = [
        "-DLEVELDB_PLATFORM_POSIX=1",
        "-DLEVELDB_IS_BIG_ENDIAN=0",
        "-DHAVE_SNAPPY=1",
    ],
    includes = [
        ".",
        "include",
    ],
    deps = [
        "@com_google_crc32c//:crc32c",
        "@com_google_snappy//:snappy",
    ],
)
