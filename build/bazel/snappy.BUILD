load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

genrule(
    name = "snappy_stubs_public_h",
    srcs = ["snappy-stubs-public.h.in"],
    outs = ["snappy-stubs-public.h"],
    cmd = "sed -e 's/$${HAVE_SYS_UIO_H_01}/1/g' " +
          "-e 's/$${PROJECT_VERSION_MAJOR}/1/g' " +
          "-e 's/$${PROJECT_VERSION_MINOR}/1/g' " +
          "-e 's/$${PROJECT_VERSION_PATCH}/9/g' " +
          "$< > $@",
)

cc_library(
    name = "snappy",
    srcs = [
        "snappy.cc",
        "snappy-c.cc",
        "snappy-internal.h",
        "snappy-sinksource.cc",
        "snappy-stubs-internal.cc",
        "snappy-stubs-internal.h",
    ],
    hdrs = [
        "snappy.h",
        "snappy-c.h",
        "snappy-sinksource.h",
        ":snappy_stubs_public_h",
    ],
    copts = ["-DHAVE_BUILTIN_CTZ=1", "-DHAVE_BUILTIN_EXPECT=1"],
    includes = ["."],
)
