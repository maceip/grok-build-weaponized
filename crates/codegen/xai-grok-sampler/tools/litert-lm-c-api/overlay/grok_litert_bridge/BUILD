package(default_visibility = ["//visibility:public"])

cc_binary(
    name = "liblitert_lm_c.dylib",
    srcs = [
        "exports.cc",
        "grok_litert_bridge.h",
    ],
    linkopts = [
        "-Wl,-install_name,@rpath/liblitert_lm_c.dylib",
    ],
    linkshared = True,
    deps = [
        "//c:engine",
        "//runtime/engine:engine_settings",
        "//runtime/util:scoped_file",
    ],
)
