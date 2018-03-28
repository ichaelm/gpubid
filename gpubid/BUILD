py_binary(
    name = "gbs",
    srcs = ["gbs.py"],
    deps = [
        ":crypto_price",
        ":gpu_scheduler_runnable",
    ],
)

py_library(
    name = "gpu_scheduler_runnable",
    srcs = ["gpu_scheduler_runnable.py"],
)

py_library(
    name = "crypto_price",
    srcs = ["crypto_price.py"],
)

py_runtime(
    name = "python-3.5.2",
    files = [],
    interpreter_path = "/usr/bin/python3",
    visibility = ["//visibility:public"],
)
