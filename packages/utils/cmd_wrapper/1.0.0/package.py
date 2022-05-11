name = "cmd_wrapper"

version = "1.0.0"

variants = [
    ["platform-windows"],
]

tools = ["cmd-wrapper"]


def commands():
    env.PATH.append("{root}")
