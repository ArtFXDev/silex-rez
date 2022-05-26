name = "substance_painter"
version = "7.2.1"

requires = [
    "python-3.7",
]

tools = [
    "python",
    "pyside2-rcc",
]

variants = [
    ["platform-windows"],
]

def commands():
    import sys

    sys.path.append(root)

    executable = '"Adobe Substance 3D Painter"'
    alias("painter", f"{executable}")

    import spenv
    spenv.commands(env, root)