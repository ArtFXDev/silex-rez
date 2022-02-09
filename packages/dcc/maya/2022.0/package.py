name = "maya"
version = "2022.0"

requires = [
    "python-3.7",
]


tools = [
    "designer",
    "fcheck",
    "imconvert",
    "imgcvt",
    "lconvert",
    "maya",
    "mayabatch",
    "mayapy",
    "pyside2-rcc",
    "rcc",
    "shiboken2",
    "uconv",
    "uic",
]

variants = [
    ["platform-windows"]
]

def commands():
    import sys
    sys.path.append(root)
    import mayaenv
    mayaenv.commands(env, root)
