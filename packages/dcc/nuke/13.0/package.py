name = "nuke"
version = "13.0"

requires = [
    "python-3.7",
]

tools = [
    "designer",
    "fcheck",
    "imconvert",
    "imgcvt",
    "lconvert",
    "nuke13.0",
    "python",
    "pyside2-rcc",
    "rcc",
    "shiboken2",
    "uconv",
    "uic",
]

variants = [
    ["platform-windows"],
]

def commands():
    import sys
    
    sys.path.append(root)
    import nukeenv
    nukeenv.commands(env, root)

