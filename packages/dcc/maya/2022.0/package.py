name = "maya"
version = "2022.0"

requires = [
    "python-3.7",
]


tools = [
    "fcheck",
    "imgcvt",
    "maya",
    "mayabatch",
    "mayapy",
    "rcc",
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
