name = "houdini"
version = "18.5.596"

tools = [
    "pyside2-rcc",
    "rcc",
    "uic",
    "hbatch",
    "houdinifx",
    "hrender",
    "hscript",
    "hython",
]

variants = [
    ["platform-windows"],
    ["platform-linux"],
]

def commands():
    import sys
    
    sys.path.append(root)
    import houdinienv
    
    houdinienv.commands(env, root)
