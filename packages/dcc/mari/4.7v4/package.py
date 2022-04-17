name = "mari"
version = "4.7v4"

requires = [
    "python-3.7",
]

tools = [
    "python",
    "mari",
    "pyside2-rcc",
]

variants = [
     ["platform-windows"],
]

def commands():
    import sys
    
    alias("mari", "Mari5.0v2")

    sys.path.append(root)
    import marienv
    marienv.commands(env, root)