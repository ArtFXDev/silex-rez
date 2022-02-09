name = "blender"
version = "2.93"

tools = [
    "blender"
]

variants = [
    ["platform-windows"],
    ["platform-linux"],
]

def commands():
    import sys
    sys.path.append(root)
    import blenderenv
    blenderenv.commands(env, root)
