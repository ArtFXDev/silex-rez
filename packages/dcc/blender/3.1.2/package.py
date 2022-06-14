name = "blender"
version = "3.1.2"

tools = [
    "blender"
]

variants = [
    ["platform-windows"],
]

def commands():
    import sys
    sys.path.append(root)
    import blenderenv
    blenderenv.commands(env, root)
