name = "arnold"
version = "5.6.3.0"

variants = [
    ["platform-windows", "maya"],
    ["platform-windows", "houdini"],
    ["platform-linux", "maya"],
    ["platform-linux", "houdini"],
]

def commands():
    import sys
    sys.path.append(root)
    import arnoldenv
    arnoldenv.commands(env, root)
