name = "arnold"
version = "5.6.3.0"

variants = [
    ["houdini"],
    ["maya"]
]

def commands():
    import sys
    sys.path.append(root)
    import arnoldenv
    arnoldenv.commands(env, root)
