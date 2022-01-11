name = "vray"
version = "5"

variants = [
    ["maya"],
    ["houdini"],
]

def commands():
    import sys
    sys.path.append(root)
    import vrayenv
    vrayenv.commands(env, root)
