name = "natron"
version = "2.4.3"


tools = [
    "Natron",
    "NatronRenderer"
]

variants = [
    ["platform-windows"]
]

def commands():
    import sys
    sys.path.append(root)
    import natron_env
    natron_env.commands(env, root)
