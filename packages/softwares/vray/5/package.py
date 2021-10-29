name = "vray"
version = "18.5.596"
build_command = False

variants = [
    ["houdini"],
    ["maya"]
]
def commands():
    env.PATH.append('{root}')  # noqa
    env.PYTHONPATH.append('{root}')  # noqa
    import sys
    sys.path.append(root)
    
    import vrayenv
    vrayenv.commands(env)
    