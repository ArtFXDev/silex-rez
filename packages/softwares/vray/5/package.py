name = "vray"
version = "5"

variants = [
    ["maya"],
    ["houdini"],
]


def commands():
    import vrayenv

    vray_install_path = "C:/Maya2022/Maya2022/vray/bin"
    env.PATH.append(vray_install_path)

    vrayenv.commands(env, root)
