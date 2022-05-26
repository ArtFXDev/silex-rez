def commands(env, root):
    env.PATH.prepend(root)
    env.PYTHONPATH.prepend(root)
    # Set this variable to your houdini install path
    houdini_install_paths = ["C:/Houdini18"]
    houdini_preferences_path = "C:/Users/etudiant/Documents/houdini18.5"

    for houdini_install_path in houdini_install_paths:
        env.HFS.set(houdini_install_path)
        env.PATH.append(f"{houdini_install_path}/bin")
        env.PYTHONPATH.append(f"{houdini_install_path}/python37/lib/site-packages")
        env.PYTHONPATH.append(f"{houdini_install_path}/houdini/python3.7libs")
        env.PATH.append(f"{houdini_install_path}/toolkit/include")
        env.HOUDINI_PATH.append(f"{houdini_install_path}/houdini")
    env.HOUDINI_PATH.append(houdini_preferences_path)

    env.HOUDINI_NO_ENV_FILE = 1
    env.HOUDINI_ACCESS_METHOD = 2  # noqa needed to stream abc/fbx on network
