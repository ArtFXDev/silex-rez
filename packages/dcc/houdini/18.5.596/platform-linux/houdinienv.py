def commands(env, root):
    print(root)
    env.PYTHONPATH.append(root)
    env.PATH.append(root)
   
    houdini_install_path = "/opt/hfs18.5.759"

    env.HFS.set(houdini_install_path)
    env.PATH.append(f"{houdini_install_path}/bin")
    env.PYTHONPATH.append(
        f"{houdini_install_path}/python/lib/python3.7/site-packages"
    )
    env.HOUDINI_PATH.append(f"{houdini_install_path}/houdini")

    env.HOUDINI_NO_ENV_FILE = 1
    env.HOUDINI_ACCESS_METHOD = 2
