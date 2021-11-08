name = "houdini"
version = "18.5.596"

tools = [
    "pyside2-rcc",
    "rcc",
    "uic",
    "hbatch",
    "houdinifx",
    "hrender",
    "hscript",
    "hython",
]

def commands():
    # Set this variable to your houdini install path
    houdini_install_path = "C:/Houdini18"
    env.HFS.set(houdini_install_path)
    env.PATH.append(f"{houdini_install_path}/bin")  # noqa
    env.PYTHONPATH.append(f"{houdini_install_path}/python37/lib/site-packages")  # noqa
    env.PYTHONPATH.append(f"{houdini_install_path}/houdini/python3.7libs")  # noqa
    env.PATH.append(f"{houdini_install_path}/toolkit/include")  # noqa
    env.HOUDINI_PATH.append(f"{houdini_install_path}/houdini") # noqa

    env.HOUDINI_NO_ENV_FILE = 1 # noqa
    env.HOUDINI_PACKAGE_SKIP = 1 # noqa

