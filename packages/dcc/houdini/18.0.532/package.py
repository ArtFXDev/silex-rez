name = "houdini"
version = "18.0.532"

requires = [
    "python-2.7",
]
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

variants = [["platform-windows"]]

def commands():
    # Set this variable to your houdini install path
    houdini_install_path = "C:/Houdini18/bin"

    env.PATH.append(f"{houdini_install_path}/bin")  # noqa
    env.PYTHONPATH.append(f"{houdini_install_path}/python27/lib/site-packages")  # noqa
    env.PYTHONPATH.append(f"{houdini_install_path}/houdini/python2.7libs")  # noqa
    env.PATH.append(f"{houdini_install_path}/toolkit/include")  # noqa
