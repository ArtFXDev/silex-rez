name = "nuke"
version = "13.0"

requires = [
    "python-3.7",
]
tools = [
    "designer",
    "fcheck",
    "imconvert",
    "imgcvt",
    "lconvert",
    "nuke13.0",
    "python",
    "pyside2-rcc",
    "rcc",
    "shiboken2",
    "uconv",
    "uic",
]


def commands():
    # Set this variable to your nuke install path
    nuke_install_path = "C:/Nuke13.0v3"

    env.PATH.append(f"{nuke_install_path}")  # noqa
    env.PYTHONPATH.append(f"{nuke_install_path}/pythonextensions/site-packages")  # noqa
    env.QT_PLUGIN_PATH.append(f"{nuke_install_path}/qtplugins")  # noqa
    env.PATH.append(f"{nuke_install_path}/include")  # noqa

