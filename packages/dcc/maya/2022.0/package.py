name = "maya"
version = "2022.0"

requires = [
    "python-3.7",
]
tools = [
    "designer",
    "fcheck",
    "imconvert",
    "imgcvt",
    "lconvert",
    "maya",
    "mayabatch",
    "mayapy",
    "pyside2-rcc",
    "rcc",
    "shiboken2",
    "uconv",
    "uic",
]

variants = [["platform-windows"]]

def commands():
    # Set this variable to your maya install path
    maya_install_path = "C:/Maya2022/Maya2022"

    env.PATH.append(f"{maya_install_path}/bin")  # noqa
    env.PYTHONPATH.append(f"{maya_install_path}/Python37/Lib/site-packages")  # noqa
    env.QT_PLUGIN_PATH.append(f"{maya_install_path}/qt-plugins")  # noqa
    env.PATH.append(f"{maya_install_path}/devkit")  # noqa
    env.PATH.append(f"{maya_install_path}/include")  # noqa

