def commands(env, root):
    # Set this variable to your nuke install path
    nuke_install_path = "C:/Nuke13.0v3"

    # Alias nuke13.0 to nukeX for silex launch...
    env.SILEX_DCC_BIN = "nuke13.0"
    env.SILEX_DCC_BIN_ARGS = "--nukex"

    # Alias nuke command in the shell
    alias("nuke", "nuke13.0 --nukex")

    env.PATH.append(f"{nuke_install_path}")  # noqa
    env.PYTHONPATH.append(f"{nuke_install_path}/pythonextensions/site-packages")  # noqa
    env.QT_PLUGIN_PATH.append(f"{nuke_install_path}/qtplugins")  # noqa
    env.PATH.append(f"{nuke_install_path}/include")  # noqa