def commands(env, root):
    mari_install_path = "C:/Program Files/Mari5.0v2"
    env.SILEX_DCC_BIN = "Mari5.0v2"

    env.PATH.append(f"{mari_install_path}/Bundle/bin")
    env.QT_PLUGIN_PATH.append(f'{mari_install_path}/Bundle/bin/qtplugins')

    if "FOUNDRY_LICENSE" in env.keys():
        env.FOUNDRY_LICENSE.set(str(env.FOUNDRY_LICENSE))

