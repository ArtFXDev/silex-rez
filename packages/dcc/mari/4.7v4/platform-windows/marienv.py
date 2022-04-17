def commands(env, root):
    mari_install_path = "C:/Program Files/Mari4.7v4"
    env.SILEX_DCC_BIN = "Mari4.7v4"

    env.PATH.append(f"{mari_install_path}/Bundle/bin")
    env.QT_PLUGIN_PATH.append(f'{mari_install_path}/Bundle/bin/qtplugins')

    if "FOUNDRY_LICENSE" in env.keys():
        env.FOUNDRY_LICENSE.set(str(env.FOUNDRY_LICENSE))

    env.MARI_SCRIPT_PATH.append("D:/rez/dev_packages/silex_mari/startup")