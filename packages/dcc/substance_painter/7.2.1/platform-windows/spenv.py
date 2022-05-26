def commands(env, root):
    sPainter_install_path = "C:/Program Files/Allegorithmic/Adobe Substance 3D Painter"

    env.PATH.append(f"{sPainter_install_path}")
    env.QT_PLUGIN_PATH.append(f"{sPainter_install_path}")

    executable = 'Adobe Substance 3D Painter'
    env.SILEX_DCC_BIN = f"{executable}"

