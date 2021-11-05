def commands(env):
    INSTALL_ROOT = r'C:\Maya2022\Maya2022\vray' # noqa
    
    env.VRAY_FOR_MAYA2022_MAIN.append(f'{INSTALL_ROOT}') # noqa
    env.VRAY_FOR_MAYA2022_PLUGINS.append(f'{INSTALL_ROOT}/vrayplugins') # noqa
    env.VRAY_OSL_PATH_MAYA2022.append(f'{INSTALL_ROOT}/Maya 2022 for x64/opensl') # noqa
    env.VRAY_TOOLS_MAYA2022.append(f'{INSTALL_ROOT}/Maya 2022 for x64/bin') # noqa