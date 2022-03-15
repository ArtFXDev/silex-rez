def commands(env, root):
    # Set this variable to your maya install path
    maya_install_path = "C:/Maya2022/Maya2022"
    env.PYTHONPATH.append(r"//multifct/tools/Softwares/AnimBot")  # noqa
    env.PATH.append(f"{maya_install_path}/bin")  # noqa
    #env.PYTHONPATH.append(f"{maya_install_path}/Python37/Lib/site-packages")  # noqa
    #env.QT_PLUGIN_PATH.append(f"{maya_install_path}/qt-plugins")  # noqa
    env.PATH.append(f"{maya_install_path}/devkit")  # noqa
    env.PATH.append(f"{maya_install_path}/include")  # noqa
    
    #env.VRAY_TOOLS_MAYA2022 = 'C:\\Program Files\\Chaos Group\\V-Ray\\Maya 2022 for x64/bin',
    #env.VRAY_FOR_MAYA2022_MAIN = 'C:\\Maya2022\\Maya2022\\vray',
    #env.VRAY_FOR_MAYA2022_PLUGINS = 'C:\\Maya2022\\Maya2022\\vray/plug-ins',
    #env.VRAY_OSL_PATH_MAYA2022 = 'C:\\Program Files\\Chaos Group\\V-Ray\\Maya 2022 for x64/opensl',
    #env.VRAY_SEND_FEEDBACK = '0',
    #env.VRAY_FOR_MAYA2022_PLUGINS = ""
    #env.VRAY_FOR_MAYA2022_MAIN = ""
    #env.VRAY_OSL_PATH_MAYA2022 = ""
    #env.VRAY_TOOL_MAYA2022 = ""
    #env.VRAY_SEND_FEEDBACK = ""
