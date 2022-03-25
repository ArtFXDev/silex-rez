def commands(env, root):
    # Set this variable to your maya install path
    maya_install_paths = ["C:/Maya2022/Maya2022", "C:/Autodesk/Maya2022"]

    for maya_install_path in maya_install_paths:
        env.PYTHONPATH.append(r"//multifct/tools/Softwares/AnimBot")
        env.PATH.append(f"{maya_install_path}/bin")
        env.PATH.append(f"{maya_install_path}/devkit")
        env.PATH.append(f"{maya_install_path}/include")
