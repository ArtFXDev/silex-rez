def commands(env, root):    
    env.VFH_ROOT="/opt/vray_adv_52002_houdini18.5.759"

    env.VRAY_APPSDK=f"{env.VFH_ROOT}/appsdk"
    env.VRAY_UI_DS_PATH=f"{env.VFH_ROOT}/ui"
    env.VFH_HOME.append(f"{env.VFH_ROOT}/vfh_home")

    env.PATH.append(f"{env.VRAY_APPSDK}/bin")
    env.PATH.append(f"{env.VFH_HOME}/bin")
    env.PATH.append(f"/usr/ChaosGroup/V-Ray/Standalone_for_centos6/bin")

    env.VFH_PATH.append(f"{env.VRAY_APPSDK}/bin")

    env.PYTHONPATH.append(f"{env.VRAY_APPSDK}/python37")
    