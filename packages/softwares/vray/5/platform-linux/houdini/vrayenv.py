def commands(env, root):
    #env.HOUDINI_PACKAGE_DIR = "/opt/hfs.18.5.759/packages"
    #env.HFS = "/opt/hfs.18.5.759/"
    #env.HOUDINI_USER_PREF_DIR = "/opt/hfs.18.5.759/packages"
    
    env.VFH_ROOT="/opt/vray_adv_52002_houdini18.5.759"

    env.VRAY_APPSDK=f"{env.VFH_ROOT}/appsdk"
    env.VRAY_UI_DS_PATH=f"{env.VFH_ROOT}/ui"
    env.VRAY_FOR_HOUDINI_AURA_LOADERS=f"{env.VFH_ROOT}/vfh_home/libs"
    env.VFH_PATH=f"{env.VFH_ROOT}/bin"
    env.VFH_PATH.append(f"{env.VRAY_APPSDK}/bin")
    env.VFH_PATH.append(f"{env.VRAY_FOR_HOUDINI_AURA_LOADERS}")
    env.VFH_HOUDINI_PATH=f"{env.VFH_ROOT}/vfh_home"

    env.PATH.append(f"{env.VFH_PATH}")
    env.HOUDINI_PATH.prepend(f"{env.VFH_HOUDINI_PATH}")
    
    return
    # Path pointing to QT platform plugins so V-Ray can load dependencies.
    QT_QPA_PLATFORM_PLUGIN_PATH="${HFS}/bin/Qt_plugins/platforms"

    HOUDINI13_VOLUME_COMPATIBILITY=1
    HDF5_DISABLE_VERSION_CHECK=1
    
    
    
    INSTALL_ROOT = "/opt/vray_adv_52002_houdini18.5.759"

    env.VRAY_APPSDK.append(f"{INSTALL_ROOT}/appsdk")
    env.VRAY_UI_DS_PATH.append(f"{INSTALL_ROOT}/ui")
    env.VFH_HOME.append(f"{INSTALL_ROOT}/vfh_home")
    
    env.PATH.append(f"{env.HFS}/bin")
    env.PATH.append(f"{env.VRAY_APPSDK}/bin")
    env.PATH.append(f"{env.VFH_HOME}/bin")
    
    env.VFH_DSO_PATH.append(f"{env.VFH_HOME}/dso_py3")
    
    env.PYTHONPATH.append(f"{env.VRAY_APPSDK}/python37")
    
    #env.VFH_ASSET_PATH = # todo
    
    # env.HOUDINI_DSO_PATH = env.VFH_DSO_PATH
    #env.HOUDINI_GALLERY_PATH = env.VFH_ASSET_PATH
    env.VRAY_UI_DS_PATH = f"{INSTALL_ROOT}/ui"
    env.VRAY_FOR_HOUDINI_AURA_LOADERS = f"{INSTALL_ROOT}/vfh_home/libs"
    env.VFH_PATH.append(f"{env.VRAY_FOR_HOUDINI_AURA_LOADERS}")
    
    env.VFH_HOUDINI_PATH = f"{INSTALL_ROOT}/vfh_home"
    
    env.VRAY_OSL_PATH = f"{env.VRAY_APPSDK}/bin"
    
    env.PATH.append(f"{env.VFH_PATH}")
    env.HOUDINI_PATH.prepend(f"{env.VFH_HOUDINI_PATH}")
    
    # env.VFH_ASSET_PATH = r"C:/Users/admin/Documents/V-Ray Material Library/assets"
    env.HOUDINI_GALLERY_PATH.append(f"{env.VFH_HOME}/gallery")
    # Path pointing to QT platform plugins so V-Ray can load dependencies.
    env.QT_QPA_PLATFORM_PLUGIN_PATH = f"{env.HFS}/bin/Qt_plugins/platforms"
    
    env.HOUDINI13_VOLUME_COMPATIBILITY=1
    env.HDF5_DISABLE_VERSION_CHECK=1