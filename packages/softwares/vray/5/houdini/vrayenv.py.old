def commands(env, root):
    VFH_ROOT = r"C:\Program Files\Chaos Group\V-Ray\Houdini 18.5.596"
    env.VRAY_APPSDK = f"{VFH_ROOT}/appsdk"
    env.VRAY_UI_DS_PATH = f"{VFH_ROOT}/ui"
    env.VRAY_FOR_HOUDINI_AURA_LOADERS = f"{VFH_ROOT}/vfh_home/libs"
    env.VFH_HOME = f"{VFH_ROOT}/vfh_home"
    env.VFH_PATH.append(f"{VFH_ROOT}/bin")
    env.VFH_PATH.append(f"{env.VRAY_APPSDK}/bin")
    env.VFH_PATH.append(f"{env.VRAY_FOR_HOUDINI_AURA_LOADERS}")
    env.VFH_PATH.append(f"{env.VFH_HOME}/bin")
    
    env.VFH_HOUDINI_PATH = f"{VFH_ROOT}/vfh_home"
    
    
    env.PYTHONPATH.append(f"{VFH_ROOT}/appsdk/python27")
    env.VRAY_OSL_PATH = f"{env.VRAY_APPSDK}/bin"
    
    env.PATH.append(f"{env.VFH_PATH}")
    env.HOUDINI_PATH.prepend(f"{env.VFH_HOUDINI_PATH}")
    env.VFH_ASSET_PATH = r"C:/Users/admin/Documents/V-Ray Material Library/assets"
    env.HOUDINI_GALLERY_PATH.append(f"{env.VFH_HOME}/gallery")
    # Path pointing to QT platform plugins so V-Ray can load dependencies.
    env.QT_QPA_PLATFORM_PLUGIN_PATH = f"{env.HFS}/bin/Qt_plugins/platforms"
    env.HOUDINI13_VOLUME_COMPATIBILITY=1
    env.HDF5_DISABLE_VERSION_CHECK=1
