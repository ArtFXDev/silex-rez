def commands(env):
    INSTALL_ROOT = r'C:\Program Files\Chaos Group\V-Ray\Houdini 18.5.596' # noqa
    
    env.VRAY_APPSDK.append(f'{INSTALL_ROOT}/appsdk') # noqa
    env.VRAY_OSL_PATH.append(f'{INSTALL_ROOT}/appsdk/bin') # noqa
    env.VRAY_UI_DS_PATH.append(f'{INSTALL_ROOT}/ui') # noqa
    env.VFH_HOME.append(f'{INSTALL_ROOT}/vfh_home') # noqa
    env.PYTHONPATH.append(f'{INSTALL_ROOT}/appsdk/python27') # noqa
    env.PATH.append(f'{env.VRAY_APPSDK}') # noqa
    env.PATH.append(f'{env.VFH_HOME}/bin') # noqa
    env.HOUDINI_GALLERY_PATH.append(f'{env.VFH_HOME}/gallery') # noqa
    env.VFH_ASSET_PATH.append(r'C:\Users\admin\Documents\V-Ray Material Library/assets')