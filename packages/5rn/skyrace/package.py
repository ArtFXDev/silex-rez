# -*- coding: utf-8 -*-
name = 'skyrace'
version = '1.0'

authors = ['ArtFx TD gang']


variants = [
    ["platform-windows"],
    ["platform-windows", "houdini-18"],
]

requires = [
    'vray',
    'aces',
    "vray_sdk",
    "dmnk_tools",
    "megascan_library"
]

timestamp = 1635410671

vcs = 'git'

format_version = 2

def commands():
    # Custom Nuke tools
    env.NUKE_PATH.append(r"\\prod.silex.artfx.fr\rez\nuke\skyrace_tools")

    # Houdini Redshift
    env.PATH.prepend("C:/ProgramData/redshift/bin")
    env.HOUDINI_PATH.append("C:/ProgramData/redshift/Plugins/Houdini/18.5.596")
    env.HOUDINI_PATH.append("//prod.silex.artfx.fr/rez/houdini/skyrace_tools/")
    env.SILEX_SIMPLE_MODE = "1"

    env.SILEX_ACTION_CONFIG.prepend("{root}/config")
