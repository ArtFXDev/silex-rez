# -*- coding: utf-8 -*-
name = "test_pipe"
version = "1.0"

authors = ["ArtFx TD gang"]

requires = [
    "vray",
    "aces",
    "texturetotx",
    "vray_sdk",
    "substance_plugin",
]

timestamp = 1635410671

vcs = "git"

format_version = 2


def commands():
    # Houdini Redshift
    env.PATH.prepend("C:/ProgramData/redshift/bin")
    env.HOUDINI_PATH.append("C:/ProgramData/redshift/Plugins/Houdini/18.5.596")

    env.ARNOLD_PLUGIN_PATH.append(r"\\prod.silex.artfx.fr\rez\arnold\watchers\shaders")

    env.PATH.append("C:/Users/etudiant/htoa/htoa-5.6.3.0_ra766b1f_houdini-18.5.596/htoa-5.6.3.0_ra766b1f_houdini-18.5.596/scripts/bin")
    env.HOUDINI_PATH.append("C:/Users/etudiant/htoa/htoa-5.6.3.0_ra766b1f_houdini-18.5.596/htoa-5.6.3.0_ra766b1f_houdini-18.5.596")
