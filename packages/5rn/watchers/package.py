# -*- coding: utf-8 -*-
name = 'watchers'
version = '1.0'

authors = ['ArtFx TD gang']

requires = [
    'vray',
    'aces',
]

timestamp = 1635410671

vcs = 'git'

format_version = 2


def commands():
    env.ARNOLD_PLUGIN_PATH.append(r"\\prod.silex.artfx.fr\rez\arnold\watchers\shaders")
    env.SILEX_SIMPLE_MODE = "1"

    if "houdini" in request or "silex_houdini" in request:
        env.HOUDINI_PATH.prepend("C:/Users/etudiant/htoa/htoa-5.6.3.0_ra766b1f_houdini-18.5.596.py3/htoa-5.6.3.0_ra766b1f_houdini-18.5.596.py3")
        env.PATH.prepend("C:/Users/etudiant/htoa/htoa-5.6.3.0_ra766b1f_houdini-18.5.596.py3/htoa-5.6.3.0_ra766b1f_houdini-18.5.596.py3/scripts/bin")
        env.HOUDINI_PATH.append("//prod.silex.artfx.fr/rez/houdini/watchers_tools/")
