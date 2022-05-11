# -*- coding: utf-8 -*-
name = 'la_mouche'
version = '1.0'

authors = ['ArtFx TD gang']

requires = [
    'vray',
    'aces',
    "texturetotx",
]

timestamp = 1635410671

vcs = 'git'

format_version = 2

def commands():
    env.SILEX_SIMPLE_MODE = "1"

    if "houdini" in request or "silex_houdini" in request:
        env.HOUDINI_PATH.append("//prod.silex.artfx.fr/rez/houdini/lamouche_tools/")

