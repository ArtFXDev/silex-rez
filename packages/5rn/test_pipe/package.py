# -*- coding: utf-8 -*-
name = 'quit_smoking'
version = '1.0'

authors = ['ArtFx TD gang']

variants = [["maya"], ["houdini"], ["nuke"], ["silex_client"], ["silex_maya"], ["silex_houdini"]]

requires = [
    'vray',
    'aces',
    "texturetotx",
]

timestamp = 1635410671

vcs = 'git'

format_version = 2

def commands():
    env.SILEX_ACTION_CONFIG.prepend("{root}/config")
