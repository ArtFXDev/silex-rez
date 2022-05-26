# -*- coding: utf-8 -*-
name = 'test_pipe'
version = '1.0'

authors = ['ArtFx TD gang']

variants = [["houdini-18"], ["nuke"], ["silex_client"], ["silex_maya"], ["silex_houdini"], ["silex_nuke"], ["silex_mari"]]

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
