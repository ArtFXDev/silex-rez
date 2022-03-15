# -*- coding: utf-8 -*-
name = "nellie_bly"
version = "1.0"

authors = ["ArtFx TD gang"]

variants = [["maya"], ["houdini"], ["nuke"], ["silex_client"], ["silex_maya"], ["silex_houdini"]]

requires = [
    "vray",
    "aces",
]

timestamp = 1635410671

vcs = "git"

format_version = 2


def commands():
    env.SILEX_ACTION_CONFIG.append("{root}/config")
    env.PYTHONPATH.append("{root}")
    env.SILEX_SIMPLE_MODE = "1"
