# -*- coding: utf-8 -*-
name = "nellie_bly"
version = "1.0"

authors = ["ArtFx TD gang"]

variants = [["silex_client"], ["silex_maya"], ["silex_houdini"]]

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
