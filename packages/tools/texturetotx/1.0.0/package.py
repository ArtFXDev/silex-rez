# -*- coding: utf-8 -*-
name = "texturetotx"
version = "1.0.0"

authors = ["ArtFx TD gang"]

variants = [["silex_client"], ["silex_maya"], ["silex_houdini"]]

requires = [
    "maketx",
]

timestamp = 1635410671

vcs = "git"

format_version = 2


def commands():
    env.SILEX_ACTION_CONFIG.append("{root}/config")
    env.PYTHONPATH.append("{root}")
