# -*- coding: utf-8 -*-
name = "texturetotx"
version = "1.0.0"

authors = ["ArtFx TD gang"]

variants = [
    ["platform"],
    ["platform", "silex_maya"],
    ["platform", "silex_houdini"]
]

requires = [
    "maketx",
]

timestamp = 1635410671

vcs = "git"

format_version = 2


def commands():
    env.SILEX_ACTION_CONFIG.append("{root}/config")
    env.PYTHONPATH.append("{root}")
