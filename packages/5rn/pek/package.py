# -*- coding: utf-8 -*-
name = "pek"
version = "1.0"

authors = ["ArtFx TD gang"]

requires = [
    "vray",
    "aces",
    "texturetotx",
    "houdini-18"
]

variants = [
    ["platform-windows"],
    ["platform-windows", "houdini-18"],
]

timestamp = 1635410671

vcs = "git"

format_version = 2


def commands():
    env.NUKE_PATH.append(r"\\prod.silex.artfx.fr\rez\nuke\skyrace_tools")
    env.SILEX_SIMPLE_MODE = "1"
