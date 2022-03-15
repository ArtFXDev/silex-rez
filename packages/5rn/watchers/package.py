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
