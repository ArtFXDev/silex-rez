# -*- coding: utf-8 -*-
name = 'skyrace'
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
    env.NUKE_PATH.append(r"\\prod.silex.artfx.fr\rez\nuke\skyrace_tools")
