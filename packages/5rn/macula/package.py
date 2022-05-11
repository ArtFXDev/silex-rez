# -*- coding: utf-8 -*-
name = 'macula'
version = '1.0'

authors = ['ArtFx TD gang']

requires = [
    'vray',
    'aces',
    'substance_plugin',
]

timestamp = 1635410671

vcs = 'git'

format_version = 2

def commands():
    env.SILEX_SIMPLE_MODE = "1"
    
    # Custom Nuke tools
    env.NUKE_PATH.append(r"\\prod.silex.artfx.fr\rez\nuke\macula_tools")
    env.NUKE_PATH.append(r"\\prod.silex.artfx.fr\rez\nuke\NukeSurvivalToolkit")
