# -*- coding: utf-8 -*-
name = 'francetv_3rn'
version = '1.0'

authors = ['ArtFx TD gang']

requires = [
    'aces',
]

def commands():
    env.SILEX_ACTION_CONFIG.prepend("{root}/config")
