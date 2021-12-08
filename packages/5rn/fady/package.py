# -*- coding: utf-8 -*-
name = 'fady'
version = '1.0'

authors = ['ArtFx TD gang']

requires = [
    'vray',
]

timestamp = 1635410671

vcs = 'git'

format_version = 2



def command():
    env.HOUDINI_PATH.append('"C:/Users/etudiant/Documents/houdini18.5/DMNK-Tools-master"') 
    env.HOUDINI_PATH.append("C:/Users/etudiant/Documents/houdini18.5/MOPS") 