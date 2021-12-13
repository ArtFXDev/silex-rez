# -*- coding: utf-8 -*-

name = 'gazu'

version = '0.8.22'

description = \
    """
    Gazu is a client for Zou, the API to store the data of your CG production.
    """

authors = ['CG Wire frank@cg-wire.com']

requires = ['Deprecated-1.2.13+<1.2.13.1']

variants = [['python-3.7', 'requests-2.26.0+<2.26.0.1', 'python_socketio-4.6.1+<4.6.1.1']]

def commands():
    env.PYTHONPATH.append('{root}/python')

help = [['Home Page', 'https://gazu.cg-wire.com/']]

timestamp = 1633012158

hashed_variants = True

is_pure_python = True

from_pip = True

pip_name = 'gazu (0.8.22)'

format_version = 2
