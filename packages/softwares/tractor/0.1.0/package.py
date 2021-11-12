# pylint: skip-file
name = "tractor"
version = "0.1.0"

authors = ["ArtFx TD gang"]

description = \
    """
    Set of python 3 module for tractor lib
    """

requires = ["python-3.7"]

def commands():
    env.PYTHONPATH.append("{root}")