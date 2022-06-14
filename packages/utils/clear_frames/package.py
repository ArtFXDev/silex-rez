from ast import alias
from logging import root


name = "clear_frames"
timestamp = 0
version = "0.1.0"

authors = ["ArtFx TD gang"]

description = """
    Delete any file under 10 Ko
    """

requires = [
    "python-3.7",
    "Fileseq",
]

vcs = "git"


def commands():
    env.PYTHONPATH.append("{root}")
    alias("clear_frames", f"python {root}/main.py")
