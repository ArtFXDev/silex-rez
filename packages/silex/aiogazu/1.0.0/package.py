# pylint: skip-file
name = "aiogazu"
timestamp = 0
version = "1.0.0"

authors = ["ArtFx TD gang"]

description = """
    Fork from CGwire's gazu library, with asyncio support
    """

requires = [
    "python-3.7",
    "aiohttp",
]

vcs = "git"

build_command = "python {root}/script/build.py {install}"


def commands():
    """
    Set the environment variables for silex_client
    """
    env.PYTHONPATH.append("{root}")
