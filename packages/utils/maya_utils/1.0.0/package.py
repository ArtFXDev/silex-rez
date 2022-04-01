name = "maya_utils"

version = "1.0.0"

requires = ["maya"]


def commands():
    env.PYTHONPATH.append("{root}")
