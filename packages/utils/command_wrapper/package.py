name = "command_wrapper"

version = "1.0.0"

timestamp = 0

requires = ["python-3"]


def commands():
    env.PYTHONPATH.append("{root}")
