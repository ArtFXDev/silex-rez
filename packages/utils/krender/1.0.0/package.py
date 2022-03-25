name = "krender"

version = "1.0.0"

requires = ["python-3", "kick", "Fileseq"]


def commands():
    env.PYTHONPATH.append("{root}")
