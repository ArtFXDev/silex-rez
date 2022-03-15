name = "vrender"

version = "1.0.0"

requires = ["python-3", "vray_sdk"]


def commands():
    env.PYTHONPATH.append("{root}")
