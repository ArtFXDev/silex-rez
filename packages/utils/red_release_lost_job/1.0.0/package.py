name = "red_release_lost_job"

version = "1.0.0"

requires = ["python-3", "tractor"]


def commands():
    env.PYTHONPATH.append("{root}")
