name = "aces"

version = "1.2"

timestamp = 0

def commands():
    env.PATH.append("{root}")
    env.OCIO.append(r"{root}/config.ocio")
