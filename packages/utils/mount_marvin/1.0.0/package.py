name = "mount_marvin"

version = "1.0.0"

timestamp = 0
 
variants = [
    ["platform-windows"],
]

def commands():
    env.PATH.append("{root}")

