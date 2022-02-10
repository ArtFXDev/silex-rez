name = "mount_render_drive"

version = "1.0.0"

timestamp = 0
 
variants = [
    ["platform-windows"],
    ["platform-linux"]
]

tools = ["mount"]
def commands():
    env.PATH.append("{root}")

