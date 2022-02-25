name = "vray"
version = "5"

variants = [
    ["platform-windows", "maya"],
    ["platform-windows", "houdini"],
    ["platform-linux", "maya"],
    ["platform-linux", "houdini"],
    ["platform-linux"],
]

def commands():
    import sys
    sys.path.append(root)

    import vrayenv

    vrayenv.commands(env, root)

    # This should be in order
    # See: https://docs.chaos.com/display/VMAYA/Installation+from+zip#Installationfromzip-SetupforMaya
    # And: https://forums.chaos.com/forum/v-ray-for-maya-forums/v-ray-for-maya-problems/1125078-maya-2020-2022-ui-integrity-check-mismatch-qt-version-5-12-5
    

