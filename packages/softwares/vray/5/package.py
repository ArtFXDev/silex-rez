name = "vray"
version = "5"

variants = [
    ["maya"],
    ["houdini"],
]


def commands():
    import sys

    sys.path.append(root)

    import vrayenv

    vrayenv.commands(env, root)

    # This should be in order
    # See: https://docs.chaos.com/display/VMAYA/Installation+from+zip#Installationfromzip-SetupforMaya
    # And: https://forums.chaos.com/forum/v-ray-for-maya-forums/v-ray-for-maya-problems/1125078-maya-2020-2022-ui-integrity-check-mismatch-qt-version-5-12-5
    env.PATH.append("C:/Maya2022/Maya2022/vray/bin/hostbin")
    env.PATH.append("C:/Maya2022/Maya2022/vray/bin")

