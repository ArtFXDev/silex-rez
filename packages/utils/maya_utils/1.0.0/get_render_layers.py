import argparse
import json

import maya.standalone

maya.standalone.initialize("python")
import maya.cmds as cmds


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scene", "-sc", help="Maya scene path", type=str, required=True
    )
    parser.add_argument(
        "--tmp", "-tmp", help="Temporary file to write to", type=str, required=True
    )
    args = parser.parse_args()

    """renderlayers = sorted(cmds.ls(type='renderLayer'), reverse=True, key=lambda r:mc.getAttr(r + ".displayOrder"))

    for renderlayer in renderlayers:
        if ':' not in renderlayer:
        print(renderlayer)"""

    cmds.file(args.scene, open=True, loadNoReferences=True)
    render_layers = cmds.ls(type="renderLayer")

    with open(args.tmp, "w") as f:
        f.write(json.dumps(render_layers))


if __name__ == "__main__":
    main()
