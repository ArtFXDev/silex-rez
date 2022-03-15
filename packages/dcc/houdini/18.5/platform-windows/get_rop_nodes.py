import argparse
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hip", "-hip", help="Houdini scene path", type=str)
    parser.add_argument("--tmp", "-tmp", help="Temporary file to write to", type=str)
    args = parser.parse_args()

    hip_file = args.hip
    hou.hipFile.load(hip_file)

    render_nodes = [
        rn.path() for rn in hou.node("/").recursiveGlob("*", hou.nodeTypeFilter.Rop)
    ]

    print(args.tmp)

    with open(args.tmp, "w") as f:
        f.write(json.dumps(render_nodes))


if __name__ == "__main__":
    main()
