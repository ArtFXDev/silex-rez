import argparse

from .CliCommandBlock import wrap


def main():
    parser = argparse.ArgumentParser(
        description="Wraps a comand in pre-, post- and cleanup commands"
    )
    parser.add_argument("--pre")
    parser.add_argument("--command")
    parser.add_argument("--post")
    parser.add_argument("--cleanup")

    args = parser.parse_args()

    preCmds = []
    commandCmd = ""
    postCmds = []
    cleanupCmds = []

    if not args.pre is None:
        preCmds = [cmd.strip() for cmd in args.pre.split(",")]

    if not args.command is None:
        commandCmd = args.command.strip()

    if not args.post is None:
        postCmds = [cmd.strip() for cmd in args.post.split(",")]

    if not args.cleanup is None:
        cleanupCmds = [cmd.strip() for cmd in args.cleanup.split(",")]

    wrap(
        precommands=preCmds,
        command=commandCmd,
        postcommands=postCmds,
        cleanups=cleanupCmds,
    )


if __name__ == "__main__":
    main()
