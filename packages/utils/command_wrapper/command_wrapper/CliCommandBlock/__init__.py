import sys
from typing import List

from .CliCommand import CliCommand
from .CliCommandBlock import CliCommandBlock
from .CliCommandBlockExecuter import CliCommandBlockExecuter


def wrap(
    precommands: List[str] = [],
    command: str = "",
    postcommands: List[str] = [],
    cleanups: List[str] = [],
):

    commandBlock: CliCommandBlock = CliCommandBlock()

    if len(precommands) > 0:
        for commandString in precommands:
            commandBlock.add_pre_command(CliCommand.build_from_string(commandString))

    if command:
        commandBlock.set_command(CliCommand.build_from_string(command))

    if len(postcommands) > 0:
        for commandString in postcommands:
            commandBlock.add_post_command(CliCommand.build_from_string(commandString))

    if len(cleanups) > 0:
        for commandString in cleanups:
            commandBlock.add_cleanup_command(
                CliCommand.build_from_string(commandString)
            )

    cmd = CliCommandBlockExecuter(commandBlock)

    error = cmd.execute()

    if error:
        sys.exit(999999)
