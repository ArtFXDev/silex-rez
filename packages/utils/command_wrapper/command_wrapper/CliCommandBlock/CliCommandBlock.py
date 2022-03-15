from typing import List

from .CliCommand import CliCommand


class CliCommandBlock():
    """A command surrounded by a pre, post and cleanup command
    
    The pre and post commands are executed before and after the command.
    If one of the commands fails the execution of the command block is stopped.
    The cleanup command is always executed regardless if the previous command/s 
    succeded or not.
    """

    def __init__(self) -> None:
        self.preCommands: List[CliCommand] = []
        self.command: CliCommand = []
        self.postCommands: List[CliCommand] = []
        self.cleanupCommands: List[CliCommand] = []

    def add_pre_command(self, command: CliCommand):
        self.preCommands.append(command)

    def set_command(self, command: CliCommand):
        self.command = command
    
    def add_post_command(self, command: CliCommand):
        self.postCommands.append(command)

    def add_cleanup_command(self, command: CliCommand):
        self.cleanupCommands.append(command)