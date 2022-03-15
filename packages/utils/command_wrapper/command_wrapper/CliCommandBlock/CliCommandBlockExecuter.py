import subprocess
import time

from .CliCommand import CliCommand
from .CliCommandBlock import CliCommandBlock


class CliCommandBlockExecuter:
    def __init__(self, commandBlock: CliCommandBlock):
        self.commandBlock = commandBlock
        self.error = False

    def execute(self):
        for command in self.commandBlock.preCommands:
            if self.error:
                break
            print("--- PRE-COMMAND ---")
            self.execute_single_command(command)

        if not self.error:
            print("--- COMMAND ---")
            self.execute_single_command(self.commandBlock.command)

        for command in self.commandBlock.postCommands:
            if self.error:
                break
            print("--- POST-COMMAND ---")
            self.execute_single_command(command)

        for command in self.commandBlock.cleanupCommands:
            print("--- CLEANUP-COMMAND ---")
            self.execute_single_command(command)

        return self.error

    def execute_single_command(self, command: CliCommand):
        # add shell=True parameter is command should be executed in a shell
        process = subprocess.Popen(command.as_command_string(), shell=True)

        while process.poll() == None:
            time.sleep(0.5)

        statusCode = process.returncode

        if statusCode != 0:
            self.error = True
