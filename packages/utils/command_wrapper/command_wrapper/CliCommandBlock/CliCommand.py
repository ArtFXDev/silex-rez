import shlex
from typing import List


class CliCommand():
    """Command represents a single command with optional args
    """

    def __init__(self) -> None:
        self.command: str = ""
        self.args: List[str] = []
    
    @classmethod
    def build_from_list(cls, items: List[str]):
        self = cls()

        if len(items) < 1:
            return self

        self.command = items[0]
        self.args = items[1:]

        return self
    
    @classmethod
    def build_from_string(cls, commandString: str):
        self = cls()

        items: List[str] = shlex.split(commandString)

        if len(items) < 1:
            return self

        self.command = items[0]
        self.args = items[1:]

        return self
    
    def set_command(self, command: str):
        self.command = command
    
    def add_arg(self, arg: str):
        self.args.append(arg)
    
    def set_args(self, args: List[str]):
        self.args = args
    
    def as_command_string(self):
        return " ".join([self.command] + self.args)