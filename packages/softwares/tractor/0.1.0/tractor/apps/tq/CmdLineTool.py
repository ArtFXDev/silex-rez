"""L{rpg.CmdLineTool} subclasses useful when creating tools that reference
MisterD."""

import rpg.CmdLineTool as CLT
import rpg.sql.DBCmdLineTool as DBCLT
import re

__all__ = (
    "UnknownFormat",
    "NoTasksFound",
    "NoCommandsFound",
    "KeylessCmdLineTasks",
    "KeylessCmdLineCommands",
    )

class CmdLineTasksError(CLT.CmdLineToolError):
    """Base error for the CmdLineTasks object."""
    pass

class UnknownFormat(CmdLineTasksError):
    """The command line arguments were not in a recognizable format."""
    pass

class NoTasksFound(CmdLineTasksError):
    """No tasks were found in the arguments."""
    pass

class NoCommandsFound(CmdLineTasksError):
    """No commands were found in the arguments."""
    pass

class KeylessCmdLineTasks(object):
    """Creates task objects based on raw jid/tid values on the command line.  The
    arguments must be in the form:
      tq tasks:
        [$user[@$host]] $jid $tid [$tid2 $tid3 ...]
    """

    _tinare = re.compile("^([^@]+)@([^:\s]+) (\d+)((?: \d+)*)")
    _jidre  = re.compile("^(\w+\s+\w+\s+)?(\d+)(\s+\d+)*$")

    def __init__(self, JobClass, TaskClass, args, requireTasks=True):
        self.job = None
        self.tasks = []
        self.JobClass = JobClass
        self.TaskClass = TaskClass
        self.requireTasks = requireTasks
        # create the tasks
        self.setTasks(args)

    def setTasks(self, args):
        del self.tasks[:]

        # maybe all they gave us was a jid and tid
        if self._jidre.match(' '.join(args)):
            job = self.JobClass()
            try:
                job.jobid = int(args[0])
                tids = args[1:]
            except ValueError:
                job.jobid = int(args[2])
                tids = args[3:]
            self.job = job

        # maybe this is for tina tasks
        elif self._tinare.match(' '.join(args)):
            job = self.JobClass()
            self.job = job
            match = self._tinare.match(' '.join(args))
            if match:
                job.user  = match.group(0)
                job.host  = match.group(1)
                job.jobid = int(match.group(2))
                tids      = match.group(3).split()

        # or maybe they aren't tasks at all
        else:
            raise UnknownFormat("unrecognizable task string")

        if not tids and self.requireTasks:
            raise NoTasksFound("no task ids were found after the job")

        # make the tasks now that we have a job object
        for tid in tids:
            task = self.TaskClass()
            task.taskid = int(tid)
            task.Job    = job
            self.tasks.append(task)

    def getWhere(self):
        """Get where string that can be used to query the database for only
        the tasks in our list."""
        if not self.job:
            return None
        where = "jobid=%d" % self.job.jobid
        taskPhrases = []
        for task in self.tasks:
            taskPhrases.append("taskid=%d" % task.taskid)
        if taskPhrases:
            where += " and (%s)" % ' or '.join(taskPhrases)
        return where

    def __getitem__(self, index):
        return self.tasks[index]

    def __len__(self):
        return len(self.tasks)


class KeylessCmdLineCommands(object):
    """Creates command objects based on raw jid/cid values on the command line.  The
    arguments must be in the form:
      tq commands:
        [$user[@$host]] $jid $cid [$cid2 $cid3 ...]
    """

    _tinare = re.compile("^([^@]+)@([^:\s]+) (\d+)((?: \d+)*)")
    _jidre  = re.compile("^(\w+\s+\w+\s+)?(\d+)(\s+\d+)*$")

    def __init__(self, JobClass, CommandClass, args, requireCommands=True):
        self.job = None
        self.commands = []
        self.JobClass = JobClass
        self.CommandClass = CommandClass
        self.requireCommands = requireCommands
        # create the commands
        self.setCommands(args)

    def setCommands(self, args):
        del self.commands[:]

        # maybe all they gave us was a jid and tid
        if self._jidre.match(' '.join(args)):
            job = self.JobClass()
            try:
                job.jobid = int(args[0])
                tids = args[1:]
            except ValueError:
                job.jobid = int(args[2])
                tids = args[3:]
            self.job = job

        # maybe this is for tina commands
        elif self._tinare.match(' '.join(args)):
            job = self.JobClass()
            self.job = job
            match = self._tinare.match(' '.join(args))
            if match:
                job.user  = match.group(0)
                job.host  = match.group(1)
                job.jobid = int(match.group(2))
                tids      = match.group(3).split()

        # or maybe they aren't commands at all
        else:
            raise UnknownFormat("unrecognizable command string")

        if not tids and self.requireCommands:
            raise NoCommandsFound("no command ids were found after the job")

        # make the commands now that we have a job object
        for tid in tids:
            command = self.CommandClass()
            command.commandid = int(tid)
            command.Job    = job
            self.commands.append(command)

    def getWhere(self):
        """Get where string that can be used to query the database for only
        the commands in our list."""
        if not self.job:
            return None
        where = "jobid=%d" % self.job.jobid
        commandPhrases = []
        for command in self.commands:
            commandPhrases.append("cid=%d" % command.commandid)
        if commandPhrases:
            where += " and (%s)" % ' or '.join(commandPhrases)
        return where

    def __getitem__(self, index):
        return self.commands[index]

    def __len__(self):
        return len(self.commands)
