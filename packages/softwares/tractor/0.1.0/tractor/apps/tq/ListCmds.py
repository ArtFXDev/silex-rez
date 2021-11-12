"""efinition of all the tq commands that list records from the database."""

import re

import rpg.CmdLineTool as CmdLineTool
import rpg.sql.Where as SQLWhere
import tractor.base.EngineDB as EngineDB

from .. import tq
from . import CmdLineTool

__all__ = (
    "JobsCmd",
    "TasksCmd",
    "CommandsCmd",
    "InvocationsCmd",
    "BladesCmd",
    "ParamsCmd",
    "NotesCmd"
    )

class JobsCmd(tq.QueryCmd):

    usage = "jobs"

    description = """
    List jobs.  A job is a group of related tasks with dependencies,
    and is usually rendering one or more frames from a shot.  The tier
    and priority of the job determine the placement of the job's tasks within
    the queue.
    """

    examples = """
  Syntax for listing jobs is:
    > tq jobs [OPTIONS] SEARCH_CLAUSE [OPTIONS]
    > tq jobs jid [jid2 jid3 ...]

  Examples:
    list all jobs for a user
      > tq jobs user=joe
    list all the errored jobs for a user
      > tq jobs user=carlos and error
    list all the ready jobs and sort by number of ready tasks
      > tq jobs ready -s numready
    list all jobs with priority over 400 for a group of users (notice it's
     quoted because of the greater than character).
      > tq jobs "priority > 400" and user in [sandy joe carlos]
  """

    defaultSort     = ["user", "spooltime"]
    defaultDistinct = ["jobid"]

    # regexp used to check for the altnerate syntax
    _jidre  = re.compile("^(\d+)(\s+\d+)*$")
    
    def __init__(self, **kwargs):
        super(JobsCmd, self).__init__(EngineDB.EngineClientDB.JobTable, **kwargs)

    def parseArgs(self, *args, **kwargs):
        result = super(JobsCmd, self).parseArgs(*args, **kwargs)
        
        # check if the where string is in the form:
        #   jid [jid2 jid3 ...]
        match = self._jidre.match(self.where)
        if match:
            self.where = "jobid in [%s]" % self.where

        return result


class TasksCmd(tq.QueryCmd):

    usage = "tasks"

    description = """
    List tasks.  A task is a container for a list of commands that run
    sequentially.  This is the unit of work that typically
    occomplishes a single, non-parallelizable goal like rendering one
    element or comping a frame. Tasks have serivce keys which restrict what
    blades they can run on.  A well-targeted SEARCH_CLAUSE is recommended
    due to the potentially enormous number of tasks that may be returned.
    """

    examples = """
  Syntax for listing tasks is:
    > tq tasks [OPTIONS] [SEARCH_CLAUSE] [OPTIONS]
    > tq tasks jid [tid1 tid2 tid3 ...]

  Examples:
    list all active tasks for a user
      > tq tasks active and user=lee
    list all errored tasks for a user
      > tq tasks error and user=murphy
    list all tasks that have errored in the past hour
      > tq tasks error and "statetime > -1h"
      """

    defaultSort     = ["jid", "tid"]
    defaultDistinct = ["jid", "tid"]

    # argument aliases that can be used for this command
    aliases = {
        "longrunning": '-s -statetime -c +cpu "state=active and '
                       'statetime < -12h"',
        }
    
    def __init__(self, **kwargs):
        super(TasksCmd, self).__init__(EngineDB.EngineClientDB.TaskTable, **kwargs)

    def parseArgs(self, *args, **kwargs):
        result = super(TasksCmd, self).parseArgs(*args, **kwargs)

        # check if the where string is in the form:
        #   jid [tid ...]
        try:
            tasks = CmdLineTool.KeylessCmdLineTasks(
                EngineDB.Row, EngineDB.Row,
                self.args, requireTasks=False)
            self.where = tasks.getWhere()
        except CmdLineTool.UnknownFormat:
            pass

        return result


class CommandsCmd(tq.QueryCmd):

    usage = "commands"

    description = """
    List commands.  A command is an argument list that gets executed on a blade.
    A well-targeted SEARCH_CLAUSE is recommended due to the potentially enormous
    number of commands that may be returned.
    """

    examples = """
  Syntax for listing commands is:
    > tq commands [OPTIONS] [SEARCH_CLAUSE] [OPTIONS]
    > tq commands jid [cid1 cid2 cid3 ...]

  Examples:
    list all active expand commands
      > tq commands state=active and expand
      """

    defaultSort     = ["jid", "cid"]
    defaultDistinct = ["jid", "cid"]

    # argument aliases that can be used for this command
    aliases = {}
    
    def __init__(self, **kwargs):
        super(CommandsCmd, self).__init__(EngineDB.EngineClientDB.CommandTable, **kwargs)

    def parseArgs(self, *args, **kwargs):
        result = super(CommandsCmd, self).parseArgs(*args, **kwargs)

        # check if the where string is in the form:
        #   jid [tid ...]
        try:
            commands = CmdLineTool.KeylessCmdLineCommands(
                EngineDB.Row, EngineDB.Row,
                self.args, requireCommands=False)
            self.where = commands.getWhere()
        except CmdLineTool.UnknownFormat:
            pass

        return result


class BladesCmd(tq.QueryCmd):

    usage = "blades"

    description = """
    List blades.  A blade is a remote execution server that makes requests 
    to the engine for tasks to run.  A blade has a profile which allows
    the engine to match tasks with blades that can run them.  A blade also
    reports metrics such as available memory and disk space, which is used
    by the engine for matching tasks with minimum requirements.
    """

    examples = """
  Syntax for listing blades is:
    > tq blades [OPTIONS] SEARCH_CLAUSE [OPTIONS]

  Examples:
    list all registered blades (ones that have registered since being cleared)
      > tq blades
    list all registered blades that are considered up (have registered in last 6 minutes)
      > tq blades up
    list all registered blades with the Linux profile
      > tq blades profile=Linux
    list all registered blades with the Windows or Linux profile with less than 1Gb of free disk or memory
      > tq blades "profile in [Windows, Linux] and (mem < 1G or disk < 1G)"
    list all blades, including ones that have not registered since being cleared
      > tq blades --all
    list all blades that have not registered since being cleared but are still considered up
      > tq blades --all not registered and up
  """

    defaultSort     = ["name", "ipaddr"]
    defaultDistinct = ["name"]

    def __init__(self, **kwargs):
        super(BladesCmd, self).__init__(EngineDB.EngineClientDB.BladeTable, **kwargs)

    def parseArgs(self, *args, **kwargs):
        result = super(BladesCmd, self).parseArgs(*args, **kwargs)
        if not self.parent.opts.archives and not self.opts.archives:
            # look only at blades that have registered
            if self.where:
                self.where = "registered and (%s)" % self.where
            else:
                self.where = "registered"
        return result

class ParamsCmd(tq.QueryCmd):

    usage = "params"

    description = """
    List engine configuration and statistical paramters.
    """

    examples = """
  Syntax for listing paramters is:
    > tq params [OPTIONS] SEARCH_CLAUSE [OPTIONS]

  Examples:
    list all parameters
      > tq params
    list all paramters with "rate"
      > tq params name like rate
    list all the blades with a value of "off"
      > tq params value=off
  """

    defaultSort     = ["name"]

    def __init__(self, **kwargs):
        super(ParamsCmd, self).__init__(EngineDB.EngineClientDB.ParamTable, **kwargs)


class NotesCmd(tq.QueryCmd):

    usage = "notes"

    description = """
    List notes.
    """

    examples = """
  Syntax for listing notes is:
    > tq notes [OPTIONS] SEARCH_CLAUSE [OPTIONS]

  Examples:
    list all job comments written in the last hour
      > tq notes "itemtype=job and notetype=comment and notetime > -1h"
    list all blade notes containing reboot or shutdown
      > tq notes itemtype=blade and notetext like "reboot|shutdown"
  """

    defaultSort     = ["notetime"]

    def __init__(self, **kwargs):
        super(NotesCmd, self).__init__(EngineDB.EngineClientDB.NoteTable, **kwargs)


class InvocationsCmd(tq.QueryCmd):

    usage = "invocations"

    description = """
    List command invocations.  An invocation is the single execution of a
    command running on one or more blades.  A well-targeted SEARCH_CLAUSE
    is recommended due to the potentially enormous number of invocations
    that may be returned.
    """

    examples = """
  Syntax for listing invocations is:
    > tq invocations [OPTIONS] [SEARCH_CLAUSE] [OPTIONS]

  Examples:
    list all invocations that ran on c1234 in the last hour
      > tq invocations "blade=c1234 and starttime > -1h"
    list all invocations that errored out in the last 5 minutes
      > tq invocations "rcode > 0 and stoptime > -5m"
  """

    defaultSort     = ["jid", "tid", "cid", "iid"]

    def __init__(self, **kwargs):
        super(InvocationsCmd, self).__init__(EngineDB.EngineClientDB.InvocationTable, **kwargs)


def test():

    class ListCmds(CmdLineTool.MultiCmdLineTool):
        """Helper object for testing."""

        options = [
            CmdLineTool.BooleanOption("--force",
                                      help="force all operations.  This will "
                                           "allow any operation to be "
                                           "performed on a task or job, even "
                                           "if you are not."),

            CmdLineTool.BooleanOption  ("-d", "--debug",
                                        help="print debug info."),
        
        ] + CmdLineTool.MultiCmdLineTool.options


        commands = {
            "jobs"        : JobsCmd,
            "tasks"       : TasksCmd,
            }

    lcmds = ListCmds()
    lcmds.run()


if __name__ == "__main__":
    test()
