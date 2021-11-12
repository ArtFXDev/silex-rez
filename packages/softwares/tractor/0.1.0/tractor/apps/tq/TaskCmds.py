"""Definition of all the tq commands that perform an operation on a list
of tasks returned from the database."""

import re
import rpg.listutil as listutil
import rpg.terminal as terminal
import rpg.CmdLineTool

import tractor.api.query as query
import tractor.base.EngineDB as EngineDB
from .. import tq
from . import CmdLineTool

__all__ = (
    "RetryTaskCmd",
    "ResumeTaskCmd",
    "KillTaskCmd",
    "SkipTaskCmd",
    "PrintLogCmd",
    )


class CmdLineTasks(CmdLineTool.KeylessCmdLineTasks):
    def __init__(self, *args, **kwargs):
        super(CmdLineTasks, self).__init__(
            tq.OperationRow, tq.OperationRow, *args, **kwargs)

class TaskOperationCmd(tq.OperateCmd):
    """Base class for task operations."""
    
    defaultSort     = ["-Job.priority", "Job.spooltime"]
    defaultDistinct = ["jobid", "tid"]

    def __init__(self, **kwargs):
        """The constructor is hard-coded to contact the main database."""
        super(TaskOperationCmd, self).__init__(EngineDB.EngineDB.TaskTable, **kwargs)

    def parseArgs(self, *args, **kwargs):
        result = super(TaskOperationCmd, self).parseArgs(*args, **kwargs)
        # check if the where string is in the form:
        #   jid [tid ...]
        try:
            tasks = CmdLineTasks(self.args)
            self.where = tasks.getWhere()
        except tq.CmdLineTool.UnknownFormat:
            pass
        return result

    def runQuery(self):
        """Overloaded from the super so a different Task object
        can be returned."""
        # check if we need to contact the db
        if self.objects:
            result = self.objects
        else:
            # only query existing jobs
            # AWG: not deleted not getting translated right yet
            #where  = "(%s) and not deleted" % self.where
            where  = self.where
            self.members = listutil.getUnion(self.members + ["cids"])
            result = self.db.getTasks(members=self.members,
                                      where=where,
                                      limit=self.opts.limit,
                                      orderby=self.opts.sortby,
                                      objtype=tq.OperationRow)
        if not result:
            print("no tasks found")
        return result


class RetryTaskCmd(TaskOperationCmd):

    usage = "retry"

    description = """
    Retry one or more tasks.  Retrying a task will put the task as
    well as entire subtree of its ancestor tasks back on the queue;
    and if the task is active it will be killed first.  By default the
    user is prompted before each task is retried, unless the --yes
    flag is set.  Also. you can only retry your own tasks, unless the
    --force flag is set.
    """

    examples = """
  Syntax to retry tasks is:
    > tq retry WHERE_STRING
    > tq retry jid tid [tid2 tid3 ...]

  Examples:
    retry all errored tasks for thing
      > tq retry user=thing and error
    retry a specific task
      > tq retry jid=10795593 and tid=23
    same as previous example
      > tq retry 10795593 23
    retry all tasks that errored on a u3701 after 3pm today for all users
      > tq --force retry error and "statetime >= 3pm" and slots like u3701
    """

    def processObject(self, obj):
        """Operate on the provided task object."""
        # ask the user if we should continue
        if not obj.pre(self, "Retry this task?"):
            return
        # try to run the operation
        query.retry(obj)
        obj.post(self, "retried")


class ResumeTaskCmd(TaskOperationCmd):

    usage = "resume"

    description = """
    Resume one or more tasks.  Resuming a task will put the task and
    the entire subtree of its ancestor tasks back on the queue; if the
    task is active it will be killed first.  The resume is different
    from a retry in that %r (recover) substitution will evaluate to 1
    instead of 0, which is typically used to indicate to rendering
    commands that checkpointed results are to be used to continue the
    render.  If the task has resumepin set to True, it will run on the
    same host.  By default the user is prompted before each task is
    resumed, unless the --yes flag is set.  Also. you can only resume
    your own tasks, unless the --force flag is set.
    """

    examples = """
  Syntax to resume tasks is:
    > tq resume WHERE_STRING
    > tq resume jid tid [tid2 tid3 ...]

  Examples:
    resume all errored tasks for thing
      > tq resume user=thing and error
    resume a specific task
      > tq resume jid=10795593 and tid=23
    same as previous example
      > tq resume 10795593 23
    resume all tasks that errored on a u3701 after 3pm today for all users
      > tq --force resume error and "statetime >= 3pm" and slots like u3701
    """

    def processObject(self, obj):
        """Operate on the provided task object."""
        # ask the user if we should continue
        if not obj.pre(self, "Resume this task?"):
            return
        # try to run the operation
        query.resume(obj)
        obj.post(self, "resumed")


class KillTaskCmd(TaskOperationCmd):

    usage = "kill"

    description = """
    Kill one or more tasks.  Killing a task will set the task's state
    to error and thus prevents other tasks that were waiting on it
    from proceeding.  Users are prompted before each task is killed,
    unless the --yes flag is set.  Also, users can only kill tasks
    from their own jobs unless the --force flag is set, and the
    JobEditAccessPolicies in crews.config allows it.
    """

    examples = """
  Syntax to kill tasks is:
    > tq kill WHERE_STRING
    > tq kill jid tid [tid2 tid3 ...]

  Examples:
    kill all active render tasks for user thing
      > tq kill user=thing and active and title like render
    kill a specific task
      > tq kill jid=10795593 and tid=23
    same as previous example
      > tq kill 10795593 23
      """

    def processObject(self, obj):
        """Operate on the provided task object."""
        # ask the user if we should continue
        if not obj.pre(self, "Kill this task?"):
            return
        # try to run the operation
        query.kill(obj)
        obj.post(self, "killed")


class SkipTaskCmd(TaskOperationCmd):

    usage = "skip"

    description = """
    Skip one or more tasks.  Skipping a task will set the task's state
    to done and thus allows other tasks that were waiting on it to
    proceed.  Users are prompted before each task is skipped, unless
    the --yes flag is set.  Also, users can only skip tasks from their
    own jobs unless the --force flag is set, and the
    JobEditAccessPolicies in crews.config allows it.
    """

    examples = """
  Syntax to skip tasks is:
    > tq skip WHERE_STRING
    > tq skip jid tid [tid2 tid3 ...]

  Examples:
    skip all errored clean tasks for thing
      > tq skip user=thing and error and title like Clean
    skip a specific task
      > tq skip jid=10795593 and tid=23
    same as previous example
      > tq skip 10795593 23
      """

    def processObject(self, obj):
        """Operate on the provided task object."""
        # ask the user if we should continue
        if not obj.pre(self, "Skip this task?"):
            return
        # try to run the operation
        query.skip(obj)
        obj.post(self, "skipped")


class ChangeKeysCmd(TaskOperationCmd):

    usage = "chkeys"

    description = """
    Change the keys of one or more tasks.  The keys of a task
    determine which blades the task can run on.  Blades have service
    keys to describe what kind of services they can perform
    (e.g. RENDER, linux64, etc.), and tasks specify which services
    they need in the key string.  By default the user is prompted
    before each task has its keys changed, unless the --yes flag is
    set.  Also, you can only change the keys for your own tasks,
    unless the --force flag is set.
    """

    examples = """
  Syntax to change keys for tasks is:
    > tq chkeys WHERE_STRING -k key_str
    > tq chkeys jid tid [tid2 tid3 ...] -k key_str

  Examples:
    change the keys of all thing's tasks to include linux64
      > tq chkeys user=thing -k '(RENDER,linux64)'
      """

    # add an option to get the key string
    options = [
        rpg.CmdLineTool.StringOption ("-k", "--keystr",
                                      help="give the exact key string that each "
                                      "task should have.  If the key "
                                      "contains any special shell "
                                      "characters it will need to be quoted.")
        ] + TaskOperationCmd.options

    def parseArgs(self, *args, **kwargs):
        result = super(ChangeKeysCmd, self).parseArgs(*args, **kwargs)
        # make sure a priority was given
        if self.opts.keystr is None:
            raise tq.TqError("no key string provided")
        return result

    def processObject(self, obj):
        """Operate on the provided task object."""
        # ask the user if we should continue
        if not obj.pre(self, "Change keys of this task?"):
            return
        # try to run the operation
        obj.setKeys(self.opts.keystr, debug=self.parent.opts.debug)
        obj.post(self, "keys changed")


class ChangeTaskAttributeCmd(TaskOperationCmd):

    usage = "tattr"

    description = """
    Change an attribute of one or more tasks.  The attribute and value of a task
    must be specified, and may require a different format, depending on the
    attribute.  For example, certain attributes may expect lists, where others
    a string.  By default the user is prompted before each command has its
    attribute changed, unless the --yes flag is set.  Also, you can only change
    the attributes of your own tasks, unless the --force flag is set.
    """
    
    examples = """
  The typical syntax to change the attributes of commands is:
    > tq tattr WHERE_STRING -k attribute {-a|--add|-r|--remove|-v|--value} value

  Examples:
    set the service keys of the thing's blocked tasks to "pixarRender"
      > tq tattr user=thing and state=blocked -k service -v pixarRender
    add "high_mem" to the service keys of all thing's errored tasks
      > tq tattr user=thing and state=error -k service -a high_mem
      """

    # add options to specify task attribute and value
    options = [
        rpg.CmdLineTool.StringOption ("-k", "--key",
                                      help="the name of the job attribute "
                                      "that will be changed"),
        rpg.CmdLineTool.StringOption ("-v", "--value",
                                      help="the value the job attribute "
                                      "that will be set to; can be a"
                                      "commad-delimited list for lists"),
        rpg.CmdLineTool.StringOption ("-a", "--add",
                                      help="value will be appended to current value"),
        rpg.CmdLineTool.StringOption ("-r", "--remove",
                                      help="value will be removed from current value"),
        ] + TaskOperationCmd.options

    IntListAttrs = []
    StrListAttrs = []
    
    def parseArgs(self, *args, **kwargs):
        result = super(ChangeTaskAttributeCmd, self).parseArgs(*args, **kwargs)

        # make sure key and value were specified
        if self.opts.key is None:
            raise tq.TqError("no key provided")
        if self.opts.value is None:
            raise tq.TqError("no value provided")

        if self.opts.key in self.IntListAttrs:
            values = stringutil.str2list(self.opts.value)
            for value in values:
                if not value.isdigit():
                    raise tq.TqError("value must be a comma separated list of integers")

        return result

    def processObject(self, obj):
        """Operate on the provided job object."""

        # ask the user if we should continue
        if not obj.pre(self, "Change %s of the task?" % self.opts.key):
            return

        # represent value in type appropriate to member; it will be packed for transmission
        if self.opts.key in self.IntListAttrs:
            value = [int(v) for v in stringutil.str2list(self.opts.value)]
        elif self.opts.key in self.StrListAttrs:
            value = stringutil.str2list(self.opts.value)
        else:
            value = self.opts.value
        
        # try to run the operation
        query.tattr(obj, key=self.opts.key, value=value)
        obj.post(self, "%s changed" % self.opts)


class PrintLogCmd(TaskOperationCmd):

    usage = "log"

    description = """
    Get the output log of one or more tasks from a tq job.  Each task
    saves all the output from each its commands, and appends the output to
    the same log when the task is retried.  This gives the equivalent
    output that the 'see Output log' option does when clicking on a task
    box in the tq UI.
    """

    examples = """
  Syntax to fetch task output logs is:
    > tq log WHERE_STRING
    > tq log jid tid [tid2 tid3 ...]

  Examples:
    view the output log for a specific task
      > tq log jid=10795593 and tid=23
    same as previous example
      > tq log 10795593 23
    view all the output logs from thing's errored tasks
      > tq log user=thing and error
    view only the last attempt for each of thing's errors
      > tq log user=thing and error -l
      """

    defaultSort = ["statetime"]
    defaultDistinct = ["jid", "tid", "owner"]

    # add options specific to fetching logs
    options = [
        rpg.CmdLineTool.BooleanOption ("-l", "--lasttry",
                                       help="only show the output from the last "
                                       "attempt, this will search for the "
                                       "last 'retry' message (if one exists) "
                                       "and print the output that follows "
                                       "it."),

        rpg.CmdLineTool.BooleanOption ("-n", "--nh", "--noheader", dest="noheader",
                                       help="by default an identifier of the "
                                       "task is printed before each log is "
                                       "printed (except when --unique is "
                                       "set).  This will prevent any summary "
                                       "from being printed."),

        rpg.CmdLineTool.BooleanOption ("-u", "--unique",
                                       help="print the user, host, jid, and tid "
                                       "of the task on each line to "
                                       "distinguish it from other tasks.  "
                                       "Useful when grepping though more "
                                       "than one log."),

        ] + TaskOperationCmd.options

    def parseArgs(self, *args, **kwargs):
        """Ensure the force and yes flags are always True."""
        result = super(PrintLogCmd, self).parseArgs(*args, **kwargs)
        
        self.parent.force = True
        self.parent.yes   = True

        return result

    def runQuery(self):
        """Overloaded so the header can be suppressed if only one log
        is being printed.  Also, we want to be able to look at logs
        from deleted jobs."""

        # check if we need to contact the db
        if self.objects:
            result = self.objects
        else:
            result = self.db.getTasks(members=self.members,
                                      where=self.where,
                                      limit=self.opts.limit,
                                      orderby=self.opts.sortby,
                                      objtype=tq.OperationRow)

        if not result:
            raise tq.TqError("no tasks found")

        # do not display the header
        if len(result) == 1:
            self.opts.noheader = True

        return result

    def processObject(self, obj):
        """Operate on the provided task object."""

        # fetch the log for task; result is in a dictionary, so extract for this (jid, tid)
        log = query.log(obj).get((obj.jid, obj.tid), "")

        # print a header if that is desired
        if not self.opts.noheader:
            hdr = self.formatter.format(obj)
            if self.parent.color:
                hdr = terminal.TerminalColor('yellow').colorStr(hdr)
            print(hdr)

        # make each line unique if that is desired
        if self.opts.unique:
            pre = '[%s %d %d] ' % (obj.Job.user, obj.jid, obj.tid)
            log = pre + log.replace('\n', '\n' + pre)

        if log:
            print(log)
