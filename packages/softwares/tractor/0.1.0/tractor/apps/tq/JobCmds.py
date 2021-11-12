"""Definition of all the tq commands that perform an operation on a list
of jobs returned from the database."""

import re, os, sys

import rpg.stringutil as stringutil
import rpg.timeutil as timeutil
import rpg.CmdLineTool as CmdLineTool

import tractor.api.query as query
import tractor.base.EngineDB as EngineDB
from .. import tq

__all__ = (
    "JobOperationCmd",
    "ChangeCrewsCmd",
    "ChangePriorityCmd",
    "DeleteJobCmd",
    "DelayJobCmd",
    "DumpJobCmd",
    "ChangeJobAttributeCmd",
    "OpenGUICmd",
    "PauseJobCmd",
    "LockJobCmd",
    "InterruptJobCmd",
    "RestartJobCmd",
    "RetryAllErrorsCmd",
    "SkipAllErrorsCmd",
    "UndelayJobCmd",
    "UndeleteJobCmd",
    "UnlockJobCmd",
    "UnpauseJobCmd",
    )


class JobOperationCmd(tq.OperateCmd):
    """Base class for job operations.""" 

    # regexp used to check for the altnerate syntax
    _jidre  = re.compile("^((\d+)(\s+\d+)*)$")
    # default sort is priority order
    defaultSort     = ["-priority", "spooltime"]
    defaultDistinct = ["jobid"]

    def __init__(self, **kwargs):
        """The constructor is hard-coded to contact the main database."""
        super(JobOperationCmd, self).__init__(EngineDB.EngineDB.JobTable, **kwargs)

    def parseArgs(self, *args, **kwargs):
        result = super(JobOperationCmd, self).parseArgs(*args, **kwargs)
        # check if the where string is in the form:
        #   jid [jid2 jid3 ...]
        match = self._jidre.match(self.where)
        if match:
            # check if user and host were provided
            if match.group(1):
                self.where = ' '.join(self.args)
            self.where = "jobid in [%s]" % self.where
        return result

    def runQuery(self):
        """Overloaded from the super so a different Job object
        can be returned."""
        # check if we need to contact the db
        if self.objects:
            result = self.objects
        else:
            result = self.db.getJobs(
                members=self.members, where=self.where,
                limit=self.opts.limit,
                orderby=self.opts.sortby,
                only=not self.parent.opts.archives,
                objtype=tq.OperationRow)
        if not result:
            sys.stderr.write("no jobs found\n")
        return result


class ChangeCrewsCmd(JobOperationCmd):

    usage = "chcrews"

    description = """
    Change the crews of one or more jobs.  The crews of a job
    determine which blades the job's tasks will potentially run on.
    Crews can be added or removed from a job's existing list, or replaced
    entirely with a new list of crews.  By default the user is prompted
    before each job has its crews changed, unless the --yes flag is set.
    Also, you can only change the crews of your own job, unless the
    --force flag is set.
    """
    
    examples = """
  Syntax to change the crews of jobs:
    > tq chcrews SEARCH_CLAUSE [-k|--crews|-a|-r] crew1[,crew2,..]
    > tq chcrews jid [jid2 jid3 ...] [-k|--crews|-a|-r] crew1[,crew2,..]

  Examples:
    change the crews of all of joni's jobs to lighting
      > tq chcrews user=joni -k lighting
    add animation to all of joni's jobs
      > tq chcrews user=joni -a animation
    remove the animation crew from a specific job
      > tq chcrews 10795593 -r animation
      """

    # add option to get new crews
    options = [
        CmdLineTool.StrListOption ("-a", "--add", default=[],
                                   help="a comma delimited list of "
                                        "crews that will be added to "
                                        "each job's current list."),

        CmdLineTool.StrListOption ("-k", "--crews",
                                   help="a comma delimited list of "
                                        "crews to set as the job's "
                                        "crews."),

        CmdLineTool.StrListOption ("-r", "--remove", default=[],
                                   help="a comma delimited list of "
                                        "crews that will be removed from "
                                        "each job's current list."),

        ] + JobOperationCmd.options

    def parseArgs(self, *args, **kwargs):
        """Make sure we grab the current crews if we need to add or
        remove."""
        result = super(ChangeCrewsCmd, self).parseArgs(*args, **kwargs)

        # make sure crews were provided
        if not (self.opts.add or self.opts.remove or self.opts.crews):
            raise tq.TqError("no crews provided")

        # if the add or remove options are set, then force a database call
        if self.objects and (self.opts.add or self.opts.remove):
            phrases = ['jobid=%d' % job.jobid for job in self.objects]
            self.where = " or ".join(phrases)
            # reset the objects list so we do a database call
            self.objects = []

        return result

    def pre_execute(self):
        super(ChangeCrewsCmd, self).pre_execute()
        # add the crews if we are modifying
        if self.opts.add or self.opts.remove:
            self.members.append("Job.crews")

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Change the crews of the job?"):
            return
        if self.opts.add or self.opts.remove:
            # make a copy so we can add/remove crews
            newcrews = tq.copyModifyList(obj.crews, remove=self.opts.remove, append=self.opts.add)
            if newcrews == obj.crews:
                obj.post(self, "crews did not change")
                return
        else:
            newcrews = self.opts.crews
        # try to run the operation
        query.chcrews(obj, crews=newcrews)
        obj.post(self, "crews changed")


class ChangePriorityCmd(JobOperationCmd):

    usage = "chpri"

    description = """
    Change the priority of one or more jobs.  The priority of a job
    determines its tasks' placement in the queue, and raising the priority will
    move its tasks closer to the front of the queue.  The tie breaker for equal
    priority tasks is the job's spool time, and the job submitted first takes
    precedence.  By default the user is prompted before each job has its
    priority changed, unless the --yes flag is set.  Also, you can only change
    the priority of your own job, unless the --force flag is set.
    """
    
    examples = """
  The typical syntax to change the priority of jobs is:
    > tq chpri SEARCH_CLAUSE -p pri
    > tq chpri jid [jid2 jid3 ...] -p pri

  Examples:
    change the priority of all of chachi's jobs to 200
      > tq chpri user=chachi -p 200
    change the priority of all of chachi's jobs submitted after 5pm today
      > tq chpri user=chachi and "spooltime >= 5pm" -p 200
    change the priority of all of chachi's jobs that are equal to 100
      > tq chpri user=chachi and priority=100 -p 200
      """

    # add option to specify priority
    options = [
        CmdLineTool.FloatOption ("-p", "--priority",
                                 help="the priority value that each job "
                                      "will be changed to, this can be a "
                                      "floating point value."),
        ] + JobOperationCmd.options

    def parseArgs(self, *args, **kwargs):
        result = super(ChangePriorityCmd, self).parseArgs(*args, **kwargs)

        # make sure a priority was given
        if self.opts.priority is None:
            raise tq.TqError("no priority provided")

        return result

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Change the priority of the job?"):
            return
        # try to run the operation
        query.chpri(obj, priority=self.opts.priority)
        obj.post(self, "priority changed")


class ChangeJobAttributeCmd(JobOperationCmd):

    usage = "jattr"

    description = """
    Change an attribute of one or more jobs.  The attribute and value of a job
    must be specified, and may require a different format, depending on the
    attribute.  For example, certain attributes may expect lists, where others
    a string.  By default the user is prompted before each job has its
    attribute changed, unless the --yes flag is set.  Also, you can only change
    the attributes of your own job, unless the --force flag is set.
    """
    
    examples = """
  The typical syntax to change the attributes of jobs is:
    > tq jattr SEARCH_CLAUSE -k attribute [-a|--add|-r|--remove|-v|--value] value
    > tq jattr jid [jid2 jid3 ...] -k attribute [-a|--add|-r|--remove|-v|--value] value

  Examples:
    change the metadata of all potsi's jobs to "watch this job"
      > tq jattr user=potsi -k metadata -v "watch this job"
    change the afterjids of all potsi's jobs submitted after 5pm today
    > tq jattr user=potsi and "spooled >= 5pm" -k afterjid -v 10020,11021
    add "apple" to the tags of all potsi's jobs that are at priority 100
      > tq chpri user=potsi and priority=100 -k tags --add apple
      """

    # add option to specify job attribute and value
    options = [
        CmdLineTool.StringOption ("-k", "--key",
                                 help="the name of the job attribute "
                                      "that will be changed"),
        CmdLineTool.StringOption ("-v", "--value",
                                 help="the value the job attribute "
                                      "that will be set to; can be a"
                                      "commad-delimited list for lists"),
        CmdLineTool.StringOption ("-a", "--add",
                                 help="for list attributes, value will be appended to current list"),
        CmdLineTool.StringOption ("-r", "--remove",
                                 help="for list attributes, value will be removed from current list"),
        ] + JobOperationCmd.options

    IntListAttrs = ["afterjids"]
    StrListAttrs = ["tags", "envkey", "projects", "crews"]
    
    def parseArgs(self, *args, **kwargs):
        result = super(ChangeJobAttributeCmd, self).parseArgs(*args, **kwargs)

        # make sure key and value were specified
        if self.opts.key is None:
            raise tq.TqError("no key provided")
        if self.opts.value is None and self.opts.add is None and self.opts.remove is None:
            raise tq.TqError("no value provided")

        # ensure any specified values are of correct type
        if self.opts.key in self.IntListAttrs:
            self.cast = int
            values = stringutil.str2list(self.opts.value)
            for value in values:
                if not value.isdigit():
                    raise tq.TqError("--value must be a comma-separated list of integers")
            values = stringutil.str2list(self.opts.add)
            for value in values:
                if not value.isdigit():
                    raise tq.TqError("--add must be a comma-separated list of integers")
            values = stringutil.str2list(self.opts.remove)
            for value in values:
                if not value.isdigit():
                    raise tq.TqError("--remove must be a comma-separated list of integers")
        else:
            self.cast = str

        return result

    def pre_execute(self):
        super(ChangeJobAttributeCmd, self).pre_execute()
        # add the attribute being modified
        if self.opts.add or self.opts.remove:
            self.members.append(self.opts.key)

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Change %s of the job?" % self.opts.key):
            return

        if self.opts.add or self.opts.remove:
            origvalues = getattr(obj, self.opts.key)
            # make a copy so we can add/remove crews
            remove = [self.cast(item) for item in stringutil.str2list(self.opts.remove or "")]
            append = [self.cast(item) for item in stringutil.str2list(self.opts.add or "")]
            newvalues = tq.copyModifyList(origvalues, remove=remove, append=append)
            if newvalues == origvalues:
                obj.post(self, "%s did not change" % self.opts.key)
                return
            # jattr takes a single string as a value
            value = ",".join([str(v) for v in newvalues])
        else:
            # represent value in type appropriate to member; it will be packed for transmission
            if self.opts.key in self.IntListAttrs or self.opts.key in self.StrListAttrs:
                # expansion with str2list may help eliminate unnecessary whitespace
                value = ",".join(stringutil.str2list(self.opts.value))
            else:
                value = self.opts.value
        
        # try to run the operation
        query.jattr(obj, key=self.opts.key, value=value)
        obj.post(self, "%s changed" % self.opts.key)


class DeleteJobCmd(JobOperationCmd):

    usage = "delete"

    description = """
    Delete one or more jobs from the scheduler.  Deleting a job
    cancels (kills) any active tasks in the job and removes it from
    the scheduler.  This is the same as deleting a job via the UI.  By
    default the user is prompted before each job is deleted, unless
    the --yes flag is set.  Also, you can only delete your own jobs,
    unless the --force flag is set.  Be CAREFUL with this command, as
    you could accidentally delete more than you want to.
    """
    
    examples = """
  Syntax to delete jobs is:
    > tq delete SEARCH_CLAUSE
    > tq delete jid [jid2 jid3 ...]

  Examples:
    delete all done jobs for user fonzerelli
      > tq delete user=fonzerelli and done
    delete all of fonzerelli's jobs
      > tq delete user=fonzerelli
      """

    defaultSort = ["priority", "-spooltime"]
    
    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Delete this job?"):
            return
        # try to run the operation
        query.delete(obj)
        obj.post(self, "deleted")


class DelayJobCmd(JobOperationCmd):

    usage = "delay"

    description = """
    Delay one or more jobs.  Delaying a job has the same effect as
    pausing it, but the job will resume as normal at the specified
    time.  By default the user is prompted before each job is delayed,
    unless the --yes flag is set.  Also, you can only delay your own
    jobs, unless the --force flag is set.
    """
    
    examples = """
  Syntax to delay a job is:
    > tq delay SEARCH_CLAUSE -t time_str
    > tq delay jid [jid2 jid3 ...] -t time_str

  Examples:
    delay all of balki's jobs until 10pm tonight
      > tq delay user=balki -t 10pm
    delay all of balki's priority 100 jobs until April 1st
      > tq delay user=balki and priority=100 -t 4/1
      """

    # reverse priority
    defaultSort = ["priority", "-spooltime"]

    # add option to get the delay time
    options = [
        CmdLineTool.TimeOption ("-t", "--time", dest="aftertime",
                                help="time after which the job will be "
                                     "considered for scheduling. "
                                     "The time can be expressed in the "
                                     "following forms:"
                           "10am         - delay until 10am on the current day"
                           "5pm          - delay until 5pm on the current day"
                           "3/25         - delay until midnight on 3/25"
                           "3/25|12pm    - delay until 12pm on 3/25"
                           "3/25|10:30am - delay until 10:30am on 3/25"),

        ] + JobOperationCmd.options

    def parseArgs(self, *args, **kwargs):
        result = super(DelayJobCmd, self).parseArgs(*args, **kwargs)
        # make sure a delay time was given
        if self.opts.aftertime is None:
            raise tq.TqError("no time provided")
        return result

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Delay this job?"):
            return
        # try to run the operation
        query.delay(obj, aftertime=self.opts.aftertime)
        obj.post(self, "delayed")


class DumpJobCmd(JobOperationCmd):

    usage = "jobdump"

    description = """
    Emit all stored job information.
    """

    examples = """
  Syntax to get a SQL dump of one or more jobs is:
    > tq jobdump WHERE_STRING

  Examples:
    view the SQL dump for a specific job
      > tq jobdump jid=10795593
    view the SQL dump for all errored jobs
      > tq jobdump "numerrors > 0"
      """

    defaultSort = ["jid"]

    def parseArgs(self, *args, **kwargs):
        """Ensure the force and yes flags are always True."""
        result = super(DumpJobCmd, self).parseArgs(*args, **kwargs)
        self.opts.noheader = True
        self.parent.force = True
        self.parent.yes = True

        return result

    def processObject(self, obj):
        """Operate on the provided task object."""
        # fetch the sql dump for job; result is in a dictionary, so extract for this jid
        dump = query.jobdump(obj).get(obj.jid, "")
        if dump:
            print(dump)


class PauseJobCmd(JobOperationCmd):

    usage = "pause"

    description = """
    Pause one or more jobs.  Pausing a job will prevent its tasks from
    starting up.  Tasks that are active or already have slots checked out will
    continue until all commands are finished.  By default the user is prompted
    before each job is paused, unless the --yes flag is set.  Also, you can
    only pause your own jobs, unless the --force flag is set.
    """
    
    examples = """
  The typical syntax to pause a job is:
    > tq pause SEARCH_CLAUSE
    > tq pause jid [jid2 jid3 ...]

  Examples:
    pause all of mindy's jobs with priority 250
      > tq pause user=mindy and priority=250
    pause a specific job
      > tq pause jid=10795593
    same as previous example
      > tq pause 10795593
      """

    # reverse priority order
    defaultSort = ["priority", "-spooltime"]

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Pause this job?"):
            return
        # try to run the operation
        query.pause(obj)
        obj.post(self, "paused")


class LockJobCmd(JobOperationCmd):

    usage = "lock"

    description = """
    Lock one or more jobs.  Locking a job will prevent other users
    from being able to perform operations on it such as restart,
    delete, and attribute edit.  The lock owner is permitted to
    perform operations while the job is locked.  The job may be
    unlocked by user with permissions to do so.  A note can optionally
    be specified using the --note flag.  By default the user is
    prompted before each job is locked, unless the --yes flag is set.
    Also, you can only lock your own jobs, unless the --force flag is
    set.
    """
    
    examples = """
  The typical syntax to lock a job is:
    > tq lock SEARCH_CLAUSE
    > tq lock jid [jid2 jid3 ...]

  Examples:
    lock all of mindy's jobs with priority 250
      > tq lock user=mindy and priority=250
    lock a specific job, adding a note
      > tq lock jid=10795593 --note "debugging render"
    same as previous example
      > tq lock 10795593 --note "debugging render"
      """

    # reverse priority order
    defaultSort = ["priority", "-spooltime"]

    # add option to specify note
    options = [
        CmdLineTool.StringOption ("-n", "--note",
                                  help="a note associated with the lock"),
        ] + JobOperationCmd.options

    def parseArgs(self, *args, **kwargs):
        result = super(LockJobCmd, self).parseArgs(*args, **kwargs)

        # make sure a priority was given
        if self.opts.note is None:
            raise tq.TqError("no note provided")

        return result

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Lock this job?"):
            return
        # try to run the operation
        query.lock(obj, note=self.opts.note)
        obj.post(self, "locked")


class InterruptJobCmd(JobOperationCmd):

    usage = "interrupt"

    description = """
    Pause a job and kill currently active tasks. This is the same as selecting
    'Interrupt job' in the job pulldown menu of the UI.  By default the user is
    prompted before each job is interrupted, unless the --yes flag is set.  Also,
    you can only interrupt your own jobs, unless the --force flag is set.
    """
    
    examples = """
  Syntax to interrupt a job is:
    > tq interrupt SEARCH_CLAUSE
    > tq interrupt jid [jid2 jid3 ...]

  Examples:
    interrupt all jobs for shirley that are active right now
      > tq interrupt user=shirley and active
    interrupt a specific job
      > tq interrupt jid=10795593
    same as previous example
      > tq interrupt 10795593
      """

    # reverse priority order
    defaultSort = ["priority", "-spooltime"]

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Interrupt this job?"):
            return
        # try to run the operation
        query.interrupt(obj)
        obj.post(self, "interrupted")


class RestartJobCmd(JobOperationCmd):

    usage = "restart"

    description = """
    Restart entire job.  Stop (kill) any active tasks and restart one or
    more jobs from the beginning.  This is the same as selecting
    'Restart job' in the job pulldown menu of the UI.
    By default the user is prompted before each job is restarted, unless the
    --yes flag is set.  Also, you can only restart your own jobs, unless the
    --force flag is set.
    """
    
    examples = """
  Syntax to restart a job is:
    > tq restart SEARCH_CLAUSE
    > tq restart jid [jid2 jid3 ...]

  Examples:
    restart all jobs for mork that are active right now
      > tq restart user=mork and active
    restart a specific job
      > tq restart jid=10795593
    same as previous example
      > tq restart 10795593
      """

    # reverse priority order
    defaultSort = ["priority", "-spooltime"]

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Restart this job?"):
            return
        # try to run the operation
        query.restart(obj)
        obj.post(self, "restarted")


class RetryAllErrorsCmd(JobOperationCmd):

    usage = "retryallerrs"

    description = """
    Retry all the errors in one or more jobs.  This is the same as selecting
    'Retry all error tasks' in the job pulldown menu of the UI.  By
    default the user is prompted before each job has its errors retried, unless
    the --yes flag is set.  Also, you can only retry the errors of your own
    jobs, unless the --force flag is set.
    """
    
    examples = """
  Syntax to retry all errors of a job is:
    > tq retryallerrs SEARCH_CLAUSE
    > tq retryallerrs jid [jid2 jid3 ...]

  Examples:
    retry all the errors for one jobs
      > tq retryallerrs jid=10795593
    same as previous example
      > tq retryallerrs 10795593
    retry all the errors for jack's jobs that were submitted an hour ago
      > tq retryallerrs user=jack and "spooled > -1h"
      """
    
    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Retry all errors in this job?"):
            return
        # try to run the operation
        query.retryerrors(obj)
        obj.post(self, "retried all errors")


class SkipAllErrorsCmd(JobOperationCmd):

    usage = "skipallerrs"

    description = """
    Skip all the errors in one or more jobs.  This is the same as selecting
    'Skip all error tasks' in the job pulldown menu of the UI.  By
    default the user is prompted before each job has its errors skipped, unless
    the --yes flag is set.  Also, you can only skip the errors of your own
    jobs, unless the --force flag is set.
    """
    
    examples = """
  The typical syntax to skip all errors of a job is:
    > tq skipallerrs SEARCH_CLAUSE
    > tq skipallerrs jid [jid2 jid3 ...]

  Examples:
    skip all the errors for one job
      > tq skipallerrs jid=10795593
    same as previous example
      > tq skipallerrs 10795593
    skip all the errors for janet's jobs that were submitted an hour ago
      > tq skipallerrs user=janet and "spooled > -1h"
      """
    
    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Skip all errors in this job?"):
            return
        # try to run the operation
        query.skiperrors(obj)
        obj.post(self, "skipped all errors")


class UndelayJobCmd(JobOperationCmd):

    usage = "undelay"

    description = """
    Un-delay one or more jobs.  Un-delaying a job forces a delayed job
    to resume as normal.  This is the same as clicking the
    'Process job immediately' option under the 'Job huntgroup...' pulldown menu
    of a job through the UI.  By default the user is prompted before
    each job is un-delayed, unless the --yes flag is set.  Also you can only
    un-delay your own jobs, unless the --force flag is set.
    """
    
    examples = """
  Syntax to un-delay a job is:
    > tq undelay SEARCH_CLAUSE
    > tq undelay jid [jid2 jid3 ...]

  Examples:
    un-delay all delayed jobs for chrissy
      > tq undelay user=chrissy and delayed
    un-delay all of chrissy's priority 400 jobs
      > tq undelay user=chrissy and priority=400 and delayed
      """
    
    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Un-delay this job?"):
            return
        # try to run the operation
        query.undelay(obj)
        obj.post(self, "un-delayed")


class UndeleteJobCmd(JobOperationCmd):

    usage = "undelete"

    description = """
    
    Undelete one or more jobs.  Undeleting a job clears the delete
    time of the job, and moves it from the archive partitions to the
    live partitions in the database to make it visible to the system.
    The user will need to restart the job or retry tasks in order to
    have it scheduled.  By default the user is prompted before each
    job is undeleted, unless the --yes flag is set.  Also, you can
    only undelete your own jobs, unless the --force flag is set.  Note
    that this must be used with the --archive flag in order for target
    jobs to be found.  Be CAREFUL with this command, as you could
    accidentally undelete more than you want to.
    """
    
    examples = """
  Syntax to undelete jobs is:
    > tq --archive undelete SEARCH_CLAUSE
    > tq -a undelete jid [jid2 jid3 ...]

  Examples:
    undelete all done jobs for user fonzerelli
      > tq --archive undelete user=fonzerelli and done
    undelete all of fonzerelli's jobs
      > tq -a undelete user=fonzerelli
      """

    defaultSort = ["priority", "-spooltime"]
    
    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Undelete this job?"):
            return
        # try to run the operation
        query.undelete(obj)
        obj.post(self, "undeleted")


class UnpauseJobCmd(JobOperationCmd):

    usage = "unpause"

    description = """
    Unpause one or more jobs.  Unpausing a job forces the job to resume
    as normal and all ready tasks are added to the queue.  By default the user
    is prompted before each job is unpaused, unless the --yes flag is set.
    Also, you can only unpause your own jobs, unless the --force flag is set.
    """
    
    examples = """
  The typical syntax to unpause a job is:
    > tq unpause SEARCH_CLAUSE
    > tq unpause jid [jid2 jid3 ...]

  Examples:
    unpause all of florence's paused jobs
      > tq unpause user=florence and paused
    unpause a specific job
      > tq unpause jid=10795593
    same as previous example
      > tq unpause 10795593
      """

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Unpause this job?"):
            return
        # try to run the operation
        query.unpause(obj)
        obj.post(self, "unpause")


class UnlockJobCmd(JobOperationCmd):

    usage = "unlock"

    description = """
    Unlock one or more jobs.  Unlocking a job will allow other users
    to perform operations on it such as restart, delete, and attribute
    edit, if they have permissions to do so.  The job may be unlocked
    by user different from the lock owner so long as they have
    permissions to do so.  By default the user is prompted before each
    job is unlocked, unless the --yes flag is set.  Also, you can only
    unlock your own jobs, unless the --force flag is set.
    """
    
    examples = """
  The typical syntax to unlock a job is:
    > tq unlock SEARCH_CLAUSE
    > tq unlock jid [jid2 jid3 ...]

  Examples:
    unlock a specific job
      > tq unlock jid=10795593
    same as previous example
      > tq unlock 10795593
    unlock all locked jobs
      > tq unlock locked
      """

    # reverse priority order
    defaultSort = ["priority", "-spooltime"]

    def processObject(self, obj):
        """Operate on the provided job object."""
        # ask the user if we should continue
        if not obj.pre(self, "Unlock this job?"):
            return
        # try to run the operation
        query.unlock(obj)
        obj.post(self, "unlocked")
