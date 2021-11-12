"""Definition of all the tq commands that perform an operation on a list
of blades returned from the database."""

import re, os

import rpg.listutil as listutil
import rpg.stringutil as stringutil
import rpg.terminal as terminal
import rpg.timeutil as timeutil
import rpg.CmdLineTool as CmdLineTool

import tractor.base.EngineDB as EngineDB
import tractor.api.query as query
from .. import tq

__all__ = (
    "BladeOperationCmd",
    "DelistBladeCmd",
    "EjectBladeCmd",
    "NimbyBladeCmd",
    "UnnimbyBladeCmd",
    "TraceBladeCmd",
    )

class BladeOperationCmd(tq.OperateCmd):
    """Base class for blade operations."""

    # default sort is priority order
    defaultSort     = ["name", "ipaddr"]
    defaultDistinct = ["name", "ipaddr"] # adding ipaddr here ensures that this required attribute is read from db

    def __init__(self, **kwargs):
        """The constructor is hard-coded to contact the main database."""
        super(BladeOperationCmd, self).__init__(EngineDB.EngineDB.BladeTable, **kwargs)

    def parseArgs(self, *args, **kwargs):
        result = super(BladeOperationCmd, self).parseArgs(*args, **kwargs)
        if not self.parent.opts.archives:
            # look only at blades that have registered
            if self.where:
                self.where = "registered and (%s)" % self.where
            else:
                self.where = "registered"
        return result

    def runQuery(self):
        """Overloaded from the super so a different Blade object
        can be returned."""
        # check if we need to contact the db
        if self.objects:
            result = self.objects
        else:
            result = self.db.getBlades(
                members=self.members, where=self.where,
                limit=self.opts.limit,
                orderby=self.opts.sortby, objtype=tq.OperationRow)
        if not result:
            print("no blades found")
        return result


class DelistBladeCmd(BladeOperationCmd):

    usage = "delist"

    description = """
    Clear blade stats for one or more blades.  Delisting on a blade
    will remove its record from the backend database.  A subsequent
    heartbeat by a running blade would cause a record to reapper.  By
    default the user is prompted before each blade is delisted, unless
    the --yes flag is set.
    """
    
    examples = """
  The typical syntax to delist a blade is:
    > tq delist WHERE_STRING

  Examples:
    delist a blade named boxy
      > tq delist name=boxy
    delist all blades on the 192.168.10.* subnet
      > tq delist ipaddr like 192.169.10.
    delist all blades with a loadavg over 2
      > tq delist "loadavg > 2"
      """

    def processObject(self, obj):
        """Operate on the provided blade object."""
        # ask the user if we should continue
        if not obj.pre(self, "Delist this blade?"):
            return
        # try to run the operation
        query.delist(obj)
        obj.post(self, "delist")


class EjectBladeCmd(BladeOperationCmd):

    usage = "eject"

    description = """
    Retry the tasks currently running one or more blades.  If the
    blade has not been nimbied, it will continue to request for more
    work.  By default the user is prompted before each blade is
    ejected, unless the --yes flag is set.
    """
    
    examples = """
  The typical syntax to eject a blade is:
    > tq eject WHERE_STRING

  Examples:
    eject a blade named boxy
      > tq eject name=boxy
    eject all blades on the 192.168.10.* subnet
      > tq eject ipaddr like 192.169.10.
    eject all blades with a loadavg over 2
      > tq eject "loadavg > 2"
      """

    def processObject(self, obj):
        """Operate on the provided blade object."""
        # ask the user if we should continue
        if not obj.pre(self, "Eject this blade?"):
            return
        # try to run the operation
        query.eject(obj)
        obj.post(self, "ejected")


class NimbyBladeCmd(BladeOperationCmd):

    usage = "nimby"

    description = """
    Nimby one or more blades.  Setting nimby on a blade will prevent
    it from requesting for work from the engine; however, if a
    username is specified with the --allow option, it will request
    only for tasks owned by that user.  Currently active tasks are
    allowed to continue running when nimby has been issued.  By
    default the user is prompted before each blade is nimbied, unless
    the --yes flag is set.  Clearing the nimby status of a blade is
    done with the unnimby command.
    """
    
    examples = """
  The typical syntax to nimby a blade is:
    > tq nimby WHERE_STRING

  Examples:
    nimby a blade named boxy
      > tq nimby name=boxy
    clear nimby status of a blade named boxy
      > tq unnimby name=boxy
    nimby all blades on the 192.168.10.* subnet
      > tq nimby ipaddr like 192.169.10.
    nimby all blades with a loadavg over 2
      > tq nimby "loadavg > 2"
    only allow jobs owned by oprah to run on blades with a profile of bookclub
      > tq nimby --allow oprah profile=bookclub
      """

    options = [
        CmdLineTool.StringOption(
            "--allow", help="allow tasks owned by specified user to run on blade")
        ] + BladeOperationCmd.options

    def processObject(self, obj):
        """Operate on the provided blade object."""
        # ask the user if we should continue
        if not obj.pre(self, "Nimby this blade?"):
            return
        # try to run the operation
        query.nimby(obj, allow=self.opts.allow)
        obj.post(self, "nimbied")


class UnnimbyBladeCmd(BladeOperationCmd):

    usage = "unnimby"

    description = """
    Unnimby one or more blades.  Setting unnimby on a blade will allow it to
    request for work from the engine.  By default the user is prompted before
    each blade is unnimbied, unless the --yes flag is set.
    """
    
    examples = """
  The typical syntax to unnimby a blade is:
    > tq unnimby WHERE_STRING

  Examples:
    unnimby a blade named boxy
      > tq unnimby name=boxy
    unnimby all blades on the 192.168.10.* subnet
      > tq unnimby ipaddr like 192.169.10.
    unnimby all blades with a loadavg under 1
      > tq unnimby "loadavg < 1"
      """

    def processObject(self, obj):
        """Operate on the provided blade object."""
        # ask the user if we should continue
        if not obj.pre(self, "Unnimby this blade?"):
            return
        # try to run the operation
        query.unnimby(obj)
        obj.post(self, "unnimbied")


class TraceBladeCmd(BladeOperationCmd):

    usage = "trace"

    description = """
    Run a tracer on one or more blades.  Running a tracer on a blade will
    display output regarding its decision making process to run a task.
    """
    
    examples = """
  The typical syntax to trace blade is:
    > tq trace WHERE_STRING

  Examples:
    trace a blade named boxy
      > tq trace name=boxy
      """

    options = [
        CmdLineTool.BooleanOption ("-u", "--unique",
                                   help="print the name of the blade "
                                        "on each line to "
                                        "distinguish output from blades.  "
                                        "Useful when grepping though more "
                                        "than one trace."),
        ] + BladeOperationCmd.options

    def runQuery(self):
        """Overloaded so the header can be suppressed if only one trace
        is being printed."""
        # check if we need to contact the db
        if self.objects:
            result = self.objects
        else:
            result = self.db.getBlades(members=self.members,
                                       where=self.where,
                                       limit=self.opts.limit,
                                       orderby=self.opts.sortby,
                                       objtype=tq.OperationRow)
        if not result:
            raise tq.TqError("no blades found")
        # do not display the header
        if len(result) == 1:
            self.opts.noheader = True
        return result

    def processObject(self, obj):
        """Operate on the provided blade object."""
        # print a header if that is desired
        if not self.opts.noheader:
            hdr = self.formatter.format(obj)
            if self.parent.color:
                hdr = terminal.TerminalColor('yellow').colorStr(hdr)
            print(hdr)

        trace = query.trace(obj).get((obj.name, obj.ipaddr), "")

        # make each line unique if that is desired
        if self.opts.unique:
            pre  = '[%s] ' % obj.name
            trace = pre + trace.replace('\n', '\n' + pre)
    
        print(trace)
        obj.post(self, "End of trace.")
