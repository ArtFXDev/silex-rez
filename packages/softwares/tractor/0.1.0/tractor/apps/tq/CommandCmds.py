"""Definition of all the tq commands that perform an operation on a list
of commands returned from the database."""

import re
import rpg.stringutil as stringutil
import rpg.terminal as terminal
import rpg.CmdLineTool

import tractor.api.query as query
import tractor.base.EngineDB as EngineDB
from . import CmdLineTool
from .. import tq

__all__ = (
    "ChangeKeysCmd",
    "ChangeCommandAttributeCmd",
    )


class CmdLineCommands(tq.CmdLineTool.KeylessCmdLineCommands):
    def __init__(self, *args, **kwargs):
        super(CmdLineCommands, self).__init__(
            tq.OperationRow, tq.OperationRow, *args, **kwargs)

class CommandOperationCmd(tq.OperateCmd):
    """Base class for command operations."""
    
    defaultSort     = ["-Job.priority", "Job.spooltime"]
    defaultDistinct = ["jobid", "cid"]

    def __init__(self, **kwargs):
        """The constructor is hard-coded to contact the main database."""
        super(CommandOperationCmd, self).__init__(EngineDB.EngineDB.CommandTable, **kwargs)

    def parseArgs(self, *args, **kwargs):
        result = super(CommandOperationCmd, self).parseArgs(*args, **kwargs)
        # check if the where string is in the form:
        #   jid [cid ...]
        try:
            commands = CmdLineCommands(self.args)
            self.where = commands.getWhere()
        except tq.CmdLineTool.UnknownFormat:
            pass
        return result

    def runQuery(self):
        """Overloaded from the super so a different Command object can be returned."""
        # check if we need to contact the db
        if self.objects:
            result = self.objects
        else:
            # only query existing jobs
            # AWG: not deleted not getting translated right yet
            #where  = "(%s) and not deleted" % self.where
            where  = self.where
            result = self.db.getCommands(members=self.members,
                                         where=where,
                                         limit=self.opts.limit,
                                         orderby=self.opts.sortby,
                                         objtype=tq.OperationRow)
        if not result:
            print("no commands found")
        return result


class ChangeKeysCmd(CommandOperationCmd):

    usage = "chkeys"

    description = """
    Change the keys of one or more commands from tq jobs.  The keys of a
    command determine which blades the command can run on.  By default the
    user is prompted before each command has its keys changed, unless the
    --yes flag is set.  Also, you can only change the keys for your own
    commands, unless the --force flag is set.
    """

    examples = """
  Syntax to change keys for commands is:
    > tq chkeys SEARCH_CLAUSE -k key_str
    > tq chkeys jid tid [tid2 tid3 ...] -k key_str

  Examples:
    change the keys of all vlad's commands to be "pixarRender,linux64"
      > tq chkeys user=vlad -k 'pixarRender,linux64'
      """

    # add an option to get the key string
    options = [
        rpg.CmdLineTool.StringOption(
            "-k", "--keystr",
            help="give the exact key string that each command should have.  If the key "
            "contains any special shell characters it will need to be quoted.")
        ] + CommandOperationCmd.options

    def parseArgs(self, *args, **kwargs):
        result = super(ChangeKeysCmd, self).parseArgs(*args, **kwargs)
        # make sure a priority was given
        if self.opts.keystr is None:
            raise tq.TqError("no key string provided")
        return result

    def processObject(self, obj):
        """Operate on the provided command object."""
        # ask the user if we should continue
        if not obj.pre(self, "Change keys of this command?"):
            return
        # try to run the operation
        query.chkeys(obj, keystr=self.opts.keystr)
        obj.post(self, "keys changed")


class ChangeCommandAttributeCmd(CommandOperationCmd):

    usage = "cattr"

    description = """
    Change an attribute of one or more commands.  The attribute and value of a command
    must be specified, and may require a different format, depending on the
    attribute.  For example, certain attributes may expect lists, where others
    a string.  By default the user is prompted before each command has its
    attribute changed, unless the --yes flag is set.  Also, you can only change
    the attributes of your own commands, unless the --force flag is set.
    """
    
    examples = """
  The typical syntax to change the attributes of commands is:
    > tq cattr SEARCH_CLAUSE -k attribute {-a|--add|-r|--remove|-v|--value} value

  Examples:
    set the service keys of the commands of thing's blocked tasks to "pixarRender"
      > tq cattr user=thing and state=blocked -k service -v pixarRender
    add "high_mem" to the service keys of commands of all of thing's errored tasks
      > tq cattr user=thing and state=error -k service -a high_mem
      """

    # add options to specify command attribute and value
    options = [
        rpg.CmdLineTool.StringOption ("-k", "--key",
                                      help="the name of the command attribute "
                                      "that will be changed"),
        rpg.CmdLineTool.StringOption ("-v", "--value",
                                      help="the value the command attribute "
                                      "that will be set to; can be a"
                                      "comma-delimited list for lists"),
        rpg.CmdLineTool.StringOption ("-a", "--add",
                                      help="value will be appended to current value"),
        rpg.CmdLineTool.StringOption ("-r", "--remove",
                                      help="value will be removed from current value"),
        ] + CommandOperationCmd.options

    IntListAttrs = [] # ["retryrcodes"] # currently unsupported by engine
    StrListAttrs = ["argv", "tags", "envkey"] # [, "resumewhile"]  # unsupported
    
    def parseArgs(self, *args, **kwargs):
        result = super(ChangeCommandAttributeCmd, self).parseArgs(*args, **kwargs)

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
        super(ChangeCommandAttributeCmd, self).pre_execute()
        # add the attribute being modified
        if self.opts.add or self.opts.remove:
            self.members.append(self.opts.key)

    def processObject(self, obj):
        """Operate on the provided job object."""

        # ask the user if we should continue
        if not obj.pre(self, "Change %s of the command?" % self.opts.key):
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
        query.cattr(obj, key=self.opts.key, value=value)
        obj.post(self, "%s changed" % self.opts.key)
