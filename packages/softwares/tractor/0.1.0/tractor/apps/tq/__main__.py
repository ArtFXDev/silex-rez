#!/usr/bin/python2 

import sys, os, getpass, errno, traceback

import tractor.base.rpg as rpg
import rpg.OptionParser
import rpg.CmdLineTool as CmdLineTool
import rpg.sql.DBCmdLineTool as DBCmdLineTool

import tractor.base.EngineClient as EngineClient
import tractor.base.TrHttpRPC as TrHttpRPC

from . import ListCmds
from . import JobCmds
from . import TaskCmds
from . import CommandCmds
from . import BladeCmds
from . import AdminCmds
from . import HelpCmds

# global var gets set to true if full traceback is to be shown when there is an exception
ShowFullTraceback = False

class TractorCLT(CmdLineTool.MultiCmdLineTool):
    """The main command object for the tool just includes all the sub-command objects."""
    version = "2.4 (2091325) - 17 Aug 2020 05:40:13"
    DEFAULT_THREADS = 10
    DEFAULT_TIMEOUT = 30
    DEFAULT_TIMEFMT = os.environ.get("TRACTOR_TIMEFMT")

    # setup all the commands in this app
    commands = {
        "jobs"          : ListCmds.JobsCmd,
        "tasks"         : ListCmds.TasksCmd,
        "commands"      : ListCmds.CommandsCmd,
        "cmds"          : ListCmds.CommandsCmd,
        "invocations"   : ListCmds.InvocationsCmd,
        "invos"         : ListCmds.InvocationsCmd,
        "blades"        : ListCmds.BladesCmd,
        "params"        : ListCmds.ParamsCmd,
        "notes"         : ListCmds.NotesCmd,

        "chcrews"       : JobCmds.ChangeCrewsCmd,
        "chpri"         : JobCmds.ChangePriorityCmd,
        "delay"         : JobCmds.DelayJobCmd,
        "delete"        : JobCmds.DeleteJobCmd,
        "jattr"         : JobCmds.ChangeJobAttributeCmd,
        "jobdump"       : JobCmds.DumpJobCmd,
        "lock"          : JobCmds.LockJobCmd,
        "pause"         : JobCmds.PauseJobCmd,
        "interrupt"     : JobCmds.InterruptJobCmd,
        "restart"       : JobCmds.RestartJobCmd,
        "retryallerrs"  : JobCmds.RetryAllErrorsCmd,
        "skipallerrs"   : JobCmds.SkipAllErrorsCmd,
        "undelay"       : JobCmds.UndelayJobCmd,
        "undelete"      : JobCmds.UndeleteJobCmd,
        "unlock"        : JobCmds.UnlockJobCmd,
        "unpause"       : JobCmds.UnpauseJobCmd,

        "retry"         : TaskCmds.RetryTaskCmd,
        "resume"        : TaskCmds.ResumeTaskCmd,
        "kill"          : TaskCmds.KillTaskCmd,
        "skip"          : TaskCmds.SkipTaskCmd,
        "log"           : TaskCmds.PrintLogCmd,

        "chkeys"        : CommandCmds.ChangeKeysCmd,
        "cattr"         : CommandCmds.ChangeCommandAttributeCmd,
        
        "delist"        : BladeCmds.DelistBladeCmd,
        "eject"         : BladeCmds.EjectBladeCmd,
        "nimby"         : BladeCmds.NimbyBladeCmd,
        "unnimby"       : BladeCmds.UnnimbyBladeCmd,
        "trace"         : BladeCmds.TraceBladeCmd,

        "ping"          : AdminCmds.PingCmd,
        "dbreconnect"   : AdminCmds.DBReconnectCmd,
        "queuestats"    : AdminCmds.QueueStatsCmd,
        "reloadconfig"  : AdminCmds.ReloadConfigCmd,

        "attributes"    : HelpCmds.FieldsHelp,
        "help"          : HelpCmds.MainHelp,
        None            : HelpCmds.MainHelp,

        # "alias"         : HelpCmds.AliasHelp,
        }

    # command only viewable by rpg folk
    adminCommands = {
        }

    # command aliases
    aliases = {
        "aliases"   : "alias",
        "machs"     : "machines",
        "queue"     : "jobs -s pri,-spooled not errwait and",
        }

    # set the options that the main program will have
    options = [
        CmdLineTool.BooleanOption ("-y", "--yes",
                                   help="answer 'yes' to all questions"),
        
        CmdLineTool.BooleanOption ("-a", "--archives",
                                   help="search archive tables"),
        
        CmdLineTool.BooleanOption ("--force",
                                   help="force all operations.  This will "
                                        "allow any operation to be performed "
                                        "on a task or job, even if you are "
                                        "not the owner."),

        CmdLineTool.BooleanOption ("--nocolor", dest="color", default=True,
                                   help="do not display any output in color."),
        
        CmdLineTool.BooleanOption ("--zeros",
                                   help="some fields do not display a value "
                                        "if it is equal to zero, this will "
                                        "force zeros to be printed no matter "
                                        "what."),

        CmdLineTool.BooleanOption ("--nothreads", dest="threads", const=1,
                                   help="do not use threads, when performing "
                                        "task/job operations, threads are "
                                        "only used when --yes is applied."),

        CmdLineTool.IntOption     ("--threads", default=DEFAULT_THREADS,
                                   help="maximum number of concurrent "
                                        "threads, default=%d, setting this "
                                        "to 1 or lower is the same as "
                                        "--nothreads." % DEFAULT_THREADS),

        CmdLineTool.SecondsOption ("-p", "--pause",
                                   help="pause p seconds inbetween each "
                                        "action (e.g. retry, skip, etc.) "
                                        "This can be a floating point value."),

        CmdLineTool.SecondsOption ("-t", "--timeout", default=DEFAULT_TIMEOUT,
                                   help="timeout in seconds before canceling "
                                        "an operation, default=%d seconds." %
                                   DEFAULT_TIMEOUT),

        CmdLineTool.BooleanOption ("--login", default=False,
                                   help="explicitly log in to engine rather than use existing session id "
                                   "query"),

        CmdLineTool.BooleanOption ("--logout", default=False,
                                   help="automatically log out from engine after performing "
                                   "query"),

        CmdLineTool.BooleanOption ("--no-save-session", default=False, dest="noSaveSession",
                                   help="do not write session file"),

        CmdLineTool.StringOption ("--session-filename", default=None, dest="sessionFilename",
                                  help="set session filename"),

        DBCmdLineTool.RawDataOption(),
        
        DBCmdLineTool.TimeFormatOption(default=DEFAULT_TIMEFMT),
        
        DBCmdLineTool.FullTimeOption(),
        
        CmdLineTool.StringOption   ("--engine",
                                    help="<hostname>:<port> of engine"),
        CmdLineTool.StringOption   ("-u", "--user", help="engine username"),
        CmdLineTool.StringOption   ("--password", "--pw", help="engine user password"),
        CmdLineTool.StringOption   ("--password-file", "--pwfile", help="path to json file containing engine username and password"),
        CmdLineTool.BooleanOption  ("--traceback", help="show full traceback on exceptions (developer option)"),

        CmdLineTool.BooleanOption  ("-d", "--debug", help="display debug info"),

        CmdLineTool.VersionOption  ("-v", "--version", help="display version info"),
        
        ] + CmdLineTool.MultiCmdLineTool.options


    def __init__(self, *args, **kwds):
        self.__userPrefs = None
        super(TractorCLT, self).__init__(*args, **kwds)

    def getUserPrefs(self):
        """Return a pointer to the user prefs object."""
        if self.__userPrefs is None:
            self.__userPrefs = {} # cli.UserPrefs()
        return self.__userPrefs
    userPrefs = property(fget=getUserPrefs)

    def parseArgs(self, *args, **kwds):
        super(TractorCLT, self).parseArgs(*args, **kwds)

        self.opts.nocolor = not self.opts.color
        # set all the options as members of ourself
        for var in ("yes", "force", "color", "zeros", "threads", "pause",
                    "timeout"):
            setattr(self, var, getattr(self.opts, var))

        global ShowFullTraceback
        ShowFullTraceback = self.opts.traceback

    def getHelpStr(self):
        """Print a custom help message."""
        # get the help command
        helpcmd = self.getCommand("help")
        # run the main help
        helpcmd.run()

    def getNewCommand(self, cmd, *args, **kwds):
        """Get a L{CmdLineTool} instance that should be used for the
        provided command.

        @param cmd: name of the command being referenced.
        @type cmd: string

        @return: L{CmdLineTool} that will be used
        @rtype: L{CmdLineTool} instance
        """
        kwds["raiseArgErrors"] = True
        try:
            return self.commands[cmd](*args, **kwds)
        except KeyError:
            raise CmdLineTool.UnknownCommand(cmd)

    def getCommandToRun(self):
        try:
            return super(TractorCLT, self).getCommandToRun()
        except CmdLineTool.UnknownCommand as err:
            # check for the fields command which we will redirect to the
            # main help command
            if self.args and self.args[0] == "fields":
                command = "help"
                cmdobj = self.getCommand(command)
                args = ["fields"] + self.args[1:]

                return (command, cmdobj, args)

            # check the user prefs for command aliases
            try:
                alias = self.userPrefs.aliases.command[self.args[0]]
            except (AttributeError, KeyError):
                # check the global command alias list
                alias = self.aliases.get(self.args[0])

            if alias:
                # check for additional arguments in the alias
                import rpg.stringutil as stringutil
                aliasArgs = stringutil.quotedSplit(alias, removeQuotes=True)
                command   = aliasArgs[0]
                cmdobj    = self.getCommand(command)
                args      = aliasArgs[1:] + self.args[1:]
                return (command, cmdobj, args)

            raise CmdLineTool.UnknownCommand(err)


def main():
    rcode = 0
    try:
        tractorCLT = TractorCLT()
        rcode = tractorCLT.run()
    except (rpg.Error, rpg.OptionParser.HelpRequest, TrHttpRPC.TrHttpError, EngineClient.EngineClientError) as err:
        if ShowFullTraceback:
            print(traceback.format_exc())
        print(err, file=sys.stderr)
        rcode = 2
    except IOError as err:
        # only suppress a broken pipe error
        if errno.errorcode[err.errno] != "EPIPE":
            raise
    except KeyboardInterrupt:
        sys.stderr.write("interrupted\n")
        sys.stderr.flush()
        rcode = 2
    except:
        raise

    return rcode


if __name__ == "__main__":
    sys.exit(main())
