"""
CmdLineTool objects
===================
  The L{Option} classes defined in L{rpg.OptionParser} could be used with a 
  standard optparse parser, but they are intended to be used with the 
  L{CmdLineTool} class.  When writing a command line tool program, you can 
  subclass from L{CmdLineTool} and define a list of options your program 
  should have.  Then you immediately will have a working program that parses 
  options and prints a B{usage} statement.

  Below is an example of a program that searches for core files and 
  optionally deletes them::

    >>> class FindCores(CmdLineTool):
    ...    usage = "findcores [OPTIONS] [host1 host2 host3 ...]"
    ...
    ...    description = \"\"\"
    ...    Scan machines for core files and optionally delete them.
    ...    When deleting, only cores older than 1 day are removed.  By
    ...    default, only a sum of all the cores is reported.  If no cores are
    ...    found then nothing is returned.
    ...    \"\"\"
    ...    
    ...    options = [
    ...        StringOption("-d", "--dir",
    ...                     help="the base directory to start the search "
    ...                          "from, the default is /usr/anim/local"),
    ...        ListFileOption("-f", "--file",
    ...                       help="filename containing hosts to search"),
    ...        BooleanOption("--list", dest="listcores",
    ...                      help="list the cores individually"),
    ...        SecondsOption("--minage",
    ...                      help="the minimum age that a core file can "
    ...                           "have rbefore it is deleted.  t given in "
    ...                           "seconds, but appending m, h, d, or w "
    ...                           "treats the value as minutes, hours, "
    ...                           "days, weeks respectively."),
    ...        BooleanOption("--remove",
    ...                      help="actually remove core files found"),
    ...        SecondsOption("--timeout",
    ...                      help="timeout used when searching for cores, "
    ...                           "default is 1200 seconds")
    ...        ] + CmdLineTool.options
    ...
    ...    def execute(self, *args, **kwargs):
    ...        print "find some cores"

  Now that we've defined a class for our program, we can execute it with:

    >>> fc = FindCores()
    >>> fc.run()
    find some cores

  The L{CmdLineTool} constructor will use the arguments found in sys.argv
  by default.  Subclasses should overload the C{execute} method and call the
  C{run} method, which is a wrapper around L{execute}.

  If a C{-h} or {--help} option is found, then L{CmdLineTool} subclasses
  will automatically have a usage statement printed.  The usage for
  C{FindCores} would be::
    
    usage: findcores [OPTIONS] [host1 host2 host3 ...]

    description:
      Scan machines for core files and optionally delete them.  When 
      deleting, only cores older than 1 day are removed.  By default, only 
      a sum of all the cores is reported.  If no cores are found then 
      nothing is returned.

    options:
      -d, --dir d    the base directory to start the search from, the default
                     is /usr/anim/local
      -f, --file f   filename containing hosts to search
      --list         list the cores individually
      --minage m     the minimum age that a core file can have before it is
                     deleted.  t given in seconds, but appending m, h, d, or
                     w treats the value as minutes, hours, days, weeks
                     respectively.
      --remove       actually remove core files found
      --timeout t    timeout used when searching for cores, default is 1200
                     seconds
      -q, --quiet    suppress all output, except for errors
      -v, --verbose  adjust the level of verbosity
      -h, --help     this message

"""

import sys
import os
import re
import optparse
import rpg
import rpg.osutil as osutil

import rpg.OptionParser
from rpg.OptionParser import *

__all__ = [
    "CmdLineToolError",
    "CmdLineTool",
    "BasicCmdLineTool",
    "DaemonCmdLineTool",
    "NoCommandFound",
    "UnknownCommand",
    "MultiCmdLineTool",
    "HelpCommand",
    ] + rpg.OptionParser.__all__

def encodeArgs(args):
    """Encode args such that negative numerical args start with _"""
    if not args:
        return args
    return [
        "_" + arg if arg[0] == "-" and arg[1].isdigit() else arg for arg in args
        ]

def decodeArgs(args):
    """Decode args that start with _ and are negative numbers afterward."""
    if not args:
        return args
    return [
        arg[1:] if arg[:2] == "_-" and arg[2].isdigit() else arg for arg in args
        ]

# ---------------------------------------------------------------------------

class CmdLineToolError(rpg.Error):
    """Base error type for CmdLineTool."""
    pass

class NoCommandFound(CmdLineToolError):
    """Raised when a multi-command line tool does not find its command."""
    pass

class UnknownCommand(CmdLineToolError):
    """Raised when an invalid command is referenced."""
    def __init__(self, command):
        self.command = command

    def __str__(self):
        return 'unknown command: ' + str(self.command)

# ---------------------------------------------------------------------------

class CmdLineTool(object):
    """Base class for command line programs that provides an easy-to-use
    interface for parsing command line options/arguments.  Python's optparse
    module is used to parse the options/arguments and to format a usage
    statement.

    @cvar: a one line string to describe the usage of the tool, for example,
      [OPTIONS] file1 [file2 ...]

    @cvar description: a detailed summary describing what this tool does
      and possibly some examples.

    @cvar examples: a string containing examples for this command.  The
      formatting in this string will not be altered and will be added to
      the final description as-is.

    @cvar version: version of this tool

    @cvar options: a list of the L{Option} instances that should be used
      when parsing the command line arguments.

    @cvar optionStyle: the option parsing style that will be used for this
      command.  Can be one of::
        - interspersed: options and arguments can exist anywhere
        - isolated:     options can only surround one or more arguments
        - getopt:       all options must come before arguments and parsing
                        stops when the first non-option is encountered.

    @ivar opts: the object returned from optparse after it has parsed the
      dashed options.
    @ivar args: list of non-dashed arguments

    """

    # a string indicating the name of the program, should it be difficult to determine automatically.
    progname = None

    # a generalized string to describe the usage of this tool.
    usage = None

    # a brief description of what this tool does.
    description = None

    # a brief set of examples on how to use this tool.
    examples = None

    # the current version of this tool
    version = None

    # provide a list of options (via Option objects) that this command
    # will recognize.  By default, a help option is supported
    options = [
        HelpOption()
        ]

    # the option parsing style that will be used for this command.  Can be
    # one of:
    #  - interspersed: options and arguments can exist anywhere
    #  - isolated:     options can only surround one or more arguments
    #  - getopt:       all options must come before arguments and parsing
    #                  stops when the first non-option is encountered.
    optionStyle = "interspersed"

    def __init__(self,
                 parent=None,
                 raiseArgErrors=False,
                 **kwargs
            ):
        """
        @param raiseArgErrors: If True, Raise errors when parsing the arguments
        @type raiseArgsErrors: bool
        
        @param parent: parent MultiCmdLineTool (defaults to None)
        @type parent: L{MultiCmdLineTool}
        
        """

        # when the arguments are parsed they will be saved to separate vars
        # all options can be referenced through 'options' as class members
        # of the object
        self.opts = None
        # a list of the non-option arguments
        self.args = None

        self.parent = parent
        self.raiseArgErrors = raiseArgErrors

        super(CmdLineTool, self).__init__(**kwargs)


    def getParser(self, options=None, optionStyle=None, **kwargs):
        if options is None:
            options = self.options

        if optionStyle is None:
            optionStyle = self.optionStyle
            
        return OptionParser(option_list=options, 
                            optionStyle=optionStyle, 
                            **kwargs)
       

    def help(self, file=None, exit=2):
        """Write the help statement to stdout that should be displayed to
        the user when -h,--help is encountered.

        @param file: file handler that the usage should be written to, by
          default it is written to sys.stdout.
        @type file: file

        @param exit: by default, sys.exit() will be called as a convenience
          for applications.  The value of exit will be used passed to
          sys.exit() as the programs return value.  If you do not wish for
          the program to exit, then set this to None or False.
        @type exit: int
        """

        # get the string
        mystr = self.getHelpStr()

        # write the string to a file
        if file is None:
            file = sys.stdout
        file.write(mystr)

        # abort
        if not (exit is None or exit is False):
            sys.exit(exit)


    def getHelpStr(self):
        """Get the help statement that would be printed if -h,--help was
        encountered.

        @return: the help string
        @rtype: string
        """

        # first get the usage string and put some newline on the end
        mystr = self.getUsage() + '\n\n'

        # get the description if it exists
        descrip = self.getDescription()
        if descrip:
            mystr += descrip + '\n\n'

        # get the examples if it exists
        examples = self.getExamples()
        if examples:
            mystr += examples + '\n\n'

        # now format the options
        mystr += self.getOptionsHelp()

        return mystr
        

    def getUsage(self):
        """Get the usage of this command.

        If the string C{%prog} appears in the usage text, it is replaced with
        the basename of the executing script.
        """

        #print self.usage, self, self.__class__

        mystr = "usage: "
        scriptname = self.progname or os.path.basename(sys.argv[0])
        if self.usage:
            mystr += self.usage.replace("%prog", scriptname)
        else:
            mystr += scriptname

        return mystr


    def getDescription(self):
        """Get the detailed description of this command."""

        mystr = ''
        if self.description:
            mystr = "description:\n"
            import textwrap
            tw = textwrap.TextWrapper(initial_indent="  ",
                                      subsequent_indent="  ",
                                      width=78,
                                      replace_whitespace=True,
                                      fix_sentence_endings=True)
  
            description = self.description.strip()

            # clean up the spaces surrounding newlines
            description = re.sub(r'[^\n\S]*\n[^\n\S]*', '\n', description)

            # replace the non-newline whitespace and a newline not surrounded 
            # by newlines with a space
            description = re.sub(r'[^\n\S]+|((?<!\n)\n(?!\n))', ' ', description)

            # in order to preserve the remaining newlines, we need to 
            # apply the textwrap per-line
            lines = [tw.fill(line) for line in description.split('\n')]
            mystr += '\n'.join(lines)

        return mystr


    def getExamples(self):
        """Return the string that should be use as the examples section
        in the help usage statement."""
        
        # we do something slightly differently from the description as the
        # examples may need specific whitespace requirements
        mystr = ''
        if self.examples:
            mystr = 'examples:\n'
            import textwrap
            examples = textwrap.dedent(self.examples).strip().split('\n')
            examples = ['  ' + s for s in examples]
            mystr += '\n'.join(examples)
                       
        return mystr

    def getOptionsHelp(self):
        """Get a formatted listing of the available command line options."""
        if self.options:
            # create a new parser so we can format the help
            return self.getParser().format_help()
        return ''

    def displayVersion(self, file=None, exit=2):
        """Display the current version of this command line tool."""
        # write the string to a file
        if file is None:
            file = sys.stdout
        file.write(self.getVersionStr())
        # abort
        if not (exit is None or exit is False):
            sys.exit(exit)

    def getVersionStr(self):
        """Get the version string statement that would be printed if -v,--version were encountered.

        @return: the version string
        @rtype: string
        """
        return (self.version or "version not set") + "\n"
    
    def parseArgs(self, args=None, defaults=None):
        parser = self.getParser()

        # parse the input
        # markup numerical args that start with - to avoid annoying the parser
        encodedArgs = encodeArgs(args)
        try:
            self.opts, self.args = parser.parse_args(encodedArgs, values=defaults)
            self.args = decodeArgs(self.args)
        except HelpRequest:
            if self.raiseArgErrors:
                raise HelpRequest(self.getHelpStr())

            self.help()
        except VersionRequest:
            if self.raiseArgErrors: 
                raise
            
            self.displayVersion()
        except optparse.OptParseError as err:
            # change the error type
            raise UnknownOption(str(err))


    def pre_execute(self, *args, **kwargs):
        """Called before the L{execute} method is called to give
        subclasses the option of setting up variables before doing the
        real work."""
        pass


    def execute(self, *args, **kwargs):
        """The real work of the command should get done in this method.

        @return: exit status that the program should return.  Returning
          None is equivalent to 0.
        @rtype: int
        """
        return None


    def post_execute(self, *args, **kwargs):
        """Called after the L{execute} method has run, even if an exception
        is raised."""
        pass


    def run(self, args=None, defaults=None, **kwargs):
        """This is intended to be called from the main function of a
        program.  Clients can call it and pass the return code off
        to sys.exit, i.e. sys.exit(mycmd.run()).

        @return: exit status that the program should return
        @rtype: int
        """
        # parse args before running
        self.parseArgs(args=args, defaults=defaults)

        # run any setup
        rcode = self.pre_execute(**kwargs)
        if rcode:
            return rcode

        # wrap everything in a try, finally so we can call the post_execute
        try:
            rcode = self.execute(**kwargs)
        finally:
            self.post_execute(**kwargs)

        return rcode

# ---------------------------------------------------------------------------

class BasicCmdLineTool(CmdLineTool):
    """Intended for writing basic scripts that will have options for
    -q/--quiet and -v/--verbose."""

    options = [
        QuietOption(),
        VerboseOption(),
        DebugOption(),

        ] + CmdLineTool.options


    def __init__(self, lock=False, **kwargs):
        """
        @param lock: acquire a process lock before proceeding.  This is
          useful if only one instance of the program can run at once.
        @type lock: boolean
        """
        self.lock     = lock
        self.lockFile = None
        self.lockName = None
        super(BasicCmdLineTool, self).__init__(**kwargs)


    def getProcessLock(self, filename=None):
        """Get a lock for this process so another one doesn't start
        up on top of us.  This does not close the file handler, use
        setProcessLock() to finish the lock."""

        progName = os.path.basename(sys.argv[0])

        if filename:
            self.lockName = filename
        else:
            self.lockName = "/tmp/%s.lock" % progName

        # check if one is already running
        try:
            self.lockFile = osutil.getProcessLock(self.lockName,
                                                  command=progName)
        except rpg.Error as err:
            raise CmdLineToolError(err)


    def setProcessLock(self):
        """Set the process lock by adding our process id to the file.
        This way we don't need to keep a lock on the file and can close it."""

        try:
            osutil.setProcessLock(self.lockFile)
        except rpg.Error as err:
            raise CmdLineToolError(err)


    def pre_execute(self):
        # get a process lock
        if self.lock:
            self.getProcessLock()
            self.setProcessLock()


    def post_execute(self):
        if self.lockFile and os.path.exists(self.lockName):
            os.remove(self.lockName)
            self.lockFile = None
            self.lockName = None


# ---------------------------------------------------------------------------

class DaemonCmdLineTool(BasicCmdLineTool):
    """Extends the basic command line tool to create a daemon."""

    logfile = "/tmp/%s.log" % os.path.basename(sys.argv[0])

    options = [
        BooleanOption ("--nf", "--nofork", dest="nofork",
                       help="don't fork (daemonize) the process"),

        StringOption  ("-l", "--log",
                       help="path of file where output will be "
                       "logged to if the process is daemonized."),

        ] + BasicCmdLineTool.options


    def setProcessLock(self):
        """Before setting the lock with our pid, daemonize."""
        # fork off
        if not self.opts.nofork:
            if self.opts.log:
                log = self.opts.log
            else:
                log = self.logfile
            import rpg.pathutil as pathutil
            pathutil.makedirs(os.path.dirname(log))
            osutil.stdoutToFile(log)
            osutil.daemonize()

        # finish the locking now
        super(DaemonCmdLineTool, self).setProcessLock()


# ---------------------------------------------------------------------------

class MultiCmdLineTool(CmdLineTool):
    """Base class for command line tool programs that implement one or
    more sub-commands.

    Here is an example of a basic multi-command tool::

        >>> class FooCommand(BasicCmdLineTool):
        ...     description = "prints 'foo'"
        ...     def execute(self, *args, **kwargs):
        ...         print "foo"
        ...
        ... class BarCommand(CmdLinTool):
        ...     description = "prints 'bar'"
        ...     def execute(self, *args, **kwargs):
        ...         print "bar"
        ...
        ... class Frobnicator(MultiCmdLineTool):
        ...     usage = "%prog COMMAND [OPTIONS]"
        ...
        ...     description = "Invokes meaningless commands."
        ...
        ...     commands = {
        ...             'foo' : FooCommand,
        ...             'bar' : BarCommand,
        ...             'help': HelpCommand,
        ...             None  : HelpCommand,
        ...             }

    The C{None} entry ensures that the help is displayed when the top-level
    command is run with no arguments. Note that the description of each
    sub-command when shown in help is generated from their C{description}
    strings.

    @cvar commands: mapping of command names to L{CmdLineTool} objects
      that can be used. 
    """

    commands = {}

    # the option style needs to be 'getopt' so that we stop processing the
    # arguments when we reach the first non-dash argument.  We assume
    # it is the command name to be executed.
    optionStyle = "getopt"

    def __init__(self, *args, **kwargs):
        self.cachedCommands = {}
        super(MultiCmdLineTool, self).__init__(*args, **kwargs)


    def getNewCommand(self, cmd, *args, **kwargs):
        """Get a L{CmdLineTool} instance that should be used for the
        provided command.

        @param cmd: name of the command being referenced.
        @type cmd: string

        @return: L{CmdLineTool} that will be used
        @rtype: L{CmdLineTool} instance
        """
        try:
            return self.commands[cmd](*args, **kwargs)
        except KeyError:
            raise UnknownCommand(cmd)


    def getCommand(self, cmd):
        """Get a L{CmdLineTool} instance that should be used for the 
        provided command.
        
        @param cmd: name of the command being referenced.
        @type cmd: string

        @return: L{CmdLineTool} that will be used
        @rtype: L{CmdLineTool} instance
        """

        try:
            return self.cachedCommands[cmd]
        except KeyError:
            obj = self.cachedCommands[cmd] = self.getNewCommand(cmd, 
                    parent=self, 
                    raiseArgErrors=self.raiseArgErrors)

            return obj


    def getCommandToRun(self):
        # assume the first argument is the command we are looking for
        try:
            command = self.args[0]
        except IndexError:
            command = None
        
        cmdobj = self.getCommand(command)
        return command, cmdobj, self.args[1:]


    def execute(self, **kwargs):
        """Overloaded from the base so we can create the necessary
        CmdLineTool object and call the run() method for it.

        @return: exit status that the program should return
        @rtype: int
        """
        command, cmdobj, args = self.getCommandToRun()
        self.command = command

        return cmdobj.run(args, **kwargs)

# ---------------------------------------------------------------------------

class HelpCommand(CmdLineTool):
    """
    Simple CmdLineTool to print out all the possible command from a 
    MultiCmdLineTool parent object
    """

    usage = 'help [commands]'

    description = "print help information on the commands"
       
    def execute(self):
        if self.args:
            # print out the help for the specified command
            for name in self.args:
                command = self.parent.getCommand(name)
                sys.stdout.write(command.getHelpStr())
                sys.stdout.flush()
        else:
            # print out the parent task's description
            descrip = self.parent.getDescription()
            if descrip:
                print(descrip)
                print()

            # print out a list of all the possible commands
            lines = []
            maxname = 0
            for command, class_ in self.parent.commands.items():
                if command is None:
                    continue

                maxname = max(maxname, len(command))
                try:
                    description = class_.short_description
                except AttributeError:
                    description = class_.description or ''

                description = description.strip()

                if len(description) > 70:
                    description = description[:67] + '...'
                
                lines.append((command, description))

            lines.sort()

            print('subcommands:')
            for name, description in lines:
                print('  %s    %s' % (name.ljust(maxname), description))
