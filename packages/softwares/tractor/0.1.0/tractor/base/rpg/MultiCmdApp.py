import getopt

from rpg import stringutil

__all__ = (
        'MultiCmdAppError',
        'UnknownCommand',
        'UsageError',
        'CmdUsageError',
        'MultiCmdApp',
        )

# ----------------------------------------------------------------------------

class MultiCmdAppError(Exception):
    pass
class UnknownCommand(MultiCmdAppError):
    pass
class UsageError(MultiCmdAppError):
    pass
class CmdUsageError(UsageError):
    pass
    
# ----------------------------------------------------------------------------

class MultiCmdApp:

    def __init__(self, **args):
        self.quiet   = 0
        self.debug   = 0

        # set any additional args
        for k, v in list(args.items()):
            self.__dict__[k] = v

    def summary(self, indent=0):
        """Give a one line summary of each command that is defined.  The
        summary is taken from the __doc__ string of the do_cmd function."""

        myStr = ''
        # get all the defined commands from ListenerManager and add them
        # to the usage string.
        dofuncs = {}
        for func in list(self.__class__.__dict__.keys()):
            sfunc = func.split('_')
            if sfunc[0] == 'do':
                docstr = getattr(self, func).__doc__
                dofuncs[sfunc[1]] = docstr.strip().split('.')[0] + '.'

        keys = list(dofuncs.keys())
        formatStr = "%s%%-%ds %%s\n" % (' ' * indent,
                                        stringutil.maxStrLen(keys) + 1)
        keys.sort()
        for k in keys:
            myStr += formatStr % (k, dofuncs[k])

        return myStr

    def appArgs(self, args):
        """Parse the global application arguments.  This should return
        the remaining arguments as a list."""
        return args

    def getCommandUsage(self, command):
        """Return a command usage string that describes the usage of
        'command'."""

        usageStr = 'usage for: %s\n\nDescription:\n' % command
        dofunc   = getattr(self, 'do_' + command)
        docLines = dofunc.__doc__.strip('\n').rstrip().split('\n')
        for line in docLines:
            usageStr += '%s\n' % line

        usageStr += '\nOPTIONS:\n'
        try:
            argsfunc = getattr(self, 'args_' + command)
        except AttributeError:
            pass
        else:
            docLines = argsfunc.__doc__.strip('\n').split('\n')
            for line in docLines:
                usageStr += '%s\n' % line

        usageStr += '  -h, --help            this message'

        return usageStr

    def parseCommandArgs(self, args):
        """Parses the remaining command line arguments that are specific
        to a command.  Returns a tuple containing the command and
        command arguments."""
        
        # get the command that is to be executed
        try:
            command = args.pop(0)
        except IndexError:
            raise UsageError('no command specified')

        # see if the command is defined
        try:
            cmdfunc = getattr(self, 'do_' + command)
        except AttributeError:
            raise UnknownCommand('unknown command: ' + command)

        # check for a help request
        if '-h' in args or '--help' in args:
            raise CmdUsageError(self.getCommandUsage(command))

        # check if the command has defined a function for parsing the
        # arguments, if not assume none are there
        try:
            argsfunc = getattr(self, 'args_' + command)
        except AttributeError:
            cmdargs  = {}
        else:
            try:
                cmdargs = argsfunc(args)
            except getopt.GetoptError as err:
                raise CmdUsageError(self.getCommandUsage(command))

        return command, cmdargs

    def _handleDo(self, dofunc, args):
        """A wrapper around the do_ functions so that things are printed
        in a uniform manner and threads can be incorporated effortlessly."""

        try:
            result = dofunc(*args)
        except MultiCmdAppError as errMsg:
            result = str(errMsg)

        if result and not self.quiet:
            sresult = result.strip().split('\n')
            for line in sresult:
                if line:
                    print(line.strip())

    def initialize(self, command):
        """Initialize the App before we excecute the provided command."""
        pass

    def execute(self, args):
        """Execute the application based on the provided arguments.
        The global arguments will be parsed first, followed by any
        additional command arguments."""

        # first take care of all the global arguments
        try:
            args = self.appArgs(args)
        except getopt.GetoptError as err:
            raise UsageError(str(err))

        # now all the command arguments
        command,cmdargs = self.parseCommandArgs(args)

        # run any initialization that is needed before we execute
        # the passed in command
        self.initialize(command)

        # execute a pre_ method if defined
        try:
            prefunc = getattr(self, 'pre_' + command)
        except AttributeError:
            pass
        else:
            prefunc(cmdargs)

        # execute the do_ command
        try:
            dofunc = getattr(self, 'do_' + command)
        except AttributeError:
            raise UnknownCommand('unknown command: ' + command)
        else:
            try:
                baseformat = getattr(self, 'print_' + command)
            except AttributeError:
                baseformat = None

            self._handleDo(dofunc, (cmdargs,))

        # exectue a post_ method if defined
        try:
            postfunc = getattr(self, 'post_' + command)
        except AttributeError:
            pass
        else:
            postfunc(cmdargs)

def test():
    import sys
    
    class MyApp(MultiCmdApp):
        def args_hello(self, args):
            """This function takes no arguments."""
            return {'option': 'world'}

        def do_hello(self, args):
            """This function prints 'hello world'"""
            print('hello', args['option'])

    myapp = MyApp()
    try:
        myapp.execute(sys.argv[1:])
    except CmdUsageError as err:
        print(err)
    except UsageError:
        print(myapp.summary())
    

if __name__ == "__main__":
    test()

