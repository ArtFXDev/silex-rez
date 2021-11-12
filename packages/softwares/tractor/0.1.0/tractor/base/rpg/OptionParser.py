"""
Wrapper around optparse to provide an object-oriented interface for 
writing command line tools.

Introduction
============
  The amount of code needed to provide users of a command line tool with a
  positive experience can be daunting, and it is tedious to write.  Moreover,
  option parsing code is rarely reused from a module, but rather copy/pasted
  from an existing program.  This results in:

   - multiple versions of the same code
   - difficult to establish a standard style for command line tools
   - wasted time for developers
 
  This module strives to remedy this by encapsulating the most commonly 
  needed command line options into objects and defining a base class type for
  applications.  The goal is to enable developers to focus on functionality
  of the command line tool.

Option classes
==============
  Python's optparse module is useful, but it does not subclass to distinguish
  type and take advantage of polymorphism.  We get around this by subclassing
  from optparse's base Option type and feed the appropriate (static) type
  identifiers to the constructor of Option.  This allows us to create option
  types whose purpose is obvious (e.g. L{StringOption}, L{BooleanOption},
  L{IntOption}, etc.).

  For example, in order to create a boolean option using optparse to
  indicate whether a program should be in debug mode requires the
  following code::

    optparse.Option("-d", "--debug", action="store_true", help="debug mode")

  The L{BooleanOption} class encapsulates this in its constructor, so the
  user only needs::

    BooleanOption("-d", "--debug", help="debug mode")

Creating Custom Options
=======================
  At first glance the above example probably doesn't seem like a huge win,
  but having this kind of framework also simplifies the steps needed to 
  create your own custom option types.  Most of the basic option types are 
  already defined, but L{CallbackOption} can be subclassed to easily define 
  a new type.

  For example, L{ListFileOption} assumes its argument is a file containing a
  list of items delimited by a newline.  Instead of leaving it up to the user
  to read and parse the file, the option does it all for you::

    >>> class ListFileOption(CallbackOption):
    ...    # Option whose value is a file containing a list of strings, each
    ...    # of which is separated by a newline.
    ...
    ...    def store(self, option, opt, value, parser):
    ...        # Read the data from the file, make a list and add it to the
    ...        # parser.
    ...
    ...        # read the file
    ...        try:
    ...            file = open(value)
    ...            data = file.read()
    ...            file.close()
    ...        except (OSError, IOError), err:
    ...            raise optparse.OptionValueError("error reading file " \\
    ...                                            "from %s arg: %s" % \\
    ...                                            (opt, err))
    ...
    ...        # save the result
    ...        setattr(parser.values, self.dest, data.strip().split('\\n'))

  This is a very convenient way to reuse code needed to handle a specific
  option type.

"""

import sys
import os
import re
import optparse
import textwrap
import types
import shlex
import rpg
import rpg.timeutil as timeutil
import rpg.unitutil as unitutil
import rpg.osutil as osutil

from optparse import OptParseError

__all__ = [
    "OptionParserError",
    "UnknownOption",
    "HelpRequest",
    "VersionRequest",
    "ArgsNotIsolated",
    "Option",
    "CallbackOption",
    "CountOption",
    "StringOption",
    "BooleanOption",
    "RegexOption",
    "NumberOption",
    "IntOption",
    "FloatOption",
    "PathOption",
    "FileOption",
    "DirOption",
    "ListOption",
    "StrListOption",
    "IntListOption",
    "FloatListOption",
    "ListFileOption",
    "BytesOption",
    "KiloBytesOption",
    "MegaBytesOption",
    "GigaBytesOption",
    "HertzOption",
    "SecondsOption",
    "TimeOption",
    "HelpOption",
    "VersionOption",
    "DebugOption",
    "VerboseOption",
    "QuietOption",
    "YesNoOption",
    "YesOption",
    "NoOption",
    "TestOption",
    "HelpSeparator",
    "HelpFormatter",
    "OptionParser",
    ]

# ---------------------------------------------------------------------------

class OptionParserError(rpg.Error):
    """Base error type for OptionParser."""
    pass

class UnknownOption(OptParseError, OptionParserError):
    pass

class HelpRequest(OptParseError, OptionParserError):
    pass

class VersionRequest(OptParseError, OptionParserError):
    pass

class ArgsNotIsolated(OptParseError, OptionParserError):
    pass

# ---------------------------------------------------------------------------

class Option(optparse.Option, object):
    """Custom interface to the optparse Option class to enable a more object
    oriented approach, but still taking advantage of the optparse code."""

    def _set_dest(self):
        """Set a value for 'dest' if the instance wasn't initialized
        with one.  This is needed for subclasses that use a callback,
        as the base class doesn't call it for us."""

        # ****** this was copied from optparse.py ********
        if self.dest is None:
            # No destination given, and we need one for this action.
            # Glean a destination from the first long option string,
            # or from the first short option string if no long options.
            if self._long_opts:
                # eg. "--foo-bar" -> "foo_bar"
                self.dest = self._long_opts[0][2:].replace('-', '_')
            else:
                self.dest = self._short_opts[0][1]

    def formatOptString(self):
        """All options are formatted the same by default, but subclasses
        can alter this."""
        base = ", ".join(self._short_opts + self._long_opts)
        if self.takes_value():
            base += " " + self.dest[0]
        return base

    def take_action(self, action, dest, opt, value, values, parser):
        """Overloaded so -h,--help doesn't result in a call to sys.exit()."""
        if action == "help":
            raise HelpRequest(parser.format_help())
        elif action == "version":
            raise VersionRequest(parser.print_version())
        return optparse.Option.take_action(self, action, dest, opt,
                                           value, values, parser)


class CallbackOption(Option):
    """Options that will define a callback can subclass from here to
    ensure that everything is properly initialized.  The only requirement
    is that the callback method is called 'store'"""

    def __init__(self, *args, **kwargs):
        # tell the base class what our callback method is
        kwargs["type"]     = "string"
        kwargs["action"]   = "callback"
        kwargs["callback"] = self.store
        super(CallbackOption, self).__init__(*args, **kwargs)
        # make sure the dest is set
        self._set_dest()

    def store(self, option, opt, value, parser):
        """Subclasses should overload this to properly handle the option."""
        pass


class CountOption(Option):
    """Options that will count the number of times it was specified and store
    it into a variable"""

    def __init__(self, *args, **kwargs):
        # we want to increment the counter after each occurrence
        kwargs["action"]   = "count"
        kwargs.setdefault("help", "count the number of times argument is " \
                "specified")
        super(CountOption, self).__init__(*args, **kwargs)

        
class StringOption(Option):
    """By default all options are considered strings."""
    pass


class BooleanOption(Option):
    """Option whose presence toggles a boolean."""

    def __init__(self, *args, **kwargs):
        # check if we should store a const
        if kwargs.get("const") is not None:
            kwargs["action"] = "store_const"
        else:
            # if the option is present then we will negate the default value
            default = kwargs.setdefault("default", False)
            if default is True:
                kwargs["action"] = "store_false"
            else:
                kwargs["action"] = "store_true"
        super(BooleanOption, self).__init__(*args, **kwargs)


class NumberOption(Option):
    """Base class for all numbers, e.g. ints, floats, etc."""
    pass


class IntOption(NumberOption):
    """Option type for integers."""

    def __init__(self, *args, **kwargs):
        # set the type keyword, so the base class knows our type
        kwargs["type"] = "long"
        super(IntOption, self).__init__(*args, **kwargs)


class FloatOption(NumberOption):
    """Option type for floats."""

    def __init__(self, *args, **kwargs):
        # set the type keyword, so the base class knows our type
        kwargs["type"] = "float"
        super(FloatOption, self).__init__(*args, **kwargs)


class PathOption(StringOption):
    """Option type for an existing path option."""
    
    def check_value(self, opt, value):
        # we check if it's a file as well in order to catch broken links
        if os.path.exists(value) or os.path.isfile(value):
            return super(PathOption, self).check_value(opt, value)
        else:
            raise OptionParserError(
                    'option %s: invalid path value: %s' % (opt, value))


class FileOption(StringOption):
    """Option type for an existing file option."""

    def check_value(self, opt, value):
        if not os.path.isfile(value):
            raise OptionParserError(
                    'option %s: invalid file value: %s' % (opt, value))
        else:
            return super(FileOption, self).check_value(opt, value)
            

class DirOption(StringOption):
    """Option type for an existing directory option."""
    
    def check_value(self, opt, value):
        if not os.path.isdir(value):
            raise OptionParserError(
                    'option %s: invalid directory value: %s' % (opt, value))
        else:
            return super(DirOption, self).check_value(opt, value)


class YesNoOption(CallbackOption, StringOption):
    """Option type that requires y/yes or n/no.  Value is encoded as a boolean internally.
    This is useful in cases where a value of False/n/no needs to be to distinguished
    from the option not having been set at all (which is detect with a value of None)."""

    def check_value(self, opt, value):
        if type(value) is not str or value.lower() not in ['y', 'yes', 'n', 'no']:
            raise OptionParserError(
                    'option %s: invalid yes/no value: %s' % (opt, value))
        else:
            return super(YesNoOption, self).check_value(opt, value)

    def store(self, option, opt, value, parser):
        """Store yes/no as a boolean."""
        setattr(parser.values, self.dest, value.lower() in ["y", "yes"])


class RegexOption(CallbackOption):
    """Convert the argument into a regular expression."""

    def __init__(self, *args, **kwargs):
        self.flags = kwargs.pop('flags', 0)
        super(RegexOption, self).__init__(*args, **kwargs)

    def store(self, option, opt, value, parser):
        try:
            regex = re.compile(value, flags=self.flags)
        except (TypeError, re.error) as e:
            raise optparse.OptionValueError(
                    'error compiling regular expression %r: %s' % (value, e))

        setattr(parser.values, self.dest, regex)


class ListOption(CallbackOption):
    """Option whose value is a comma delimited list of values."""

    def store(self, option, opt, value, parser):
        """Split the comma delimited list into items, convert each one, and
        store the result in the parser."""
        # split into items and call the cast method
        vals = [self.cast(item) for item in value.split(',')]
        # save the result
        setattr(parser.values, self.dest, vals)

    def cast(self, val):
        """By default, the value is kept as a string."""
        return val

    def formatOptString(self):
        """All options are formatted the same by default, but subclasses
        can alter this."""
        return ", ".join(self._short_opts + self._long_opts) + " a,b,c"


class StrListOption(ListOption):
    """Option whose value is a comma delimited list of strings."""
    pass


class IntListOption(ListOption):
    """Option whose value is a comma delimited list of integers."""

    def cast(self, val):
        """Cast the value to an integer."""
        return int(val)


class FloatListOption(ListOption):
    """Option whose value is a comma delimited list of floats."""

    def cast(self, val):
        """Cast the value to an float."""
        return float(val)


class ListFileOption(CallbackOption, FileOption):
    """Option whose value is a file containing a list of strings, each
    of which is separated by a newline."""

    def store(self, option, opt, value, parser):
        """Read the data from the file, make a list and add it to the
        parser."""
        # read the file
        try:
            file = open(value)
            data = file.read()
            file.close()
        except (OSError, IOError) as err:
            raise optparse.OptionValueError("error reading file from %s "
                                            "arg: %s" % (opt, err))

        # save the result
        setattr(parser.values, self.dest, data.strip().split('\n'))


class BytesOption(CallbackOption):
    """Option whose value will be converted to total bytes.  However,
    the input can contain operators for kilobytes, megabytes, and etc."""

    scale = 1.0

    def store(self, option, opt, value, parser):
        """Convert the input value into total bytes."""
        try:
            bytes = unitutil.parseBytes(value) / self.scale
        except unitutil.UnitConversionError as e:
            # error out if we had a conversion problem
            raise optparse.OptionValueError('option %s: %s' % (option, e))

        setattr(parser.values, self.dest, bytes)


class KiloBytesOption(BytesOption):
    """Option whose value will be converted to total kilobytes. However,
    the input can contain operators for kilobytes, megabytes, and etc."""

    scale = 1024.0


class MegaBytesOption(BytesOption):
    """Option whose value will be converted to total megabytes. However,
    the input can contain operators for kilobytes, megabytes, and etc."""

    scale = 1024.0 * 1024.0


class GigaBytesOption(BytesOption):
    """Option whose value will be converted to total gigabytes. However,
    the input can contain operators for kilobytes, megabytes, and etc."""

    scale = 1024.0 * 1024.0 * 1024.0


class HertzOption(CallbackOption):
    """Option whose value will be converted to total hertz.  However,
    the input can contain operators for kilohertz, megahertz and etc."""

    def store(self, option, opt, value, parser):
        """Convert the input value into total bytes."""
        try:
            hertz = unitutil.parseHertz(value)
        except unitutil.UnitConversionError as e:
            # error out if we had a conversion problem
            raise optparse.OptionValueError('option %s: %s' % (option, e))

        setattr(parser.values, self.dest, hertz)


class SecondsOption(CallbackOption):
    """Option whose value will be converted to total seconds.  However,
    the input can contain operators for minutes, hours, days, and etc."""

    def store(self, option, opt, value, parser):
        """Convert the input value into total seconds."""
        try:
            seconds = unitutil.parseSeconds(value)
        except unitutil.UnitConversionError as e:
            # error out if we had a conversion problem
            raise optparse.OptionValueError('option %s: %s' % (option, e))
    
        setattr(parser.values, self.dest, seconds)


class TimeOption(CallbackOption):
    """Option whose value will be converted to seconds since the epoch.
    The input should be in the form excepted by timeutil.timestr2secs."""

    def store(self, option, opt, value, parser):
        """Convert the input value into seconds since epoch."""
        mytime = timeutil.timestr2secs(value)
        # check if we got a good value
        if mytime <= 0:
            raise optparse.OptionValueError("format of time string '%s' is "
                                            "not valid" % value)
        # set the value
        setattr(parser.values, self.dest, mytime)


class HelpOption(Option):
    """Option for displaying the help/usage statement of a program.  By
    default, the option is referenced via -h, --help."""

    def __init__(self, *args, **kwargs):
        # tell the base class what our callback method is
        kwargs["action"] = "help"
        if not args:
            args = ["-h", "--help"]
        kwargs.setdefault("help", "this message")
        super(HelpOption, self).__init__(*args, **kwargs)


class VersionOption(Option):
    """Explicit option for displaying the version of a program.  By default,
    the option is referenced via --version."""

    def __init__(self, *args, **kwargs):
        kwargs["action"] = "version"
        if not args:
            args = ["--version"]
        kwargs.setdefault("help", "show the current version of this program")
        super(VersionOption, self).__init__(*args, **kwargs)


class DebugOption(BooleanOption):
    """Explicit option for adding a -d,--debug option."""

    def __init__(self, *args, **kwargs):
        if not args:
            args = ["-d", "--debug"]
        kwargs.setdefault("help", "print debug messages")
        super(DebugOption, self).__init__(*args, **kwargs)


class TestOption(BooleanOption):
    """Explicit option for adding --test option."""

    def __init__(self, *args, **kwargs):
        if not args:
            args = ['--test']

        kwargs.setdefault('help', "skip performing any permanent operations")
        super(TestOption, self).__init__(*args, **kwargs)


class VerboseOption(CountOption):
    """Explicit option for setting the desired level of verbosity.  By
    default, the option is referenced via -v,--verbose and each occurrence
    of the option increments a counter."""

    def __init__(self, *args, **kwargs):
        if not args:
            args = ["-v", "--verbose"]
        kwargs.setdefault("help", "adjust the level of verbosity")
        super(VerboseOption, self).__init__(*args, **kwargs)


class QuietOption(BooleanOption):
    """Explicit option for turning all unnecessary output off.  By default,
    the option is referenced via -q,--quiet."""

    def __init__(self, *args, **kwargs):
        if not args:
            args = ["-q", "--quiet"]
        kwargs.setdefault("help", "suppress all output, except for errors")
        super(QuietOption, self).__init__(*args, **kwargs)


class YesOption(BooleanOption):
    """Explicit option for assuming yes for each question asked."""

    def __init__(self, *args, **kwargs):
        if not args:
            args = ['-y', '--yes']

        kwargs.setdefault('help', "answer 'yes' to all questions")
        super(YesOption, self).__init__(*args, **kwargs)


class NoOption(BooleanOption):
    """Explicit option for assuming no for each question asked."""

    def __init__(self, *args, **kwargs):
        if not args:
            args = ['-n', '--no']

        kwargs.setdefault('help', "answer 'no' to all questions")
        super(NoOption, self).__init__(*args, **kwargs)
    
# ---------------------------------------------------------------------------

class HelpSeparator(Option):
    CHECK_METHODS = []

    def __init__(self):
        super(HelpSeparator, self).__init__([])

    def _check_opt_strings(self, opts):
        return []

    def formatOptString(self):
        return ''


class HelpFormatter(optparse.IndentedHelpFormatter):
    """Subclassed from the default option formatter so we can format the
    options the way we like them."""

    def format_option_strings (self, option):
        """Return a comma-separated list of option strings & 
        metavariables."""
        # call the options format method if this is our type
        if issubclass(option.__class__, Option):
            return option.formatOptString()
        # otherwise call the default
        return optparse.IndentedHelpFormatter.format_option_strings(self,
                                                                    option)

# ---------------------------------------------------------------------------

class OptionParser(optparse.OptionParser):
    """Subclassed so we can raise an exception when an error occurs,
    instead of calling sys.exit().  Also implements a couple other options
    for customizing option/argument syntax."""

    styles = ("interspersed", "isolated", "getopt")

    def __init__(self, formatter=None, optionStyle="interspersed",
                 add_help_option=False, usage=optparse.SUPPRESS_USAGE,
                 numericArgs=True, **kwargs):
        # specify a different formatter
        if not formatter:
            formatter = HelpFormatter()
        #self.requireOptSpaceArg = requireOptSpaceArg
        self.optionStyle = optionStyle
        # allow dashed arguments to be integers
        self.numericArgs = numericArgs
        optparse.OptionParser.__init__(self, formatter=formatter,
                                       add_help_option=add_help_option,
                                       usage=usage, **kwargs)

    def _get_args(self, args):
        """split args if it is a string, then call parent class's 
        _get_args"""
        if isinstance(args, (str,)):
            args = shlex.split(args)

        return optparse.OptionParser._get_args(self, args)

    def _process_args (self, largs, rargs, values):
        """Process command-line arguments and populate 'values', consuming
        options and arguments from 'rargs'.  If 'allow_interspersed_args' is
        false, stop at the first non-option argument.  If true, accumulate 
        any interspersed non-option arguments in 'largs'.
        """
        # keep track of whether arguments are isolated (i.e. have options
        # on either side).
        isolated = False
        while rargs:
            arg = rargs[0]
            if arg[0:2] == "--" and len(arg) > 2:
                # if we have already accumulated some args and 
                # process a single long option (possibly with value(s))
                self._process_long_opt(rargs, values)
                if largs: isolated = True
            elif arg[:1] == "-" and len(arg) > 1 and \
                 (self.numericArgs or not arg[1].isdigit()):
                # process a cluster of short options (possibly with
                # value(s) for the last one only)
                self._process_short_opts(rargs, values)
                if largs: isolated = True
            elif self.optionStyle == "isolated" and isolated:
                raise ArgsNotIsolated("arguments cannot be interspersed "
                                      "with dashed options.")
            elif self.optionStyle in ("interspersed", "isolated"):
                largs.append(arg)
                del rargs[0]
            # this is the getopt style
            else:
                return                  # stop now, leave this arg in rargs


    def error(self, msg):
        raise OptParseError(msg)
