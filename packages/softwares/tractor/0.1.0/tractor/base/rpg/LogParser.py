"""Base class for log parsing classes."""

import re, string
import sys, os, errno
import signal

__all__ = (
        'LogParserError',
        'LogEOF',
        'NoDataAvailable',
        'LogParser',
        )

# ----------------------------------------------------------------------------

class LogParserError(Exception):
    """Error related to a LogParser type."""
    pass
class LogEOF(LogParserError):
    """The filehandler has reached the end of file."""
    pass
class NoDataAvailable(LogParserError):
    """No data is available for reading."""
    pass

# ----------------------------------------------------------------------------

class LogParser:
    """Base class for all log parsing classes.  Use of this class should
    be done in two steps.
    
      1. Subclass this one and define the regular expressions needed
         to parse lines that are deemed important.
      2. Subclass the subclass of this one and define do_type functions
         so they are called when a line of that type is encountered.
         A do_type function is characterized by 'type' and is the
         name of the regular expression that matched the current line.

    I{Eventually this class should be extended to support a general
    delimiter type, but for now it is assumed all entries to a log are
    delimited by a newline.}

    U{Example<examples/logparser_ex.py>} of subclassing LogParser to
    create a log file type and then subclassing it to provide some
    functionality::

      # file: logparser_ex.py

      import sys, re
      import rpg.LogParser

      class SomeLogType(rpg.LogParser.LogParser):
          # Subclass from LogParser to define the log type and
          # define some regular expressions for matching lines.
          
          def __init__(self, filename=None):
              # initialize the object by calling the LogParser.__init__
              # method to set it up properly.  Then create the
              # regular expressions.
              
              LogParser.LogParser.__init__(self, filename)
              
              self.regexp['pigs'] = re.compile('bacon')
              self.regexp['cows'] = re.compile('milk')
              self.regexp['quit'] = re.compile('(quit)|(exit)')

      class MyLogParser(SomeLogType):
          # Subclassed from SomeLogType to provide functionality when
          # a line of a particular type is encountered.
          
          def __init__(self, filename=None):
              # nothing special, just initialize with
              # SomeLogType.__init__              
              SomeLogType.__init__(self, filename)

          def do_pigs(self, match):
              # called whenever the regular expression type 'pigs' is
              # found
              print 'the pigs type was matched, string: ', match.string,

          def do_cows(self, match):
              # called whenever the regular expression type 'cows' is
              # found
              print 'the cows type was matched, string: ', match.string,

          def do_quit(self, match):
              # called whenever the regular expression type 'quit' is
              # found
              print 'the quit type was matched, string: ', match.string,
              sys.exit(0)

          def default(self, unmatchedStr):
              # called when a line is not matched
              print 'unknown string: ' + unmatchedStr.strip()

      myParser = MyLogParser(sys.stdin)
      myParser.processFile()

      # end file: logparser_ex.py

      > python logparser_ex.py
      hello
      unknown line: hello
      bacon
      the pigs type was matched, string:  bacon
      milk
      the cows type was matched, string:  milk
      cows
      unknown line: cows
      quit
      the quit type was matched, string:  quit
      >

    For for examples refer to @see: AlfredLogs
    """
            
    def __init__(self, fhandler=None):
        """Initialize an object with a predefined file handler to
        use when reading strings.
        """

        # file handler of log to be read
        self.fhandler = fhandler
        # a dictionary of regular expressions used when attempting to
        # parse a string
        self.regexp = {}

        # if the file is opened via a fork we keep the pid around
        # so we can properly clean up if we need to quit the forked
        # process
        self.pid = 0

        # also if we forked then we want to check the stderr during every
        # stdout check.
        self.forkerr = None

    def matchString(self, myStr):
        """Attempts to find a match in the regexp dictionary for a
        given string.  Returns a tuple containing the type of match as
        defined in self.regexp, and the match object (defined by re).
        Returns None if no match is found."""

        for (type, reg) in list(self.regexp.items()):
            match = reg.match(myStr)
            if match:
                return type, match

        return None

    def handleMatch(self, type, match):
        """Attempts to call a do_type method for the type and returns
        the result or None if no do_type method is defined."""

        try:
            func = getattr(self, 'do_' + type)
        except AttributeError:
            self.default(match.string)
        else:
            func(match)

    def processString(self, myStr):
        """Calls matchString to try and find a valid match, if found
        handleMatch() is called.  Otherwise the method 'default' is
        called."""

        type_match = self.matchString(myStr)
        if type_match:
            self.handleMatch(type_match[0], type_match[1])
        else:
            self.default(myStr)

    def default(self, myStr):
        """Called when the string is not matched and/or does not have
        a do_type function defined."""
        pass

    def _readError(self):
        """Read the stderr from an optional forked process (if there is any).
        This only should be called if self.forkerr is defined."""

        errStr = ''
        # check until we get a would block exception.
        while 1:
            try:
                buf = self.forkerr.readline()
            except IOError as errObj:
                if errno.errorcode[errObj[0]] in ('EAGAIN', 'EWOULDBLOCK'):
                    break
                else:
                    raise LogParserError("unable to read stderr of the " \
                          "forked process:\n" + str(errObj))
            else:
                # assume that the stderr has been closed
                if not buf:
                    self.forkerr.close()
                    self.forkerr = None
                    break
                else:
                    errStr += buf

        if errStr:
            raise LogParserError("error from the forked process:\n" + \
                  errStr.strip('\n'))

    def processOnce(self):
        """Process a single string from the file.

        @raise LogEOF: raised when the end of file is reached."""

        # check for any errors
        if self.forkerr: self._readError()

        # now check the stdout
        try:
            myStr = self.fhandler.readline()
        except IOError as errObj:
            if errno.errorcode[errObj[0]] in ('EAGAIN', 'EWOULDBLOCK'):
                raise NoDataAvailable("no data is available.")
            else:
                raise LogParserError("unable to read from the logfile:\n" \
                      + str(errObj))

        if not myStr:
            # check for errors one last time
            if self.forkerr: self._readError()
            raise LogEOF("end of file reached.")

        self.processString(myStr)

    def processFile(self):
        """Process every entry in the file."""

        while 1:
            try:
                self.processOnce()
            except (LogEOF, NoDataAvailable):
                break

    def open(self, filename, tail=0, blocking=1):
        """Open a file for reading.  If 'tail' is 1 then run
        '/usr/bin/tail -f' with the file.  If blocking is set to
        1 then when processOnce is called it will wait until data
        is available for reading."""

        if tail:
            # Q: why are we forking and not using popen?
            # A: If we want to close the file descriptor before tail
            #    is through and properly clean up, then we need to
            #    send a terminate signal to the tail process.  popen
            #    should probably do this for me, but it doesn't and
            #    I want this to be done correctly.  Otherwise, popen
            #    will hang for a second or two while in the close.
            outr,outw = os.pipe()
            er,ew     = os.pipe()
            self.pid  = os.fork()
            if not self.pid:
                os.close(outr)
                os.dup2(outw, sys.stdout.fileno())
                os.close(er)
                os.dup2(ew, sys.stderr.fileno())

                # figure out what command we want to execute
                cmd  = '/usr/bin/tail'
                # the -n 0 is so we only tail the new stuff, not what has
                # already been output.
                args = (cmd, '-n', '0', '-f', filename)
                os.execv(cmd, args)
            else:
                os.close(outw)
                self.fhandler = os.fdopen(outr, 'r')
                os.close(ew)
                self.forkerr  = os.fdopen(er, 'r')
        else:
            try:
                self.fhandler = open(filename, 'r')
            except IOError as err:
                raise LogParserError("unable to open %s:\n%s" % \
                      (filename, str(err)))

        # change the mode of the file descriptor is we want non-blocking
        import fcntl
        if not blocking:
            fcntl.fcntl(self.fhandler.fileno(), fcntl.F_SETFL, os.O_NDELAY)

        # the stderr for the forked process should always be non-blocking
        if self.forkerr:
            fcntl.fcntl(self.forkerr.fileno(), fcntl.F_SETFL, os.O_NDELAY)

    def close(self):
        """Closes the log file handler."""

        if self.fhandler:
            # clean up any child processes
            if self.pid:
                os.kill(self.pid, signal.SIGKILL)
                os.waitpid(self.pid, 0)
                
            self.fhandler.close()
            self.fhandler = None

    def fileno(self):
        """The select function expects a list of file numbers or objects
        with a fileno() function.  Having this allows a LogParser object
        to be added directly to the select list."""

        if self.fhandler:
            return self.fhandler.fileno()
        return -1
    
# ----------------------------------------------------------------------------

def test():
    class SomeLogType(LogParser):
        def __init__(self, filename=None):
            LogParser.__init__(self, filename)

            self.regexp['pigs'] = re.compile('bacon')
            self.regexp['cows'] = re.compile('milk')
            self.regexp['quit'] = re.compile('(quit)|(exit)')

    class MyLogParser(SomeLogType):
        def __init__(self, filename=None):
            SomeLogType.__init__(self, filename)

        def do_pigs(self, match):
            print('the pigs type was matched, string: ', match.string, end=' ')

        def do_cows(self, match):
            print('the cows type was matched, string: ', match.string, end=' ')

        def do_quit(self, match):
            print('the quit type was matched, string: ', match.string, end=' ')
            sys.exit(0)

    myParser = MyLogParser(sys.stdin)
    myParser.processFile()

if __name__ == "__main__":
    test()
