import time, string, getpass, socket
import smtplib

import rpg

__all__ = (
        'Error',
        'ErrorManager',
        )

# ----------------------------------------------------------------------------

class Error:
    """An error is initialized with a string describing the error."""

    def __init__(self, errMsg):
        """Initialize an Error object with an error string and set the
        time that it occured."""
        
        self.errMsg  = errMsg
        self.occured = [time.time()]
        esplit = errMsg.strip().split('\n')
        self.subject = esplit.pop()[:70] + '...'

    def __eq__(self, other):
        """Checks if two Error objects are equal by comparing the errMsg
        strings."""
        
        if self.errMsg == other.errMsg:
            return 1
        return 0

    def incrRepeat(self):
        """Adds the current time to the list of times that this error
        occured at."""
        
        self.occured.append(time.time())
        
    def getMailStr(self):
        """Returns a tuple containing (subject, message) for this error."""
        
        numErrs = len(self.occured)
        msg = 'At %s, the following error occured:\n\n' % \
              time.ctime(self.occured[numErrs - 1])

        msg += self.errMsg + '\n'

        if numErrs > 1:
            msg += 'The same error repeated %d times\n' % numErrs
            for i in range(numErrs):
                msg += '%3d: %s\n' % (i + 1, time.ctime(self.occured[i]))
            msg += '\n'

        return (self.subject, msg)

# ----------------------------------------------------------------------------

class ErrorManager:
    """Manages all errors and checks for repeating errors.  Also provides
    an option for emailing errors to a list of addresses."""
    
    def __init__(self, **args):
        """Initialize an ErrorManager object with a dictionary of
        arguments."""
        
        # header at top of each mail message
        self.header     = None
        # set to 1 if errors are to be mailed
        self.mailErrors = 0
        # list of addresses to send errors to
        self.mailto     = []
        # username of the one sending
        self.mailfrom   = getpass.getuser()
        # repeating messages are only mailed every 'repeatSecs' seconds
        self.repeatSecs = 600
        # prepend to each subject string
        self.subjectPre = ''

        # override any default value of the above with one passed in
        for (k, v) in list(args.items()):
            self.__dict__[k] = v

        # list of Error objects that are being managed
        self.errors     = []

    def addError(self, errMsg):
        """Add an error to the list.  If one already exists then increment
        its count."""

        for err in self.errors:
            # an error already exists
            if err.errMsg == errMsg:
                err.incrRepeat()
                # check how old it is
                self.checkAge(err)
                break
        else:
            # create a new Error
            err = Error(errMsg)
            # add it to the list
            self.errors.append(err)
            # mail it if option is set
            if self.mailErrors:
                self.mailError(err)

    def mailError(self, err):
        """Mails and error message defined in an Error object to the list
        of recipients defined in self.mailto."""

        # if no addresses are specified then don't do anything
        if not self.mailto:
            return

        # get the subject and message strings
        (sub, msg) = err.getMailStr()
        # prepend a header if one is set
        if self.header:
            msg = self.header + msg
        # mail the message
        try:
            rpg.sendmail(self.mailto, msg,
                                  fromaddr=self.mailfrom,
                                  subject=(self.subjectPre + sub))
        except (smtplib.SMTPException, socket.error) as errMsg:
            rpg.log("unable to send mail.")
            rpg.log(str(errMsg))

    def checkAge(self, err):
        """Checks the age of errors incase they have been repeating.
        Repeating errors are only sent every 'self.repeatSecs' seconds
        and then removed from the errors list."""

        # get the current time
        now = time.time()
        # get the time that the first error occured at
        firstErr = err.occured[0]
        # check if it is old enough
        if now - self.repeatSecs > firstErr:
            # mail it if option is set
            if self.mailErrors:
                self.mailError(err)
            # remove the error from the list
            self.errors.remove(err)
