#
# TrLogger - logging management for tractor-blade,
#            a factory and wrapper class for the built-in logging module
#
#
# ____________________________________________________________________ 
# Copyright (C) 2007-2014 Pixar Animation Studios. All rights reserved.
#
# The information in this file is provided for the exclusive use of the
# software licensees of Pixar.  It is UNPUBLISHED PROPRIETARY SOURCE CODE
# of Pixar Animation Studios; the contents of this file may not be disclosed
# to third parties, copied or duplicated in any form, in whole or in part,
# without the prior written permission of Pixar Animation Studios.
# Use of copyright notice is precautionary and does not imply publication.
#
# PIXAR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING
# ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT
# SHALL PIXAR BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES
# OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
# WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION,
# ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.
# ____________________________________________________________________ 
#

import logging
import logging.handlers
import logging.config
import traceback
import os.path
import sys
import types
import errno
import time
import pprint

# first, add a new log level to logging module
logging.TRACE=5
logging.addLevelName(5, 'TRACE')


class TrLoggerClass(logging.Logger):

    def __init__(self, logger_name):
        logging.Logger.__init__(self, logger_name)
        self.trFmt = None
        self.trHnd = None
        self.trFilename = None


    def trBasicConfig(self, opts):
        tfmt = '%m/%d %H:%M:%S'
        if logging.TRACE == opts.loglevel:
            # add milliseconds when --trace is requested
            # (python logging module docs claim that's the default but we don't see it)
            mfmt = '%(asctime)s.%(msecs)-0.3d %(levelname)-7s %(message)s'
        else:
            mfmt = '%(asctime)s %(levelname)-7s %(message)s'
        self.trFmt = logging.Formatter(mfmt, tfmt)

        emsg = None
        if opts.logfile:
            try:
                mb = opts.logrotateMax
            except Exception:
                mb = 25.0
            mb *= (1024 * 1024) # inbound in megabytes, convert to bytes

            try:
                self.trHnd = \
                    logging.handlers.RotatingFileHandler(opts.logfile, 'a',
                                        maxBytes=int(mb), backupCount=5)

                self.trFilename = opts.logfile

            except Exception:
                emsg = self.Xcpt()

        if not self.trHnd:
            self.trHnd = logging.StreamHandler()

        self.setLevel( opts.loglevel )
        self.trHnd.setFormatter( self.trFmt )
        self.addHandler( self.trHnd )

        if emsg:
            self.error( emsg + " -- continuing with stderr")


    def Close(self):
        for h in self.handlers:
            try:
                h.close()
            except Exception:
                pass

    def trace(self, msg, *args, **kwargs):
        '''implement our "logging.trace()" extension'''
        self.log(logging.TRACE, msg, *args, **kwargs)


    def updateAccessTime (self):
        # We occasionally "access" the open logfile so that things
        # that read stat.st_atime (like tmpwatch) will know that
        # the logfile is in use.  Sadly periodic *writing* to
        # the file only changes m_time, and tmpwatch only reads
        # atime by default, and doesn't check fuser by default.
        if self.trFilename:
            try:
                f = open(self.trFilename, "r")
                c = f.read(1)
                f.close()
            except:
                pass


    def isWorthFormatting (self, lvl):
        # no "getLevel" but there is getEffectiveLevel, and we usually
        # want it to decide whether to do expensive formatting
        return (lvl >= self.getEffectiveLevel())


    def printDict(self, d, dname=None):
        """debug utility to log dictionary contents"""
        if self.isWorthFormatting(logging.TRACE):
            if dname: 
                hdr = "Printing %s dictionary:\n" % dname
            else:
                hdr = "\n"
            self.trace( hdr + pprint.pformat(d) + "\n" )


    def Xcpt(self):
        if self.isWorthFormatting(logging.DEBUG):
            return traceback.format_exc()
        else:
            errclass, excobj = sys.exc_info()[:2]
            return errclass.__name__ + " - " + str(excobj)

# ------- #
#
# TrLogger is just a wrapper function that generates an instance
# of the TrLoggerClass, a factory in other words.  This could
# probably be done with a __metaclass__, but the logging module's
# own factory function (getLogger) throws a bit of a twist into
# the process.  We need it to do its magic registration of the
# the given logger into the (truly) global table of known loggers.
# We do use setLoggerClass to cause the logger to be a subclass
# of TrLoggerClass, but we can't quite do all the configuration
# in our __init__
#

def TrLogger(logger_name, options):
    '''
    Factory for the actual logging-derived logger
    '''
    try:
        trEmbraceAndExtendErrno()

        old = logging.getLoggerClass()  # save old setting
        logging.setLoggerClass(TrLoggerClass)

        #
        # Provide a logging configuration backdoor for sites that want
        # to do something sophisticated.
        #
        if options.logconfig:
            if os.path.exists(options.logconfig):
                logging.config.fileConfig(options.logconfig)
            else:
                options.logconfig = None

        logger = logging.getLogger( logger_name )

        logging.setLoggerClass(old)  # restore
        
        if not options.logconfig:
            # In the typical case that there is no logging config file,
            # apply our usual handlers.
            logger.trBasicConfig( options )

    except Exception:
        logger = TrDesperationLogger( logger_name )
        logger.exception( "logging configuration failed" )

    return logger

# ------- #

# copy logging levels as *attributes* of our factory function,
# for the convenience of the importing caller
#
TrLogger.CRITICAL = logging.CRITICAL
TrLogger.ERROR    = logging.ERROR
TrLogger.WARNING  = logging.WARNING
TrLogger.INFO     = logging.INFO
TrLogger.DEBUG    = logging.DEBUG
TrLogger.TRACE    = logging.TRACE

# ------- #

class TrDesperationLogger (object):
    ''' fallback logger in case config of stock logging module fails '''
    def __init__(self, logger_name):
        pass
    def Close (self):
        pass
    def log (self, lvl, msg, *args, **kwargs):
        t = time.strftime('%m/%d %H:%M:%S', time.localtime())
        print(t, lvl, msg % args) 
    def critical(self, msg, *args, **kwargs):
        self.log('CRITICAL', msg, *args)
    def error(self, msg, *args, **kwargs):
        self.log('ERROR   ', msg, *args)
    def warning(self, msg, *args, **kwargs):
        self.log('WARNING ', msg, *args)
    def info(self, msg, *args, **kwargs):
        self.log('INFO    ', msg, *args)
    def debug(self, msg, *args, **kwargs):
        self.log('DEBUG   ', msg, *args)
    def trace(self, msg, *args, **kwargs):
        self.log('TRACE   ', msg, *args)
    def exception(self, msg):
        return msg + self.Xcpt()
    def Xcpt(self):
        errclass, excobj = sys.exc_info()[:2]
        return "%s - %s" % (errclass.__name__, str(excobj))


# ------- #

def trEmbraceAndExtendErrno ():
    # Add some Windows error codes to errno to simplify coding elsewhere.
    # Apparently the Windows "system" numbers are not in python's errno
    # module, nor any stock module.  The winsock codes are (mostly) added
    # as errno attributes when running python on Windows, but any code
    # references cause errors on other platforms. So for now, we favor
    # exception handler code simplicity by just hacking the values that
    # we will reference directly onto errno so that they are defined
    # on all platforms.
    #
    if not hasattr(errno, "ERROR_FILE_NOT_FOUND"):
        errno.ERROR_FILE_NOT_FOUND = 2
    if not hasattr(errno, "ERROR_PATH_NOT_FOUND"):
        errno.ERROR_PATH_NOT_FOUND = 3
    if not hasattr(errno, "ERROR_ACCESS_DENIED"):
        errno.ERROR_ACCESS_DENIED = 5
    if not hasattr(errno, "ERROR_ALREADY_EXISTS"):
        errno.ERROR_ALREADY_EXISTS = 183
    if not hasattr(errno, "WSAEACCES"):
        errno.WSAEACCES = 10013
    if not hasattr(errno, "WSAEADDRINUSE"):
        errno.WSAEADDRINUSE = 10048
    if not hasattr(errno, "WSAECONNRESET"):
        errno.WSAECONNRESET = 10054
    if not hasattr(errno, "WSAECONNREFUSED"):
        errno.WSAECONNREFUSED = 10061



