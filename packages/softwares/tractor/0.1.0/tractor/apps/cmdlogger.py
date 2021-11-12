#!/usr/bin/env python
#
TrFileRevisionDate = "$DateTime: 2015/07/10 08:28:13 $"

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

import pickle
import logging
import logging.handlers
import socketserver
import struct
import socket
import os
import optparse
import sys
import time
import errno

filetemplate = None

class TractorLogStreamHandler(socketserver.StreamRequestHandler):
    """Handler for a streaming logging request.

    This basically logs the record using whatever logging policy is
    configured locally.
    """
    
    def setup(self):
        socketserver.StreamRequestHandler.setup(self)
        self.currentfile = None
        self.stream = None
        self.mode = 'a'
        
    def handle(self):
        """
        Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        
        Note: this handler bypasses the normal logging calls and
        writes files directly for Tractor cmd logs
        """
        
        logger = logging.getLogger("tractor-engine")
        logger.debug("TractorLogStreamHandler.handle")
        while 1:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            slen = struct.unpack(">L", chunk)[0]
            pkstr = ""
            while slen > 0:
                chunk = self.connection.recv(slen)
                if "" == chunk:
                    break  # socket closed by peer before end of transfer
                else:
                    pkstr += chunk
                    slen -= len(chunk)

            obj = self.unPickle(pkstr)
            record = logging.makeLogRecord(obj)
            logger.log(logging.TRACE, "TractorLogStreamHandler record %s" % repr(record))

            if "jid" in record.__dict__ and "tid" in record.__dict__ and "user" in record.__dict__ :
                jid = str(record.__dict__["jid"])
                tid = str(record.__dict__["tid"])
                user = str(record.__dict__["user"])
                fname = filetemplate.replace("%u", user).replace("%j", jid).replace("%t", tid)
                logger.debug("tractor record received: %s" % fname);
                fdir = os.path.dirname(fname)
                try:
                    oldumask = os.umask(0)
                    os.makedirs(fdir)
                    os.umask(oldumask)
                except Exception as e:
                    # it is acceptable for the directory to already exist
                    if e[0] not in \
                        (errno.EEXIST, errno.ERROR_ALREADY_EXISTS):
                        sys.stderr.write("Error creating log directory: " % fdir)
                        logger.error("Error creating log directory: " % fdir)
                        errclass, excobj = sys.exc_info()[:2]
                        sys.stderr.write("%s - %s\n" % (errclass.__name__, str(excobj)))
                        logger.error("%s - %s\n" % (errclass.__name__, str(excobj)))
                
                if fname != self.currentfile:
                    self.currentfile = fname
                    if (self.stream): self.stream.close()
                    self.stream = open(fname, self.mode)
                self.stream.write(record.msg)
                self.stream.flush()
                os.fsync(self.stream)
            else:
                if logger:
                    logger.warning("tractor record did not contain user dictionary")
                    logger.handle(record)
            

    def unPickle(self, data):
        return pickle.loads(data)

class LogRecordSocketReceiver(socketserver.ThreadingTCPServer):
    """simple TCP socket-based logging receiver suitable for testing.
    """

    allow_reuse_address = 1

    def __init__(self, host='0.0.0.0',
                 port=9180,
                 handler=TractorLogStreamHandler):
        socketserver.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort = 0
        self.timeout = 1
        self.logname = None

    def serve_until_stopped(self):
        import select
        abort = 0
        while not abort:
            rd, wr, ex = select.select([self.socket.fileno()],
                                       [], [],
                                       self.timeout)
            if rd:
                self.handle_request()
            abort = self.abort

def validateRootDir(opts):
    if opts.filetemplate == None: return
    
    logroot = opts.filetemplate[:opts.filetemplate.find("%")]
    logroot = os.path.dirname(logroot)
    
    if not os.path.exists(logroot):
        try:
            os.makedirs(logroot)
        except:
            sys.stderr.write("Error creating tractor logroot: ")
            errclass, excobj = sys.exc_info()[:2]
            sys.stderr.write("%s - %s\n" % (errclass.__name__, str(excobj)))
            (err, s)=excobj
            exit(err)
    
        
    # make sure the log root dir writable
    try:
        t = time.time()
        testdir = "%s/%s" % (logroot, str(t))
        os.mkdir(testdir)
    except:
        sys.stderr.write("Tractor logdir is not writable: %s\n" % logroot)
        errclass, excobj = sys.exc_info()[:2]
        (err,s)=excobj
        exit(err)
        
    os.rmdir(testdir)
    
    
def main():

    global filetemplate
    
    # first, add a new log level to logging module
    logging.TRACE=5
    logging.addLevelName(logging.TRACE, 'TRACE')

    # portability fix for windows
    if not hasattr(errno, "ERROR_ALREADY_EXISTS"):
        errno.ERROR_ALREADY_EXISTS = 183

    optparser = optparse.OptionParser()
    optparser.add_option("--filetemplate", dest="filetemplate",
            type="string", default="/var/spool/tractor/cmd-logs/%u/J%j/T%t.log",
            help="Template describing log file location")

    hostname = '0.0.0.0'
    optparser.add_option("--host", dest="hostname",
            type="string", default=hostname,
            help="Logger hostname [%default]")

    optparser.add_option("--port", dest="port",
            type="int", default=9180,
            help="Default logserver port")
            
    optparser.set_defaults(loglevel=logging.WARNING)
    group = optparse.OptionGroup(optparser, "Logging Options",
            "Defines logging level and logfile. [default: WARNING]")
    group.add_option("-v", "--verbose",
            action="store_const", const=logging.INFO,  dest="loglevel",
            help="log level Info and above.")
    group.add_option("--debug",
            action="store_const", const=logging.DEBUG, dest="loglevel",
            help="log level Debug and above.")
    group.add_option("--trace",
            action="store_const", const=logging.TRACE, dest="loglevel",
            help="log level Trace and above.")
    group.add_option("--warning",
            action="store_const", const=logging.WARNING, dest="loglevel",
            help="log level Warning and above.")
    group.add_option("-q", "--quiet",
            action="store_const", const=logging.CRITICAL, dest="loglevel",
            help="log level Critical only")

    group.add_option("--logfile", dest="logfile",
            type="string", default=None,
            help="Local logfile for debugging. [default %default]")

    optparser.add_option_group(group)

    (options,args) = optparser.parse_args()
    validateRootDir(options)
    filetemplate = options.filetemplate    

    if (options.logfile):
        logger = logging.getLogger("tractor-engine")
        logger.setLevel(options.loglevel)
        ch = logging.FileHandler(options.logfile)
        ch.setLevel(options.loglevel)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.info("Starting TCP server on host %s (%d) ..." % (options.hostname, options.port))


    tcpserver = LogRecordSocketReceiver(host=options.hostname, port=options.port)
    tcpserver.serve_until_stopped()

if __name__ == "__main__":
    main()
