# ____________________________________________________________________ 
# TrCmdTracker - the management and tracking context for a given
#                running command launched by the blade
#
# ____________________________________________________________________ 
# Copyright (C) 2007-2015 Pixar Animation Studios. All rights reserved.
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

import os, sys, types, errno, socket, time
import re, fnmatch
import json
import ctypes
import urllib.request, urllib.parse, urllib.error
import logging
import tempfile
import subprocess

from .TrSubprocess import TractorPopen
from tractor.base.TrHttpRPC import trSetNoInherit


## -------------------------------------------------------------- ##


class TrCmdTracker (object):
    '''
    Track a work request through the blade
    '''

    def __init__(self, cmdDict, stateDict, runner):
        self.initTime = time.time()
        self.lastStatTime = time.time()
        self.logger = runner.logger
        self.jid = -1
        self.tid = -1
        self.cid = -1
        self.rev = 0
        self.uat = None
        self.guise = ""
        self.uid = "nobody" # see below
        self.login = "nobody"
        self.udir = "."
        self.projects = ["default"]
        self.spoolhost = "localhost"
        self.slots = 1
        self.argv = ["_NO_CMD_GIVEN_"]
        self.inmsg = None
        self.env = None
        self.svckey = ''
        self.envkey = []
        self.dirmaps = []
        self.recover = 0
        self.yieldcount = 0
        self.yieldslice = 0
        self.yieldtest = []
        self.yieldchkpt = 0  # did app exit deliberately on chkpt?
        self.outbuf = ''
        self.errbuf = ''
        self.fromHold = 0
        self.altMode = ''  # ''=regular, 'adhocNRM', 'heldNRM', 'trNRM'
        self.pid = 0
        self.process = None
        self.tref = ""
        self.srv = ""
        self.exitcode = None
        self.exitcodeOverride = None
        self.launchnote = ""
        self.runtimebounds = [0,0]
        self.maxRSS = 0.0
        self.maxVSZ = 0.0
        self.maxCPU = 0.0
        self.elapsed = 0.0  # elapsed wallclock "real" time
        self.tuser = 0.0    # rusage elapsed "user" time
        self.tsys = 0.0     # rusage elapsed "system" time
        self.stats = [0.0, -1.0, -1.0, 0] # backwd compat, real user sys rss
        self.exitReported = 0
        self.wasSwept = 0
        self.shouldDie = 0
        self.mustDie = 0
        self.escalateDelay = runner.options.escalateDelay
        self.skipSIGINT = runner.options.skipSIGINT
        self.rateLimit = runner.options.progresslimit
        self.expands = 0
        self.expandfile = None
        self.xchunkID = 0
        self.progress = ['A', self.initTime]
        self.logfile = None

        # copy the as-launched envhandler states etc for possible
        # use by SiteStatusFilters looking at this cmd 
        self.stateSnapshot = stateDict.copy()

        self.bladehost = runner.bprofile.sysinfo.hostname
        self.bladeaddr = runner.bprofile.sysinfo.addrs[0]
        self.bladeport = runner.bprofile.bport
        self.ResetProfileDetails( runner.bprofile )
        self.loggingAdvisoryProc = runner.sendLoggingAdvisory
        self.deliverExpandProc = runner.deliverExpandResults
        self.logenv = runner.options.logenv
        self.cmdtee = runner.options.cmdtee
        self.appctx = runner

        self.TrFlagCmdBaseFlags = 0x0
        self.TrFlagCmdHasLogs   = 0x1
        self.flags = self.TrFlagCmdBaseFlags

        self.progressRegExp = None
        self.digitsRegExp = re.compile('\d+')  # see parseTRef below

        self.sockDict = {}
        self.sfdDict = {}
        if 'socketMap' in cmdDict:
            # copy open sockets (objects), by name, into our sockDict
            # place file-descriptor versions of them in sfdDict
            self.sockDict.update( cmdDict['socketMap'] )
            for s in self.sockDict:
                self.sfdDict[s] = self.sockDict[s].fileno()

        if "state" in cmdDict:
            if "running" == cmdDict["state"]:
                cmdDict["exitcode"] = None

        # Now add the cmd dict items as *attributes* of this object
        # creates self.jid, self.tref, self.argv, etc
        self.__dict__.update(cmdDict)

        if -1 == self.jid and self.tref.startswith("/J0/"):
            # ad hoc (manual netrender) case, -1==jid + /J0/T0/Cxxx
            self.SetAltMode('adhocNRM')
            self.parseTRef()
        elif -1 == self.jid and self.tref.startswith("/J"):
            # netrender run from inside a tractor job, -1 + /Jnnn/Tnnn/...
            self.SetAltMode('trNRM')
            self.parseTRef()
        else:
            self.applyDirMaps(True)

        self.app = self.argv[0].split('/')[-1]

        if self.envkey and type(self.envkey) in (str, str):
            self.envkey = [self.envkey]
        if self.envkey:
            # Avoid unicode strings in environment setup, mostly to enforce
            # a simple and consistent key "domain".  (also, python on
            # Windows doesn't allow unicode in env *vars* at all)
            try:
                self.envkey = [str(x) for x in self.envkey]
            except:
                self.envkey = ["KeyDroppedDueToEncoding"]

        # Note:  an unfortunate choice of dict key in the early
        # tractor protocol used the name "uid" for the inbound 
        # "user@host" dict entry. A revised protocol kept the
        # login name in "uid" and added "spoolhost". The cmd
        # launching code here uses "uid" to be the numerical
        # platform-specific user id. So we separate out those
        # concepts here. The member variable self.uat (user at)
        # will be login@host. Note that in some rare cases it is
        # possible for the inbound login to contain '@' such as
        # when linux accounts are served from AD domains.
        # 
        if not self.uat:
            if type(self.uid) in (str, str):
                self.login = self.uid
                self.uid = 0
            else:
                self.login = "nobody"

            self.uat = self.login + '@' + self.spoolhost

        if self.slots < 1:
            self.slots = 1

        if self.jid:
            self.logref = "/J%s/T%s/C%s.%s/%s" % \
                    (self.jid, self.tid, self.cid, self.rev, self.uat)
        else:
            # an ad hoc cmd, like someone typing netrender
            self.logref = "tr-%d-%s" % (self.cid, self.uat)
            self.SetAltMode('adhocNRM')

        if self.substJobCWD:
            # blade profile overrides the usual chdir to the spooling dir
            self.udir = self.substJobCWD


    def ResetProfileDetails (self, bprofile):
        self.engineHostAndPort = bprofile.getEngineHost()
        self.logdir = bprofile.cmdLogDir
        self.tasklogger = bprofile.tasklogger
        self.cmdOutputLogging = bprofile.cmdOutputLogging
        self.stFilter = bprofile.statusFilter
        self.zone = bprofile.dirmapZone.lower()
        self.fatalTXS = bprofile.fatalTrExitStatus
        self.retainInlineLogDirectives = bprofile.retainInlineLogDirectives
        self.substJobCWD = bprofile.substJobCWD


    def LaunchCmd (self, curprofile):
        xerr = None
        subpid = None
        try:
            #
            # Apply the env handlers from the current profile to create
            # a custom set of env vars for this launch, and also to
            # optionally modify the incoming cmdline itself (cmd.argv)
            #
            self.env = curprofile.applyEnvHandlers(self)

            if TractorPopen.mswindows:
                self.resolveWindowsPath()

            # The envhandler may remap some args to None, such as when
            # it tokenizes on a delimiter that is repeated.  So we
            # replace None in argv lists with empty string, to protect
            # ourself and also to avoid whatever that might do to execv
            self.argv = ["" if a is None else a for a in self.argv]

            if self.logenv:
                e = "argv: " + \
                    json.dumps(self.argv, ensure_ascii=True, indent=2) + \
                    "\n\nEnvironment: " + json.dumps(self.env, sort_keys=True, \
                    ensure_ascii=True, indent=2) + "\n\n-----------\n\n"
                self.SaveOutput( e, time.time() )

            subpid = TractorPopen(self, curprofile.inboundBaselineEnv)

            if subpid:
                self.process = subpid
                self.pid = subpid.pid
            else:
                xerr = "TractorPopen object creation failed"

        except TractorPopen.SetuidException as e:
            xerr = e[1]

        except UnicodeEncodeError as e:
            xerr = "error handling unicode characters in command name, parameters or environment: " + e[1]

        except Exception as e:
            if e[0] in (errno.ENOENT,
                        errno.ERROR_FILE_NOT_FOUND,
                        errno.ERROR_PATH_NOT_FOUND):
                xerr = "program executable not found: " + self.argv[0]
                self.launchnote += \
                    "\n  tractor-blade service dir: " + os.getcwd() + \
                    "\n  attempted job chdir: " + self.udir + \
                    "\n  PATH=" + self.env.get("PATH", "(not_set)")

            elif e[0] in (errno.EACCES, errno.ERROR_ACCESS_DENIED):
                xerr = "access denied to program executable: " + self.argv[0]
            else:
                xerr = self.logger.Xcpt()

        return xerr


    def SendStdinMsg (self):
        xerr = None
        try:
            # write the -msg text to the running app
            self.process.stdin.write( self.inmsg )
            if "\n" != self.inmsg[-1]:
                self.process.stdin.write( "\n" )

            #
            # Tractor does not support persistent remote cmds,
            # so only one -msg will be sent to any given invocation
            # of a cmd.  So we now close the stdin to the launched
            # app, having sent the -msg text.  This is exactly
            # analogous to "echo 'some text' | some.app" where
            # echo closes the pipe (and exits) after writing the
            # msg text to the app.
            #
            self.process.stdin.close()

        except Exception as e:
            xerr = self.logger.Xcpt()

        return xerr


    def resolveWindowsPath (self):

        # make sure the path is ok for windows
        invoked = os.path.normpath( self.argv[0] )
        
        # check if the path starts with a drive or '\'
        (head, tail) = os.path.split(invoked)
        fullyQualified = os.path.splitdrive(invoked)[0] or \
            (head and head[0] == os.sep)  
        
        # return if the fully qualified path (including extension) was specified
        if fullyQualified and os.path.exists(invoked):
            self.argv[0] = invoked
            return     

        # else, search through the launch environment        
        explicit_dir = os.path.dirname(invoked)

        if explicit_dir:
            path = [ explicit_dir ]
        else:
            path = self.env.get('PATH').split(os.path.pathsep)

        extensions = self.env.get('PATHEXT','.COM;.EXE;.BAT').split(os.path.pathsep)

        for dir in path:
            fullp = os.path.join(dir, invoked)
            if os.path.exists( fullp ):
                self.argv[0] = fullp
                return
            for ext in extensions:
                fpx = fullp + ext
                if os.path.exists( fpx ):
                    self.argv[0] = fpx
                    return

        # otherwise, Not Found; invoking argv[0] it will likely fail


    def wordSplitAndRejoin (self, s):
        """
        Takes in one of the arguments from the primary split.
        This is futher split by space character.  
        Then the split array is walked through and if quotes are found
        (single or double), those elements are rejoined into a single
        array value. The resulting array is returned.
        """
        v = s.split(' ')

        args = [];
        t=""
        while len(v):
            t += v.pop(0);
            if not re.search('["\']', t):
                args.append(t)
                t=""
            else:
                t += " "
        if t != "": args.append( t.strip(" ") )
        return args


    def applyDirMaps (self, dmode):
        '''
        Apply the job's dirmap(s) to any "%D(path)" argv items,
        or just look for initial "stem" matches if dmode==False
        '''

        # Filter out dirmaps which don't apply to this platform (zone).
        # Each mapping has this form: ["from", "to", "zone"]
        # the ones that match the current zone are applied to in
        # order until a match is found.
        maps = []
        for m in self.dirmaps:
            if type(m) != list or len(m) != 3:
                self.logger.info("J%s invalid dirmap defn: %s" \
                                    % (self.jid, str(m)))
            else:
                if m[2].lower() == self.zone:
                    maps.append( m[0:2] )
        
        av = []
        for a in self.argv:
            skip = dmode
            oa=a
            pre = ""
            post = ""
            if dmode:
                d = a.find("%D(")
                n = a.rfind(')')
                if d >= 0 and n > (d+3):
                    pre = a[:d]
                    post = a[n+1:]
                    a = a[d+3:n]
                    skip = False

            if not skip:
                for m in maps:
                    if a.startswith( m[0] ):
                        a = m[1] + a[len(m[0]):]
                        break

                m = "dirmap[%s] -> [%s]" % (oa, a)
                if oa != a:  self.logger.debug(m)
                else:        self.logger.trace(m)

                a = pre + a + post

            av.append( a )

        # Apply a fix-up for the case where the new argv[0] contains
        # a space.  If so, check to see if we have a new value like 
        # "maya -batch" or "maya /batch" which probably should be split
        # into two new argv items.  The wordSplitAndRejoin function will
        # split on word boundaries, however strings with quotes in them
        # will rejoin into a single argumment
        
        a0 = av[0];
        if len(a0.split(" ")) > 1:
            exp = ' +([-/])';
            v = re.split(exp, a0)
            while len(v) > 2:
                arg = v.pop()
                sep = v.pop()
                s = self.wordSplitAndRejoin( arg )
                while len(s) > 1:
                    av.insert(1, s.pop())
                av.insert(1, sep+s.pop())
                a0 = v[0]
        av[0] = a0
                
        self.argv = av;


    ## -- -- ##
    def remapBuiltIns (self):
        #
        # expect something like "TractorBuiltin File delete foo.rib *.slo"
        #
        ops = ("Sleep", "File", "Echo", "PrintEnv", "System")
        na = len(self.argv)
        op = self.argv[1].lower() if na > 1 else "unknown"

        if op not in [x.lower() for x in ops]:
            return "unknown TractorBuiltIn command: " + self.argv[1]

        if 3 > na and op != "printenv":
            return "missing parameters for TractorBuiltIn command: "+self.argv[1]

        files = []

        if op == "sleep":
            # keep only first arg ignore extraneous test args
            # assume modern linux/osx sleep which takes float seconds
            files = [self.argv[2]]  # not really files, just words

        elif op == "echo":
            files = self.argv[2:]  # not really files, just words

        elif op == "printenv":
            files = self.argv[2:]  # not really files, just optional var names

        elif op == "system":
            files = self.argv[2:]  # not really files, just args

        else:  # "File"
            op = self.argv[2]
            if op not in ("delete", "mkdir", "copy", "list"):
                return "unknown TractorBuiltin File operation: " + op

            #
            # Expand "glob-style" wildcards in each filename, if necessary.
            # (using python rather than invoking a sub-shell.)
            # Note that glob() actually checks the "local" file system
            # for the named files, so if there are no matching files
            # you get an empty list back -- this is true even for 
            # filenames with NO wildcards in them, so:
            #    glob("no_such_file") -> []
            # which is a problem for operations like mkdir or copy
            #
            for f in self.argv[3:]:
                if any([x in f for x in "*?"]):
                    files.extend( glob.glob(f) )
                else:
                    files.append( f )

            if 0 == len(files):
                # avoid executing "/bin/rm -rf" with no file args if all
                # glob expansions resolved to no files, declare an error 
                # instead.
                return "no files matched: " + " ".join(self.argv[3:])

        #
        # We try to map built-in commands to actual executables where
        # possible, since that allows user permissions, etc, to be
        # applied in the expected / consistent ways.
        #
        if TractorPopen.mswindows:
            # normalize the path on windows files.
            for index in range(0,len(files)):
                files[index] = os.path.normpath(files[index])

            self.app = op;  # show 'copy', 'list', etc rather than "cmd.exe"

            if op == 'delete' and '/' not in self.argv:
                self.argv = ['cmd.exe', '/x', '/a', '/c', 'del', '/s', '/q']
                self.app = 'del'

            elif op == 'mkdir':
                self.argv = ['cmd.exe', '/x', '/a', '/c', 'mkdir']

            elif op == 'copy':
                self.argv = ['cmd.exe', '/x', '/a', '/c', 'copy', '/b', '/y']

            elif op == 'list':
                self.argv = ['cmd.exe', '/x', '/a', '/c', 'dir']

            elif op == 'echo':
                self.argv = ['cmd.exe', '/x', '/a', '/c', 'echo']

            elif op == 'system':
                self.argv = ['cmd.exe', '/x', '/a', '/c']

            elif op == 'sleep':
                self.argv = [sys.executable, '-c', 'import sys,time; time.sleep(float(sys.argv[1]))']

            elif op == 'printenv':
                self.argv = ['cmd.exe', '/x', '/a', '/c', 'set']

        else:
            # unix variants
            if op == 'sleep':
                self.argv = ['/bin/sleep']

            if op == 'printenv':
                self.argv = ['/usr/bin/printenv']

            if op == 'delete' and '/' not in self.argv:
                self.argv = ['/bin/rm', '-rf']

            elif op == 'mkdir':
                self.argv = ['/bin/mkdir', '-p']

            elif op == 'copy':
                self.argv = ['/bin/cp', '-pf']

            elif op == 'list':
                self.argv = ['/bin/ls', '-1']

            elif op == 'echo':
                self.argv = ['/bin/echo']

            elif op == 'system':
                self.argv = ['/bin/bash', '-c']

            self.app = self.argv[0]

        # now add the filename args to the remapped argv
        self.argv.extend(files)
        
        return None  # success, no error msg


    def SaveOutput (self, txt, now=None, xbanner=""):
        if self.jid <= 0 and not self.altMode:
            self.logger.info("missing jid for logging")
            return
        try:
            isInternal = False
            if None == now:
                # this is a "system" message, generated by tractor,
                # not emitted by the cmd itself
                now = time.time()
                isInternal = True

            tm = time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(now))

            if isInternal:
                txt = "\n"+xbanner+"[" + tm + " " + txt + "]"+xbanner+"\n"

            if self.cmdtee and xbanner=="" and \
                self.logger.getEffectiveLevel() != logging.TRACE:
                self.logger.info("cmd output:\n\n" + txt)

            if self.altMode:
                return

            if self.logdir:
                # note that all cmds within the same task are
                # logged to the same (task) file
                #
                # fnm = "%s/J%s/T%s.log" % (self.logdir, self.jid, self.tid)
                #

                prior = False
                if not self.logfile:
                    (type,_,loginfo) = self.cmdOutputLogging.partition("=")
                    fnm = loginfo.replace("%u", self.login).replace("%j", str(self.jid)).replace("%t", str(self.tid))
                    prior = os.path.exists(fnm)
                    self.logfile = open(fnm, "ab")
                    trSetNoInherit( self.logfile )

                if not self.hasEverLogged():
                    if prior:
                        self.logfile.write("\n\n") # sep txt of nxt cmds

                    self.logfile.write("====[%s %s on %s ]====\n\n" %
                                      (tm, self.logref, self.bladehost))

                    # mark this command as having generated output
                    self.indicateLogging(now)

                self.logfile.write(txt)

                # don't self.logfile.close() for now,
                # leave open while cmd is running
                # ... but if you do close it, remember:  self.logfile=None

                self.logfile.flush()  # might consider this only once/sec
                
            elif self.tasklogger:
                # log modules are being served by the remote logger

                d={"user": self.login, "jid": self.jid, "tid": self.tid,
                    "cid": self.cid, "rev": self.rev}

                if not self.hasEverLogged():
                    self.tasklogger.info("====[%s %s on %s ]====\n\n" %
                                      (tm, self.logref, self.bladehost), extra=d)
    
                    # mark this command as having generated output
                    self.indicateLogging(now)
 
                self.tasklogger.info(txt, extra=d)

            else:
                self.logger.error("no cmd output logging scheme defined!")

        except Exception:
            self.logger.error("cmd output logging failed: %s" % \
                            (self.logger.Xcpt()))


    def closeSockets(self, socks):
        try:
            if self.logfile and types.FileType == type(self.logfile):
                self.logfile.close()
            self.logfile = None
        except Exception:
            self.logger.info("subprocess log close() failure, " + \
                               self.logger.Xcpt())
        if socks:
            for s in self.sockDict:
                try:
                    self.sockDict[s].close()
                except Exception:
                    self.logger.info("subprocess socket close() failure, " + \
                                        self.logger.Xcpt())

    def jsonRepr(self, x):
        if None == x:
            return '""' # rather than null
        else:
            return json.dumps(x)

    def fmtCmdState(self, now):
        '''
        creates a json-formatted dict of cmd specs and current state
        '''
        secs = self.elapsed
        if secs == 0.0:
            secs = now - self.initTime
        
        xc = 0
        if None == self.exitcode:
            status = "running"
        else:
            xc = self.exitcode
            if self.exitReported:
                status = "done"
            else:
                status = "rcwait"

        # The items in the json dict we are creating will be used
        # for several purposes.  They are sent to UIs, who expect
        # certain element names to be present.  We are also creating
        # a json dict that can be loaded back into a restarted blade
        # and used to reconstitute a husk of a previous cmd.

        s = '{ "uat": "%s", "login": "%s", "pid": %d,\n' % \
                        (self.uat, self.login, self.pid)
        s += '     "jid": %s, "tid": %s, "cid": %s, "rev": %s,\n' % \
                        (self.jid, self.tid, self.cid, self.rev)
        s += '     "slots": %d, "flags": %u, "spoolhost": "%s",\n' % \
                        (self.slots, self.flags, self.spoolhost)
        s += '     "elapsed": %.3f, "secs": %.3f,\n' % (secs, secs)
        s += '     "tuser": %.3f, "tsys": %.3f,\n' % (self.tuser, self.tsys)
        s += '     "maxRSS": %.2f, "maxVSZ": %.2f, "maxCPU": %.2f,\n' % \
                        (self.maxRSS, self.maxVSZ, self.maxCPU)
        s += '     "id": "%s", "logref": "%s",\n     "udir": ' % \
                        (self.logref, self.logref)
        s += self.jsonRepr( self.udir )
        s += ',\n     "expands": '
        s += self.jsonRepr( self.expands )
        s += ',\n     "svckey": "%s", "envkey": ' % self.svckey
        s += self.jsonRepr( self.envkey )
        s += ',\n     "dirmaps": '
        s += self.jsonRepr( self.dirmaps )
        s += ',\n     "yieldchkpt": %d' % self.yieldchkpt
        s += ',\n     "srv": '
        s += self.jsonRepr( self.srv )
        s += ',\n     "rc": %d, "exitcode": %d, "state": "' % (xc, xc)
        s += status
        s += '",\n     "argv": '
        s += self.jsonRepr( self.argv )
        s += '\n   }'
        return s


    def checkExitStatus (self, now, indicateFirstLogOnly=False):
        '''
        Returns a tuple (code, reported, output)
        - code:      None if still running, int exitcode if exit
        - reported:  boolean, was exitcode sent successfully to engine?
        - output:    boolean, was there output on this check?
        '''
        
        hadOutput = False
        skipOutputReport = (indicateFirstLogOnly and self.hasEverLogged())

        if self.process and self.exitcode is None:

            # rekill cmd that didn't die before
            if self.shouldDie > 0 and self.escalateDelay < (time.time() - self.shouldDie):
                self.sendTermination( False )

            self.exitcode, xnote, rusage = self.process.GetExitInfo()

            if None == self.exitcode:
                # subprocess still running
                hadOutput = self.checkOutput( now )
                
                elaps = now - self.initTime
                rtmax = self.runtimebounds[1]
                if rtmax > 0 and elaps > rtmax:
                    self.exitcodeOverride = 10111 # indicate killed due to bounds
                    self.shouldDie = time.time()
                    self.sendTermination( False )
                    
                    # if process exits immediately, get the exit info
                    self.exitcode, xnote, rusage = self.process.GetExitInfo()


            if None != self.exitcode:
                # subprocess has ended
                done = True

                # our subprocess extension gives us (on unix)
                # [elapsedReal, elapsedUser, elapsedSys, maxRSS_kb]
                #
                # set "self.stats" for backward compatibility with
                # older SiteStatus filters.
                #
                if not rusage:
                    # boilerplate, covers odd failure
                    elaps = now - self.initTime
                    rusage = [elaps, elaps, 0, 1]

                self.stats = rusage   # exit [Real, User, Sys, maxRSS]

                self.elapsed = rusage[0]
                self.tuser   = rusage[1]
                self.tsys    = rusage[2]

                rss = rusage[3] / 1024.0  # got kb, want MB
                if self.maxRSS < rss:
                    self.maxRSS = rss

                hadOutput = self.checkOutput( now )  # collect any remaining

                # apply ALF/TR_EXIT_STATUS override, if any
                if self.exitcodeOverride != None:
                    self.exitcode = self.exitcodeOverride
                    if 10111 == self.exitcode:
                        xnote = "maximum run time bound " + \
                                str(self.runtimebounds[1]) + \
                                "secs exceeded: killed at %g secs" % self.elapsed
                    else:
                        xnote = "process sent non-zero EXIT_STATUS"

                if self.yieldtest:
                    self.exitcode = self.applyYieldTest( self.exitcode )

                rtmin = self.runtimebounds[0]
                if self.exitcode == 0 and self.elapsed < rtmin:
                    xnote = "minimum run time bound not reached, "
                    self.exitcode = 10110  # indicate killed due to bounds

                if self.exitcode != 0:
                    self.SaveOutput( xnote+' rc='+str(self.exitcode)+',  ' +
                                     json.dumps(self.argv, ensure_ascii=True) )

                # release subprocess assets
                del self.process
                self.process = None

                self.appctx.slotsInUse -= self.slots
                if self.appctx.slotsInUse < 0:
                    self.appctx.slotsInUse = 0

                if self.hasEverLogged():
                    self.SaveOutput("process complete, exit code: " +
                                       str(self.exitcode), None, "====")

                self.closeSockets(0)

        if self.exitcode != None and not self.exitReported:
            if self.appctx.reportProcessDone(self):
                self.exitReported = 1  # reported ok

        if skipOutputReport:
            hadOutput = False

        return (self.exitcode, self.exitReported, hadOutput)


    def formatExitCodeReport (self):
        if None == self.exitcode:
            # A restarted blade found a checkpoint for a pid that had
            # not exitted when the prior blade quit running. Currently
            # we send a non-zero exit code back to the engine and UI
            # in this case so that the user can be alerted to the fact
            # that the blade can't guarantee that the command finished
            # successfully.
            xc = -223  # made-up signal number: "lost cmd tracking"
        else:
            xc = self.exitcode

        return "rc=%d&owner=%s&jid=%s&tid=%s&cid=%s&rev=%s&&flg=%u" \
               "&swept=%d&chkpt=%d&xchunks=%d" \
               "&secs=%.3f&tuser=%.3f&tsys=%.3f" \
               "&maxrss=%.2f&maxvsz=%.2f&maxcpu=%.2f" % \
               ( xc, urllib.parse.quote(self.login),
                 self.jid, self.tid,  self.cid, self.rev,
                 self.flags, self.wasSwept, self.yieldchkpt, self.xchunkID,
                 self.elapsed, self.tuser, self.tsys,
                 self.maxRSS, self.maxVSZ, self.maxCPU )


    def checkOutput (self, now):
        # log any waiting text on the pid's stderr and stdout,
        # filter out R90000 messages to generate UI progress,
        # store trailing partial lines for later passes
        # (we could map stderr to stdout during process launch,
        #  but history shows that this can cause intermixed text
        #  which can screw up R90000 processing)
        #
        if hasattr(self, 'socket'):
            return False

        if self.process:
            e = self.handleOutput(now, "stderr")
            o = self.handleOutput(now, "stdout")
        else:
            e = False
            o = False

        return (e or o)


    def handleOutput (self, now, pipename):
        subp = self.process
        if pipename == "stderr":
            trailing = self.errbuf
            whichPipe = subp.stderr
        else:
            trailing = self.outbuf
            whichPipe = subp.stdout

        gotChunks = 0

        while gotChunks < 100:

            r = subp.readpipeNONBLK(whichPipe)

            if self.exitcode != None and \
                ( (len(trailing) != 0 and len(r)==0) or \
                  (len(r) > 0 and not r.endswith("\n")) ):
                    # process has ended, so force out dangling last line
                    r += "\n"
            if not r:
                break
            else:
                gotChunks += 1  # received some, and incr throttle test

            if self.cmdtee and self.logger.isWorthFormatting(logging.TRACE):
                self.logger.trace("subprocess(%d) %s chunk(%d)\n%s\n",
                                    self.pid, pipename, gotChunks, r);

            r = trailing + r   # append to previous partial line
            trailing = ''

            #
            # we have now received a block of output, could be many
            # lines depending on the buffering characteristics of
            # readpipe and how long it has been since the last read.
            #
            if not r.endswith('\n'):
                # have a partial line
                i = r.rfind('\n')
                if (-1 == i):
                    trailing = r  # still only have a partial line
                    continue
                else:
                    trailing = r[i+1:]  # save partial line for later
                    r = r[0:i+1]        # one or more full lines

            out = ""

            #
            # We process each line independently to accommmodate future
            # site defined regex triggers. Otherwise we should probably
            # walk the inbound buffer with re.search and emit entire
            # intermediate chunks based on "span". Since we DO want to
            # emit multiple consecutive newlines in app output to the
            # logs, we must restore a newline to each split chunk. 
            # However, we don't want a spurious extra newline due to
            # the null chunk created by trailing newline. Python
            # immutable strings means we can't truncate, so copies.
            # Note that lines may end in "\r\n".
            #
            for ln in r.rstrip().split('\n'):
                if not ln:
                    out += '\n'  # input was consecutive newlines
                    continue
                else:
                    ln += "\n"   # restore newline for logging

                alttxt = None
                retain = self.retainInlineLogDirectives  # usually False

                try:
                    rc = self.stFilter.FilterSubprocessOutputLine(self, ln)
                except:
                    rc = (self.stFilter.TR_LOG_TEXT_EMIT, ln)
                    self.logger.error("FilterSubprocessOutputLine failed: " + \
                                        self.logger.Xcpt())

                if type(rc) in (tuple, list):
                    if len(rc) > 0:
                        code = rc[0]
                        if code and (code & self.stFilter.TR_LOG_ALSO_EMIT):
                            retain = True
                            code = (code & ~self.stFilter.TR_LOG_ALSO_EMIT)
                    if len(rc) > 1:
                        value = rc[1]
                    if len(rc) > 2:
                        alttxt = rc[2]
                    
                if code == self.stFilter.TR_LOG_TEXT_EMIT:
                    ln = value      # site filter can change output line
                    retain = True

                elif code == self.stFilter.TR_LOG_PROGRESS:
                    self.sendProgress( float(value), now )

                elif code == self.stFilter.TR_LOG_EXIT_CODE or \
                     code == self.stFilter.TR_LOG_FATAL_CODE:
                    # We schedule the process to be killed if it 
                    # doesn't exit by itself soon, and then we'll
                    # report the given TR_EXIT_STATUS as the exit code.
                    self.exitcodeOverride = int(value)
                    if self.fatalTXS or code==self.stFilter.TR_LOG_FATAL_CODE:
                        self.logger.info(self.logref + \
                            " terminal TR_EXIT_STATUS="+str(value))
                        self.shouldDie = time.time() + 2

                elif code == self.stFilter.TR_LOG_EXPAND_CHUNK:
                    # expect 'value' to either be a string filename,
                    #                or a two-tuple of (FILE, ALTLOC)
                    # where:
                    #   FILE is the temp file containing the expand job script
                    #   ALTLOC is the job location where the expand should land
                    if type(value) in (str, str):
                        value = (value, None)  # convert FILE to (FILE, LOC)

                    if type(value) in (tuple, list) and 2==len(value):
                        self.sendExpandChunk( value[0], value[1] )
                    else:
                        self.logger.info(self.logref + \
                                            " improper TR_LOG_EXPAND_CHUNK")
                        value = ln
                        alttxt = None
                        retain = True

                else:
                    value = ln
                    alttxt = None
                    retain = True
                    self.logger.info(self.logref+" unknown result code from " \
                                                "FilterSubprocessOutputLine:" + \
                                                repr(rc))
                if retain and ln:
                    try:
                        out += ln
                    except:
                        self.logger.info("FilterOutputLine string append failed")

                if alttxt:
                    try:
                        out += alttxt
                    except:
                        pass

            if out:
                self.SaveOutput(out, now)

        # save trailing partial line (or set to empty)
        if pipename == "stderr":
            self.errbuf = trailing
        else:
            self.outbuf = trailing

        return (gotChunks > 0)


    def applyYieldTest (self, exitcode):
        # RemoteCmd {...} -resumewhile {exitcode 123}
        # RemoteCmd {...} -resumewhile {testcheckpoint EXR some.exr}
        if len(self.yieldtest) >= 2:
            if "exitcode" == self.yieldtest[0] and \
                int(exitcode) == int(self.yieldtest[1]):
                    # this app DID exit with the designated code, so we
                    # record that it was a checkpoint, and send that back
                    self.yieldchkpt = exitcode
                    exitcode = 0
                    self.logger.debug("cmd checkpoint-resume flagged")

            elif "testcheckpoint" == self.yieldtest[0] and \
                 "EXR" == self.yieldtest[1].upper():
                try:
                    f = open( self.yieldtest[2], "rb")
                    hdr = f.read(1024)
                    f.close()
                    if -1 != hdr.find("checkpoint"):
                        self.yieldchkpt = 1
                except:
                    self.logger.info("could not read checkpoint test file")

        return exitcode


    def sendProgress (self, p, now):
        # Encode and transmit a progress message
        # these are "throw-away" UDP messages, we do not
        # (and should not) depend on them to actually be
        # delivered and accepted by the engine, they are
        # for user interface frosting only.  Furthermore, we
        # only send a progress update if the new percent-done
        # (encoding) is different than the last one, and it is
        # at least 'rateLimit' seconds since the last update,
        # and it is not an "important" state that the engine
        # will have sent to the UIs anyway.

        lastp, t = self.progress  # previous [progress, timestamp]

        if (now - t) < self.rateLimit and 'A'!=lastp:
            return  # too soon for another update

        isIntermediate = False

        c = self.exitcode  # app exit code, or -1 if running
        if c > 0:
            c = 'E'  # Error 
        elif 0 == c:
            c = 'D'  # Done
        else:
            c = self.encodeProgress( p )
            if c is not 'A':
                isIntermediate = True

        self.progress = [c,now]  # store new progress

        if isIntermediate and c != lastp:
            self.sendUdpProgressBulletin( c )


    def encodeProgress (self, d):
        #
        # We encode percent-done as 'F' to 'Z'
        # ('A' to 'E' are active->error)
        #    A = 0%, but active
        #    F = index(1)  = 2%
        #    Z = index(21) = 98%
        #    D = 100% = done
        #    E = error
        #
        # So JavaScript can decode the progress character code with:
        #   status = "K";  (or whatever comes from us here)
        #   var fraction = 0.0;
        #   if ("A" != status)
        #       fraction = 0.048 * (status.charCodeAt(0) - 69.5);
        #
        if d <= 0.0:
            p = 'A'
        else:
            p = int( 0.5 + (d / 4.7) )
            if p > 21:
                p = 21;
            p = "AFGHIJKLMNOPQRSTUVWXYZ"[p]

        return p

 
    def sendUdpProgressBulletin (self, p):
        # send a UDP 'bulletin' to the tractor 'status antenna'

        msg = '{"ts-0.3":["%s",%s,%s,%s,"%s",%u,"%s/%s",%d]}' % \
                (self.login, self.jid, self.tid, self.cid, p, self.flags,
                 self.bladehost, self.bladeaddr, self.bladeport)

        udp = None
        try:
            self.logger.trace("sendUdp %s" % msg)
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp.sendto(msg+'\r\n', self.engineHostAndPort)

        except Exception:
            err = self.logger.Xcpt()
            hh = "%s:%d" % self.engineHostAndPort
            self.logger.error("sendUdpTaskBulletin(%s): %s" % (hh,err))

        if udp:
            udp.close()  #  XXX could we safely reuse this on all platforms?


    def parseTRef (self):
        #
        # python has no scanf, resort to a simpleminded regex that
        # does no error checking.  expect: /J987/T654/C321/yoda1a2
        #
        try:
            v = urllib.parse.unquote(self.tref)
            v, p, uat = v.rpartition('/')
            uat = uat.split('@')
            self.uid = uat[0]
            if len(uat) > 1:
                self.spoolhost = uat[1]
            v = [int(n) for n in self.digitsRegExp.findall(v)]
            self.jid, self.tid, self.cid = v[0:3]
            if 3 < len(v):
                self.rev = v[3]  # v2.0 extension
        except:
            #pass
            raise


    def sendExpandChunk (self, xfile, jlocation):
        self.xchunkID += 1
        params = "jid=%d&tid=%d&cid=%d&rev=%d&xchunk=%d&owner=%s" % \
                  (self.jid, self.tid, self.cid, self.rev,
                   self.xchunkID, urllib.parse.quote(self.login))
        if jlocation:
            params += "&xdest="+urllib.parse.quote(jlocation)

        self.deliverExpandProc( self, params, xfile )


    def SetAltMode (self, mode):
        self.altMode = mode

    def GetAltMode (self):
        return self.altMode


    def reportLaunchError (self, caller, xerr):
        self.progress[0] = 'E'
        if self.launchnote:
            xerr += "\n  note: "
            xerr += self.launchnote
        self.launchnote = xerr
        if self.jid:
            # also log this to the cmd log file, so it can be seen by UIs
            try:
                self.SaveOutput("exec failed - %s\n\tcmd: %s\n" % \
                        (xerr, json.dumps(self.argv, ensure_ascii=True)) )
            except:
                self.logger.info("failed to append 'exec failed' to cmd log")

            # send codes to the engine and UIs
            self.exitcode = 20002  # not launched: 20000 + ENOENT (no such file)
            return caller.reportProcessDone( self )

        elif 'err' in self.sockDict:
            s = "launch failed - %s\r\n" % xerr
            # Ideally we would send this error string back to the netrender
            # client app via the "error msg" socket.  However, it has already
            # been closed on exception in (TrSubprocess + subprocess.Popen)
            #  self.sockDict['err'].send(s)
            self.logger.error(s)
            self.closeSockets(1)


    def hasEverLogged (self):
        return (self.flags & self.TrFlagCmdHasLogs)


    def indicateLogging (self, now):
        priorLogs = self.hasEverLogged()
        self.flags |= self.TrFlagCmdHasLogs
        if now and not priorLogs:
            # first log
            self.loggingAdvisoryProc(self, now)


    def sendTermination (self, doChk=True):
        if self.process:

            if self.mustDie:
                msg = "kill"
                self.process.kill()

            elif self.shouldDie or self.skipSIGINT:
                msg = "terminate"
                self.process.terminate()
                self.mustDie = 1

            else:
                msg = "interrupt"
                self.process.interrupt()

            msg = "sent "+msg+" to pid=" + str( self.pid )
            self.logger.info( msg )
            self.SaveOutput( msg )

            self.shouldDie = time.time() # mark to escalate kill severity on next pass

            if doChk:
                self.checkExitStatus( time.time() )

        return self.exitReported


    def createExpandFile(self):
        '''
        determine if this is an expand node, and if so,
        generate a temp file (descriptor) to hold the results,
        otherwise return None
        '''

        #
        # Tractor supports the extension that "Cmd -expand x"
        # can specify a filename explictly that the launched
        # command is known to write to, or it can be a number
        # (0 or 1) as in classic alfred scripts.
        #
        f = None
        if type(self.expands) in (str, str):
            # filename (string) given as -expand arg
            s = self.expands.strip()
            if len(s):
                try:     f = open(s, "wb")
                except:  pass
                if f:
                    trSetNoInherit( f )
                    self.expandfile = s
 
        elif self.expands > 0:
            # "classic" usage: -expand 1
            s = "trxJ%sT%sC%s.%s." % (self.jid, self.tid, self.cid, self.rev)
            try:    
                fd = tempfile.mkstemp(prefix=s, suffix=".xalf")
                f = os.fdopen(fd[0], 'w')
                trSetNoInherit( f )
                self.expandfile = fd[1]
            except:
                pass

        return f


    def getExpandFileContents (self, chunkfile):
        # open the expand file, we expect to throw the os
        # exception to the caller if the open fails
        
        if chunkfile is None:
            xfname = self.expandfile
            self.expandfile = None
        else:
            xfname = chunkfile
    
        f = open( xfname, "rb" )
        xscript = f.read()
        f.close()

        if not chunkfile:
            pattern = '((ALF|TR)_PROGRESS.*\d+%)|((ALF|TR)_EXIT_STATUS.*\d+)'
            xscript = re.sub(pattern, '', xscript)
            xscript = xscript.strip()

        k = " /J%s/T%s/C%s.%s:\n" % (self.jid, self.tid, self.cid, self.rev)
        self.logger.trace("Expand script from" + k + xscript + "\n")

        # then clean up the tmp file
        try:
            os.remove( xfname )
        except:
            pass

        return xscript

  
## --------------------------------------------------- ##
