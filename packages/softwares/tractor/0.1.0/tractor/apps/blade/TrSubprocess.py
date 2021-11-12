# _______________________________________________________________________
# TrSubprocess - a tractor-blade module that subclasses the standard
#                python subprocess module in order to apply a few
#                workarounds for problems encountered by tractor.
#
# _______________________________________________________________________
# Copyright (C) 1986-2016 Pixar Animation Studios. All rights reserved.
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
# _______________________________________________________________________
#

import os, sys, time, ctypes, signal, subprocess
from tractor.base.TrHttpRPC import trSetNoInherit, trSetInherit



if subprocess.mswindows:
    import msvcrt, ctypes.wintypes
    import _subprocess
    trFauxSIGTERM = 11115  # an unlikely 15-ish exit code that we can detect
    if not hasattr(signal, 'SIGKILL'):
        signal.SIGKILL = 9

    win32_PeekNamedPipe = ctypes.windll.kernel32.PeekNamedPipe
    win32_PeekNamedPipe.argtypes = [
            ctypes.wintypes.HANDLE,
            ctypes.POINTER(ctypes.c_char),
            ctypes.wintypes.DWORD,
            ctypes.POINTER(ctypes.wintypes.DWORD),
            ctypes.POINTER(ctypes.wintypes.DWORD),
            ctypes.POINTER(ctypes.wintypes.DWORD)
        ]
else:
    import select
    import pwd
    import grp
    import ctypes.util
    if hasattr(os, 'initgroups'):  # new in 2.7
        trInitGroups = os.initgroups
    else:
        libc = ctypes.CDLL( ctypes.util.find_library("c") )
        libc.initgroups.argtypes = [ctypes.c_char_p, ctypes.c_uint32]
        trInitGroups = libc.initgroups


## -------------------------------------------------- ##

class TractorPopen (subprocess.Popen):

    # class member to cache values from getpwnam in case LDAP is down
    getpwnam_cache = {}

    class SetuidException(Exception): pass

    def __init__(self, cmd, baselineEnv={}):
        #
        # Some subprocess module parameters are not supported on all
        # platforms, so we we create our own member variables and apply
        # the settings selectively in _execute_child.  For example,
        # on unix we'd -like- to add the parameter 'close_fds=True'
        # to Popen. But the mere presence of the parameter raises an
        # error on windows.  So rather than having multiple os-dependent
        # constructor lines here, we just set the parameter explicitly
        # in _execute_child below.  Similarly, since the subprocess
        # module doesn't have an os-independent abstraction for login
        # impersonation, we must apply those settings in _execute_child
        # as well.
        #
        self.login = cmd.login
        self.pidIsProcessGroupLeader = False
        self._child_created = False  # prevent __del__ errors if we fail
        self.launchTime = time.time() # stand-in, updated closer to launch
        self.inboundBaselineEnv = baselineEnv

        fxpnd = cmd.createExpandFile()
        
        nrm = hasattr(cmd, 'sockDict')

        if nrm and 'stdio' in cmd.sockDict:
            sIn  = cmd.sockDict['stdio']

            if fxpnd:
                sOut = fxpnd
            elif 'err' in cmd.sockDict:
                sOut = cmd.sockDict['err']
            else:
                sOut = cmd.sockDict['stdio']

            sErr = subprocess.STDOUT
            self.closeFDs = False
            try:
                trSetInherit( cmd.sockDict['dspy'] )
                trSetInherit( cmd.sockDict['file'] )
            except:
                pass
        else:
            sIn  = subprocess.PIPE
            sErr = subprocess.PIPE
            if fxpnd:
                sOut = fxpnd
            else:
                sOut = subprocess.PIPE
            self.closeFDs = True

        cmd.launchnote = ""
        cmd.guise = ""
        cmd.uid = 0

        self.cmd = cmd

        if subprocess.mswindows:
            fullapp = cmd.argv[0]
        else:
            fullapp = None

        pid = subprocess.Popen.__init__(self,
                        cmd.argv, bufsize=1, executable=fullapp,
                        stdin=sIn, stdout=sOut, stderr=sErr,
                        env=cmd.env, cwd=None)

        # ensure stray descriptors, especially netrender sockets,
        # can't be inherited by future execs
        if fxpnd:
            try:    trSetNoInherit( fxpnd )
            except: pass
        if nrm:
            for s in cmd.sockDict:
                try:    trSetNoInherit( cmd.sockDict[s] )
                except: pass

        return pid



    def readpipeNONBLK(self, cmdpipe):
        #
        # Don't block waiting for output to appear, just grab
        # whatever is available now on the pipe.  fstat on
        # bsd unix pipes gives the available-to-read count.
        # But doesn't work on linux, or on win32 CreatePipe
        # anonymous pipes (purportedly works on win32 posix pipes).
        #
        bytes = ""
        try:
            # anything available to read right now?
            pipeFN = cmdpipe.fileno()
            nAvail = 0
            if subprocess.mswindows:
                dw = ctypes.wintypes.DWORD(0)
                ok = win32_PeekNamedPipe( msvcrt.get_osfhandle(pipeFN),
                                      None, 0, None, ctypes.byref(dw), None )
                if ok:
                    nAvail = int(dw.value)
            else:
                #unix
                # unfortunately, the bsd os.fstat trick doesn't work on linux
                #   nAvail = os.fstat(x).st_size
                # whereas the select "poll" seems to work on all *nix
                if select.select([cmdpipe], [], [], 0)[0]:
                    nAvail = 262144

            if nAvail > 0:
                bytes = os.read(pipeFN, nAvail)

        except Exception:
            pass

        return bytes


    if subprocess.mswindows:
        #
        # --------------------------------------------------------
        # NOTE: this is a modified copy of the _get_handles method
        # from the stock python subprocess module. It handles an
        # additional case where the subprocess stdio is being
        # redirected to SOCKETs rather the more typical case of
        # files (or pipes). Socket handles are not "ints" on
        # Windows and must be treated as a separate case.
        # --------------------------------------------------------
        #
        def _get_handles(self, stdin, stdout, stderr):
            """
            Construct and return tuple with IO objects:
            p2cread, p2cwrite, c2pread, c2pwrite, errread, errwrite
            """
            if stdin is None and stdout is None and stderr is None:
                return (None, None, None, None, None, None)

            p2cread, p2cwrite = None, None
            c2pread, c2pwrite = None, None
            errread, errwrite = None, None

            if stdin is None:
                p2cread = _subprocess.GetStdHandle(_subprocess.STD_INPUT_HANDLE)
                if p2cread is None:
                    p2cread, _ = _subprocess.CreatePipe(None, 0)
            elif stdin == subprocess.PIPE:
                p2cread, p2cwrite = _subprocess.CreatePipe(None, 0)
            elif getattr(stdin, "getsockopt", None):
                p2cread = stdin.fileno()
            elif isinstance(stdin, int):
                p2cread = msvcrt.get_osfhandle(stdin)
            else:
                # Assuming file-like object
                p2cread = msvcrt.get_osfhandle(stdin.fileno())
            p2cread = self._make_inheritable(p2cread)

            if stdout is None:
                c2pwrite = _subprocess.GetStdHandle(_subprocess.STD_OUTPUT_HANDLE)
                if c2pwrite is None:
                    _, c2pwrite = _subprocess.CreatePipe(None, 0)
            elif stdout == subprocess.PIPE:
                c2pread, c2pwrite = _subprocess.CreatePipe(None, 0)
            elif getattr(stdout, "getsockopt", None):
                c2pwrite = stdout.fileno()
            elif isinstance(stdout, int):
                c2pwrite = msvcrt.get_osfhandle(stdout)
            else:
                # Assuming file-like object
                c2pwrite = msvcrt.get_osfhandle(stdout.fileno())
            c2pwrite = self._make_inheritable(c2pwrite)

            if stderr is None:
                errwrite = _subprocess.GetStdHandle(_subprocess.STD_ERROR_HANDLE)
                if errwrite is None:
                    _, errwrite = _subprocess.CreatePipe(None, 0)
            elif stderr == subprocess.PIPE:
                errread, errwrite = _subprocess.CreatePipe(None, 0)
            elif stderr == subprocess.STDOUT:
                errwrite = c2pwrite
            elif isinstance(stderr, int):
                errwrite = msvcrt.get_osfhandle(stderr)
            else:
                # Assuming file-like object
                errwrite = msvcrt.get_osfhandle(stderr.fileno())
            errwrite = self._make_inheritable(errwrite)

            return (p2cread, p2cwrite,
                    c2pread, c2pwrite,
                    errread, errwrite)

    def subEnvDidNotSet (self, var):
        '''Determine whether the envhandlers overwrote the given env var
           or left it alone (set to None means we apply setuid value)'''
        sb = self.subenv.get(var)
        return (not sb) or (sb == self.inboundBaselineEnv.get(var))

    def trStrHack (self, s):
        # See usage case below, attempt to return unicode strings
        # generated by the json module to their (typically) ascii
        # single byte original forms ... unless the strings really
        # are multi-byte entities.
        try:
            # alternative might be s.encode(locale.getpreferredencoding())
            r = str( s )
        except:
            # it was really unicode, not just ascii-in-unicode
            # so pass along the inbound unicode as UTF-8, if possible
            try:
                r = s.encode('utf_8')
            except:
                r = s

        return r

    def _getpwnam(self, unm):
        """Attempt to perform pwd.getpwnam(). If it should fail, revert to cache.
        """
        try:
            entry = pwd.getpwnam(unm)
            self.getpwnam_cache[unm] = entry
        except KeyError:
            # see if entry is in cache
            entry = self.getpwnam_cache.get(unm)
            if not entry:
                raise
        return entry
        
    def _execute_child (self, args, executable, preexec_fn, close_fds,
                            cwd, env, universal_newlines,
                            startupinfo, creationflags, shell,
                            p2cread, p2cwrite,
                            c2pread, c2pwrite,
                            errread, errwrite):

        ## Override the _execute_child method in subprocess.Popen
        ## in order to selectively apply setuid modes, and also to
        ## trap exceptions so we can close any still-open stdio PIPE
        ## redirections left open by the subprocess module ... rather
        ## than the stock behavior of leaking the file descriptors
        ## when the requested app isn't launchable.  dml+jrb aug'08
        #
        # We want to set some impersonation env vars in the
        # subprocess env - IF we're impersonating!  Ideally
        # this would occur during the "pre-exec" function, but
        # we're having trouble setting our "own" live env there
        # (already in the subprocess), so we'll do it here by
        # altering the env var that we're going to pass in.
        #
        # Regarding changing to the jobcwd, especially on unix
        # when impersonation is in play, we can really only make
        # a guess here in the parent as to whether the child
        # will be able to successfully chdir after the setuid.
        # But since the child has no access to our cmd.launchnote
        # or logging context, we make a test here.  Since we don't
        # currently impersonate on Windows, the access() check
        # should be pretty accurate.
        #

        prefcn = preexec_fn  # start with default

        # Note that we inherit the windows close_fds value,
        # which may or may not be handled correctly by the
        # python invocation of win32 CreateProcess.  We need
        # our stdio pipes or sockets to be inherited by the
        # child process, but only those and no others.
        # (see also http://bugs.python.org/issue7213)

        self.subenv = env.copy()  # so we don't clobber parent template

        jobcwd = self.cmd.udir
        if jobcwd:
            ax = os.access(jobcwd, os.R_OK | os.X_OK)
            if not ax:
                self.cmd.launchnote = "(advisory only) job directory at submission time may not be accessible on this blade: " + jobcwd

            if subprocess.mswindows:
                if ax:
                    # see PWD comments in attemptChdir() for unix,
                    # win32 CreateProcess will do the chdir itself
                    if self.subenv and "PWD" in self.subenv:
                        self.subenv["PWD"] = jobcwd
                else:
                    jobcwd = None  # prevent failure in CreateProcess

        try:
            if not subprocess.mswindows:  # unix

                close_fds = self.closeFDs
                prefcn = self.trPreExec
                jobcwd = None  # handled in pre-exec
                self.pidIsProcessGroupLeader = True  # after unix trPreExec

                if 0 == os.getuid():
                    # running as root on unix, so prep for setuid
                    # must set subenv here in parent in python
                    guise = os.getenv("TRACTOR_JOB_OWNER") # like "user:group"
                    if not guise:
                        guise = self.login

                    if guise:
                        self.subenv["TRACTOR_JOB_OWNER"] = guise # for app use
                        unm,p,ugrp = guise.partition(':')

                        try:
                            p = self._getpwnam(unm)
                        except KeyError:
                            raise self.SetuidException(1, unm + \
                                ': unknown job owner userid, when ' + \
                                'setting identity for command launch')

                        uid = p[2]
                        gid = p[3]
                        home = p[5]

                        if self.subEnvDidNotSet("HOME"):
                            self.subenv["HOME"] = home

                        if self.subEnvDidNotSet("USER"):
                            self.subenv["USER"] = unm
                        if self.subEnvDidNotSet("LOGNAME"):
                            self.subenv["LOGNAME"] = unm
                        if self.subEnvDidNotSet("USERNAME"):
                            self.subenv["USERNAME"] = unm
                        if self.subEnvDidNotSet("LNAME"):
                            self.subenv["LNAME"] = unm

                        if ugrp:
                            if ugrp.isdigit():
                                gid = int(ugrp)
                            else:
                                g = grp.getgrnam(ugrp)
                                gid = g[2]

                        self.cmd.guise = unm
                        self.cmd.uid = uid
                        self.cmd.gid = gid

            self.launchTime = time.time() # better estimate of exec time

            #
            # The python json module produces only python internal unicode
            # strings when parsing any json text.  Unfortunately, perhaps
            # ironically, the python (2.x) subprocess module on Windows
            # does not use the "wide character" version of the underlying
            # CreateProcess API, hence it does not accept argv lists or
            # environment variable blocks containing unicode strings.
            # (Apparently this is addressed in python-3.2)
            # So for now, we "cast" all env and argv values to ascii
            # strings (i.e. to objects of type 'str' rather than of
            # type 'unicode')
            #
            exe = None if executable==None else self.trStrHack(executable)
            jobdir = None if jobcwd==None else self.trStrHack(jobcwd)
            env = {}
            for x in self.subenv:
                env[self.trStrHack(x)] = self.trStrHack(self.subenv[x])

            # similarly for argv values, return them to ascii if possible
            argv = [ self.trStrHack(x) for x in args ]

            subprocess.Popen._execute_child(self, argv, exe,
                            prefcn, close_fds, jobdir, env,
                            universal_newlines,
                            startupinfo, creationflags, shell,
                            p2cread, p2cwrite,
                            c2pread, c2pwrite,
                            errread, errwrite)

        except Exception as e:
            # On unix, (p2cread, c2pwrite, errwrite) are closed by
            # the subprocess module after fork; on windows they are
            # left open.  On all platforms, others are left open on
            # exceptions.  Unfortunately, the stock subprocess
            # module does not set these handle/descriptor vars
            # back to None after closing them.
            #
            for fd in (p2cwrite, c2pread, errread):
                try:
                    if fd is not None:
                        os.close( fd )
                except Exception:
                    pass

            p2cwrite = None
            c2pread = None
            errread = None

            #
            # Other pipes are not closed by the python subprocess module
            # on all platforms if the launch internals throw an exception,
            # such as during setuid on unix.  The bug is known to exist
            # at least through python 2.7.2 (sys.hexversion == 34013936).
            # The Windows handles may be closed in other types of launch
            # failure after 2.7.1, but we do the boilerplate close anyway
            # "just in case" -- which hopefully won't cause spurious
            # closures of other handles if python has been fixed and
            # actually closes the descriptors itself.
            #
            for hh in (p2cread, c2pwrite, errwrite):
                if hh is not None:
                    try:
                        if subprocess.mswindows:
                            hh.Close()
                        else:
                            os.close( hh )
                    except Exception:
                        pass

            p2cread = None
            c2pwrite = None
            errwrite = None

            # now that we've cleaned up the dangling open pipes,
            # rethrow the actual launch failure up to our caller
            raise e


    def attemptChdir (self):
        #
        # The inbound cmd.udir is the current directory
        # in which the user was sitting when they spooled their
        # job.  This may be some interesting network mounted
        # project directory that is also available on every
        # farm machine, or it might be some obscure directory
        # that only exists on the user's desktop and is
        # inaccessible from anywhere else.  We attempt to
        # chdir to that directory on the off chance that it
        # is an accessible share, but we don't treat it as
        # fatal if the chdir fails.  Due to this policy, we
        # can't use the python subprocess module's cwd=dddd
        # parameter because a chdir failure there prevents
        # the command from being spawned.
        #
        # Although we do not raise an error if the desired
        # cmd.udir is inaccessible, we should log that fact
        # more verbosely somehow.  Unfortunately we are in
        # a forked child process here without access to the
        # main blade logs.  Printing to stdout/err from here
        # *will* end up in the cmd logs, but the frequent /
        # harmless case of the cd failing is probably
        # unnecessary noise in every cmd log.
        #
        # We also reset the sh/csh variable PWD since we might
        # cd to the user's current directory from when the job
        # was spooled, and some apps (like p4) rely on PWD
        # being kept in sync, and python (not being sh/csh)
        # doesn't set/change PWD on os.chdir.  Note: we don't
        # actually call os.chdir itself in the outer/parent
        # thread since it sets it for the entire python process.
        # Instead we let the subprocess module do it for the
        # child process.
        #
        jobcwd = self.cmd.udir
        if jobcwd and '.' != jobcwd:
            try:
                os.chdir( jobcwd )
            except (OSError, IOError):
                # forge ahead without the chdir
                pass

        if self.subenv and "PWD" in self.subenv:
            # Set PWD in the env copy that the child will use,
            # since python chdir doesn't set it. This variable
            # is set by bash/sh/csh only, but some apps (like p4)
            # depend on it being correct in all exec contexts.
            # On unix we are explicitly attempting to write to
            # the same env array that will be passed to execvpe,
            # by reference here.
            self.subenv["PWD"] = os.getcwd()


    def trPreExec (self):
        # note: this routine called only on unix
        # FROM WITHIN subprocess.Popen._execute_child,
        # from within the forked child process prior to execvpe
        #
        # Create a process group for this subprocess, allows us to
        # later kill the process and *also* any subprocess it might
        # create.  ... requires us to call os.killpg(-pid)
        #
        try:
            os.setpgrp()

            if 0 == os.getuid() and 0 != self.cmd.uid:
                #
                # Apply all unix impersonation settings here.
                # We are running as root (otherwise we can't setuid)
                # so be sure to setgid *before* setuid since the new
                # userid may not have permissions to setgid.
                #
                # Also, call setgid before initgroups so the the extensions
                # to "current" properly account for the desired gid.
                # (as in http://opensource.apple.com/source/shell_cmds/
                #               shell_cmds-162/su/su.c)
                #
                os.setgid(self.cmd.gid)
                trInitGroups(self.cmd.guise, self.cmd.gid)
                os.setuid(self.cmd.uid)

            # now that we (may) have the proper user permissions,
            # attempt the jobcwd chdir
            self.attemptChdir()

        except KeyError:
            # login not a known username, where should errors go?
            errclass, excobj = sys.exc_info()[:2]
            print("%s - %s" % (errclass.__name__, str(excobj)), file=sys.stderr)
        except OSError:
            # likely errno.EPERM, no permission to setuid
            # where should errors go? we're in a forked child
            errclass, excobj = sys.exc_info()[:2]
            print("%s - %s" % (errclass.__name__, str(excobj)), file=sys.stderr)


    def send_signal(self, sig):
        #
        # Overrides for the stock (2.6) subprocess.Popen terminate and kill
        # methods.  We want to support signalling the whole process group
        # on unix and process trees on windows. We can also hope to
        # eventually add something less drastic than TerminateProcess as
        # the terminate() implementation on windows, in order to give apps
        # a chance to clean up. On Windows, all three "intr, term, kill"
        # stages are mapped to TerminateProcess.
        #
        # Aside from providing an implementation that works with 2.5
        # as well as 2.6, we also want both kill() and terminate() to
        # vector through our signal_process override on all platforms.
        #
        if subprocess.mswindows:
            trTerminateProcessTreeWin32( self._handle, self.pid )
        else:
            delivered = False
            if self.pidIsProcessGroupLeader:
                try:
                    os.killpg(self.pid, sig)
                    delivered = True
                except:
                    pass  # fall through to direct process pid

            if not delivered:
                try:
                    os.kill(self.pid, sig)
                except:
                    pass  # how to report this?

    def interrupt(self):
        self.send_signal(signal.SIGINT)

    def terminate(self):
        self.send_signal(signal.SIGTERM)

    def kill(self):
        self.send_signal(signal.SIGKILL)


    def GetExitInfo (self):
        xc = None
        treal = time.time() - self.launchTime
        tuser = treal
        tsys = 0.0
        rss = 0
        if hasattr(os, 'wait4'):
            # use wait4 if available so we can get usr/sys times & rss
            v = os.wait4(self.pid, os.WNOHANG)
            if 0 == v[0]:
                xc = None # still running
            else:
                xc = v[1] # exit code
                if os.WIFEXITED(xc):
                    xc = os.WEXITSTATUS(xc)
                elif os.WIFSIGNALED(xc):
                    xc = -os.WTERMSIG(xc)

                v = v[2] # resource.struct_rusage
                tuser = v.ru_utime
                tsys = v.ru_stime
                rss = v.ru_maxrss

                if sys.platform == 'darwin':
                    # despite the getrusage manpage claims, on OSX we get
                    # rss *bytes* here, rather than kbytes. Unclear if that
                    # is a manpage bug, python bug, wait4 bug, or kernel bug.
                    rss /= 1024
        else:
            xc = self.poll()  # fall back to subprocess module

        if xc is None:
            return (None, None, []) # process running, STILL_ACTIVE

        rusage = [treal, tuser, tsys, rss]

        if xc == 0:
            return (0, "done", rusage)

        elif subprocess.mswindows:
            # check for windows exception codes (WinNT.h)
            xc = int(xc)
            if xc < 0:
                # grr, python (2.6 and prior) treats DWORD as signed int32?
                xc = int(2**32) + xc  # two's compliment of 32-bit value

            if (0xC0000000 & xc):
                s = "exit with exception - "
                if 0xC000013A == xc:  # STATUS_CONTROL_C_EXIT
                    s += "Ctrl-C"
                elif 0xC0000005 == xc:  # STATUS_ACCESS_VIOLATION
                    s += "Access Violation"
                elif 0xC0000008 == xc:  # STATUS_INVALID_HANDLE
                    s += "Invalid Process Handle"
                elif 0xC0000017 == xc:  # STATUS_NO_MEMORY
                    s += "No Memory"
                elif 0xC00000FD == xc:  # STATUS_STACK_OVERFLOW
                    s += "Stack Overflow"
                elif 0xC000001D == xc:  # STATUS_ILLEGAL_INSTRUCTION
                    s += "Illegal Instruction"
                elif 0xC0000096 == xc:  # STATUS_PRIVILEGED_INSTRUCTION
                    s += "Privileged Instruction"
                else:
                    s += "0x%0.8X" % xc

                xc = 0xffff & xc
            elif xc == trFauxSIGTERM:
                xc = -15  # unix convention in tractor
                s = "killed by tractor with TerminateProcess"
            else:
                s = "exit with non-zero status code"

            return (xc, s, rusage)

        else: # unix
            if xc < 0:
                snm = str( -xc )
                try:
                    snm = '(' + TractorPopen.signalNameMap[snm] + ')'
                except:
                    snm = ""
                snm = "killed - signal " + snm
            else:
                snm = "exit with non-zero status code"

            return (xc, snm, rusage)

# --- #

#
# copy some subprocess module attributes to attributes of our class
# as a convenience to callers importing only our module
#
TractorPopen.mswindows = subprocess.mswindows

TractorPopen.signalNameMap = {}  # local (unix) OS signal numbers to names
for s in dir(signal):
    if s.startswith("SIG") and not s.startswith("SIG_"):
        TractorPopen.signalNameMap[ str(getattr(signal,s)) ] = s

# ------------------------------------------------------------ #

# ------------------------------------------------------------ #
# Code to find and kill a tree of subprocesses on Windows

#
# Python ctypes representation of the win32 PROCESSENTRY32 struct
#
class PROCESSENTRY32 (ctypes.Structure):
    _fields_ = [("dwSize",                  ctypes.c_ulong),
                ("cntUsage",                ctypes.c_ulong),
                ("th32ProcessID",           ctypes.c_ulong),
                ("th32DefaultHeapID",       ctypes.c_void_p),
                ("th32ModuleID",            ctypes.c_ulong),
                ("cntThreads",              ctypes.c_ulong),
                ("th32ParentProcessID",     ctypes.c_ulong),
                ("pcPriClassBase",          ctypes.c_long),
                ("dwFlags",                 ctypes.c_ulong),
                ("szExeFile",               ctypes.c_wchar * 260)
               ]


def trTerminateProcessTreeWin32 (subprocHandle, rootpid) :
    '''
    Given a windows process HANDLE, subprocHandle, kill it with
    the (nasty) win32 call TerminateProcess().  If a non-zero numeric
    process id, rootpid, is also given, then proceed to also kill all
    child processes spawned by it, and their child processes too, etc.
    We do it in this "root to leaves" fashion in order make sure that
    any parent that may be waiting for a subprocess to die does not
    proceed (for example, to launch another subprocess that our snapshot
    won't know about).
    '''

    #
    # See http://msdn2.microsoft.com/en-us/library/ms686701.aspx
    # Although some of the details need to be carefully considered
    # when implementing a Python C-types port for Vista/Win7
    #
    kernel32 = ctypes.windll.kernel32
    CreateToolhelp32Snapshot    = kernel32.CreateToolhelp32Snapshot
    Process32First              = kernel32.Process32First
    Process32Next               = kernel32.Process32Next
    OpenProcess                 = kernel32.OpenProcess
    TerminateProcess            = kernel32.TerminateProcess
    CloseHandle                 = kernel32.CloseHandle

    CreateToolhelp32Snapshot.rettype = ctypes.c_void_p;
    OpenProcess.rettype = ctypes.c_void_p;
    TerminateProcess.argtypes = [ctypes.c_void_p, ctypes.c_uint]

    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1)
    TH32CS_SNAPPROCESS = 0x00000002
    PROCESS_TERMINATE = 1

    hPid = int( subprocHandle )  # special _subprocess_handle conversion
    hPid = ctypes.c_void_p( hPid )

    if not rootpid:
        if hPid:
            TerminateProcess( hPid, trFauxSIGTERM )
        return

    #
    # Inbound 'rootpid' was specified, so take a process list snapshot
    # so that we can look for the child process tree.
    #
    kidmap = {}

    hProcessList = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)

    if INVALID_HANDLE_VALUE != hProcessList:
        pe32 = PROCESSENTRY32()
        pe32.dwSize = ctypes.sizeof(PROCESSENTRY32)

        ok = Process32First(hProcessList, ctypes.byref(pe32))

        while ok:
            pid  = int( pe32.th32ProcessID )
            ppid = int( pe32.th32ParentProcessID )

            if ppid not in kidmap:
                kidmap[ppid] = [pid]
            else:
                kidmap[ppid].append( pid )

            ok = Process32Next(hProcessList, ctypes.byref(pe32))


        # Done walking the process list snapshot
        CloseHandle(hProcessList)

    if hPid:
        # kill the root process of interest, since we already
        # have its process handle, and we have now collected
        # a list of its child processes.
        TerminateProcess( hPid, trFauxSIGTERM )

    if rootpid in kidmap:
        # find all of the pids in the process tree below rootpid
        targets = []
        unroll = kidmap[rootpid]
        while len(unroll) > 0:
            p = unroll[0]
            del unroll[0]
            targets.append( p )
            if p in kidmap:
                unroll.extend( kidmap[p] )

        # now kill all of the discovered processes
        for p in targets:
            try:
                hProcess = OpenProcess(PROCESS_TERMINATE, 0, p)
                if hProcess:
                    TerminateProcess( hProcess, trFauxSIGTERM )
                    CloseHandle(hProcess)
            except:
                errclass, excobj = sys.exc_info()[:2]
                print("exception trying to kill pid=%d  h=%08x" % (p, hProcess), file=sys.stderr)
                print("%s - %s" % (errclass.__name__, str(excobj)), file=sys.stderr)

#
# -------------------------------------------------------------------
#

