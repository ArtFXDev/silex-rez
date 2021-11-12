import os
import sys
import platform
import io
import time
import select
import subprocess
import re
import errno
import getpass
import socket
import signal
import types

if sys.platform != 'win32':
    import pwd

from rpg import OSUtilError
import rpg.socketlib
import rpg.unitutil as unitutil
import rpg.timeutil as timeutil

__all__ = (
        'getlocalos',
        'getlocalhost',
        'getlocalip',
        'getusername',
        'stdoutToFile',
        'stripDomain',
        'getProcesses',
        'getProcessTree',
        'runCommand',
        'processAlive',
        'daemonize',
        'getProcessLock',
        'setProcessLock',
        'parallelProcess',
        'dirsize',
        'ownerForPath',
        'PopenTimeout',
        'Popen',
        'PIPE',
        'STDOUT',
        )

# ----------------------------------------------------------------------------

_localos = None
def getlocalos():
    global _localos
    if _localos is None:
        _localos = platform.platform().split('-')[0]
    return _localos

_localhost = None
def getlocalhost():
    global _localhost
    if _localhost is None:
        _localhost = platform.node().split('.')[0]
    return _localhost

_localip = None
def getlocalip():
    global _localip
    if _localip is None:
        _localip = socket.gethostbyname(getlocalhost())
    return _localip

_username = None
def getusername():
    global _username
    if _username is None:
        try:
            _username = pwd.getpwuid(os.geteuid()).pw_name
        except:
            try:
                _username = getpass.getuser()
            except:
                _username = ''
    return _username

# ----------------------------------------------------------------------------

def stdoutToFile(filename, rotateNum=4, append=False):
    """Opens the file named 'filename' and dups the file description to
    be equivalent to stdout and stderr.  Then returns new fds that are
    equivalent to the old stdout and stderr fds."""

    # rotate logs if we aren't appending
    if not append:
        # check if there is already a log file and append .0 to the previous
        # log to keep it from being clobbered.  Note this will clobber a
        # log already with .0
        r = list(range(rotateNum))
        r.reverse()
        for i in r:
            try:
                stat_buf = os.stat(filename + '.%d' % i)
            except OSError:            
                pass
            else:
                os.rename(filename + '.%d' % i, filename + '.%d' % (i + 1))

        try:
            stat_buf = os.stat(filename)
        except OSError:
            pass
        else:
            os.rename(filename, filename + '.0')

    # get rid of stdin because we don't need it
    fi = open('/dev/null', 'r')
    os.dup2(fi.fileno(), sys.stdin.fileno())
    
    # Redirect stdout and stderr
    if append:
        fo = open(filename, 'a')
    else:
        fo = open(filename, 'w')
    os.dup2(fo.fileno(), sys.stdout.fileno())
    os.dup2(fo.fileno(), sys.stderr.fileno())


# backwards compatibility fix
stripDomain = rpg.socketlib.stripDomain


# regexp to parse the /proc/PID/stat file and properly catch executable
# names with spaces.
_statre = re.compile("^\S+ \((?P<command>[^\)]+)\) (?P<state>\S+) "
                     "(?P<ppid>\S+) (?P<pgid>\S+) (?P<session>\S+)"
                     "(?:(?: \S+){7}) (?P<utime>\S+) (?P<stime>\S+)"
                     "(?:(?: \S+){6}) (?P<starttime>\S+) "
                     "(?P<size>\S+) (?P<rss>\S+)(?:(?: \S+)+)")
def _getProcessesLinux(user=None, fullcmd=True, boottime=None):
    """Read /proc and get info on all the processes."""

    # compute the boottime of the machine so we can later compute process
    # start times
    if not boottime:
        # get how many seconds the machine has been up
        try:
            upsecs = float(open("/proc/uptime").read().split()[0])
        except (OSError, IOError) as err:
            raise OSUtilError("unable to get uptime: %s" % err)

        # convert this to an absolute boot time
        boottime = time.time() - upsecs

    # read the file in /proc
    try:
        files = os.listdir("/proc")
    except (OSError, IOError) as err:
        raise OSUtilError("unable to read /proc: %s" % err)

    # save all the procs keyed by pid
    procs = {}
    for pid in files:
        # ignore the files in /proc that aren't pids
        try:
            pid = int(pid)
        except ValueError:
            continue

        # grab the owner of the process now, but ignore it if it's disappeared
        try:
            stat = os.stat("/proc/%d" % pid)
        except (OSError, IOError):
            continue

        # some user ids don't return from the password db
        try:
            owner = pwd.getpwuid(stat[4]).pw_name
        except KeyError:
            owner = stat[4]

        # are we checking for specific users?
        if user and owner != user:
            continue

        # save everything in a dictionary with a timestamp
        pinfo = {"pid"   : pid,
                 "user"  : owner,
                 "cpids" : [],
                 "pstime": time.time()}

        # read the goods
        try:
            stat = open("/proc/%d/stat" % pid).read()
        except (OSError, IOError):
            continue

        # read the full command string if desired
        if fullcmd:
            try:
                file = open("/proc/%d/cmdline" % pid)
                # replace all nulls for spaces
                pinfo["fullcmd"] = file.read().replace('\0', ' ').strip()
                file.close()
            except (OSError, IOError) as err:
                continue

        # parse the output
        match = _statre.match(stat)
        if not match:
            raise OSUtilError("unable to parse /proc/%d/stat: %s" %
                              (pid, stat))

        # get the match dict
        mdict = match.groupdict()
        pinfo["command"] = mdict["command"]
        pinfo["state"]   = mdict["state"]
        # cast some to ints
        for key in ("ppid", "pgid", "session"):
            pinfo[key] = int(mdict[key])

        # setup the ppids list
        pinfo["ppids"] = [pinfo["ppid"]]

        # combine user time and system time and convert to seconds
        pinfo["usersecs"] = (float(mdict["utime"]) + \
                             float(mdict["stime"])) / 100.0

        # convert size to kbytes from bytes
        pinfo["size"] = int(int(mdict["size"]) / 1024.0 + 0.5)
        # convert rss to kbytes from pages, same as (*4096/1024)
        pinfo["rss"]  = int(mdict["rss"]) * 4

        # convert starttime to an absolute time
        starttime = float(mdict["starttime"]) / 100.0 + boottime
        pinfo["starttime"] = starttime
        # now compute CPU%
        realsecs = pinfo["pstime"] - starttime
        if realsecs > 0:
            # we only want 1 decimal point, so do some hackery to keep
            # python from giving us more precision than we care about.
            cputime = float("%5.1f" %
                            (pinfo["usersecs"] / realsecs * 100))
        else:
            cputime = 0.0
        pinfo["totalcpu"] = cputime


        # add this process to the full list
        procs[pid] = pinfo


    # now iterate through each pid to build a full list of ppids and cpids
    for pid,vals in list(procs.items()):
        ppids = vals["ppids"]
        cpid  = pid
        ppid  = vals["ppid"]
        # loop until we don't find the pid in our dictionary
        while True:
            # for each ppid get the data structure set the cpid and grab
            # the next ppid
            try:
                pvals = procs[ppid]
            except KeyError:
                break
            else:
                if cpid:
                    pvals["cpids"].append(cpid)
                # set to None so we don't keep adding children
                cpid = None
                ppid = pvals["ppid"]
                # don't add the zero pid
                if ppid:
                    ppids.append(ppid)


    return procs


def getProcesses(onhost=None, timeout=0, user=None,
                 fullcmd=True):
    """Get a list of the processes currently running on the local machine
    or if onhost is set run remotely. If the user variable is set then only
    processes owned by that user will be returned.  The return value is a
    dictionary keyed by pid and the value is another dictionary with keys::

      pid        pid of the process
      starttime  starttime of the process
      user       the user of the process
      command    the command that was executed from the command line
      fullcmd    the full command of the process with arguments
      ppid       parent process id
      pgid       process group id
      ppids      list of all the parent pids, beginning of the list is the
                    first parent, last pid should always be 1 for the root
      cpids      list of all the immediate child pids
      totalcpu   percent of cpu the process has consumed
      size       full memory size of the process (in kilobytes)
      rss        resident set size of the process (in kilobytes)
      state      state of the process:
                    D   uninterruptible sleep (usually IO)
                    R   runnable (on run queue)
                    S   sleeping
                    T   traced or stopped
                    Z   a defunct ('zombie') process

                  additional letters may be added for a further description
                    W   has no resident pages
                    <   high-priority process
                    N   low-priority task
                    L   has pages locked into memory (for real-time and
                         custom IO)

    """

    # create the ps command to run
    pscmd  = "/bin/ps -o "
    pscmd += "pid,ppid,pgid,ucomm,stat,vsize,rss," \
             "pcpu,cputime,lstart,uid"
    # grab the full command string too
    if fullcmd:
        pscmd += ",command -ww"

    # grap only the procs for a specific user
    if user:
        pscmd += " -U " + user
    else:
        pscmd += " -A"

    rcode,out,err = runCommand(pscmd, onhost=onhost, timeout=timeout)
    if rcode or err:
        raise OSUtilError("ps command had errors: " + err.strip())

    # all procs will be keyed by pid
    procs = {}
    for line in out.strip().split('\n')[1:]:
        fields = line.split()
        try:
            pid = int(fields[0])

            # macs truncate the username for some reason, so just query
            # the user database
            try:
                user = pwd.getpwuid(int(fields[14])).pw_name
            except KeyError:
                user = fields[14]

            pinfo = {
                "pid"      : pid,
                "ppid"     : int(fields[1]),
                "ppids"    : [int(fields[1])],
                "cpids"    : [],
                "pgid"     : int(fields[2]),
                "session"  : 0, # FIXME: we need to remove this the db
                "user"     : user,
                "command"  : fields[3],
                "state"    : fields[4],
                "size"     : int(fields[5]),
                "rss"      : int(fields[6]),
                "totalcpu" : float(fields[7]),
                "fullcmd"  : ' '.join(fields[15:]),
                }

            # convert the cputime field into seconds
            # strip off any day args
            try:
                days,args = fields[8].split('-')
            except ValueError:
                days,args = "0",fields[8]

            args = args.split(':')
            args.reverse()

            # macs don't always print out time like 00:00:00, sometimes
            # its like 00:00.00
            kwargs = dict(s=0, m=0, h=0)
            kwargs.update(dict(list(zip(
                ('s', 'm', 'h'),
                [float(i) for i in args],
            ))))
            pinfo["usersecs"] = timeutil.hms2sec(**kwargs) + int(days)*86400

            # handle the starttime, this is a hack to make the
            # time correct with dst
            tlist   = list(time.strptime(' '.join(fields[9:14])))
            tlist[-1] = -1
            pinfo["starttime"] = int(time.mktime(tlist))

            procs[pid] = pinfo

        except (ValueError, IndexError):
            pass

    # now iterate through each pid to build a full list of ppids and cpids
    for pid,vals in list(procs.items()):
        ppids = vals["ppids"]
        cpid  = pid
        ppid  = vals["ppid"]
        # loop until we don't find the pid in our dictionary
        while True:
            # for each ppid get the data structure set the cpid and grab
            # the next ppid
            try:
                pvals = procs[ppid]
            except KeyError:
                break
            else:
                if cpid:
                    pvals["cpids"].append(cpid)
                # set to None so we don't keep adding children
                cpid = None
                ppid = pvals["ppid"]
                # don't add the zero pid
                if ppid:
                    ppids.append(ppid)

    return procs


def _buildTree(pid, allprocs, parents=0, count=0, wide=0, format=''):
    """Build a partial or complete process tree, starting with pid."""

    msg = ''
    val = allprocs[pid]
    ppids = list(val['ppids'])
    ppids.reverse()

    # add any parents that maybe needed
    if parents:
        for ppid in ppids:
            # don't worry about printing info about 1 and 0
            if ppid not in (0, 1):
                # look up ppid object
                try:
                    pval = allprocs[ppid]
                except KeyError:
                    pass
                else:
                    msg += format % (pval['user'], ppid,
                                     pval['ppids'][0],
                                     timeutil.formatTime(pval['starttime']),
                                     pval['state'],
                                     unitutil.formatBytes(pval['size'],
                                                          base='kilo'),
                                     unitutil.formatBytes(pval['rss'],
                                                          base='kilo'),
                                     pval['totalcpu'])

                    # add vertical bars to show tree structure
                    msg += '| ' * count
                    if wide:
                        msg += pval['fullcmd'] + '\n'
                    else:
                        msg += pval['fullcmd'].split()[0] + '\n'
                        
                    count += 1

    # now add calling pid
    msg += format % (val['user'], pid, ppids.pop(),
                     timeutil.formatTime(val['starttime']), val['state'],
                     unitutil.formatBytes(val['size'], base='kilo'),
                     unitutil.formatBytes(val['rss'], base='kilo'),
                     val['totalcpu'])
    msg += '| ' * count
    if wide:
        msg += val['fullcmd'] + '\n'
    else:
        msg += val['fullcmd'].split()[0] + '\n'

    cpids = val['cpids']
    cpids.sort()

    # recursively add the children
    for cpid in cpids:
        if cpid in allprocs:
            msg += _buildTree(cpid, allprocs, parents=0,
                              count=(count + 1), wide=wide,
                              format=format)

    return msg


def getProcessTree(pids, allprocs, wide=0, parents=0):
    """Make a process tree for each pid provided in the pids list.
    'allprocs' is a dictionary in the form returned from L{getProcesses}.
    If 'wide' is true then all the command arguments will be printed.
    If 'parents' is true then all the parents of the pid will be
    added to the final tree."""

    if not pids:
        return ''

    # get the max username length for pretty formatting
    maxlen  = max([len(x['user']) for x in list(allprocs.values())])
    format  = '%%-%ds' % maxlen
    msg     = format % 'USER' + '   PID  PPID START TIME  STAT  ' \
              'SIZE   RSS %CPU COMMAND\n'
    msg    += '=' * maxlen
    msg    += ' ===== ===== =========== ==== ===== ===== ==== =======\n'
    format += ' %5d %5d %11s %4s %5s %5s %4s '

    pids.sort()
    for pid in pids:
        if pid in allprocs:
            msg += _buildTree(pid, allprocs, parents=parents,
                              wide=wide, format=format)
            if pid != pids[-1]:
                msg += '\n'

    return msg


class PopenTimeout(OSUtilError):
    """Raised if a command started with L{Popen} times out."""
    pass
    

# grab the most used subprocess constants
PIPE = subprocess.PIPE
STDOUT = subprocess.STDOUT

class Popen(subprocess.Popen):
    """Custom version of L{subprocess.Popen} which supports timeouts, process
    groups, process sessions, remote execution, callbacks when new output
    is read, and will not wait indefinitely if a child quits and some
    descendant has not closed the pipe."""

    def __init__(self, args, setpgid=True, setsid=True, host=None,
                 remote_exe=None, preexec_fn=None, shell=False, **kwargs):
        """
        @param setpgid: Set to True (default) to call setpgid() before calling
                        exec() so that all descendants will be killed if the
                        child is killed.  This does not apply to remotely
                        executed commands run via ssh.
        @type setpgid: boolean

        @param setsid: Set to True (default) to call setsid() before calling
                       exec() so that any tty is removed to prevent hangs
                       if a child attempts to read from a tty.  This will
                       also set a new process group allowing all descendants
                       to be killed if necessary. This does not apply to
                       remotely executed commands run via ssh.
        @type setsid: boolean

        @param host: Host where the command should be run. Remote commands
                       can be run as a different user by using the syntax
                       user@host.
        @type host: string

        @param remote_exe: Command that will be used to run the remotely. The
                           default is '/usr/bin/ssh.' This can be a string
                           with a single executable, or a list of arguments.
                           The 'host' paramater will be appended.
        @type remote_exe: string or list

        """

        # boolean indicating whether to set the process group
        self.setpgid = setpgid
        # check if a new session should be created
        self.setsid  = setsid

        # we still want the user to be able to define their own preexec
        self.user_preexec_fn = preexec_fn

        # call our preexec
        if setpgid or setsid:
            preexec_fn = self._preexec_fn

        # check if we should run this command on a remote host
        if host and re.match("(?:[^@]+@)?(.+)", host).group(1) != getlocalhost():
            # no matter what, we're definitely not starting up through a shell
            shell = False

            # setup the new command we'll run
            if remote_exe and isinstance(remote_exe, (str,)):
                newargs = [remote_exe]
            elif remote_exe:
                newargs = list(remote_exe)
            else:
                newargs = ["/usr/bin/ssh",
                           "-o", "PasswordAuthentication no",
                           "-o", "StrictHostKeyChecking no",
                           "-o", "FallBackToRsh no"]

            # add the host we'll connect to
            newargs.append(host)

            # add the command the user provided
            if isinstance(args, (str,)):
                newargs.append(args)
            else:
                newargs.extend(list(args))

            # assign back to the original variable
            args = newargs

        # call the super
        super(Popen, self).__init__(args, preexec_fn=preexec_fn,
                                    shell=shell, **kwargs)


    def _preexec_fn(self):
        """Changes the uid of the child and/or the process group."""
        # call the user's function first
        if self.user_preexec_fn:
            self.user_preexec_fn()

        # create a new session, which also creates a new process group
        if self.setsid:
            os.setsid()
        # set the process group, but keep the same session as the parent
        elif self.setpgid:
            os.setpgid(0, 0)


    def wait(self, timeout=0):
        """Optionally wait 'timeout' seconds for the child to exit and for
        waitpid() to return something.  The default is to wait indefinitely,
        but this could block if the child is hung because of IO.

        @param timeout: Number of seconds to wait on the child before
                        abandoning it which will leave it defunct.
        @type timeout: int or float
        """

        if timeout > 0:
            import time
            
            elapsed = 0
            sleep = .1
            # call poll() until the child quits or we timeout
            while elapsed < timeout:
                if self.poll() is not None:
                    break
                time.sleep(sleep)
                elapsed += sleep

            return self.returncode

        # wait indefinitely
        else:
            return super(Popen, self).wait()


    def send_signal(self, sig):
        """Overloaded from the super (in 2.6, 2.4 does not have this method)
        so we can call killpg() if necessary, and optionally wait for the
        child to exit.

        @param sig: Signal number that will be passed to kill() or killpg().
        @type sig: int
        """
        # do not send the signal if we already have a return code set, since
        # that implies the process has finished or died already
        if self.returncode is not None:
            return

        # kill the group if we set it
        if self.setpgid:
            os.killpg(self.pid, sig)
        else:
            os.kill(self.pid, sig)


    def terminate(self, wait=0):
        """Terminate the process with SIGTERM.  Overloaded from the super to
        optionally wait for the child to exit and send SIGKILL if the wait
        times out.

        @param wait: Number of seconds to wait for child to die before
                     killing it with a KILL signal to prevent a blocking
                     scenario.  Setting to 0 (default) will not call wait()
                     at all.
        @type wait: int or float
        """

        # kill the child
        self.send_signal(signal.SIGTERM)

        # wait for the child to exit so we can fallback to SIGKILL if necessary
        if wait > 0 and self.wait(timeout=wait) is None:
            # you had your chance self.pid!
            self.kill()
            # give it 1 second, then we'll abandon it to prevent a blocking
            # scenario if the child is hung on IO
            if self.wait(timeout=1) is None:
                # fake the returncode so subsequent wait() calls don't block
                self.returncode = -signal.SIGKILL


    def kill(self):
        """Kill the process with SIGKILL."""
        # kill the child
        self.send_signal(signal.SIGKILL)


    def communicate(self,
                    input=None,
                    stdout_cb=None,
                    stderr_cb=None,
                    timeout=0,
                    termwait=0,
                    poll=0,
                    linebuffer=True):
        """Unfortunately, the L{subprocess.Popen} implementation of the
        communicate() method doesn't lend itself to subclassing for the
        features we want to add.  This version is largely a copy and paste
        of the python 2.6 version and implements the following features::

          - Instead of accumulating child output and returning the potentially
            large strings upon child completion, callbacks can be provided to
            stream the output elsewhere.  The strings sent to the callback
            functions are sent as they are received from the child and may
            not be complete lines.           

          - If the child has not provided any output after 'timeout' seconds,
            then terminate() will be called.  Additionally, kill() will be
            called if the child does not exit after 'termwait' seconds.
            kill() will then only wait 1 second for the child to exit.  This
            is useful to prevent a blocking scenario caused if the child
            is hung because of IO.

        @param input: String to be sent to the child process, or None, if
                      no data should be sent.
        @type input: string

        @param stdout_cb: Function that will be called after each successful
                          read of stdout from the child within the
                          communicate() method.  The string read will be
                          passed to the function and may or may not be
                          a full line, unless 'linebuffer' is True.
        @type stdout_cb: function

        @param stderr_cb: Function that will be called after each successful
                          read of stderr from the child within the
                          communicate() method.  The string read will be
                          passed to the function and may or may not be
                          a full line, unless 'linebuffer' is True. 
        @type stderr_cb: function

        @param timeout: Number of seconds to wait on output from the child
                        before giving up and killing it. Set to 0 for no
                        timeout (the default).
        @type timeout: int or float

        @param termwait: Number of seconds to wait for the child to exit after
                         calling terminate(), the default is 0 (indefinitely).
        @type termwait: int or float

        @param poll: It's possible for a pipe to remain open if the child
                     forks a grandchild, but does not close all files before
                     calling exec(). This leaves the grandchild with a pipe
                     to us and allowing the child to exit without our knowing,
                     resulting in a defunct child process while we wait
                     indefinitely for the pipe to close. To prevent this,
                     poll() can be periodically called to see if the child has
                     actually exited and is no longer writing to the pipe.
                     Set this to the number of seconds to wait in select()
                     before calling poll() if no output is read.  Setting to
                     0 or None (default) means poll() will not be called.
        @type poll: int or float

        @param linebuffer: Buffer incomplete lines read from the pipes and
                           only send full lines (unless EOF is encountered) to
                           the callback functions. If False, then data will
                           be sent to the callbacks as read and an empty
                           string will be sent to indicate EOF.
                           Default is True.
        @type linebuffer: boolean

        @return: tuple (stdout, stderr).  If callbacks were provided, then
                 ('', '') is returned. None is returned for stdout/stderr if
                 a pipe was not opened.
        @rtype: tuple
        """

        write_set = []
        stdout = None # Return
        stderr = None # Return

        if self.stdin:
            # Flush stdio buffer.  This might block, if the user has
            # been writing to .stdin in an uncontrolled fashion.
            self.stdin.flush()
            if input:
                write_set.append(self.stdin)
            else:
                self.stdin.close()

        # create a mapping from fd to (buffer, cb) and change the pipes
        # to be non-blocking
        import fcntl, errno, select
        fdToBuf = {}
        if self.stdout:
            stdout = []
            fdToBuf[self.stdout] = (stdout, stdout_cb)
            fcntl.fcntl(self.stdout.fileno(), fcntl.F_SETFL, os.O_NDELAY)
        if self.stderr:
            stderr = []
            fdToBuf[self.stderr] = (stderr, stderr_cb)
            fcntl.fcntl(self.stderr.fileno(), fcntl.F_SETFL, os.O_NDELAY)


        def getLines(data, buffer):
            """Return the next full line from the string, buffering any
            data that doesn't have a newline until the stream is closed.
            We can't use readline() because it dosn't properly buffer
            incomplete reads from non-blocking pipes."""

            # pipe is closing
            if not data:
                # if nothing is buffered, then we have nothing to yield
                if not buffer:
                    return
                # yield whatever is left in the buffer, and clear buffer
                line = ''.join(buffer)
                del buffer[:]
                yield line

            # search for the first newline
            curr = 0
            ind  = data.find('\n')
            while ind >= 0:
                line = ''.join(buffer) + data[curr:ind + 1]
                # clear buffer now
                del buffer[:]
                yield line

                # move our current position
                curr = ind + 1
                # search for another newline
                ind  = data.find('\n', curr)

            # save any text that doesn't end in a newline
            if curr < len(data):
                buffer.append(data[curr:])


        def read(fobj, close=False):
            """Convenience function to read from the provided file object
            and direct it to a callback or accumulate it."""

            # read from the object and ignore would-block errors
            try:
                data = fobj.read()
            except IOError as err:
                if err.args[0] not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise
                # but wait! if close==True, then we need to close the fd
                # regardless
                if close:
                    fobj.close()
                    del fdToBuf[fobj]
                return

            # direct the data read to either a callback or accumulate it in
            # a buffer
            buf,cb = fdToBuf[fobj]
            if cb:
                if linebuffer:
                    for line in getLines(data, buf):
                        cb(line)
                else:
                    cb(data)
            else:
                buf.append(data)

            # pipe has closed or we've been told to close it
            if data == "" or close:
                fobj.close()
                del fdToBuf[fobj]


        elapsed = 0
        lastpoll = 0
        input_offset = 0
        # don't stay in select forever if polling or a timeout is provided
        if poll > 0 and timeout > 0:
            select_timeout = min(poll, timeout)
        elif poll > 0:
            select_timeout = poll
        elif timeout > 0:
            select_timeout = timeout
        else:
            select_timeout = None

        # loop until the pipes are closed or the child quits
        while fdToBuf or write_set:
            try:
                rlist,wlist,xlist = select.select(list(fdToBuf.keys()), write_set,
                                                  [], select_timeout)
            except select.error as err:
                # don't let interrupts bring us down
                if err.args[0] == errno.EINTR:
                    continue
                raise

            # if we didn't read anything, then select must have timed out
            if not (rlist or wlist) and select_timeout:
                elapsed += select_timeout
                lastpoll += select_timeout
                if timeout > 0 and elapsed >= timeout:
                    # time's up!
                    self.terminate(wait=termwait)
                    self.wait()
                    raise PopenTimeout("command was unresponsive for more "
                                       "than %s seconds" % timeout)

                # check on the child if necessary
                if poll > 0 and lastpoll >= poll:
                    if self.poll() is not None:
                        break
                    # reset so we don't poll() for another 'poll' seconds
                    lastpoll = 0
            else:
                elapsed = 0
                lastpoll = 0

            if self.stdin in wlist:
                # When select has indicated that the file is writable,
                # we can write up to PIPE_BUF bytes without risk
                # blocking.  POSIX defines PIPE_BUF >= 512
                chunk = input[input_offset : input_offset + 512]
                bytes_written = os.write(self.stdin.fileno(), chunk)
                input_offset += bytes_written
                if input_offset >= len(input):
                    self.stdin.close()
                    write_set.remove(self.stdin)

            # handle those ready to be read
            for fobj in rlist:
                read(fobj)


        # if the child quit before the pipes were closed, then check one
        # last time for any output
        for fobj in list(fdToBuf.keys()):
            read(fobj, close=True)

        # All data exchanged.  Translate lists into strings.
        if stdout is not None:
            stdout = ''.join(stdout)
        if stderr is not None:
            stderr = ''.join(stderr)

        # Translate newlines, if requested.  We cannot let the file
        # object do the translation: It is based on stdio, which is
        # impossible to combine with select (unless forcing no
        # buffering).
        if self.universal_newlines and hasattr(open, 'newlines'):
            if stdout:
                stdout = self._translate_newlines(stdout)
            if stderr:
                stderr = self._translate_newlines(stderr)

        self.wait()
        return (stdout, stderr)


def runCommand(cmd, onhost=None, timeout=0, 
               blocking=0, usessh=0, user=None, termwait=30,
               input=None, setpgid=True, setsid=True, poll=0):
    """Wrapper around L{Popen} that allows for remote execution of a command
    via ssh or rsh.  Returns a tuple (rcode, out, err).

    @param cmd: Command string that will be run.
    @type cmd: string or tuple of args

    @param onhost: host that command will be run on
    @type onhost: string

    @param timeout: Number of seconds to wait before killing the command
                    if no output (stdout or stderr) is read.
    @type timeout: int or float

    @param blocking: obsolete
    @type blocking: boolean

    @param usessh: /usr/bin/ssh will be called if running remotely
    @type usessh: boolean

    @param user: User command will be run as if run remotely. Only works
                 with ssh and rsh.
    @type user: string

    @param termwait: If the command does not exit after L{Popen} tries to
                     terminate because of timing out, then SIGKILL will be
                     used after this many seconds to prevent waitpid() for
                     hanging indefinitely.  Default is to wait 30 seconds.
    @type termwait: int or float

    @return: tuple containing the return code, stdout of the command, and
             stderr of the command.
    @rtype: tuple

    @param input: String to be sent to the command, or None, if
                  no data should be sent.
    @type input: string

    @param setpgid: Start the child in a new process group (default True)
    @type setpgid: boolean

    @param setsid: Start the child in a new session to remove tty access
                   (default True)
    @type setsid: boolean

    @param poll: call poll() ever 'poll' seconds to check if the immediate
                 child has actually exited, but a pipe remains open to some
                 other descendant.  Default is 0 which will wait for all
                 pipes to be closed.
    @type poll: int or float

    """

    # support some legacy options
    if usessh:
        # this is the default for Popen
        remote_exe = None
    else:
        remote_exe = "/usr/bin/rsh"

    # set the 'host' parameter
    if onhost and user and usessh:
        host = "%s@%s" % (user, onhost)
    else:
        host = onhost

    # Popen has to know whether to use a shell or not
    if isinstance(cmd, (str,)):
        shell = True
    else:
        shell = False

    # start the command
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE,
              close_fds=True, shell=shell,
              setpgid=setpgid, setsid=setsid,
              host=host, remote_exe=remote_exe)

    # catch a timeout error
    try:
        out,err = p.communicate(input=input, timeout=timeout,
                                termwait=termwait, poll=poll)
    except PopenTimeout as err:
        # form a helpful error message
        if not shell:
            mycmd = ' '.join(cmd)
        else:
            mycmd = cmd
        out,err = "","'%s'\n%s, aborting system call." % (mycmd, str(err))

    return p.returncode,out,err


# This was originally created while testing the new Popen class and should
# be removed after all client apps are updated.
_runCommand = runCommand


def processAlive(pid, host=None, contains=None, timeout=0):
    """Returns 1 if the process with 'pid' running on the localhost is
    alive. 0 if dead, and -1 if unable to determine.  This is done by running
    os.stat on '/proc/pid'.  If the optional 'host' argument is provided and
    it is not equal to the localhost the command::

      > /usr/bin/rsh host ls -d /proc/pid

    is run with L{runCommand}.  An optional 'contains' argument can be
    provided which will verify that the process running with pid has
    'contains' in its command line string.  If it doesn't then 0 is
    returned.  'contains' can be a single string or a list of
    strings found in the command line."""

    # depending on the parameters we use different approaches to find
    # the information we need.  This is to minimize the systems calls
    # and file accesses.

    if not host or host == getlocalhost():
        local = 1
    else:
        local = 0

    # do the easiest case first
    if not contains:
        if local:
            try:
                stat = os.stat('/proc/%d' % pid)
            except (OSError, IOError) as errObj:
                if errObj.errno == 2:
                    return 0
                return -1
            else:
                return 1

        cmd = '/bin/ls -d /proc/%d' % pid
        rcode,out,err = runCommand(cmd, timeout=timeout, onhost=host)

        if not err and out:
            if out.find('/proc/%d' % pid) == 0:
                return 1
        elif err.find('/proc/%d: No such file' % pid) >= 0:
            return 0

        return -1

    # if contains is present and we are local then we can speed things
    # up by accessing /proc/$pid/cmdline
    cmdline = ''
    if local and getlocalos() == 'Linux':
        try:
            file = open('/proc/%d/cmdline' % pid)
            cmdline = file.read()
            file.close()
        except (OSError, IOError) as errObj:
            if errObj.errno == 2:
                return 0
            return -1
    else:
        # check for the OS of the host in question
        rcode,out,err = runCommand('/bin/uname -s', onhost=host, timeout=timeout)

        # if we are running Linux then throw in the ww flags so
        # we get the full command
        pscmd = '/bin/ps -f'
        if not err and out.strip() == 'Linux':
            pscmd += 'ww'
        pscmd += 'p %d -o args=' % pid
        rcode,out,err = runCommand(pscmd, onhost=host, timeout=timeout)
        if err:
            return -1

        cmdline = out.strip()

    mystrs = []
    if type(contains) is bytes:
        mystrs.append(contains)
    else:
        mystrs.extend(contains)

    # iterate through each possible contains string
    for s in mystrs:
        if not re.search(s, cmdline):
            return 0
    else:
        return 1

    # I don't know what happened
    return -1


def daemonize(files2dup=[], sockets2dup=[]):
    """Daemonize a process and optionally dup a list of file objects
    that are to remain intact after all the forking is done.  This method
    is used to fork the current process into a daemon.  Almost none of
    this is necessary (or advisable) if your daemon is being started by
    inetd. In that case, stdin, stdout and stderr are all set up for you
    to refer to the network connection, and the fork()s 
    and session manipulation should not be done (to avoid confusing inetd). 
    Only the chdir() and umask() steps remain as useful.
    ReferencesE{:}
    
      - B{UNIX Programming FAQ}
        1.7 How do I get my program to act like a daemon?
        U{http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16}

      - B{Advanced Programming in the Unix Environment}
          W. Richard Stevens, 1992, Addison-Wesley, ISBN 0-201-56317-7.

    """

    def dupfile(fobj, mode):
        """Dups the file descriptor of a file object and returns a valid
        file object.  mode is the desired mode of the resulting file
        object."""

        newfd = os.dup(fobj.fileno())
        return os.fdopen(newfd, mode)

    def dupsocket(sobj, family, type):
        """Dups the file descriptor of a socket object and returns a valid
        socket object.  mode is the desired mode of the resulting file
        object."""

        newfd = os.dup(sobj.fileno())
        return socket.fromfd(newfd, family, type)

    fdupped = []
    sdupped = []
    
    # dup all the desired file objects
    for fobj in files2dup:
        fdupped.append(dupfile(fobj, 'w'))

     # dup all the desired socket objects
    for sobj in sockets2dup:
        sdupped.append(dupsocket(sobj, socket.AF_INET, socket.SOCK_STREAM))

    # do first fork
    try:
        pid = os.fork() 
        if pid > 0:
            closeme = files2dup
            closeme.extend(sockets2dup)
            for fd in closeme:
                fd.close()
            sys.exit(0) # Exit first parent.
    except OSError as e:
        log("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    # Decouple from parent environment.
    os.chdir("/") 
    os.umask(0) 
    os.setsid()

    fdupped2 = []
    # dup everything again
    for fobj in fdupped:
        fdupped2.append(dupfile(fobj, 'w'))

    sdupped2 = []
    # dup everything again
    for sobj in sdupped:
        sdupped2.append(dupsocket(sobj, socket.AF_INET, socket.SOCK_STREAM))

    # Do second fork.
    try:
        pid = os.fork()
        if pid > 0:
            closeme = fdupped
            closeme.extend(sdupped)
            for fd in closeme:
                fd.close()
            sys.exit(0) # Exit second parent.
    except OSError as e:
        log("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    # Now I am a daemon!
    # return any dupped file objects    
    return fdupped2, sdupped2

class ProcessActiveError(OSUtilError):
    def __init__(self, pid, command):
        self.pid = pid
        self.command = command

    def __str__(self):
        s = 'A process is already running with pid %d' % self.pid
        if self.command:
            s += " and with '%s' in its command" % self.command
        return s

def getProcessLock(lockFilename, command=None, close=0):
    """This function gets an exclusive lock on the given file,
    checks to see if the process of the PID in the file exists,
    and returns the file handle if the process does not exist.
    If command is specified, then if there is a process with the
    given PID, getProcessLock will only fail the process has
    the string 'command' in its command.

    This should be used in concert with L{setProcessLock}"""

    lockFile = None
    pid = 0
    if getlocalos() == 'SunOS':
        import fcntl
        lockfd = None
        try:
            lockfd = os.open(lockFilename, os.O_CREAT|os.O_RDWR)
            fcntl.flock(lockfd, fcntl.LOCK_EX)
        except (OSError, IOError) as errMsg:
            errStr = 'unable to acquire lock for %s\n' % lockFilename
            errStr += str(errMsg)
            if lockfd: 
                os.close(lockfd)
            raise OSUtilError(errStr)

        try:
            # read in the first line
            line = ''
            while 1:
                c = os.read(lockfd, 1)
                if c == '\n' or not c: break
                line += c
            if line:
                pid = int(line)
        except (ValueError, IOError, OSError) as errMsg:
            errStr = 'unable to read %s\n' % lockFilename
            errStr += str(errMsg)
            os.close(lockfd)
            raise OSUtilError(errStr)

        # now return a valid file object
        lockFile = os.fdopen(lockfd, 'w+')

    else: # localos is not sun (tested for linux)
        import fcntl
        try:
            lockFile = open(lockFilename, 'a+')
            fcntl.flock(lockFile.fileno(), fcntl.LOCK_EX)
        except (OSError, IOError) as errMsg:
            errStr = 'unable to acquire lock for %s\n' % lockFilename
            errStr += str(errMsg)
            if lockFile: 
                lockFile.close()
            raise OSUtilError(errStr)

        try:
            # put file pointer at the beginning of the file
            os.lseek(lockFile.fileno(), 0, 0)
            line = lockFile.readline().strip()
            if line:
                pid = int(line)
        except (ValueError, IOError, OSError) as errMsg:
            errStr = 'unable to read %s\n' % lockFilename
            errStr += str(errMsg)
            lockFile.close()
            raise OSUtilError(errStr)

    if pid and pid != os.getpid():
        # if we got a pid out of it then check if it is alive.
        contains = None
        if command:
            contains = [command]
        status = processAlive(pid, contains=contains)

        if status > 0:
            lockFile.close()
            raise ProcessActiveError(pid, command)

        if status < 0:
            errStr = 'Unable to determine if a process is running ' \
                     'with pid %d\n' % pid
            lockFile.close()
            raise OSUtilError(errStr)

    # optionally close the lock now if we don't have anything else to do
    if close:
        return setProcessLock(lockFile)

    return lockFile


def setProcessLock(lockFile):
    """This function writes the current PID in the lockFile.
    getProcessLock() should be successfully called first.
    The process is broken into 2 stages so that daemonize() can
    be called before calling setProcessLock() so that the proper PID
    will be stored in the file.
    """

    pid = os.getpid()
    try:
        os.lseek(lockFile.fileno(), 0, 0)
        os.ftruncate(lockFile.fileno(), 0)
        lockFile.write("%d\n" % pid)
        lockFile.close()
    except (IOError, OSError) as err:
        errStr = 'unable to write to lock file %s:\n%s' % \
                 (lockFile.name, str(err))
        raise OSUtilError(errStr)


_threadRegistry = {}
def parallelProcess(func=None, arglist=[], maxthreads=1, cycle=0, debug=0):
    """Call the given function in parallel, in up to 'threads' concurrent
    threads, with each call an item from arglist.  If cycle=1, then
    processing will loop over the arglist."""

    if not arglist:
        print('parallelProcess(): No values in arglist.')
        return

    if maxthreads <= 0:
        print('parallelProcess(): maxthreads must be at least 1.')
        return


    def doFunc(func, args):
        if debug:
            util.log('apply(%s, %s)' % (func.__name__, args))
        # put in open except because we want to ensure registry is cleared
        try:
            func(*args)
        except:
            pass
        del _threadRegistry[args]
        

    lastKillTime = 0
    killPeriod = 15   # kill stray csh processes every 15 seconds
    index = 0
    while (1):
        if len(_threadRegistry) >= maxthreads:
            time.sleep(1)
            continue

        funcArgs = arglist[index]
        _threadRegistry[funcArgs] = 1
        thread.start_new_thread(doFunc, (func, funcArgs))
        #apply(doFunc, (func, funcArgs))

        # kill any stray csh processes (I don't know why this happens)

        now = int(time.time())
        if now - lastKillTime > killPeriod:
            killStrayCsh(debug=debug)
            lastKillTime = now

        index += 1
        if index >= len(arglist):
            if not cycle:
                # main() will call this func again
                #registry will keep track of threads
                break
            else:
                index %= len(arglist)

# ----------------------------------------------------------------------------

def dirsize(dirname, strict=0):
    """Recursively descend a directory and compute the total disk usage.
    This is just like du -s, but it will optionally ignore files that don't
    have proper permissions."""

    try:
        files = os.listdir(dirname)
    except OSError as err:
        if err.errno == 13 and not strict:
            return 0
        raise            

    total = 0
    for file in files:
        fullpath = dirname + '/' + file
        if os.path.isdir(fullpath):
            total += dirsize(fullpath)
        total += os.stat(fullpath).st_size
    return total


def ownerForPath(path):
    """Returns the owner of the specified file or directory."""
    stat = os.stat(path)
    uid = stat.st_uid
    try:
        owner = pwd.getpwuid(uid).pw_name
    except KeyError:
        # revert to uid if we can't lookup username
        owner = str(uid)
    return owner

if sys.platform == 'darwin':
    def getAvailMemory():
        cmd = '/usr/bin/vm_stat | /usr/bin/grep "Pages free"'
        rcode, out, err = runCommand(cmd)

        if rcode:
            raise OSUtilError('unable to parse %s: %s\n' % (cmd, err))

        return int(out.split()[2][:-1]) * 4096 / 1024

    def getTotalMemory():
        cmd = '/usr/sbin/sysctl hw.physmem'
        rcode, out, err = runCommand(cmd)

        if rcode:
            raise OSUtilError('unable to parse %s: %s\n' % (cmd, err))

        return int(out.split()[1]) / 1024

else:
    def getProcMemInfo(memtypes):
        """Return the available memory of this machine."""
        mem  = 0
        try:
            # read the current memory info
            file = open('/proc/meminfo')
            data = file.read()
            file.close()
            # iterate over each line and pick out the info we want
            for line in data.split('\n'):
                if not line: continue
                fields = line.split()
                memtype = fields[0]
                # avail memory is accumulated
                if memtype in memtypes:
                    mem += int(fields[1])
        except (OSError, IOError, ValueError, IndexError) as err:
            raise OSUtilError("unable to read /proc/meminfo: %s\n" % \
                  str(err))
        return mem

    def getAvailMemory():
        return getProcMemInfo(('MemFree:', 'Buffers:', 'Cached:'))

    def getTotalMemory():
        # NOTE: the kernel can underreport the total memory; dmidecode is more accurate
        return getProcMemInfo(('MemTotal:',))

def getUsedMemory():
    return getTotalMemory() - getAvailMemory()


# ----------------------------------------------------------------------------

