"""
Provides functions for processing commands in parallel on remote machines.

>>> runon(['host1', 'host2'], 'echo hi')
>>> runon(['host1', 'host2'], ['ls /tmp', 'echo "I\'m different!"'])
>>> runon(read_hosts_file(host_file), 'uptime')
"""

import select
import signal
import os, sys, time

class Job(object):
    def __init__(s, host, command, remote_shell, remote_user):
        s.command = command
        s.host = host
        s.finished = False
        s.last_read = time.time()
        pread, pwrite = os.pipe()
        pid = os.fork()
        if remote_shell == 'ssh':
            args = ['ssh', '-n',
                    '-l', remote_user,
                    '-o', 'PasswordAuthentication no',
                    '-o', 'StrictHostKeyChecking no',
                    '-o', 'FallBackToRsh no',
                    host, command]
        elif remote_shell == 'rsh':
            args = ['rsh', '-n', '-l', remote_user, host, command]
        if pid == 0:
            sys.stdin.close()
            os.close(pread)
            os.dup2(pwrite, 1)
            os.dup2(pwrite, 2)
            os.execvp(remote_shell, args)
        else:
            os.close(pwrite)
            s.pid = pid
            s.pipe = os.fdopen(pread)

    def readline(s):
        s.last_read = time.time()
        r = s.pipe.readline()
        if r:
            return r
        else:
            s.finished = True
            return None

    def fileno(s):
        return s.pipe.fileno()

    def cleanup(s):
        s.pipe.close()

    def kill(s):
        s.finished = True
        s.cleanup()
        os.kill(s.pid, signal.SIGTERM)

def read_hosts_file(name):
    """Reads hosts from a specified file, or stdin if the file is "-".

    Expects one hostname to be specified per line. Blank lines are ignored.
    """
    if name == '-':
        file = sys.stdin
    else:
        file = open(name, "r")
    hosts = [x.strip() for x in file.readlines()]
    return [host for host in hosts if host != '']

def runon(hosts, commands, remote_shell="ssh", remote_user=os.environ['USER'],
          queue_size=10, timeout=15):
    """Process commands simultaneously on multiple hosts.

    @param hosts: a list of hosts to execute commands on
    @param commands: a string or iterable of commands to run; if a string, the
                     same command is run on each host, otherwise each command
                     corresponds to the host in C{hosts} at the same index
    @param remote_shell: one of C{ssh} or C{rsh}.
    @param remote_user: username to use when logging in to remote hosts;
                        defaults to C{USER}.
    @param queue_size: maximum number of simultaneous connections allowed
    @param timeout: if a remote job goes longer than this between producing
                    lines of output, it will be killed and a C{* TIMED OUT *}
                    message will be returned for that host.
    """
    if hasattr(commands, '__iter__'):
        if len(hosts) != len(commands):
            raise ValueError("host and command lists must be same size")
    else:
        # Convert non-iterables to a repeated list the same size as hosts.
        # repeat() would probably be far more efficient, but we can't rely on
        # having itertools available.
        commands = [str(commands)] * len(hosts)

    queue = []
    signal.signal(signal.SIGCHLD, signal.SIG_IGN) # we don't care about return values
    while hosts or queue:
        # queue jobs
        while len(queue) < queue_size and hosts:
            host = hosts.pop()
            command = commands.pop()
            queue.append(Job(host, command, remote_shell, remote_user))
        (rlist, wlist, xlist) = select.select(queue, [], [], timeout)
        for job in queue:
            if job in rlist:
                # job is ready to read
                r = job.readline()
                if r:
                    print("%-10s ) %s" % (job.host, r), end=' ')
                if job.finished:
                    job.cleanup()
            else:
                # job may have expired
                if time.time() - job.last_read > timeout:
                    print("%-10s ) * TIMED OUT * " % job.host)
                    job.kill()
        queue = [j for j in queue if not j.finished]
