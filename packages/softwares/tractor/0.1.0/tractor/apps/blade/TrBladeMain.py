TrFileRevisionDate = "$DateTime: 2016/02/04 10:37:20 $"

#
# tractor-blade - the Tractor Remote Execution Server
#
# This python application runs on render farm hosts and connects to
# the site's tractor-engine to request task assignments.  It executes
# the commands it receives and informs the engine of the outcome.
# The blade also delivers brief status and progress bulletins to
# the the engine for use by the web-based Tractor Dashboard.
#
# ____________________________________________________________________ 
# Copyright (C) 2007-2016 Pixar Animation Studios. All rights reserved.
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

import os, os.path
import sys, platform, optparse
import time, datetime, fnmatch
import atexit, select, errno

from .TrLogger        import TrLogger
from .TrBladeRunner   import TrBladeRunner
from .TrSysState      import TrGetProcessOwnerName

## --------------------------------------------------- ##
def TrBladeMain ():
    '''
    tractor-blade - handle cmdline args and begin service
    '''
    appName =        "tractor-blade"
    appVersion =     "2.4"
    appProductDate = "17 Aug 2020 05:39:31"
    appRevision =    "2091325"

    appDir = os.path.dirname( os.path.realpath( __file__ ) )

    # Add the app dir to the environment so that launched apps can find
    # the installed tractor dir if needed, e.g. to run tq, etc
    xd = os.path.dirname( sys.executable )
    if fnmatch.fnmatch(sys.executable, "*/Tractor*/bin/rmanpy*"):
        xd = os.path.dirname( xd )
    elif fnmatch.fnmatch(appDir, "/Tractor-*/tractor/apps/blade"):
        xd = appDir.find("/Tractor-")
        xd = appDir.find('/', xd+1)
        xd = appDir[:xd]

    os.environ['TR_ENV_TRACTOR_ROOT'] = xd


    defaultBPort = 9005  # default port that this *blade* listens on

    defaultMtd = "tractor-engine"
    defaultMPort = 80

    # ------ #

    if not appProductDate[0].isdigit():
        appProductDate = " ".join(TrFileRevisionDate.split()[1:3])

    if not appVersion[0].isdigit():
        appVersion = "dev"

    appBuild = "%s %s (%s) %s" % \
        (appName, appVersion, appProductDate, appRevision)

    # Collect these items early, do NOT call os.getlogin() since it
    # queries our controlling terminal on linux (why?) and will fail
    # in situations where we have no controlling terminal, such as
    # when running as a boot-time daemon or cron job.
    bUsername = TrGetProcessOwnerName()

    optparser = optparse.OptionParser(prog=appName,
                                      usage="%prog [options]",
                                      version=appBuild)

    optparser.add_option("--log", dest="logfile", metavar="FILE",
            type="string", default=None,
            help="tractor-blade activity log file, auto-rotated")
    optparser.add_option("--logrotate", metavar="MB", dest="logrotateMax",
            type="float", default=25.0,
            help="auto-rotate size for the --log file, in MB (%default)")

    optparser.set_defaults(loglevel=TrLogger.WARNING)
    optparser.add_option("-v", "--verbose",
            action="store_const", const=TrLogger.INFO,  dest="loglevel",
            help="log level Info and above")
    optparser.add_option("--debug",
            action="store_const", const=TrLogger.DEBUG, dest="loglevel",
            help="log level Debug and above")
    optparser.add_option("--trace",
            action="store_const", const=TrLogger.TRACE, dest="loglevel",
            help="log level Trace and above")
    optparser.add_option("--warning",
            action="store_const", const=TrLogger.WARNING, dest="loglevel",
            help="log level Warning and above, the default")
    optparser.add_option("-q", "--quiet",
            action="store_const", const=TrLogger.CRITICAL, dest="loglevel",
            help="log level Critical only")

    optparser.add_option("--no-auto-update", dest="autoUpdate",
            action="store_false", default=True,
            help="disable self-update to versions specified in blade.config")

    optparser.add_option("--logconfig", dest="logconfig",
            type="string", default=None, metavar="FILE",
            help="external config file for use by python logging module, "
                 "use either --logconfig OR --log, not both")

    optparser.add_option("--engine", dest="mtdhost", metavar="HOSTNAME[:PORT]",
            type="string", default=defaultMtd,
            help="hostname[:port] of the central tractor engine and job queue, "
                 "default is '"+defaultMtd+"' - usually a DNS alias. "
                 "The default engine port is 80.")
    optparser.add_option("-P", dest="mport", metavar="PORT",
            type="int", default=defaultMPort,
            help="port on which the studio's central tractor-engine process "
                 "is listening, default is %default. "
                 "Note: usually specified using --engine=host:port ")

    optparser.set_defaults(binterface="") # i.e. INNADR_ANY
    optparser.add_option("--listen", "-L", dest="bport", metavar="PORT",
            type="string", default=str(defaultBPort),
            help="port on which this blade listens for admin queries, "
                 "%default is the default. Use the value 0 (zero) "
                 "to let the operating system pick an open port. "
                 "Firewall openings may be required to allow the engine "
                 "to connect back to the blade on this port. "
                 "A specific network interface can optionally be "
                 "specificed using '--listen=addr:port'")

    optparser.add_option("--hname", dest="altname",
            type="string", default=None,
            help="alternate name for the blade host, registered with the "
                 "engine, and also sent to peers who may need to connect "
                 "back; default is the hostname derived from a reverse "
                 "look up of the blade's address as seen by the engine. "
                 "Use the special form --hname=. to cause the blade to use "
                 "only the locally derived hostname.")

    optparser.add_option("--slots", dest="slots", metavar="COUNT",
            type="int", default=-1,
            help="max concurrent slots; overrides the Capacity parameter "
                 "'MaxSlots' in blade.config, the value 0 will set slots "
                 "to the number of CPU cores detected on the host")

    optparser.add_option("--env", dest="envfile",
            type="string", default="",
            help="filename of environment for apps (e.g. from printenv), "
                 "convenience alternative to EnvKeys in blade.config")

    optparser.add_option("--logenv", dest="logenv",
            action="store_true", default=False,
            help="log the as-launched command line and environment for "
                 "every command launched by the blade")

    optparser.add_option("--cmdtee", dest="cmdtee",
            action="store_true", default=False,
            help="copy the output of each launched command to the blade's "
                 "own log, as well as writing to the per-task logs; "
                 "typically only used for debugging blade actions.")

    optparser.add_option("--zone", dest="altZone", metavar="ZONE",
            type="string", default="",
            help="zone name for dirmap indexing")

    optparser.add_option("--nimby", dest="nimby", metavar="NAME",
            type="string", default=None,
            help="restrict jobs that this blade will execute; give a user "
                 "name to accept only that user's jobs, or '1' to accept "
                 "only local Cmds in jobs spooled from this blade's host")

    optparser.add_option("--supersede", "--supercede", dest="supersede",
            action="store_true", default=False,
            help="allows a newly started blade process to gracefully take "
                 "over from a still-running previous blade.  The new blade "
                 "sends a 'drain and exit' message to the old blade, if one "
                 "is still running on the same port, then it waits for the "
                 "old one to exit before proceeding to become the new blade "
                 "service.  In drain mode, the old blade stops requesting new "
                 "work from the engine and then exits when all tasks that it "
                 "previously launched are complete.")

    optparser.add_option("--chdir", dest="chdir", metavar="DIR",
            type="string", default=None,
            help="the blade will change to the given directory on start-up")

    optparser.add_option("--daemon", dest="isDaemon",
            action="store_true", default=False,
            help="(unix only) disconnects the blade process from the "
                 "controlling terminal session and auto-backgrounds "
                 "the blade process so it can run as a system daemon")

    optparser.add_option("--pidfile", dest="pidfile", metavar="FILE",
            type="string", default=None,
            help="the blade will write its process-id (pid) to the "
                 "named file on start-up, useful for daemons")

    optparser.add_option("--pythonhome", dest="pythonhome", metavar="DIR",
            type="string", default=None,
            help="sets PYTHONHOME for python apps launched by tractor")

    optparser.add_option("--ld_library_path", dest="ld_library_path",
            type="string", default="None", metavar="PATH",
            help="sets LD_LIBRARY_PATH for apps launched by tractor")

    optparser.add_option("--dyld_framework_path", dest="dyld_framework_path",
            type="string", default="None", metavar="PATH",
            help="sets LD_FRAMEWORK_PATH for apps launched by tractor")

    optparser.add_option("--progresslimit", dest="progresslimit",
            type="float", default=1.0, metavar="SECS",
            help="max frequency of percent-done updates, in seconds")

    optparser.add_option("--minsleep", dest="minsleep",
            type="float", default=1.0, metavar="SECS",
            help="minimum idle sleep interval, in seconds")
    optparser.add_option("--maxsleep", dest="maxsleep",
            type="float", default=30.0, metavar="SECS",
            help="max idle sleep interval, in seconds")

    optparser.add_option("--killdelay", dest="escalateDelay",
            type="float", default=2.0, metavar="SECS",
            help="time between escalation from SIGINT to SIGTERM to SIGKILL, "
                 "when sweeping subprocesses, in seconds")

    optparser.add_option("--no-sigint", dest="skipSIGINT",
            action="store_true", default=False,
            help="begin termination attempts with SIGTERM rather than SIGINT")

    optparser.add_option("--startupdelay", dest="startupdelay",
            type="float", default=0.0, metavar="SECS",
            help="delay before starting up and requesting profile, in seconds")

    optparser.add_option("--profile", dest="profileOverride",
            type="string", default=None, metavar="NAME",
            help="specify a blade profile name to use, skipping usual matching, "
                 "intended for testing scenarios only")

    optparser.add_option("--winservice", dest="AsWindowsService",
            action="store_true", default=("1"==os.getenv("TR_BLADE_SERVICE")),
            help=optparse.SUPPRESS_HELP)

    optparser.add_option("--tethered", dest="tethered",  # for regressions
            action="store_true", default=False,
            help=optparse.SUPPRESS_HELP)

    optparser.add_option("--nrmtest", dest="nrmtest",  # standalone test
            action="store_true", default=False,
            help=optparse.SUPPRESS_HELP)

    # optionally skip reading the checkpoint file so that the blade does not
    # attempt to resolve prior running tasks with engine; helpful for testing
    # or other scenarios when trying to start from a clean slate.
    optparser.add_option("--skip-checkpoint", dest="skipCheckpoint",
            action="store_true", default=False,
            help=optparse.SUPPRESS_HELP)

    rc = 1
    xcpt = None
    trLog = None
    tblade = None

    try:
        (options,args) = optparser.parse_args()
        if len(args) > 0:
            optparser.error("unrecognized option: " + str(args))
            return 1

        options.processOwner = bUsername;

        if ':' in options.bport:
            i,_,p = options.bport.partition(':')
            options.binterface = i
            options.bport = int(p)
        else:
            options.bport = int( options.bport )

        if options.isDaemon:
            if "win32" == sys.platform:
                optparser.error("--daemon is not available on Windows")
                options.isDaemon = False
            else:
                daemonize()

        if options.chdir:
            try:
                os.chdir( options.chdir )
            except:
                optparser.error("chdir failed: " + options.chdir)

        #
        # If --engine=host:port is given on the tractor-blade command line,
        # then use that as our target engine hostname. Otherwise check for
        # "TRACTOR_ENGINE" in our inbound environment, or fallback to the
        # DNS alias "tractor-engine".
        #
        if options.nrmtest:
            options.mtdhost = "localhost:7"

        if options.mtdhost == defaultMtd:
            options.mtdhost = os.getenv('TRACTOR_ENGINE', defaultMtd)

        if options.mtdhost != defaultMtd:
            m,n,p = options.mtdhost.partition(":")
            if p:
                # if a port was specified, separate hostname and port
                options.mtdhost = m
                options.mport = int(p)
            # if --mtd was specified then use it for both engine and monitor

        trLog = TrLogger('tractor-blade', options)
        trLog.info( appBuild )
        trLog.info("Copyright (c) 2007-%d Pixar. All rights reserved." % 
                    datetime.datetime.now().year)
        trLog.info("PID = " + str( os.getpid() ) + " running as: " + bUsername)

        if options.startupdelay > 0.0:
            trLog.info( "Delaying startup for %0.1f seconds" % options.startupdelay )
            time.sleep(options.startupdelay)

        if options.supersede:
            waitToSupersedePriorBlade( options, trLog )
        else:
            testPriorInstance( options.pidfile, trLog )

        if options.pidfile:
            try:
                f = open(options.pidfile, 'w')
                f.write( "%d\n" % int( os.getpid() ))
                f.close()
                atexit.register(trRemovePidFile, options.pidfile)
            except:
                x = "failed to write pidfile: " + options.pidfile
                sys.stderr.write( x )
                trLog.error( x )

        if options.isDaemon:
            trLog.info("running in daemon mode, pidfile="+str(options.pidfile))

        # Compensate for "python -m module" setting sys.path[0] to '',
        # whereas we need the path to contain the more typical $cwd so that
        # site-plugins, like envhandlers, can find their tractor base classes.
        # For tractor-blade running inside TractorBladeService.exe on Windows
        # we won't have an empty item, but $cwd is still missing.  So add it
        # at the end in either case.
        sys.path.append(appDir)
        
        #
        # Create the main blade context
        #
        tblade = TrBladeRunner( options, appName, appVersion,
                                appRevision, appProductDate )

        rc = tblade.run()  # ... time passes doing tractorish things

        # all done!

    except SystemExit as e:
            rc = e

    except KeyboardInterrupt:
        xcpt = "received keyboard interrupt"
        rc = 0

    except TrBladeRunner.PortException as e:
        xcpt = e[1]

    except:
        if trLog:
            trLog.exception(" FATAL exception: ")
        else:
            errclass, excobj = sys.exc_info()[:2]
            xcpt = errclass.__name__ + " - " + str(excobj)

    if trLog:
        if rc == 0:
            if xcpt: trLog.info(xcpt)
            trLog.warning(appName + " exit.")
        else:
            if xcpt:   trLog.error(xcpt)
            if tblade: trLog.critical(appName + " exit.")
        trLog.Close()
    else:
        if xcpt:
            sys.stderr.write( xcpt + '\n' )
        elif tblade:
            sys.stderr.write(appName + " exit.\n")

    return rc

## ------------------------------------------------------------- ##

def waitToSupersedePriorBlade (opts, logger):
    '''
    Send a 'drain and exit' message to the prior blade, if it
    exists, then wait for it to actually exit.
    '''
    from tractor.base.TrHttpRPC import TrHttpRPC
    try:
        interface = opts.binterface
        if not interface:
            interface = "127.0.0.1"

        logger.debug("probe for prior blade instance")
        s = TrHttpRPC(interface, opts.bport, None, {}, "", 30)

        code, emsg = s.Transaction("/blade/drain_exit", None)
        if 0 != code:
            logger.info("no prior blade instance to supersede (rc="+str(code)+")")
        else:
            logger.warning("supersede: waiting for drain and exit of prior blade")
            while 0==code:
                time.sleep(10)
                code, emsg = s.Transaction("/blade/ping", None)

            # no longer able to connect to old blade, declare it done
            logger.warning("supersede: new blade continuing start-up,"
                            " pid=" + str(os.getpid()) )

    except Exception:
        logger.exception("supersede check error:")


## ------------------------------------------------------------- ##

def testPriorInstance (pidfile, logger):
    if not pidfile or not os.path.exists( pidfile ):
        return False

    priorIsRunning = False
    try:
        f = open(pidfile, 'rb')
        pid = f.read()
        f.close()
        pid = int( pid )

        if pid == os.getpid():
            return False # pid is ours, likely due to auto-update restart

        # Now test to see if the given pid is still running.
        # We'll get an exception if it is NOT running.

        os.kill(pid, 0)  #  XXX meaning of "0" on windows?

        # no exception here, so pidfile pid IS still running
        priorIsRunning = True

    except OSError as e:
        if e.errno == errno.ESRCH:
            return False # prior instance pid NOT running, go baby go
    except:
        errclass, excobj = sys.exc_info()[:2]
        logger.debug("prior pidfile: " + \
                      errclass.__name__ + " - " + str(excobj) )

    # if pidfile pid IS still running, throw an exception to our caller
    if priorIsRunning:
        raise TrBladeRunner.PortException \
            (1, "no --supersede, and prior pid=%d "
                "in --pidfile still running" % pid)

    # no verifiable prior instance, proceed, with regrets
    logger.warning("could not verify prior pid in --pidfile")
    return False

## ------------------------------------------------------------- ##

def daemonize ():
    '''
    Perform various actions necessary to place this process
    in the background, disconnected from the launching terminal
    and process group, as expected for unix-style daemons.
    '''
    try:
        sys.stdout.flush()
        sys.stderr.flush()

        pid = os.fork()
        if pid > 0:
            # child created, exit first parent
            os._exit(0)

        # start a new session, disconnected from parent
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # fork again to ensure no connections with controlling terminal
        pid = os.fork()
        if pid > 0:
            # child created, exit from second parent
            os._exit(0)

        # redirect stdio descriptors
        sin  = file(os.devnull, 'r')
        sout = file(os.devnull, 'a+')
        serr = file(os.devnull, 'a+', 0)
        os.dup2(sin.fileno(),  sys.stdin.fileno())
        os.dup2(sout.fileno(), sys.stdout.fileno())
        os.dup2(serr.fileno(), sys.stderr.fileno())

    except OSError as e:
        sys.stderr.write("daemonize failed %d, %s\n" % (e.errno, e.strerror))
        sys.exit(1)
    except:
        errclass, excobj = sys.exc_info()[:2]
        sys.stderr.write( errclass.__name__ + " - " + str(excobj) + '\n' )
        sys.exit(1)


def trRemovePidFile (pidfile):
    try:
        os.remove(pidfile)
    except:
        pass
 
## ------------------------------------------------------------- ##

