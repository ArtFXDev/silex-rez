# ____________________________________________________________________ 
# TrBladeRunner - the main event-loop for the Tractor remote execution
#                 server, mostly concerned with requesting work from
#                 the tractor job queue and launching the corresponding
#                 subprocess on the server host.
#
# ____________________________________________________________________ 
# Copyright (C) 2007-2020 Pixar Animation Studios. All rights reserved.
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

import os, sys, time, platform, errno
import ctypes, socket, select, glob, getpass
import logging, threading
import json, urllib.request, urllib.parse, urllib.error, queue

from .TrCmdTracker import TrCmdTracker
from .TrBladeProfile import TrBladeProfile
from tractor.base.TrHttpRPC import trSetNoInherit
from functools import reduce

_TrServiceDisposition = ""

## --------------------------------------------------- ##

class TrBladeRunner( object ):
    '''
    A thread devoted to handling task execution
    '''

    def __init__(self, options, appname, appvers, apprev, appdate):

        self.options = options
        self.internalID = appvers + "-" + apprev

        self.minSleep = options.minsleep
        self.maxSleep = options.maxsleep
        self.slotsInUse = 0
        self.activeCmds = []
        self.cmdReqPending = False
        self.delayedReports = []
        self.cidsToBeSwept = []
        self.activeHolds = {}
        self.listener = None
        self.socketList = []
        self.pendingReqs = {}

        self.logger = logging.getLogger("tractor-blade")
        self.lastReqTm = 0.0
        self.lastHeartBt = 0.0
        self.taskingStandby = 10
        self.lastLogTouch = 0.0
        self.excludeTracking = {}
        self.svckeyTally = {}
        self.nrmSessions = {}
        self.runState = "normal"
        self.needsToRecoverCheckpoint = not options.skipCheckpoint
        self.reExecArgs = None

        self.lastTaskingErr = 0
        self.lastTaskErrCount = 0
        self.lastDelayedReport = 0
        self.eventxcpt = 0
        self.okRecentLaunch = 0

        self.timerHeartbeat = 60    # send status proactively, if no tasks
        self.timerActiveHold = 120  # how long to wait for a Hold
        self.timerDelayedReport = 30 # retry interval for failed exit status
        self.timerCmdFirstLog = 5   # didLog update delay, if no other msg
        self.timerLogAccess = (8*60*60) # update log atime for logrotate
        self.timerMinMetricsWait = 15

        self.lastMetricsCollectTime = 0
        self.lastLoad = 0
        self.lastMem = 0
        self.lastDisk = 0
        self.lastProvidedServiceKeys = None

        self.recentErrors = []
        self.recentErrHiatusStart = 0

        self.eventQueue = queue.Queue()

        self.setupListener()

        self.bprofile = TrBladeProfile( self.options, appname,
                                        appvers, apprev, appdate )

        #
        # Load the "still-active" checkpoint entries from the previous
        # instance of this blade immediately, but delay logging them until
        # a profile has been acquired.  The immediate load allows the blade
        # to answer cmd verification requests from the engine as soon as
        # the engine requests them, such as when an engine is also being
        # restarted, or users are pushing buttons in the dashboard after
        # "restarting the farm" -- we may get those probes even though the
        # blade has not yet attempted to re-register with the engine.  The
        # blade should avoid reporting orphaned cmds until after it acquires
        # its profile from the engine so that it knows which cmd logging
        # discipline it should be using.
        #
        if self.needsToRecoverCheckpoint:
            self.loadPreviousCheckpoint()


    ## ------------------------------------------ ##
    def run (self):
        #
        # The tractor-blade main event loop.
        #
        self.logger.debug("begin main event loop")

        # fork off the async listener for inbound events
        t = threading.Thread( target=self.incomingListenerEvents )
        t.daemon = True
        t.start()

        backoff = 0
        while self.listener and self.runState != "shutdown":
            #
            # first, check on running pids, and report status
            #
            now = time.time()

            # Protect against time skew, such as big ntp jumps,
            # manual admin clock resets, or even daylight savings shifts.
            if now < self.lastReqTm or now < self.lastHeartBt:
                self.lastReqTm = now - 300.0
                self.lastHeartBt = now - 300.0

            reqdt = now - self.lastReqTm  # time since last request

            # clamp task work-request back-off
            backoff = min(max(self.minSleep, backoff), self.maxSleep)

            activity = self.checkPids( now )
            
            if activity == 0 and self.okRecentLaunch == 1:
                # recently had a successful cmd launch, but no exit yet
                activity = 1
            self.okRecentLaunch = 0  # reset

            if activity:
                # cmd exit (or launch), slot may be avail
                if activity > 0 and "immediate"==self.bprofile.taskBidTuning:
                    # successful task exit or launch,
                    # tuning says check immediately if we can handle more work
                    backoff = reqdt
                else:
                    # task had an error, or tuning says be less aggressive
                    backoff = self.minSleep

            st = None
            if backoff > reqdt:
                self.logger.trace("too soon for next request %f" % (backoff - reqdt))
            elif self.cmdReqPending:
                backoff *= 2
                self.logger.trace("task request pending")
            else:
                ecode, st = self.examineReadiness( now )

                if ecode:
                    # not ready, got a non-zero code (scale factor)
                    # but may send a heartbeat anyway
                    backoff *= ecode
                    if st and (now - self.lastHeartBt) > self.timerHeartbeat:
                        self.sendAsyncStatusAndTaskReq( st, False, False )  # heartbeat

                elif st and not self.cmdReqPending:
                    # otherwise we got a valid state snapshot,
                    # so make a reqest for new work

                    self.sendAsyncStatusAndTaskReq( st, True, True )  # new task request

                    reqdt = 0.0
                    backoff *= 2

            if self.taskingStandby > 0:
                backoff = self.minSleep
                self.taskingStandby -= 1

            # clamp task work-request back-off
            backoff = min(max(self.minSleep, backoff), self.maxSleep)

            # nap is the time between local pid activity checks, when
            # we have active pids, otherwise make sure that we don't
            # wait more than 'backoff" secs between task requests

            if self.slotsInUse > 0:
                if self.quickCheckPendingCmdOutput():
                    nap = 0.001
                else:
                    nap = min(1.0, self.minSleep)
            else:
                if reqdt > backoff: reqdt = 0;
                nap = max(1.0, backoff - reqdt)
                nap = self.checkDrains( nap )

            self.logger.trace("%d used, wait(%f)" % (self.slotsInUse, nap))

            sock = None
            event = None

            try:
                sock, event = self.eventQueue.get(True, nap)  # wait here

            except queue.Empty:
                # nap timeout, no event
                if nap == self.maxSleep and self.slotsInUse == 0:
                    self.bprofile.CloseTaskLoggerConnection() # can be no-op

            except KeyboardInterrupt:
                raise  # send onward

            except:
                self.eventxcpt += 1
                if self.eventxcpt > 10:
                    raise # fatal
                else:
                    self.logger.error("blade eventQueue exception: " + \
                                        self.logger.Xcpt())

            if event:
                self.eventxcpt = 0 # reset count of adnormal wakes
                try:
                    if self.handleRequest( sock, event ):
                        # handling complete for this event, and the inbound
                        # activity type resets our backoff naps
                        backoff = 0
                except:
                    self.logger.error("handleEvent failed: " + self.logger.Xcpt())
                    if sock: sock.close()



    ## ------------------------------------------------ ##

    class PortException(Exception): pass  # an internal exception type

    ## ------------------------------------------------ ##
    def setupListener (self):
        #
        # Prepare for receiving activity on pending *inbound* sockets.
        #
        if self.options.binterface:
            portStr = str(self.options.binterface)+':'+str(self.options.bport)
        else:
            portStr = str( self.options.bport )

        try:
            if "win32" == sys.platform:
                # Create a throw-away socket to alter the "global" winsock
                # creation mode flag for *subsequent* sockets.  This is
                # needed because the C-side of the python socket module uses
                # the "posix" socket() call when creating python socket
                # objects.  The Windows socket() call creates sockets in
                # win32 "overlapped i/o" mode by default. These overlapped
                # sockets cannot be inherited across CreateProcess.  One
                # would typically switch to using WSASocket() instead to
                # create non-overlapped sockets, but that would require
                # us to ship a patched python binary or custom C module
                # to match every windows customer's python. So instead,
                # we use this deprecated global flag to switch all future
                # socket() calls to non-overlapped, and importantly for the
                # tractor-blade / netrender case, also sockets created by
                # accept() from a listener created with socket().

                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                SO_OPENTYPE = 0x7008
                SO_SYNCHRONOUS_NONALERT = 0x20
                s.setsockopt(socket.SOL_SOCKET, SO_OPENTYPE,
                                SO_SYNCHRONOUS_NONALERT)
                s.close()

            #
            # create the listener socket
            #
            s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )

            if "win32" != sys.platform:
                # Do NOT set "reuse_addr" on windows, it allows
                # several processes to listen on the same port (?!)
                # We use it on unix to allow a restarted server to
                # reacquire the listener port quickly on restart,
                # rather than having to wait for the full TIME_WAIT
                #
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            s.bind( (self.options.binterface, self.options.bport) )
            trSetNoInherit( s )

            s.listen( socket.SOMAXCONN )

            # Now retrieve our as-bound listener port, in case "-L 0"
            # was given on the command line, meaning "let the OS pick."
            # We need to return this to the global options list so
            # the correct port value can be registered in the engine's
            # BladeCatalog
            p = s.getsockname()[1]
            self.logger.info("started blade listener, port %d" % p)
            self.options.bport = p

            self.listener = s

        except Exception as e:
            if e[0] in (errno.EADDRINUSE, errno.WSAEADDRINUSE):
                raise self.PortException \
                        (1, "listener port already in use!  " + portStr)

            elif e[0] in (errno.EACCES, errno.WSAEACCES):
                raise self.PortException \
                        (1, "listener port permission denied!  " + portStr)
            else:
                raise


    ## ------------------------------------------------ ##
    def sendAsyncStatusAndTaskReq (self, stateDict, blocksTasks, requestTask):
        '''send a request for work or status heartbeat, wait for reply in a thread'''

        if requestTask:
            self.logger.trace("main loop spawn: task request")
        else:
            self.logger.trace("main loop spawn: heartbeat")

        if blocksTasks:
            self.cmdReqPending = True  # note, don't change it if blockTasks=false

        t = threading.Thread( target=self.taskRequestWrap,
                              args=(stateDict.copy(), blocksTasks, requestTask) )
        t.daemon = True
        t.start()


    ## ------------------------------------------------ ##
    def incomingListenerEvents (self):
        #
        # Wait for activity on pending *inbound* sockets,
        # or timeout periodically to give Windows a chance to ctrl-c
        #
        if "win32" == sys.platform:
            dt = 3.0  # block in select for 3sec max on windows for ctrl-c
        else:
            # sigh, we have to call select.select() with a timeout arg
            # to support the Windows case above, but it doesn't support
            # passing some arg (like None) to mean "blocking".
            dt = 300.0

        self.socketList = [self.listener]

        while self.listener:
            n = len(self.socketList)
            if 0 == n or None==self.listener \
                or self.listener != self.socketList[0]:
                self.logger.warning("listener socket tracking failed")
                break

            self.logger.trace("select(%d, %f) wait for async ..." % (n, dt))

            try:
                r,w,x = select.select(self.socketList,[],[], dt)

                # if r has items in it, then
                # some sort of activity has occurred ...
                for s in r:
                    if s == self.listener:
                        # new inbound connection!
                        s, addr = self.listener.accept()
                        self.logger.debug("connect from: " + str(addr))
                        s.setblocking(0)
                        trSetNoInherit( s )
                        self.socketList.append(s)
                        self.pendingReqs[s] = ""
                    else:
                        # more bytes have arrived on an existing (partial)
                        # http request
                        event = self.getHttpRequest( s )
                        if event:
                            # the entire http request body has arrived
                            del self.pendingReqs[s]
                            self.socketList.remove(s)

                            # now queue it over to the main loop for handling
                            self.eventQueue.put( (s,event) )

            except Exception:
                if self.runState != "shutdown":
                    self.logger.exception("error processing inbound events")

        if self.listener or self.runState == "normal":
            # unexpected thread loop exit
            self.eventQueue.put( (None, {'?': "_thread_death"}) )


    ## ------------------------------------------ ##
    def checkDrains (self, nap):
        '''Check exit/restart behavior when blade becomes idle'''
        global _TrServiceDisposition

        # Note!  assumes self.slotsInUse is zero
        if self.runState.startswith("drain"):
            self.logger.warning("drain complete for " + self.runState)

	    if self.runState == "drain_restart":
                if self.reExecArgs:
                    autoUpdate = True
                else:
                    self.runState = "normal"  # canceled restart
                    return nap
            else:
                autoUpdate = False

            try:
                # clear pending exit reports
                self.sendDelayedExitReports( time.time() )
            except:
                pass

            self.runState = "shutdown"  # such as for supersede

            L = self.listener
            self.listener = None  # keepRunning = False
            L.close()             # may cause select wake in other thread

            nap = 0 # proceed to blade exit

            if autoUpdate:
                # handle special auto-restart case with the given argv
                _TrServiceDisposition = "pyz: " + self.reExecArgs[1]

                if self.options.AsWindowsService:
                    # When running as TractorBladeService.exe, the variable
                    # _TrServiceDisposition is inspected by the C side and
                    # causes the service code to create a new python interp
                    # with the blade code from the given pyz file.
                    # Since we are multithreaded, we must kill threads or
                    # the interp clean-up doesn't happen correctly, so we
                    # ensure that our 3sec listener select() on windows
                    # terminates so that the async listener thread exits.
                    nap = 3
                    self.logger.critical("attempting service reload " + \
                                         "using %s\n\n" % self.reExecArgs[1])
                else:
                    # On success execv creates a restarted blade instance.
                    # On unix this current instance quits and the daemon
                    # pid is reused by the new exec.
                    # On windows a new pid is started and this one exits.

                    self.logger.info("auto-update using: " + \
                                     " ".join(self.reExecArgs))
                    self.logger.critical("attempting restart\n\n")
                    os.execv( sys.executable, self.reExecArgs )


        return nap


    ## ------------------------------------------ ##
    def getBasicState (self, now, xcmd=None):
        #
        # Add a few "basic" dynamic items to the essentially static
        # profile state, these are table/variable based and therefore
        # not expensive to acquire.
        #
        if self.slotsInUse < 0:
            self.slotsInUse = 0

        basic = {
            "uptime":           now - self.bprofile.startTime,
            "slotsInUse":       self.slotsInUse,
            "slotsAvailable":   0,
            "activeCmds":       self.enumActiveCmds( xcmd ),
            "exclusiveKeys":    list(self.excludeTracking.keys()),
            "recentErrors":     self.fmtRecentErrors(now),
            "profileName":      None,
            "excuse":           None
        }
        
        oldptime = self.bprofile.profileUpdateTime

        go = self.bprofile.GetProfileState( basic, now )

        # now that we have the last profile's maxSlots settings,
        # compute the real slotsAvailable value
        if self.bprofile.IsInService():
            avail = self.bprofile.maxSlots - self.slotsInUse
            if avail < 0:
                avail = 0
        else:
            # no slots available when InService: 0 (even if no slots in use)
            avail = 0

        basic["slotsAvailable"] = avail

        if None == self.lastProvidedServiceKeys:
            self.lastProvidedServiceKeys = basic.get("svckeys")
             
        if go:
            self.getLocalMetrics( basic, now )

            go = self.checkRecentErrorThrottle( basic, now )

        if oldptime != self.bprofile.profileUpdateTime:
            # began using a new profile, send full blade heartbeat trivia
            self.logger.trace("new profile, send heartbt info")
            go = False # don't request work while registration thread runs
            if not self.cmdReqPending:
                self.sendAsyncStatusAndTaskReq(basic, True, False)  # heartbeat

        if go:
            go = self.checkServiceKeyTally( basic )

        return (basic, go)


    ## ------------------------------------------ ##
    def checkServiceKeyTally (self, basic):

        if 0==len(self.bprofile.countedKeys) and \
           0==len(self.bprofile.afterKeys):
           return True  # can use svckeysFromProfile as is

        keepKeys = []
        for fullkey in basic['svckeys'].split(','):
            # test whether any restrictions keep us from advertising each
            # of the nominal service keys from the profile "Provides" list

            x = fullkey.find('(')
            if -1 == x:
                key = fullkey
            else:
                key = fullkey[:x]

            key = key.lower()  # canonical key format

            if key in self.bprofile.countedKeys:
                max = self.bprofile.countedKeys[key]
                if key in self.svckeyTally and self.svckeyTally[key] >= max:
                        continue  # already too many in use

            if key in self.bprofile.afterKeys:
                after = self.bprofile.afterKeys[key]
                #
                # 'key' can't be advertised until 'after' has been used,
                # and furthermore, if 'after' is also counted, then
                # *all* of those must first be in use
                #
                if after in self.svckeyTally:
                    n = self.svckeyTally[after]
                    if n == 0 or \
                        (after in self.bprofile.countedKeys \
                         and self.bprofile.countedKeys[after] > n):
                            continue

            # otherwise, keep it
            keepKeys.append( fullkey )

        if keepKeys:
            basic['svckeys'] = ','.join(keepKeys)
            return True
        else:
            basic['svckeys'] = ""
            return False
        
    ## ------------------------------------------ ##
    def checkRecentErrorThrottle (self, basic, now):
        # deal with RecentErrorThrottle checks
        go = True
        if self.bprofile.recentErrTrigger > 0:
            t = self.recentErrHiatusStart
            if t != 0 and (now-t) < self.bprofile.recentErrHiatus:
                go = False
                msg = "error accrual hiatus"
            elif len(self.recentErrors) >= self.bprofile.recentErrTrigger:
                # too many recent errors, let things cool down
                # ("window" affects len(errs) fmtRecentErrors)
                go = False
                if -1 == self.bprofile.recentErrHiatus:
                    self.bprofile.SetNimbyOverride("too_many_errors", "")
                else:
                    # begin cool-down hiatus period
                    self.recentErrHiatusStart = now
                msg = "throttled: too many recent errors"
                self.logger.warning(msg)
            if go:
                self.recentErrHiatusStart = 0
            else:
                basic['excuse'] = msg
                self.lastHeartBt = 0   # trigger a heartbeat msg

        return go


    ## ------------------------------------------ ##
    def examineReadiness (self, now):
        #
        # Now apply a two-step procedure to determine whether we
        # should ask the engine for new tasks to launch on this
        # blade.  We initialize a dict with some "cheap to acquire"
        # static state for this blade, then pass it to a site-defined
        # routine (derived from our TrStatusFilter class) which will
        # optionally modify some of those values and then do "cheap"
        # early-out tests based on those values.  If these easy tests
        # succeed, then we proceed to collect more "expensive" dynamic
        # state values and similarly call site-defined routines to
        # analyze those values.  If this second set of tests pass,
        # then we proceed to ask the engine for new work.
        #
        denialCode = 0
        self.bprofile.SetExcuse( None )

        if (now - self.lastLogTouch) > self.timerLogAccess:
            self.lastLogTouch = now
            self.logger.updateAccessTime()

        curstate, shouldRequestWork = self.getBasicState(now)

        rev = self.bprofile.revisionUpdateNeeded()
        if rev:
            if self.options.autoUpdate:
                self.initiateRevisionAutoUpdate( rev )
                shouldRequestWork = False
                denialCode = 1
            else:
                self.logger.info("blade --no-auto-update refusing VersionPin="+rev)

        if shouldRequestWork and self.needsToRecoverCheckpoint:
            # We have finally connected to an engine and received a
            # workable profile, so now report on any cmd checkpoints
            # that were saved by the prior blade instance, and loaded
            # earlier at blade start-up.  We need a valid profile to do
            # this only so that we can put something into the proper
            # cmd output log location, as defined by this profile's
            # logging scheme.
            #
            self.reportPreviousCheckpoint()

            # initialization finally done
            self.needsToRecoverCheckpoint = False
            self.logger.info("beginning requests for work")

        try:
            lastNimby = curstate["nimby"]
        except Exception:
            lastNimby = None

        # after collecting state (for heartbeat), skip the
        # task request if we're draining
        if shouldRequestWork and self.runState.startswith("drain"):
            curstate['excuse'] = "pending " + self.runState
            shouldRequestWork = False

        if shouldRequestWork:
            csbak = curstate.copy()  # save a copy, see below

            try:
                self.bprofile.statusFilter.FilterBasicState(curstate, now)

                shouldRequestWork = \
                    self.bprofile.statusFilter.TestBasicState(curstate, now)

            except Exception:
                e = self.logger.Xcpt()
                self.logger.error("Filter/Test Basic State: " + e)
                curstate = None

            if not curstate:
                # site filter failed, or they somehow nuked the dict
                # rather than just modifying its contents, so just
                # restore the basic built-in state, for heartbeats
                curstate = csbak
                shouldRequestWork = False

        if shouldRequestWork:
            # trivial tests succeeded, so now proceed with the
            # more "expensive" dynamic tests
            
            csbak = curstate.copy()  # save a copy, see below
            try:
                self.bprofile.statusFilter.FilterDynamicState(curstate, now)

                if self.bprofile.statusFilter.TestDynamicState(curstate, now):
                    #
                    # All tests passed! we're going to make a request.
                    #
                    # Record the "excuse" anyway in case it is a note that
                    # the SiteStatusFilter wants to display in the dashboard.
                    #
                    self.bprofile.SetExcuse( curstate["excuse"] )
                else:
                    shouldRequestWork = False

                # Save the last-collected service keys from the profile and site mods,
                # this will be sent to Dashboards when users click on a specific blade
                # (the async "probe" case) to show them what the blade has recently
                # advertised. Don't change the reporting string if the site code has
                # wiped out the svckeys entry here, continue showing last sent.
                resolvedKeys = curstate.get("svckeys")
                if resolvedKeys:
                    self.lastProvidedServiceKeys = resolvedKeys

            except Exception:
                e = self.logger.Xcpt()
                self.logger.error("Filter/Test Dynamic State: " + e)
                shouldRequestWork = False
                # now restore dynamic state from backup,
                # in case it is needed for a heartbeat update
                curstate = csbak
        else:
            # trivial reject based on static state, so we're
            # offline for some reason, don't pester the engine
            denialCode = 10

        try:
            nimby = curstate["nimby"]
        except Exception:
            nimby = None

        if lastNimby != nimby:
            # nimby setting has changed, apply the new setting,
            # and possibly trigger a heartbeat if necessary.
            self.bprofile.SetNimbyOverride( nimby, None )
            self.lastHeartBt = 0
            self.backoff = 1

        if not shouldRequestWork:
            if 0 == denialCode:
                denialCode = 2 # scales caller's backoff
            if "excuse" in curstate:
                e = self.bprofile.SetExcuse( curstate["excuse"] )
                # if e:
                    # self.lastHeartBt = 0 # excuse changed, trigger heartbeat

        return (denialCode, curstate)


    ## ------------------------------------------ ##
    def getLocalMetrics (self, curstate, now):
        #
        # Take a current status snapshot, like a self ping.
        # Then either decide not to ask for new work at all,
        # or ask for new tasks that are ok with the current state.
        #
        if (now - self.lastMetricsCollectTime) > self.timerMinMetricsWait:
            self.lastMetricsCollectTime = now
            self.lastLoad = self.bprofile.sysinfo.GetProcessorUsage()
            self.lastMem = self.bprofile.sysinfo.GetAvailableRAM()
            self.lastDisk = \
                self.bprofile.GetAvailableDisk( curstate["minDiskDrive"] )

        # update the dict with the new or recent values
        curstate["cpuLoad"] = self.lastLoad
        curstate["freeRAM"] = self.lastMem
        curstate["freeDisk"] = self.lastDisk


    ## ------------------------------------------ ##
    def taskRequestWrap (self, curstate, blocksReqs, doRequestTasks):
        '''Request new work, or just send a heartbeat, in a new thread from caller'''

        eventName = 'asyncDone'
        now = time.time()
        self.lastHeartBt = now  # last engine contact attempt, any kind

        try:
            if doRequestTasks:
                eventName = self.requestTaskAssignment( curstate )
                self.lastReqTm = time.time()  # time request completed
            else:
                eventName = self.sendHeartbeat( curstate )

            if not self.bprofile.profileOK:
                self.lastReqTm = now  # engine unresponsive, backoff a bit

        except KeyboardInterrupt:
            eventName = 'shutdown'

        except:
            self.logger.warning("task request failed: " + self.logger.Xcpt())
            eventName = 'asyncDone'

        finally:
            self.logger.trace("req thread done, task req = " + str(doRequestTasks))

            if dict == type(eventName):
                # note, requestTaskAssignment returns a dict when it gets launch info
                ev = eventName
            else:
                ev = {'?': eventName}

            ev['reqpending'] = blocksReqs  # add the rp item

            self.eventQueue.put( (None, ev) )  # async post to main thread


    ## ------------------------------------------ ##
    def requestTaskAssignment (self, curstate):
        #
        # request more work from the job queue
        #
        formdata = self.bprofile.mkUrlParamsFromState("q=nextcmd", curstate)
        self.logger.trace("task req: " + formdata)

        err, reply = \
            self.bprofile.engineRPC.Transaction("task",formdata,"task_reply")

        eventName = 'asyncDone'

        if 404 == err:
            # no work available (for us) right now
            self.logger.trace("tasking: 404, " + str(reply))

        elif 412 == err and reply and 'msg' in reply \
            and " profile " in reply['msg']:
            #
            # our profile is older than the current blade.config
            #
            self.logger.info("Profile out of date, will reload. (tasking)")
            eventName = 'profileOutOfDate'

        elif err:
            x = "tasking(%d) %s" % (err, str(reply))
            if err != self.lastTaskingErr or self.lastTaskErrCount > 29:
                if self.lastTaskErrCount > 1:
                    self.logger.info(
                        "Note: tasking(%d) error repeated %d times" % \
                        (self.lastTaskingErr, self.lastTaskErrCount) )
                self.lastTaskErrCount = 1
                self.logger.info( x )
            else:
                self.lastTaskErrCount += 1
                self.logger.trace( x )

            self.lastTaskingErr = err

            if self.options.tethered:
                self.logger.critical("blade lost engine tether, start exit")
                eventName = 'shutdown'

        elif reply:
            # reply is cmd dict, already parsed from the http content
            # (note that inbound "uid" is misnamed, it is "user@host" in
            #  this context, not the local "numeric" user token)

            if 'argv' not in reply or 'jid' not in reply or 'uid' not in reply:
                self.logger.error("task reply: missing cmd items")
            else:
                #
                # Create a new tracking object for this cmd spec.
                # Note that dict keys in 'reply' are converted to
                # object attributes of 'cmd'.
                #
                cmd = None
                emsg = "setting up cmd object"
                try:
                    self.bprofile.OpenTaskLoggerConnection(reuse=True)
                    self.logger.trace( str(reply) )

                    a = reply['argv']
                    if (not a) or (not a[0]):
                        reply['argv'] = ["<<empty_cmd>>"]

                    emsg = str(reply['argv'])  # argv for error msgs

                    cmd = TrCmdTracker(reply, curstate, self)
                    if cmd:
                        eventName = {'?': 'launch', 'cmd': cmd}

                except Exception:
                    emsg = self.logger.Xcpt()

                if not cmd:
                    self.logger.error("command prep failed: " + emsg)
        else:
            self.logger.error("task request internal error")

        self.taskingStandby = 0

        return eventName


    ## ------------------------------------------ ##
    def quickCheckPendingCmdOutput (self):
        '''check whether there is pending subprocess output pending to read'''

        if "win32" == sys.platform:
            return 0  ## FIXME poll all pipes (can't use select)

        pipes = []
        for cmd in self.activeCmds:
            if cmd.process.stdout:
                pipes.append(cmd.process.stdout)
            if cmd.process.stderr:
                pipes.append(cmd.process.stderr)

        r = []
        if len(pipes) > 0:
            r,w,x = select.select(pipes,[],[], 0.001)
        
        return len(r)

    ## ------------------------------------------ ##
    def sendHeartbeat (self, curstate):
        # No requests in a while, likely because we're running a
        # long task, or nimby is active.  Send current state
        # to the blade catalog, for monitoring purposes.

        formdata = self.bprofile.mkUrlParamsFromState( "q=bpulse",
                                                        curstate,
                                                        False, True )
        self.logger.trace( formdata )
        e, x = \
            self.bprofile.engineRPC.Transaction("btrack",formdata,"heartbeat")

        if 412 == e:
            # our profile is older than the current blade.config
            self.logger.info("Profile out of date, will reload. (heartbeat)")
            self.bprofile.profileOK = False  # causes refetch
            return 'profileOutOfDate'  # event name
        elif e:
            self.logger.info("heartbeat: %d, %s" % (e,x))
            return 'asyncDone'  # event name
        else:
            return 'sentHeartbeat'  # event name


    ## ------------------------------------------ ##
    def nrmConnect (self, sock, event):
        #
        # Accept a series of connections from netrender.exe, storing
        # the open socket until all have been setup, then launch prman.
        #
        # The netrender client connects each of the sockets in a
        # particular order and sends prman 'argv' info along with the
        # 'last' session-socket setup request.  However, TCP allows
        # several connect() requests to be in flight simultaneously,
        # so the netrender client will get control back from connect()
        # before we have actually accept()-ed it. This occurs frequently
        # on newer Windows clients, but can occur on any platform.
        # So we may find several sockets waiting to accept, and we
        # might accept them in a different order than the client
        # connect() order.  Thus we need to actively validate that
        # we are holding all 4 of the nrm sockets before proceeding.
        #
        if 'S' not in event or 'tref' not in event or not sock:
            self.logger.error("unexpected netrender request format")
            try:
                sock.close()
            except:
                pass
            return

        sname = event['tref']
        stype = event['S']

        sock.setblocking(1)  # revert nrm sockets back to blocking

        if sname not in self.nrmSessions:
            self.nrmSessions[sname] = { 'socketMap': {} }

        self.nrmSessions[sname]['socketMap'][stype] = sock

        if 'argv' in event:
            # note: this may arrive before the *last* connection
            self.nrmSessions[sname].update( event )

        smap = self.nrmSessions[sname]['socketMap']
        slist = ('dspy', 'stdio', 'err', 'file')

        #
        # did we get all four of the slist connections,
        # and the argv list?
        #
        if reduce(lambda x,y: x and y, [s in smap for s in slist],
                'argv' in self.nrmSessions[sname]):
            #
            # yes, we did!
            #
            # create a new TrCmdTracker cmd object
            # note: this step will also convert dict keys from the
            # argv 'event' into object attributes of 'cmd'
            cmd = None
            try:
                # acknowledge argv, client can send data
                smap['file'].sendall("ok\r\n")

                self.bprofile.OpenTaskLoggerConnection(reuse=True)
                now = time.time()
                st = self.getBasicState(now)[0]

                cmd = TrCmdTracker(self.nrmSessions[sname], st, self)

                if self.launchCmd(cmd, now):
                    del cmd  # immediate exit
                    cmd = None

            except Exception:
                self.logger.info( self.logger.Xcpt() )

            if not cmd:
                self.logger.error("command prep failed: " + sname)

            # close pre-fork parent versions of sockets on success,
            # or the uninherited same on failure
            for s in slist:
                try:
                    smap[s].close()
                except:
                    pass

            del self.nrmSessions[sname]


    def reportProcessDone (self, cmd):
        # note: self.slotsInUse decremented in TrCmdTracker::checkExitStatus
        # since we may be called several times here until successfully sent

        if cmd.exitcode != 0 and not cmd.wasSwept:
            self.recentErrors.append( (time.time(), cmd.jid, cmd.cid) )

        try:
            if cmd.exitcode == 20002:
                self.bprofile.statusFilter.SubprocessFailedToStart( cmd )
            else:
                self.bprofile.statusFilter.SubprocessEnded( cmd )
        except Exception:
            e = self.logger.Xcpt()
            self.logger.error("Filter/Subprocess Exit: " + e)

        for sk in cmd.svckey.split():
            sk = sk.lower()
            if sk in self.excludeTracking:
                if 1 < self.excludeTracking[ sk ]:
                    self.excludeTracking[ sk ] -= 1
                else:
                    del self.excludeTracking[ sk ]

            if sk in self.svckeyTally:
                if 0 < self.svckeyTally[ sk ]:
                    self.svckeyTally[ sk ] -= 1

        if cmd.GetAltMode() or cmd.jid <= 0:
            return True  # not a tracked engine cmd, it can be deleted

        elif self.sendCmdExitStatus(cmd):
            return True  # status delivered, cmd can be deleted
        else:
            # must retain it and report later
            self.delayedReports.append(cmd)
            return False  # should not be deleted


    def checkPids(self, now):
        activity = 0
        ctmp = []

        # first deal with previously queued kill sweep
        klist = []
        for c in self.activeCmds:
            keepIt = True
            if c.process and (c.jid, "%d.%d" % (c.cid, c.rev)) in self.cidsToBeSwept:
                msg = "kill sweep %s pid=%d" % (c.logref, c.pid)
                self.logger.info( msg )
                c.SaveOutput( msg )

                if c.sendTermination():
                    # child process killed, and exit reported to engine
                    # (otherwise c.mustDie is set and it will be rekilled)
                    klist.append( c )
                    keepIt = False
                    activity = 1

            if keepIt:
                ctmp.append( c )

        self.cidsToBeSwept = [] # reset
        self.activeCmds = ctmp  # new list, without purged items
        for c in klist:
            del c

        #
        # Then check for exits
        #
        ctmp = []
        activeOutput = False

        for cmd in self.activeCmds:
            # note: checkExitStatus calls *our* reportProcessDone method!
            x,r,o = cmd.checkExitStatus( now, True )
            if x == None:
                #  did not exit
                ctmp.append(cmd)    # list of still active cmds
                if o:
                    activeOutput = True # had first output on this check?
            else:
                # process did exit, x is exitcode, zero on success
                if x != 0:
                    activity = -1   # some process had an error
                elif activity == 0:
                    activity = 1    # all processes exit with success
                if r:
                    del cmd  # process is done, AND status reported

        self.activeCmds = ctmp
        self.bprofile.sysinfo.ResourceProbeForPIDs( self.activeCmds )

        # if the engine has been down, we may have exit status
        # from previously assigned commands that still needs to be
        # reported back to the job database
        if self.delayedReports and self.bprofile.profileOK and \
            ((now - self.lastDelayedReport) > self.timerDelayedReport):
            self.sendDelayedExitReports( now )
        elif activity or activeOutput:
            self.saveCmdCheckpoint()

        for r in list(self.activeHolds.keys()):
            clist = self.activeHolds[r]
            clist = [c for c in clist if self.holdchk(r, c, now)]
            if 0 == len( clist ):
                del self.activeHolds[r]
                if activity == 0:
                    activity = 1   # wake up! (but no error)
            else:
                self.activeHolds[r] = clist

        return activity


    def enumActiveCmds (self, xcmd=None):
        if not self.activeCmds:
            return "_"

        ctxt = ""
        comma = 0
        for cmd in self.activeCmds:
            if cmd == xcmd:
                continue
            if comma:
                ctxt += ","
            ctxt += "%s:%s.%s" % (cmd.jid, cmd.cid, cmd.rev)
            comma = 1
        return ctxt


    def holdchk (self, ref, c, now):
        if (now - c.initTime) > self.timerActiveHold:
            self.logger.info("purge overdue hold for " + ref)
            self.slotsInUse -= c.slots
            return False
        else:
            return True  # still valid


    def fmtStatus(self, event):
        now = time.time()
        lct = time.localtime(now)

        s = ""
        if 'jsoncallback' in event: 
            s += event['jsoncallback'] + "("

        x = self.bprofile.GetExcuse()
        if not x:
            x = len(self.activeCmds)
            if x == 0:
                x = "idle"
            else:
                x = str(x) + " running"

        nh = len( self.activeHolds )
        if nh > 0:
            x += " (and pending holds)"

        s += '{\n   "status": "' + x + '",\n'
        s += '   "nimby": "%s",\n' % self.bprofile.GetNimbyStatus()
        s += '   "hnm": "' + self.bprofile.sysinfo.hostname + '",\n'
        s += '   "port": %d,\n' % self.bprofile.bport
        s += '   "uptime": %d,\n' % int(now - self.bprofile.startTime)
        s += '   "now": %d,\n' % now
        s += '   "vers": "' + self.bprofile.bladeVersion + '",\n'
        s += '   "build": "' + self.bprofile.bladeDate + ' ' + \
                               self.bprofile.bladeRevision + '",\n'
        s += '   "engine": "%s:%d",\n' % self.bprofile.getEngineHost()
        s += '   "profile": "%s",\n' % self.bprofile.GetProfileName()
        s += '   "svckeys": "%s",\n' % self.lastProvidedServiceKeys
        s += '   "cpuCount": %d,\n' % self.bprofile.sysinfo.nCPUs
        s += '   "cpuLoad": %.2f,\n' % self.bprofile.sysinfo.GetProcessorUsage()
        s += '   "memFree": %.2f,\n' % self.bprofile.sysinfo.GetAvailableRAM()
        s += '   "memPhys": %.2f,\n' % self.bprofile.sysinfo.physRAM
        s += '   "diskFree": %.2f,\n' % self.bprofile.GetAvailableDisk()
        s += '   "slotsMax": %d,\n' % self.bprofile.maxSlots
        s += '   "slotsInUse": %d,\n' % self.slotsInUse
        s += '   "platform": "%s",\n' % self.bprofile.sysinfo.osPlatform
        s += '   "pyversion": "%s",\n' % self.bprofile.sysinfo.python_vers
        s += '   "invoc": "%s",\n' % ' '.join(sys.argv).replace('\\', '/')
        s += '   "bladeUser": "' + self.bprofile.sysinfo.processOwner + '",\n'

        clist = self.activeCmds + self.delayedReports
        for r in self.activeHolds:
            clist.extend( self.activeHolds[r] )

        s += '   "pids": ['
        comma = ''
        for cmd in clist:
            s += comma + '\n    ' + cmd.fmtCmdState(now)
            comma = ','

        if comma:
            s += "\n  "

        s += "]"

        if 'printenv' in event and event['printenv']:
            s += ',\n   "printenv": '
            try:
                #s += repr( os.environ )
                s += json.dumps(os.environ.copy(), sort_keys=True,
                                ensure_ascii=True, indent=0)
            except:
                s += '{ "formatting": "failed" }'

        s += "\n}"
        if 'jsoncallback' in event: s += ")"
        s += "\n"

        return s


    def sweepJobPids (self, action, sjid, scid, reqby):
        #
        # The engine is requesting that we kill specific running pids
        # associated with a particular task (command), or that we
        # blindly kill anything related to a particular job -- usually
        # because the job is being deleted.
        #
        # note: inbound "scid" will be a list of cid.rev pairs,
        # we want to make sure to sweep the specific attempt id (rev)
        # and not some newer one that may have been started due to
        # to auto or manual retry.
        #
        jid = int(sjid)
        sweptnm = 'J'+sjid
        found = 0

        cids = None
        if scid:
            try:
                cids = scid.split(',')
            except:
                cids = []  # bogus scid spec, kill none

        if action == "jdelete":
            logtxt = "kill"
        else:
            logtxt = action

        self.logger.info( "%s requested %s sweep of J%s cid(s)=%s" % \
                          (reqby, logtxt, sjid, str(cids)) )

        for c in self.activeCmds:
            oldc = str(c.cid)  # if connected to pre-2.0 engine
            cid = "%d.%d" % (c.cid, c.rev)
            if c.jid == jid and c.process and \
                (cids==None or cid in cids or oldc in cids):
                found += 1
                sweptnm += ', C' + cid

                if action == "jdelete":
                    # queue these cids to be killed,
                    # don't do it here because it can be high latency,
                    # get a reply back to the engine ASAP
                    c.wasSwept = 1
                    self.cidsToBeSwept.append( (c.jid, cid) )

        if not found:
            # check to see if we are being asked to sweep a cmd that
            # has already finished but which has not yet been reported
            for c in self.delayedReports:
                oldc = str(c.cid)  # if connected to pre-2.0 engine
                cid = "%d.%d" % (c.cid, c.rev)
                if jid==c.jid and (None==cids or cid in cids or oldc in cids):
                        found += 1
        if found:
            self.lastDelayedReport = 0  # retry exitcode send immediately
            return (200, "OK, "+ logtxt +" "+ sweptnm)
        else:
            return (404, "Warning, no pids found for sweep: " + sweptnm)


    def launchCmd (self, cmd, now):
        #
        # Popen will throw an os error if the executable can't be found.
        # Otherwise, if the launch *mechanism* succeeds, then return
        # success from this routine.
        # If it was an unknown cmd, tell the mtd;
        # if it ran and finished already, tell the mtd.
        # Updates our list of active cmds.
        #

        if (not cmd.argv) or (not cmd.argv[0]) or \
            "<<empty_cmd>>"==cmd.argv[0]:
                return cmd.reportLaunchError(self, "invalid empty cmd args")

        if cmd.argv[0] == "TractorBuiltIn":
            if cmd.argv[1] == "Hold":
                # A multi-slot blade may end up with several holds
                # for the same command, for example running several
                # prman chunks on behalf of netrender.  Ideally these
                # will be squashed into the same single prman instance
                # by the netrender client itself.
                if cmd.logref in self.activeHolds:
                    self.activeHolds[cmd.logref].append( cmd )
                else:
                    self.activeHolds[cmd.logref] = [cmd]

                self.logger.debug("assigned: Hold for " + cmd.logref)
                self.slotsInUse += cmd.slots
                self.okRecentLaunch = 1
                return False
            else:
                errmsg = cmd.remapBuiltIns()
                if errmsg:
                    return cmd.reportLaunchError(self, errmsg)
  
        if cmd.logref in self.activeHolds:
            hlist = self.activeHolds[cmd.logref]
            hcmd = hlist[0]
            del hlist[0]
            if 0 == len(hlist):
                del( self.activeHolds[cmd.logref] )

            self.logger.debug("resolving hold: " + cmd.logref)
            cmd.SetAltMode("heldNRM")
            self.slotsInUse -= hcmd.slots
            if hcmd.envkey and not cmd.envkey:
                cmd.envkey = hcmd.envkey
            if hcmd.dirmaps and not cmd.dirmaps:
                cmd.dirmaps = hcmd.dirmaps
                cmd.applyDirMaps(False)

        dbgtype = str(getattr(cmd, 'cmdtype', "old"))
        if cmd.expands: dbgtype += ",expands"

        if self.logger.isWorthFormatting(logging.DEBUG):
            dbg = "cmd %s:  type=%s  spoolhost='%s' %s  " % \
                  (cmd.logref, dbgtype, cmd.spoolhost,
                   getattr(cmd,'spooladdr',''))

            # dump some parameters, like argv list
            dbg += json.dumps( {"envkey": cmd.envkey, "svckey": cmd.svckey},
                                    ensure_ascii=True )
            if cmd.yieldtest:
                dbg += json.dumps( {"resumewhile": cmd.yieldtest},
                                    ensure_ascii=True )

            dbg += json.dumps( {"argv":   cmd.argv}, ensure_ascii=True )

            if cmd.inmsg:
                dbg += " stdin='%s'" % cmd.inmsg

            self.logger.debug( dbg )

        # create the cmd log directory for output for this job,
        # if it doesn't exist (and this is a "spooled" job)
        if cmd.jid:
            self.bprofile.makeCmdLogDir(user=cmd.login, jid=cmd.jid,
                                        tid=cmd.tid, cid=cmd.cid,
                                        rev=cmd.rev)

        #
        # prep and exec the command line
        #
        xerr = cmd.LaunchCmd( self.bprofile )
        if xerr:
            # launch failed.  report it, record it.
            return cmd.reportLaunchError(self, xerr)
 
        self.slotsInUse += cmd.slots

        self.lastMetricsCollectTime = 0  # force resampling after launch

        self.bprofile.statusFilter.SubprocessStarted( cmd )

        # handle immediate exit, will call reportProcessDone
        cxit, crep, cout = cmd.checkExitStatus( now )
        if cxit == 0:
            self.okRecentLaunch = 1
        if crep:
            return True  # immediate exit and successful report

        if cmd.inmsg and cxit is None:
            # still running, and we have a "stdin msg" to send it
            xerr = cmd.SendStdinMsg()
            if xerr:
                return cmd.reportLaunchError(self, xerr)

            cxit, crep, cout = cmd.checkExitStatus( now )
            if cxit == 0:
                self.okRecentLaunch = 1
            if crep:
                return True  # exit and successful report

        if cxit is None:
            self.activeCmds.append(cmd)

        self.saveCmdCheckpoint() # needs activeCmds; instead on any list edit?

        if cxit is None:
            self.okRecentLaunch = 1
        else:
            return False  # exit, but not yet reported, don't delete it

        # look for service key (aka 'Provides') annotations
        for sk in cmd.svckey.split():
            sk = sk.lower()
            if sk in self.bprofile.exclusiveKeys:
                if sk in self.excludeTracking:
                    self.excludeTracking[ sk ] += 1
                else:
                    self.excludeTracking[ sk ] = 1
            if sk in self.svckeyTally:
                self.svckeyTally[sk] += 1
            else:
                self.svckeyTally[sk] = 1

        # The UI's get the initial "active" notice for this task
        # from the engine, so we do NOT also send a UDP status
        # update from the blade too because a lot of chatter and
        # engine activity for no change to the UI.

        return False  # cmd is active, don't delete it


   ## -- -- ##
    def sendCmdExitStatus(self, cmd):
        if not cmd or 0 == cmd.jid:
            return True  # this was a no-op, or a manual netrender, no report

        emsg = None
        err = 0

        formdata = cmd.formatExitCodeReport()

        st = self.getBasicState( time.time(), cmd )[0]
        formdata = self.bprofile.mkUrlParamsFromState(formdata, st)

        regularExit = True
        if cmd.expandfile and 0 == cmd.exitcode:
            # expand processing constitutes an exit report,
            # but if delivery fails, then exit rc is forced to non-zero
            if self.deliverExpandResults(cmd, formdata, None):
                cmd.exitcode = 20003
                formdata += "&rc=" + str(cmd.exitcode)  # override
            else:
                regularExit = False  # q=exitcode delivered via expand results
        
        if regularExit:
            formdata = "q=exitcode&" + formdata
            err, json = self.bprofile.engineRPC.Transaction("task", formdata)
            if 0 == err:
                # expect "{'rc': 0, ...}"
                try:
                    r = eval( json )
                    err = r['rc']
                    emsg = r['note']
                except:
                    err = 1
                    emsg = "problem parsing exitcode reply"

        if err:
            # caller must retain unreported cmds and retry delivery "later"
            self.logger.error("delivering exitcode /J%s/T%s/C%s.%s: %s (%s)" % \
                              (cmd.jid, cmd.tid, cmd.cid, cmd.rev, emsg, err))
            if -91 == err:
                err = 0  # discontinue delivery retry attempts for this cmd

        return (0 == err)


    def fmtRecentErrors (self, now):
        out = None  # no recent errors
        tmp = []
        for e in self.recentErrors:
            errt, errj, errc = e
            if (now - errt) < self.bprofile.recentErrWindow:
                # error occurred within the last N seconds, report it
                tmp.append( e ) # keep it on the list
                if out:
                    out += ",%d.%d" % (errj, errc)
                else:
                    out = "%d.%d" % (errj, errc)

        self.recentErrors = tmp
        return out


    def sendDelayedExitReports (self, now):
        ctmp = []
        activity = 0
        for cmd in self.delayedReports:
            if self.sendCmdExitStatus( cmd ):
                self.logger.info("delayed exitcode delivered " \
                                 "/J%s/T%s/C%s.%s" % \
                                 (cmd.jid, cmd.tid, cmd.cid, cmd.rev) )
                del cmd  # exit status finally delivered!
                activity = 1
            else:
                ctmp.append( cmd )  # still can't deliver it
        
        if activity:
            self.delayedReports = ctmp
            self.lastDelayedReport = now
            self.saveCmdCheckpoint()


    def sendLoggingAdvisory(self, cmd, now):
        '''
        notify the engine that this task has started generating logs,
        which allows UI log queries to be directed the right place
        '''
        st = self.getBasicState(now)[0]

        formdata = "q=cstatus&jid=%s&cid=%s&rev=%s&st=%s&flg=%u" % \
                    (cmd.jid, cmd.cid, cmd.rev, cmd.progress[0], cmd.flags)
        
        formdata = self.bprofile.mkUrlParamsFromState(formdata, st)

        err,reply = self.bprofile.engineRPC.Transaction("task", formdata)
        if err:
            self.logger.error("logging advisory, %d: %s" % (err,reply))


    def deliverExpandResults (self, cmd, formdata, chunkfile):
        """
        Send the output of an "expand" command back to the engine
        to be integrated into the job as new tasks/cmds.
        """
        #
        # Read and deliver the expand job script generated by the cmd.
        #
        # Note: the site's (optional) SubprocessEnded callback has
        # already been called prior to getting here, BETWEEN the
        # process ending and this "finishing" step of delivering the
        # expand output to the engine. This gives the site filter a
        # chance to modify the cmd.expandfile if they want to, before
        # it is delivered.  The downside might be however that if there
        # is an error delivering or integrating the expand script, then
        # the blade might declare an *error* here rather than the the
        # apparently successful task completion implied by calling
        # SubprocessEnded.
        #
        # By tractor convention, given a special content-type header,
        # the transfered job script text will not be treated as typical
        # http "form" key-value data.
        #
        # The expand snippet delivery is also an implicit q=exitcode
        # notification, although actual success or failure will depend on
        # whether the uploaded script can be parsed and integrated correctly.
        #
        emsg = None
        try:
            xscript = cmd.getExpandFileContents( chunkfile )
            if not xscript:
                emsg = "empty job text from expand %s/J%s/T%s/C%s.%s" % \
                       (cmd.login, cmd.jid, cmd.tid, cmd.cid, cmd.rev)
            else:
                e,r = self.bprofile.engineRPC.Transaction(
                            "spool?expanded=1&"+formdata, xscript, None,
                            {"Content-Type": "application/tractor-expand"}
                        )
                if e:
                    emsg = "err="+str(e)
                    if r:
                        emsg += " " + str(r)

        except Exception:
            emsg = "deliverExpandResults: " + self.logger.Xcpt()

        if emsg:
            self.logger.error(emsg)
            cmd.SaveOutput( emsg )

        return (None != emsg)


    def loadPreviousCheckpoint (self):
        '''
        Load a cmd-state checkpoint written by a previous blade process.  
        '''
        # FIXME:  Search for self.pid in the host's current process
        # list and somehow determine if the app launched by the previous
        # blade is still running.  Mere existance of a same-numbered pid
        # is insufficient, especially on Windows where pids are aggressively
        # reused, we need to examine the process in detail to confirm that
        # it matches the checkpoint. Furthermore, if we DO find a still
        # running process, we need a way to continue logging its output;
        # that might be accomplished by always causing commands to write
        # to a local file on launch, then future blades can read from that
        # file.  We also need a way to collect the pid's *exit status* when
        # it finally exits, even though we are not its parent process.
        # If we can't do that, but we have positively identified a prior
        # pid, then we should probably provide a restart policy choice
        # to *kill* it here, to free licenses, since we are going to
        # report an error anyway in reportPreviousCheckpoint().
        #
        try:
            f = self.bprofile.sysinfo.GetCheckpointFilename()
            self.logger.trace( "reading prior blade checkpoint: " + f )
            f = open( f, "rb" )
        except Exception as e:
            if e[0] != errno.ENOENT:
                self.logger.info( self.logger.Xcpt() )
            return

        try:
            chkpt = f.read()
            f.close()
        except:
            self.logger.info( self.logger.Xcpt() )
            return

        if not chkpt:
            return

        try:
            chkpt = eval( chkpt.strip() )
        except:
            self.logger.info( "parsing blade checkpoint file: " + \
                                self.logger.Xcpt() )
            return
        
        if 'CmdCheckpoint' in chkpt:
            # expect a list of dicts, each dict describes a cmd
            self.bprofile.OpenTaskLoggerConnection(reuse=True)
            for c in chkpt['CmdCheckpoint']:
                try:
                    self.logger.info("recovering chkpt of /%s/J%u/T%u/C%u" % \
                                 (c['login'], c['jid'], c['tid'], c['cid']))

                    # the dict keys in 'c' will become attributes of 'cmd'
                    # and by design the checkpoint file has entries that
                    # match our member variable names (plus others)
                    cmd = TrCmdTracker(c, {}, self)

                    cmd.indicateLogging(0) # skip, we'll send full exit report soon

                    self.delayedReports.append(cmd)

                except:
                    self.logger.info( self.logger.Xcpt() )

        if 'nimby' in chkpt:
            # conditiionally restore prior blade nimby state
            try:
                self.bprofile.SetNimbyOverride( chkpt['nimby'], None )
            except:
                self.logger.info( self.logger.Xcpt() )


    def reportPreviousCheckpoint(self):
        '''Report any unreported status results to the engine.'''
        ctmp = self.delayedReports # copy
        self.delayedReports = []
        for cmd in ctmp:
            try:
                cmd.ResetProfileDetails( self.bprofile )
                if None == cmd.exitcode:
                    # See the FIXME note in loadPreviousCheckpoint().
                    # Until then, cause the orphaned entry to be reported
                    # as an error, being conservative, so that the user knows
                    # that something weird has happened on the blade, and so
                    # the results of that run aren't reliable.  Probably they
                    # will want to retry the cmd.
                    #
                    cmd.exitcode = 1
                    self.bprofile.makeCmdLogDir(cmd.login, cmd.jid,
                                                cmd.tid, cmd.cid, cmd.rev)
                    cmd.SaveOutput("active cmd orphaned by blade restart, rerun?")
                    self.reportProcessDone( cmd )
                else:
                    self.delayedReports.append(cmd)
            except:
                self.logger.info( self.logger.Xcpt() )
                self.delayedReports.append(cmd)

        if len(self.delayedReports) > 0:
            now = time.time()
            self.checkPids( now )  # causes the reports to be sent

        self.saveCmdCheckpoint()
        self.recentErrors = [] # ignore error throttle on checkpoint restart
        self.recentErrHiatusStart = 0


    def saveCmdCheckpoint (self):
        '''
        Write a cmd-state checkpoint file.  It can be used
        by a subsequent blade invocation if we crash before
        reporting cmd disposition back to the engine.
        '''
        now = time.time()
        chkpt = "{\n \"CmdCheckpoint\": ["
        comma = ""
        for cmd in (self.activeCmds + self.delayedReports):
            chkpt += comma + "\n   " + cmd.fmtCmdState(now)
            comma = ","

        if comma:
            chkpt += "\n "

        chkpt += "],\n\n \"nimby\": "
        chkpt += json.dumps( self.bprofile.GetNimbyStatus("override") )
        chkpt += "\n}\n"

        try:
            chkfile = self.bprofile.sysinfo.GetCheckpointFilename()

            f = open( chkfile, "wb" )
            f.write(chkpt)
            f.close()
        except:
            self.logger.info( self.logger.Xcpt() )

        
    def getHttpRequest(self, sock ):
        event = {}
        txt = ""
        try:
            txt = sock.recv(8192)
        except Exception as e:
            if errno.EAGAIN == e.errno:
                return None # nothing to read yet
            else:
                self.logger.exception("recv")
        
        if len(txt) == 0:
            event["_sock_closed"] = 1
            return event
        else:
            self.pendingReqs[sock] += txt

        hdrMark = self.pendingReqs[sock].find("\r\n\r\n")
        if -1 == hdrMark:
            return None  # incomplete
        
        # got a full set of headers
        hdrs = self.pendingReqs[sock][:hdrMark].split('\n')

        url = hdrs[0]  # first line

        if (not url.startswith("GET ")) and (not url.startswith("POST ")):
            self.logger.warning("unrecognized http request: " + url)
            event["_sock_closed"] = 1
            return event

        # expect:
        # /blade/logs&jid=1234&cid=42
        # /blade/status
        #
        url = url.split()[1]  # "GET /the/url HTTP/1.0"

        contentLen = -1
        for m in hdrs:
            if m and m.lower().startswith("content-length: "):
                contentLen = int( m.split()[1] )
                break

        if contentLen < 0:
            contentLen = 0 # assume url-only if we have complete hdrs

        hdrMark += 4  # account for \r\n\r\n
        if len(self.pendingReqs[sock]) < (contentLen + hdrMark):
            return None  # incomplete request

        p = self.pendingReqs[sock][hdrMark:]
        if p:
            p = p.strip()
            if p.startswith("{"):
                try:
                    event.update({'argv': ['_:_cmd_transfer_failed_']})
                    event.update( eval(p) )
                except Exception:
                    self.logger.exception("inbound request")
                    return None

        ctx,p,args = url.partition('?')
        event["?"] = ctx
        n = 0
        for p in args.split("&"):
            nm,p,val = p.partition('=')
            if val:
                event[nm] = val
            else:
                event[n] = nm  # no nm=val, so just set positional arg
                n += 1

        client = sock.getpeername()[0]
        event["_peer"] = client
        self.logger.debug(client + " " + ctx + " " + str(args))

        return event


    def handleRequest(self, sock, event):
        '''handle one event from the inbound asynchronous event queue'''

        if "_sock_closed" in event:
            #  peer closed, just close our end
            if sock: sock.close()
            return False  # not a wake/backoff=0 event 

        reqpend = "reqpending" in event and event["reqpending"]

        reply = None
        code = 0  # default to "ok, and log.info(reply)"
        filepattern = None
        req = None
        resetBackoff = True

        if "?" in event:
            req = event["?"]
            if req.startswith("/blade/"):
                req = req[7:]  # skip initial "/blade/"
            else:
                # prep negative answers, will be overwritten below if OK
                reply = "unknown blade request type: " + req
                code = 404
        else:
            reply = "unknown http request type"
            code = 404
            resetBackoff = False

        if req:
            if req == 'status':
                reply = self.fmtStatus(event)
                code = 200

            elif req in ('jdelete', 'jvalidate'):
                if 'reqby' in event:
                    reqby = event['reqby']
                else:
                    reqby = 'engine'

                jid = None
                tid = None
                cid = None
                if 'jid' in event:
                    jid = event['jid']

                if 'cids' in event:
                    cid = event['cids']  # can be "cid.rev" (2.0)
                elif 'cid' in event:
                    cid = event['cid']   # 1.x engine, just cid

                if jid:
                    code, reply = self.sweepJobPids(req, jid, cid, reqby)
                else:
                    reply = "invalid jid for " + req
                    code = 404

            elif req == "ping":
                reply = "OK pid=" + str( os.getpid() )

            elif req == "cue":
                # begin collecting connections, or launch prep,
                # e.g. inbound netrender sockets for prman use
                self.nrmConnect( sock, event )
                return True   # *no* reply, holding sockets for app, but wake requests

            elif req == "shutdown":
                self.runState = req
                L = self.listener
                self.listener = None  # keepRunning = False
                L.close()
                reply = "received shutdown, begin exit"
                self.logger.critical(reply)
                resetBackoff = False

            elif req == "asyncDone":
                # asyncDone carries reqpending, "task request no longer pending"
                resetBackoff = False

            elif req == "ctrl":
                if 'nimby' in event:
                    id = "(none)"
                    if 'pv' in event and '_peer' in event:
                        id = event['pv'] + ":" + event['_peer']

                    reply = self.bprofile.SetNimbyOverride( event['nimby'], id )
                    self.lastHeartBt = 0
                    self.backoff = 1
                    code = 200
                    # nimby change also resets "error accrual hiatus"
                    # providing a way for users/admins to reset during tests
                    self.recentErrors = []
                    self.recentErrHiatusStart = 0
                    self.saveCmdCheckpoint()

                elif 'wake' in event:
                    reply = "waking"
                    code = 200
                    self.lastReqTm = 1 # "long ago" to ensure new request for work

            elif req == 'drain_exit':
                self.runState = 'drain_exit'
                self.logger.warning("begin drain_exit")
                reply = "begin drain_exit"
                resetBackoff = False

            elif req == '_thread_death':
                self.runState = 'drain_exit'
                self.logger.warning("blade internal thread died, begin drain_exit")

            elif req == 'profileOutOfDate':
                self.bprofile.profileOK = False  # causes refetch

            elif req == 'sentHeartbeat':
                # successful heartbeat, possibly reconnect
                self.sendDelayedExitReports( time.time() )
                resetBackoff = False

            elif req == 'launch':
                #
                # received a command!  launch it!
                #
                try:
                    cmd = event['cmd']
                    if self.launchCmd( cmd, time.time() ):
                        del cmd   # immediate exit, reported
                except Exception as e:
                    reply = "exception on launch: " + str(e)
                    code = 500

            else:
                self.logger.info("received unknown async request: " + req)
                reply = "unknown blade action: " + req
                code = 404

        if reply and sock:
            if 200 != code:
                if 0 == code:
                    code = 200
                if not filepattern:
                    self.logger.info(reply)
                reply = self.fmtSimpleReply(reply, event)

            self.httpReply(sock, code, reply, filepattern)

        if reqpend:
            # async processing complete, safe to ask for new tasks
            self.cmdReqPending = False

        # socket, if any, can be closed
        if sock: sock.close()

        return resetBackoff


    def fmtSimpleReply(self, text, event):
        """
        turn these simple replies into json so that they are compatible
        with UI queries
        """

        s = ""
        if 'jsoncallback' in event:
            s += event['jsoncallback'] + "("


        s += "{\n  \"bladereply\": \""
        s += text
        s += "\"\n}"
        if 'jsoncallback' in event: s += ")"
        s += "\n"

        return s


    # ----- #
    def httpReply(self, sock, code, text, filepattern):
        """
        simple-minded http reply for our few cases
        """
        text += "\r\n"
        sz = len(text)

        if filepattern:
            # glob expand if necessary
            sz = 0
            files = []
            files.extend( glob.glob( filepattern ))
            filepattern = files

        if sz:
            sz = 'Content-Length: %d\r\n' % sz
        else:
            sz = ""
        
        hmsgs = {
            200: "OK",
            404: "Not Found"
        }
        if code in hmsgs:
            hmsg = hmsgs[code]
        else:
            hmsg = "error"

        hdr =   'HTTP/1.0 %d %s\r\n'                                \
                'Server: Pixar tractor-blade\r\n'                   \
                'Content-Type: text/plain; charset="us-ascii"\r\n'  \
                '%s\r\n' % (code, hmsg, sz)

        # assuming a "short" message, just attach the msg to
        # the header so we get a good shot at an atomic write
        hdr += text

        # write the header
        sock.sendall(hdr)
        
        if filepattern:
            # now dump the contents of the file, it might be big
            # XXX do something higher performance here!
            for fnm in filepattern:
                f = None
                try:
                    f = open(fnm, "rb")
                    for x in f:
                        sock.sendall(x)

                    sock.sendall("\n")
                finally:
                    if f:
                        f.close()


    # ----- #
    def initiateRevisionAutoUpdate (self, newrev):
        '''begin the auto-restart process to pick up the new version'''

        self.logger.warning("profile VersionPin requires restart: " \
                            + newrev)

        # Attempt to clear any pending exit reports, since the reason
        # we are restarting may be due to actually having connected to
        # and engine again after a disconnect.
        #
        self.sendDelayedExitReports( time.time() )

        pyz = "tractor-blade-%s.pyz" % newrev

        #
        # local filename for the downloaded copy of the pyz file
        #
        tmpdir = self.bprofile.sysinfo.GetAppTempDir()
        if not tmpdir:
            self.logger.info( "no tmp dir for blade VersionPin download" )
            return False  # blade update failed

        newBlade = tmpdir + '/' + pyz

        # check to see if it has already been downloaded by a prior instance.
        if os.access(newBlade, os.R_OK):
            self.logger.debug("already downloaded: "+pyz )
        else:
            #
            # Fetch the new tractor-blade from the engine's config dir
            # (but wait a random interval in case the entire farm is doing this)
            #
            import random
            dt = 10.0 * random.random()
            self.logger.debug("collision avoidance nap: %.1f sec" % dt)
            time.sleep( dt )
            bytes = self.bprofile.fetchOtherConfigFile( pyz, False,
                                                        "&revpin="+newrev )
            if not bytes:
                self.logger.info( "blade VersionPin download failed for: "+pyz )
                return False  # blade update failed
            else:
                self.logger.debug("successfully downloaded: "+pyz )

            # bytes now contains the text of the new tractor-blade app!
            try:
                f = open( newBlade, "wb" )
                f.write( bytes )
                f.close()
            except:
                self.logger.info( self.logger.Xcpt() )
                return False  # blade update failed

        #
        # Setup to re-exec the new blade script after this blade drains.
        # NOTE:  assumes that the temp file containing the new blade script
        # will still be where we just wrote it by the time this running blade
        # finally drains!  ... could be hours or days from now
        #
        rmanpy = sys.executable
        if "win32" == sys.platform:
            rmanpy = '"'+rmanpy+'"'

        self.reExecArgs = [rmanpy, newBlade]

        self.reExecArgs.extend( sys.argv[1:] )

        self.logger.warning("beginning drain for auto update")
        self.runState = 'drain_restart'

        return True  # it *was* a blade update ... now in progress

## --------------------------------------------------- ##

