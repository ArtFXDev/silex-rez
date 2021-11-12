#
# TrBladeProfile - a class for handling tractor-blade config profiles.
#
# The tractor-blade process must connect to the tractor-engine 
# network port in order to function.  By default it attempts to
# connect to port 80 on the host named "tractor-engine" on the
# local network.  A site will typically add "tractor-engine" as
# a hostname alias in their nameserver (e.g. DNS) as an alternate
# name for whatever host the engine service is running on.
#
# An alternative is to specify the hostname directly as a
# tractor-blade startup command-line option: --engine=host:port
# however that approach mostly used only for developer testing.
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

import os, sys, platform, types, errno, time, string
import logging, copy, socket, fnmatch, urllib.request, urllib.error, urllib.parse, json
import imp

from . import TrEnvHandler
from tractor.base.TrHttpRPC import TrHttpRPC
from .TrSysState import TrSysState

#
# Note: Sites may have their own TractorSiteStatusFilter module
# installed in the SiteModulesPath ahead of the default one.
#
from . import TractorSiteStatusFilter

## --------------------------------------------------- ##

class TrBladeProfile (object):

    def __init__(self, opts, appname, appvers, apprev, appdate):
        self.mtdhost = opts.mtdhost
        self.mport = opts.mport
        self.bport = opts.bport
        self.optSlots = opts.slots
        self.profileOverride = opts.profileOverride
        self.nrmtest = opts.nrmtest
        self.nimbyOption = opts.nimby
        self.nimbyOverride = None
        self.nimbyProfile = None
        self.nimbyConnectPolicy = 1.5
        self.altZone = opts.altZone
        self.UniversalDesirabilityIndex = 1;
        self.profileParams = {}
        self.profileOK = False
        self.inService = 0
        self.cmdLogDir = None
        self.cmdOutputLogging = None
        self.substJobCWD = None
        self.allowProfileMsg = True
        self.taskBidTuning = 0
        self.lastFetchTime = time.time()
        self.profileUpdateTime = 0
        self.lastTriviaTime = 0
        self.inboundBaselineEnv = None
        self.inboundBaselineModPath = None
        self.fatalTrExitStatus = 1
        self.retainInlineLogDirectives = 0
        self.globalenv = None
        self.envhandlers = []
        self.exclusiveKeys = []
        self.countedKeys = {}
        self.afterKeys = {}
        self.lastExcuse = "starting"
        self.lastInfoExcuse = None
        self.startTime = int( time.time() )
        self.bladeDate = appdate
        self.bladeVersion = appvers
        self.bladeRevision = apprev
        self.recentErrTrigger = 5
        self.recentErrWindow = 30
        self.recentErrHiatus = 120
        self.pinnedRev = None
        self.tethered = opts.tethered
        self.tmpProfileLMT = "(not_set)"
        self.lastProfileLMT = "(not_set)"
        self.lastProfileSTUN = None
        self.lmthdr = {
            'X-Tractor-Blade': "0",
            'User-Agent': "Pixar-%s/%s (%s %s)" % \
                        (appname, appvers, appdate, apprev)
        }

        self.logger = logging.getLogger("tractor-blade")

        self.taskLoggerHostPort = ("tractor-engine", 9180)
        self.tasklogger = None
        self.socketHandler = None

        if self.nrmtest:
            self.engineRPC = TrBladeDummyRPC(self.mtdhost, self.mport)
        else:
            self.engineRPC = TrHttpRPC(self.mtdhost, self.mport,
                                        self.logger, self.lmthdr)

        # bootstrap our address as seen by engine
        x, d = self.engineRPC.Transaction("ipreflect", None, "ip discovery")
        if not x and d and "addr" in d: a = d['addr']
        else: a = None

        self.sysinfo = TrSysState(opts, a)

        self.statusFilter = TractorSiteStatusFilter.TractorSiteStatusFilter()

        if 0 >= self.optSlots:
            # default setting is -1 meaning "default", or
            # user specified "0" means: max tasks = nCPUs
            # (-1 setting allows blade.config values to apply, below)
            self.maxSlots = self.sysinfo.nCPUs  # may be reset by profile
        else:
            self.maxSlots = self.optSlots

        self.maxLoad = 1.5
        self.minRAM = self.sysinfo.physRAM * 0.05
        self.minDisk = self.minRAM * 2
        self.minDiskDrive = None
        self.svckeys = ""
        self.svckeysFromProfile = ""

        self.logger.info("Platform: %s  python %s" % \
                         (self.sysinfo.osExtPlatform, self.sysinfo.python_vers) )
        self.logger.info("HostUUID: " + self.sysinfo.hostUUID)
        self.logger.info("RAM:  %g GB" % self.sysinfo.physRAM)
        self.logger.info("CPUs: %d" % self.sysinfo.nCPUs)
        self.logger.info("GPU:  %s (%d)" % (self.sysinfo.gpu.gpuLabel,
                                            self.sysinfo.gpu.gpuCount))
        self.logger.info("engine = %s:%s" % (self.mtdhost, self.mport))

        self.staticDetails = {
            "bladeHost":        self.sysinfo.hostname,
            "bladePort":        self.bport,
            "bladeVersion":     self.bladeVersion,
            "bladeRevision":    self.bladeRevision,
            "hostBootTime":     self.sysinfo.boottime,
            "hostUUID":         self.sysinfo.hostUUID,
            "osType":           self.sysinfo.osType,
            "osInfo":           self.sysinfo.osInfo,
            "nCPUs":            self.sysinfo.nCPUs,
            "totalRAM":         self.sysinfo.physRAM,
            "gpuCount":         self.sysinfo.gpu.gpuCount,
            "gpuLevel":         self.sysinfo.gpu.gpuLevel,
            "gpuLabel":         self.sysinfo.gpu.gpuLabel
        }
        self.urlParamMap = None

        # snapshot inbound values
        self.inboundBaselineEnv = self.collectEnvironment(opts)
        self.inboundBaselineModPath = list(sys.path) # copy

        # init global env, but don't do check for default handler
        # because a blade profile has not yet been loaded
        self.initDefaultEnv(False)

        if self.altZone:
            self.dirmapZone = self.altZone
        elif sys.platform == 'win32':
            self.dirmapZone = 'unc'
        else:
            self.dirmapZone = 'nfs'

        # the profileFallback dictionary is used as the base values for
        # every inbound profile from blade.config; each incoming profile
        # is just treated as diffs/adds to these values
        self.profileFallback = {
            'ProfileName': "default",
            'Hosts': {'Platform': '*'},
            'InService': 1,
            'VersionPin': '',
            'Access': {'Crews': ['*']},
            'Provides': ['PixarRender', 'PixarNRM',
                         'RfMRender', 'RfMRibGen', 'PixarMTOR'],
            'UDI': 1,
            'NIMBY': 0,
            'Capacity': {
                'MaxSlots': self.maxSlots, # 0 -> use number of system CPUs
                'MaxLoad':  self.maxLoad,  # CPU load, normalized by cpu count
                'MinRAM':   self.minRAM,   # gigabytes, from above
                'MinDisk':  self.minDisk,  # gigabytes, from above
            },
            'SiteModulesPath': '',
            'DirMapZone': self.dirmapZone,
            'TR_EXIT_STATUS_terminate': 0,
            'RetainInlineLogDirectives': 0,
            'RecentErrorThrottle': [5, 30, 120], # 120s hiatus on 5 errs in 30s
            'CmdOutputLogging': 'logserver=${TRACTOR_ENGINE}:9180',
            'EnvKeys': [
                { 'keys': ['default'],  'envhandler': 'default' },
                { 'keys': ['setenv *'], 'envhandler': 'setenvhandler' }
            ],

            #
            # URLRequestParamsMap: each dict entry defines which
            # state entry (the keys) are sent in a task request URL.
            # For each "...&name=value&..." url parameter derived
            # from an entry in a current-state dict, the key in the
            # dict below is looked up in the inbound state dict,
            # the value tuple below is (pname, pformat) -- the
            # inbound value is formatted with pformat and written
            # into the url as "&pname=pformat(value)".
            # See mkUrlParamsFromState()
            #
            "URLRequestParamsMap": {
                "bladeHost":    ("hnm", None),
                "bladePort":    ("lp", "%d"),
                "bladeVersion": ("bvers", None),
                "bladeRevision":("brev", None),
                "hostBootTime": ("hboot", "%d"),
                "uptime":       ("uptime", "%d"),
                "hostUUID":     ("huuid", None),
                "profileName":  ("pnm", None),
                "osType":       ("ostype", None),
                "osInfo":       ("osinfo", None),
                "nCPUs":        ("nCPUs", "%d"),
                "cpuLoad":      ("cpu", "%.2f"),
                "gpuCount":     ("gpucount", "%d"),
                "gpuLevel":     ("gpulevel", "%d"),
                "gpuLabel":     ("gpulabel", None),
                "totalRAM":     ("totalmem", "%.2f"),
                "freeRAM":      ("mem", "%.3f"),
                "freeDisk":     ("disk", "%.3f"),
                "slotsAvailable": ("sa", "%d"),
                "activeCmds":   ("acids", None),
                "recentErrors": ("errs", None),
                "svckeys":      ("svckeys", None),
                "nimby":        ("nimby", None),
                "UDI":          ("udi", "%.3f"),
            }
        }

        # now create a "default" env handler
        self.envkeyList = []
        self.envkeyList.extend(self.profileFallback["EnvKeys"])
        self.initEnvHandlers()
        

    ## ------------------------------------------ ##
    def mkUrlParamsFromState (self, url, stdict,
                              defaultMap=False,
                              addtrivia=False):

        if self.urlParamMap and not defaultMap:
            umap = self.urlParamMap  # site-provided variant
        else:
            umap = self.profileFallback["URLRequestParamsMap"]

        if not addtrivia:
            now = time.time()
            if (now - self.lastTriviaTime) > 60:
                # periodically send verbose blade details anyway
                addtrivia = True
                self.lastTriviaTime = now

        for key in umap:
            if key not in stdict:
                continue

            if not addtrivia and \
                key in ("bladeRevision", "hostBootTime", "osInfo", "gpuLabel"):
                    continue  # don't send verbose strings usually

            # lookup the given value, 
            v = stdict[key]
            if v == None or v == '':
                continue  # don't send empty values

            nm, fmt = umap[key]  # url will contain roughly "&nm=fmt(val)"
            if nm == "svckeys":
                if v == self.svckeysFromProfile:
                    continue # no customization, engine can use profile copy
            elif nm == "sa":
                # special case for "slotsAvailable" formatting,
                # and also don't advertise any available slots if
                # we've never received a profile.
                if "(not_set)" == self.lastProfileLMT:
                    v = "0,0,0"
                else:
                    v = "%d,%d,%d" % (v, self.maxSlots, stdict["slotsInUse"])
            elif nm == "nimby":
                # nimby may be a user name, or 0/1
                v = urllib.parse.quote( str(v) )
            elif nm == "udi":
                v = float( v )
                #if v == float( self.UniversalDesirabilityIndex ):
                #   continue # no customization, engine can use profile copy
                v = fmt % v

            elif fmt:
                v = urllib.parse.quote( fmt % v )

            else:
                # just use 'v' as is
                if type(v) != str:
                    v = str(v)
                v = urllib.parse.quote( v )

            url += "&" + nm + "=" + v

        if self.lastExcuse:
            # excuse may contain arbitrary chars
            url += "&note=" + urllib.parse.quote( self.lastExcuse )

        return url


    def captureProfileXHdrs (self, htxt, errcode):
        # Extract the special tractor last-modified header field here.
        # Then store the LMT for *return* to the engine on future
        # requests.  It is used like a cookie to validate that we're
        # operating from the current blade.config. 
        # Also collect the (2.0) "STUN" header giving us back our
        # ip address (route) as seen by the engine, in case we are
        # behind a NAT (or VPN that is changing) or on a machine 
        # with multiple active interfaces.

        if 0 == errcode:
            n = htxt.find('\nX-Tractor-Lmt')
            if (n > 0):
                e = htxt.find('\r\n', n)
                self.tmpProfileLMT = htxt[n:e].split()[1]

            n = htxt.find('\nX-Tractor-STUN')
            if (n > 0):
                e = htxt.find('\r\n', n)
                self.lastProfileSTUN = htxt[n:e].split()[1]


    def chkAddrSeenByEngine (self):
        # the 2.0+ engine will send the client's observed addr as a header
        # otherwise we ask the 1.x engine in a separate transaction

        try:
            if not self.lastProfileSTUN:
                x, d = self.engineRPC.Transaction("ipreflect", None,
                                                    "ip discovery")
                if not x and d and "addr" in d:
                    self.lastProfileSTUN = d['addr']

            if self.lastProfileSTUN:
                self.sysinfo.resolveHostname( self.lastProfileSTUN )
        except:
            pass


    def fetchProfiles (self, bstate, now):
        #
        # Fetch the server profiles from the central server.
        # If this fails then we need to "be alive"
        # but inactive while periodically retrying.
        #
        if self.nrmtest:
            self.logger.info("blade profile load skipped during nrmtest")
            try:
                self.applyProfile("nrmtest", copy.deepcopy( self.profileFallback ))
            except:
                pass
            self.profileOK = True
            return

        cfgfile = "blade.config"
        cfghost = "%s:%d" % (self.engineRPC.host, self.engineRPC.port)
        self.logger.debug("requesting %s/%s" % (cfghost, cfgfile))

        url = self.mkUrlParamsFromState( "config?q=profiles&file=" +
                                urllib.parse.quote(cfgfile), bstate, True )

        # make the http request for the config file, and also extract 
        # the tractor custom 'Last Modified Time' id from the headers
        errcode,reply = \
            self.engineRPC.Transaction(url, None, cfgfile, {}, None,
                                        self.captureProfileXHdrs)
        if errcode:
            if self.allowProfileMsg:
                self.logger.info("%s %s %s (code %d)" % \
                            (cfgfile, cfghost, reply, errcode))

            if self.tethered:
                self.logger.critical("blade lost engine tether, exit")
                sys.exit(62);

        elif not reply:
            self.logger.info("engine sent empty reply for %s/%s" % \
                                (cfghost, cfgfile))
        else:
            try:
                self.chkAddrSeenByEngine()
                self.staticDetails['bladeHost'] = self.sysinfo.hostname

                if 'TRACTOR_ENGINE' not in self.inboundBaselineEnv:
                    a = self.engineRPC.GetLastPeerQuad() # addr:port
                    self.globalenv['TRACTOR_ENGINE'] = a
                    self.globalenv['TRACTOR_MONITOR'] = a

                pdefaults = copy.deepcopy( self.profileFallback )

                # optional ProfileDefaults
                if 'ProfileDefaults' in reply:
                    self.recursiveUpdate( pdefaults, reply['ProfileDefaults'] )

                altEnumGPU = pdefaults.get('EnumPlatformGPU')
                altExcludeGPU = pdefaults.get('GPUExclusionPatterns')
                if not altEnumGPU:
                    altEnumGPU = reply.get('EnumPlatformGPU')
                if not altExcludeGPU:
                    altExcludeGPU = reply.get('GPUExclusionPatterns')
                if altEnumGPU or altExcludeGPU:
                    #
                    # blade.config contains an alternate GPU discovery scheme or
                    # filter, defined inside ProfileDefaults or at the root level.
                    # So run that to regenerate gpu info in staticDetails, then
                    # we can run the profile matching tests on the new attributes
                    #
                    self.sysinfo.gpu.ResolveGPUInfo( altEnumGPU, altExcludeGPU )
                    self.logger.info("GPU: (after exclusions) %s (%d)" % \
                        (self.sysinfo.gpu.gpuLabel, self.sysinfo.gpu.gpuCount))
                    self.staticDetails.update( {
                        "gpuCount":  self.sysinfo.gpu.gpuCount,
                        "gpuLevel":  self.sysinfo.gpu.gpuLevel,
                        "gpuLabel":  self.sysinfo.gpu.gpuLabel
                    } )

                bstate.update( self.staticDetails )

                # required BladeProfiles
                proflist = reply.get('BladeProfiles')
                if proflist:
                    n = len(proflist)
                    s = ("" if n == 1 else "s") # plural
                    self.logger.info( "received %d profile%s from %s" % \
                                        (n, s, cfghost))

                    self.profileOK = \
                        self.findMatchingProfile(proflist, pdefaults)

                    self.allowProfileMsg = True
                    self.lastFetchTime = time.time()

                else:
                    self.logger.error("missing required BladeProfiles in %s/%s" % \
                                (cfghost, cfgfile))

            except Exception:
                self.logger.error("%s/%s %s" % \
                            (cfghost, cfgfile, self.logger.Xcpt()))

        if self.profileOK and 0 == len(self.envhandlers):
            # a profile was loaded, but it defined no envhandlers
            self.logger.debug("no handlers defined, loading default")
            self.envkeyList = []
            self.envkeyList.extend(self.profileFallback["EnvKeys"])
            self.initEnvHandlers()


    def fetchOtherConfigFile(self, filename, doparse=True, addopts=None):
        # Fetch an arbitrary file from the config directory
        self.logger.debug("requesting config/"+filename)
        cfg = "config?q=get&file=" + urllib.parse.quote( filename )

        if addopts:
            cfg += addopts

        ctx = filename if doparse else None

        errcode,reply = self.engineRPC.Transaction(cfg, None, ctx)

        if errcode:
            self.logger.debug("fetch config: %s %s %s %d" % \
                        (filename, self.mtdhost, reply, errcode))
            reply = None

        return reply


    def recursiveUpdate (self, A, B):
        '''
        dictionary merge, similar to A.update(B) but recursive
        caller must be careful about copy by reference issues
        '''
        for bkey,bval in B.items():
            if bkey in A and isinstance(A[bkey], dict):
                self.recursiveUpdate( A[bkey], bval )
            else:
                A[bkey] = bval


    def findMatchingProfile (self, profiles, pdefault):
        #
        # Extract config data from dict supplied by tractor-engine
        # and apply policy settings.  Also save relevant values
        # for use in future task requests.
        #

        try:
            ##
            ## now loop over the profiles, in order, looking for a match
            ##
            for p in profiles:
                pd = copy.deepcopy( pdefault )
                self.recursiveUpdate( pd, p )

                pname = pd['ProfileName']

                ##
                ## determine if this profile matches this server,
                ## that is:  are we one of its target blades?
                ##
                self.logger.trace("profile match test: %s" % pname)

                if pname == self.profileOverride or \
                   self.testProfileMatch(pd['Hosts'], pname):
                       return self.applyProfile( pname, pd )

            self.logger.warning("NO profile matches: " + self.sysinfo.hostname)

        except Exception:
            self.logger.error("applyProfile: '%s' %s" % \
                                (pname, self.logger.Xcpt()))

        return False  # unable to apply the profile


    def prfInt (self, dct, key, dflt=0):
        try:
            return int( dct[key] )
        except:
            self.logger.info("NOTE blade.config expected integer for "+key)
            return dflt

    def prfFloat (self, dct, key, dflt=0):
        try:
            return float( dct[key] )
        except:
            self.logger.info("NOTE blade.config expected a float for "+key)
            return dflt


    def applyProfile (self, pname, pd):
        #
        # note: we log this msg with our now-current
        # hostname, every time we bind to a new profile
        # (typically only once, at startup)
        #
        self.logger.info("applying matched profile: " + pname)
        self.logger.printDict(pd, "Matched Profile") # trace level

        #
        # Build up a tmp dict of the new profile parameters, then apply
        # them to the profile object. However, if parsing fails then
        # we'll throw the exception and leave the currently running
        # profile values intact.  Thus: be very careful about changes
        # to "self" here!
        #
        newprf = {
            "profileParams": {},
            "exclusiveKeys": [],
            "countedKeys": {},
            "afterKeys": {},
        }
        
        newprf['inService'] = self.prfInt( pd,'InService', 1 )

        newprf['fatalTrExitStatus'] \
            = self.prfInt( pd,'TR_EXIT_STATUS_terminate', 1 )

        newprf['retainInlineLogDirectives'] \
            = self.prfInt( pd,'RetainInlineLogDirectives', 0 )

        newprf['svckeysFromProfile'] = ",".join(pd['Provides'])

        for key in pd['Provides']:
            #
            # look for special service annotations from blade.config Provides,
            # like:
            #   "benchmark(X)" -- eXclusive no other work while this is in use
            #   "admintest(R)" -- RemoteCmd *must* request this Required key
            #   "prman(max:3)" -- no more than 3 of these in use at a time
            #   "nuke(after:prman)" -- nuke "filler" only after prman main
            #
            # beware lists:  nuke(after:prman,max:2)
            #
            p = key.find('(')
            if p >= 0:
                q = key.find(')', p)
                mods = key[p+1:q].lower()
                key = key[:p].lower()

                # annotations in 'mods' can be a list!
                for x in mods.split(','):
                    if x == 'X':
                        newprf['exclusiveKeys'].append( key )

                    x,sep,v = x.partition(':')
                    if x == "max" and 0 < int(v):
                        newprf['countedKeys'][key] = int(v)
                    if x == "after":
                        newprf['afterKeys'][key] = v

        if not self.altZone:
            newprf['dirmapZone'] = pd['DirMapZone']
            self.logger.debug("dirmapZone: " + newprf['dirmapZone'])

        if -1 == self.optSlots:
            # no blade cmdline slots setting in place, so allow
            # the profile to override, otherwise use defaults
            # applied above.
            slots = self.prfInt(pd['Capacity'],'MaxSlots', None)
            if slots is not None:   # (as distinct from zero)
                if slots <= 0:
                    # site specifies: max tasks = nCPUs
                    newprf['maxSlots'] = self.sysinfo.nCPUs
                else:
                    newprf['maxSlots'] = slots

        newprf['maxLoad'] = self.prfFloat(pd['Capacity'],'MaxLoad', 1)
        newprf['minRAM']  = self.prfFloat(pd['Capacity'],'MinRAM', 0.25)

        newprf['minDisk'] = 0.1
        newprf['minDiskDrive'] = None
        minDisk = pd['Capacity']['MinDisk']
        if minDisk:
            d = minDisk
            if type(d) == list:
                newprf['minDiskDrive'] = d[1]
                d = d[0]
            try:
                newprf['minDisk'] = float(d)
            except:
                self.logger.info("NOTE blade.config expected numeric 'minDisk'");

        if 'RecentErrorThrottle' in pd:
            # expect [count, span, hiatus] like [5, 30, 120]
            # like: 5 errs within 30 secs causes 120 second hiatus
            #       in requests for new work from this blade.
            # -1 for hiatus causes an auto-nimby instead.
            #  0 for count disables the feature
            #
            # this is an advanced feature, syntax errors here raise
            # exceptions that scrap the new profile load
            #
            v = pd['RecentErrorThrottle']
            if v:
                if len(v) < 2: v.append(30)
                if len(v) < 3: v.append(120)
                newprf['recentErrTrigger'] = int(v[0])
                newprf['recentErrWindow']  = int(v[1])
                newprf['recentErrHiatus']  = int(v[2])

        newprf['UniversalDesirabilityIndex'] = self.prfFloat(pd,'UDI')
        newprf['nimbyProfile'] = pd['NIMBY']
        v = self.nimbyConnectPolicy
        a = pd['Access']
        if 'NimbyConnectPolicy' in a:
            try:
                newprf['nimbyConnectPolicy'] = float( a['NimbyConnectPolicy'] )
            except:
                pass

        if "URLRequestParamsMap" in pd:
            newprf['urlParamMap'] = pd["URLRequestParamsMap"]
        else:
            newprf['urlParamMap'] = None

        #
        # TaskBidTuning controls the aggressiveness of the blade with
        # respect to how quickly it asks for new tasks when there are
        # slots available, such as when a prior task exits. Older blades
        # always waited at least "minsleep" seconds in order to allow
        # some interleaving of requests from other blades on the engine.
        # This setting might someday be a range of policy values like
        # "conservative", "balanced", "greedy", or more role-based
        # like "workhorse", "alternate", or "reserves"
        # but for now it is basically just a boolean picking between
        # the old minsleep wait and an immediate request for work
        # on prior task exit IF the exit was ok, non-error. If the
        # engine responds with "no work available right now" then
        # the blade will begin the doubling backoff waits as usual.
        #
        if "TaskBidTuning" in pd:
            v = pd["TaskBidTuning"] in (1, "immediate", "greedy")
        elif "BringItOnExit" in pd:
            v = (1 == pd["BringItOnExit"])  # early undocumented experiments
        if v:
            newprf['taskBidTuning'] = "immediate"
        else:
            newprf['taskBidTuning'] = "sleepy"
        
        v = None
        if "SubstituteJobCWD" in pd:
            v = pd['SubstituteJobCWD']
        if v:
            newprf['substJobCWD'] = v

        newprf['lastProfileLMT'] = self.tmpProfileLMT

        if 'CmdOutputLogging' in pd and pd['CmdOutputLogging']:
            self.setupLogging(pd['CmdOutputLogging'])

        newprf['envkeyList'] = []
        for entry in pd['EnvKeys']:
            if type(entry) in (str, str):
                offset = entry.find("@merge(")
                if offset != -1:
                    # call self.merge('f1', 'f2', 'f3')
                    mergestr = "self." + entry[offset + 1:].strip()
                    try:
                        entry = eval(mergestr)
                        self.logger.trace("merging env list: " \
                                            + str(entry) )
                    except:
                        continue
            if type(entry) == list:
                newprf['envkeyList'].extend( entry )
            elif type(entry) == dict:
                newprf['envkeyList'].append( entry )
            else:
                self.logger.info("unexpected item in EnvKeys list")

        if len(newprf['envkeyList']) == 0:
            newprf['envkeyList'].extend(self.profileFallback["EnvKeys"])

        #
        # Having gotten this far, apply the inbound settings to the
        # real profile, prior to re-init of the envhandlers.
        # Do this by overlaying the newprf dict that we just collected
        # onto our object __dict__ item to update our *attributes*
        # all at once.
        #
        self.__dict__.update(newprf)

        ## ---- now procede with the new profile settings ---- ##

        self.lmthdr['X-Tractor-Blade'] = self.lastProfileLMT

        self.profileParams = {
            "profileName":  pname,
            "isInService":  self.inService,
            "maxSlots":     self.maxSlots,
            "maxLoad":      self.maxLoad,
            "minRAM":       self.minRAM,
            "minDisk":      self.minDisk,
            "minDiskDrive": self.minDiskDrive,
            "svckeys":      self.svckeysFromProfile,
            "nimby":        self.GetNimbyStatus(),
            "UDI":          self.UniversalDesirabilityIndex
        }

        v = self.bladeVersion + '-' + self.bladeRevision
        pin = pd.get("VersionPin", '')
        self.logger.debug("v=%s, profile vpin='%s'" % (v, pin))
        if pin and v != pin:
            # profile requires version update ... which we may refuse later
            self.pinnedRev = pin

        #
        # if SiteModulesPath specified, add that to the sys.path
        # for importing custom envhandlers.  May be colon
        # separated.  Add the given path items before the
        # original inbound sys.path, creating a new sys.path
        # for this bound profile.
        #
        if 'SiteModulesPath' in pd and pd['SiteModulesPath']:
            ppaths = pd['SiteModulesPath'].split( os.pathsep )
            self.logger.debug("add module paths: " + repr(ppaths))

            ppaths.extend( self.inboundBaselineModPath )
            sys.path = list(ppaths) # copy

        # instantiate all listed envkey handlers,
        # also modifies the globalenv dictionary in place
        self.initEnvHandlers()

        # --- #
        siteFilterName = "TractorSiteStatusFilter"
        try:
            if siteFilterName in sys.modules:
                del sys.modules[siteFilterName]

            (file, path, desc) = imp.find_module(siteFilterName)
            mod = imp.load_module(siteFilterName, file, path, desc)
            if self.statusFilter:
                del self.statusFilter
                self.statusFilter = mod.TractorSiteStatusFilter()
        except ImportError:
            pass # TractorSiteStatusFilter not found, continue with prior
        except:
            self.logger.warning("error attempting to load '%s' %s" % \
                                 (siteFilterName, self.logger.Xcpt()))
        # --- #

        self.profileUpdateTime = time.time()

        self.logger.info("Begin service as Profile: "+pname)

        self.logger.info("max slots = %d (on %d CPUs)" % \
                    (self.maxSlots, self.sysinfo.nCPUs))

        return True  # becomes self.profileOK


    def revisionUpdateNeeded (self):
        # returns new required version exactly ONCE
        if self.pinnedRev:
            v = "" + self.pinnedRev
            self.pinnedRev = None
            return v
        else:
            return None

    def merge(self, *mergelist):
        self.logger.trace("EnvKeys @merge: " + str(mergelist))
        merged=[]
        for file in mergelist:
            try:
                jarray = self.fetchOtherConfigFile(file)
            except Exception as e:
                self.logger.error("error fetching config file: "+file+" "+str(e))
            if type(jarray) == dict:
                self.logger.error("incorrect format merge file found: %s" % file)
                continue
            merged.extend(jarray)
            
        return merged
    
    def makeCmdLogDir(self, user=None, jid=None, tid=None, cid=None, rev=None):
        if self.cmdLogDir == None:
            return # using python "logging" module log server, not files
        
        dirname = self.cmdLogDir
        try:
            (type, sep, loginfo) = self.cmdOutputLogging.partition("=")
            fname = loginfo.replace("%u", user).replace("%j", str(jid)).replace("%t", str(tid))
            dirname = os.path.dirname(fname)
        except:
            pass
        
        try:
            if not os.access(dirname, os.W_OK+os.X_OK):
                oldumask = os.umask(0)
                os.makedirs(dirname)
                os.umask(oldumask)

        except Exception as e:
            # it is acceptable for the directory to already exist
            if e[0] not in \
                (errno.EEXIST, errno.ERROR_ALREADY_EXISTS):
                self.logger.error("cmd output dir: " \
                        + dirname + " " \
                        + self.logger.Xcpt())


    def setupLogging (self, logtemplate):
        ''' (re)connect to the log server, or establish direct write paths'''
        # NOTE: self.cmdLogDir == None indicates log server logging
        #       self.cmdLogDir will point to root log directory if direct logging
        
        self.cmdOutputLogging = logtemplate
        (logtype, sep, loginfo) = logtemplate.partition("=")
        logtype = logtype.lower()

        if logtype == "logserver":
            # Prep for connections to the remote tractor-cmdlogger.py server.
            # Note that actual connections are created as needed, and
            # torn down when this blade becomes idle, in an effort to
            # minimize open descriptors and use of network state, etc.

            self.CloseTaskLoggerConnection() # clean up prior, if any
    
            if '$' in loginfo:
                # expand env var refs
                e = string.Template(loginfo)
                loginfo = e.safe_substitute( self.globalenv )

            (host, _, port) = loginfo.partition(":")
            if host == "": host = "tractor-logger"
            if ":" in port:
                _,__,port = port.partition(":")  # like harpy:80:9180
            if port == "": port = 9180
            port = int(port)

            self.taskLoggerHostPort = (host, port)

            if self.cmdLogDir:
                self.cmdLogDir = None
    
        elif logtype == "logfile":
            # attempt to find a reasonable log dir root
            self.CloseTaskLoggerConnection() # if changing to direct write
            dir = os.path.dirname(loginfo[:loginfo.find("%")])
            self.cmdLogDir = dir.rstrip("/")  #remove trailing slash
            self.makeCmdLogDir()

        else:
            self.logger.warning("Invalid cmdOutputLogging entry: %s" % logtemplate)

        self.logger.info("CmdOutputLogging: " + self.cmdOutputLogging)


    def OpenTaskLoggerConnection (self, reuse=True):
        '''(re)open task logger connection to remote tractor-cmdlogger.py'''
        if reuse and self.socketHandler:
            return
        else:
            self.CloseTaskLoggerConnection() # clean up prior

        host, port = self.taskLoggerHostPort

        self.tasklogger = logging.getLogger('tasklogger')
        self.tasklogger.setLevel(logging.DEBUG)
        self.socketHandler = logging.handlers.SocketHandler(host, port)
        self.tasklogger.addHandler(self.socketHandler)


    def CloseTaskLoggerConnection (self):
        '''close task logger connection to remote tractor-cmdlogger.py'''
        if self.tasklogger != None:
            if self.socketHandler:
                self.socketHandler.close()
                self.tasklogger.removeHandler(self.socketHandler)
                self.socketHandler = None
            self.tasklogger = None

    
    def getEnvHandler(self, envkey):
        if not envkey:
            return None

        for h in self.envhandlers:
            if h.handlesEnvkey(envkey):
                return h

        self.logger.debug("no handler for envkey: '"+envkey+"', using default")
        return None


    def initEnvHandlers(self):
        #
        # instantiates envkey handler objects,
        # flatten incoming envkey dictionary, and apply
        # that into the incoming global env to make a launch env
        #

        # this is an init action.  Any previous envhandlers should be removed
        # code below will handle modules, if they are being reloaded.
        self.envhandlers=[]
        
        for envdict in self.envkeyList:
            if 'envhandler' not in envdict:
                raise Exception("no 'envhandler' defined in the profile")

            handlerfile = envdict['envhandler']
            handlername = "TrEnvHandler.%s" % handlerfile
            keys = envdict['keys']

            #
            # This might be a reload of config files.  So remove the
            # handler from sys.modules and self.envhandlers so it can
            # be loaded from potentially different SiteModulesPath
            #
            if handlerfile in sys.modules:
                del sys.modules[handlerfile]
                self.logger.debug("removing sys.module : " + handlerfile)

            try:
                handler = None
                self.logger.trace("check for built-in env handler: " + handlername)
                exec('handler = %s(handlerfile, envdict, keys)' % handlername)
            except:
                handlername = "%s.%s" % (handlerfile, handlerfile)
                self.logger.trace("importing env handler: " + handlername)
                try:
                    handler = None
                    exec('import %s' % handlerfile)
                    exec('handler = %s(handlerfile, envdict, keys)' % handlername)
                    self.logger.debug("env handler imported: " + handlername)
                except:
                    self.logger.error("TrEnvHandler setup error: %s" % self.logger.Xcpt())

            if handler:
                self.envhandlers.append(handler)

        self.initDefaultEnv(True)


    def initDefaultEnv (self, checkdefaulthandler):
        # We're going to create a new default env for cmd launches
        # using the settings from the (new) profile.  The "globalenv"
        # is always initialized from the original inbound start-up
        # environment.
        self.globalenv = self.inboundBaselineEnv.copy()
        self.logger.printDict(self.globalenv, "Global Environment")

        if checkdefaulthandler:
            defaultHandler = self.getEnvHandler('default')
            if not defaultHandler:
                self.logger.error("Default handler not defined");

        if 'TRACTOR_ENGINE' not in self.globalenv:
            self.globalenv['TRACTOR_ENGINE'] = self.mtdhost
        if 'TRACTOR_MONITOR' not in self.globalenv:
            self.globalenv['TRACTOR_MONITOR'] = self.mtdhost  # bkwd compat


    def applyEnvHandlers (self, cmd):
        '''
        Apply the env handlers from the current profile to create a
        custom set of env vars for ONE launch, and also to optionally
        modify the incoming cmdline itself (cmd.argv)
        '''

        inboundargv = cmd.argv[:]  # save for reference below

        # first, COPY the globalenv to make a modifiable env for this cmd
        env = self.globalenv.copy()

        # the cmd's envkey can be a list or simple string, force to list
        # and ensure that the key "default" is present
        envkey = cmd.envkey
        if type(envkey) in (str, str):
            envkey = [envkey]

        if "default" not in envkey:
            envkey.insert(0, "default")

        #
        # Collect a list of unique handlers and the command keys that
        # that matched THIS current cmd.  Note that one handler may
        # handle several keys.  Also create a temporary reverse map
        # from the handler names to keys requested by this command.
        #
        handlers = []
        hnm2keys = {}

        for key in envkey:
            envHnd = self.getEnvHandler(key)
            if envHnd:
                if envHnd not in handlers:
                    handlers.append(envHnd)
                if envHnd.name in hnm2keys:
                    hnm2keys[envHnd.name].append(key)
                else:
                    hnm2keys[envHnd.name] = [key]

        # First perform updateEnvironment for ALL handlers to get
        # the aggregate env settled.
        for envHnd in handlers:
            env = envHnd.updateEnvironment(cmd, env, hnm2keys[envHnd.name])

        # THEN apply each remapCmdArgs to expand env var references
        # in the commandline args, using the aggregate env
        for envHnd in handlers:
            cmd.argv = envHnd.remapCmdArgs(cmd, env, cmd.bladehost)

        if inboundargv != cmd.argv:
            cmd.app = cmd.argv[0].split('/')[-1]
            self.logger.debug("cmdline remapped by envhandler(s): " + \
                                " ".join([h.name for h in handlers]) + \
                                "\n            argv: " + \
                                json.dumps(cmd.argv, ensure_ascii=True))

        return env


    def collectEnvironment(self, opts):
        '''
        Obtain the "baseline" incoming environment prior to
        per-profile manipulation.  It will be restored as
        the baseline whenever a new profile is loaded.
        '''
        outenv = os.environ.copy()

        # The following three variables are special cased in the blade, and
        # optionally get restored.  This is done prior to parsing the 
        # envfile, if specified in the commandline options.

        # PYTHONHOME may need to be restored to the site value
        if opts.pythonhome != None and opts.pythonhome != "None":
            outenv['PYTHONHOME'] = opts.pythonhome

        # LD_LIBRARY_PATH or DYLD_FRAMEWORK_PATH must be set in the blade
        # launch environment.  Delete these from the environment, but
        # restore IF the commandline arguments were not set to "None"

        if 'DYLD_FRAMEWORK_PATH' in outenv:
            del outenv['DYLD_FRAMEWORK_PATH']
        if opts.dyld_framework_path and not opts.dyld_framework_path == "None":
            outenv['DYLD_FRAMEWORK_PATH'] = opts.dyld_framework_path

        if 'LD_LIBRARY_PATH' in outenv:
            del outenv['LD_LIBRARY_PATH']
        if opts.ld_library_path and opts.ld_library_path != "None":
            outenv['LD_LIBRARY_PATH'] = opts.ld_library_path


        envfile = opts.envfile
        if len(envfile) > 0:
            self.logger.info("restore envfile: " + envfile)
            try:
                f = open(envfile, "r")
                data = f.readlines()
                f.close()
                #os.unlink(envfile)
                self.logger.debug("%d incoming environment items" % len(data))

                outenv = {}
                for line in data:
                    (var,sep, val) = line.partition("=")
                    outenv[var] = val.rstrip("\n")

            except Exception:
                # there''s been an error opening the file treat
                # as if there is no file
                self.logger.warning("env file '%s' %s" % \
                                   (envfile,self.logger.Xcpt()))
            

        return outenv

    def testProfileMatch(self, matchers, pname):
        #
        # First failed test eliminates this profile,
        # otherwise, ALL tests must succeed.
        #
        for mkey in matchers:
            pattern = matchers[mkey]

            if 'Name' == mkey:
                if type(pattern) != list:
                    pattern = [pattern]
                found = False
                for pp in pattern:
                    if self.chkHostMatch(pp):
                        found = True
                        break
                if not found:
                    return False

            elif 'Platform' == mkey:
                if not fnmatch.fnmatch(self.sysinfo.osExtPlatform, pattern):
                    return False

            elif 'NCPU' == mkey or 'NCores' == mkey:
                if int(pattern) != int(self.sysinfo.nCPUs):
                    return False

            elif 'MinNCPU' == mkey:
                if int(pattern) > int(self.sysinfo.nCPUs):
                    return False

            elif 'MinNGPU' == mkey:
                if int(pattern) > int(self.sysinfo.gpu.gpuCount):
                    return False

            elif 'GPU.count' == mkey:
                if int(pattern) != int(self.sysinfo.gpu.gpuCount):
                    return False

            elif 'GPU.label' == mkey:
                if type(pattern) != list:
                    pattern = [pattern]
                found = False
                for pp in pattern:
                    if fnmatch.fnmatch(self.sysinfo.gpu.gpuLabel, pp):
                        found = True # don't return True here, other tests run
                if not found:
                    return False

            elif 'GPU.level' == mkey:
                # abstract tractor gpu capability number
                if int(pattern) > int(self.sysinfo.gpu.gpuLevel):
                    return False

            elif 'GPU.tags' == mkey:
                # abstract tractor gpu capability string
                if not fnmatch.fnmatch(self.sysinfo.gpu.gpuTags, pattern):
                    return False

            elif 'MinPhysRAM' == mkey:
                if float(pattern) > float(self.sysinfo.physRAM):
                    return False

            elif 'PhysRAM' == mkey:
                if int(pattern) != int(self.sysinfo.physRAM):
                    return False

            elif 'PathExists' == mkey:
                if type(pattern) != list:
                    pattern = [pattern]
                for pp in pattern:
                    if not os.path.exists(pp):
                        return False

            else:
                self.logger.error("profile %s, unknown Hosts key: '%s'" % \
                            (pname, mkey))
                return False
        return True


    def chkHostMatch(self, hpattern):
        # check all hostnames and addresses for this host
        if fnmatch.fnmatch(self.sysinfo.hostname, hpattern):
            return True

        for a in self.sysinfo.aliases:
            if fnmatch.fnmatch(a, hpattern):
                return True

        for a in self.sysinfo.addrs:
            if fnmatch.fnmatch(a, hpattern):
                return True

        # Last resort, check whether 'pattern' resolves to a known hostname,
        # and if so whether its address is the same as (one of) ours. This 
        # is the only way to match names-by-alias at a site where names are
        # resolved via dns, which does not return aliases directly via
        # gethostbyname.
        if -1==hpattern.find('*') and -1==hpattern.find('?'):
            try:
                nm,al,paddr = socket.gethostbyname_ex(hpattern)
                for a in self.sysinfo.addrs:
                    if a in paddr:
                        return True
            except Exception:
                pass

        return False


    def getEngineHost(self):
        return (self.mtdhost, self.mport)


    def GetProfileState (self, bstate, now):
        #
        # Fetch blade.config from the engine, if necessary,
        # then return the updated state values dict to
        # include the defns from the current profile.
        #
        bstate.update( self.staticDetails )

        if ( not self.profileOK or
            (not self.inService and (now - self.lastFetchTime) > 300.0)):
                self.fetchProfiles( bstate, now )

        if self.profileOK and self.profileParams:
            bstate.update( self.profileParams )
            rc = True
        else:
            # still not acquired
            x = "blade profile load failed"
            self.logger.trace( x + ", retrying" )
            self.SetExcuse( x )
            rc = False

        bstate["excuse"] = self.GetExcuse()
        return rc


    def GetProfileName (self):
        if self.profileOK and "profileName" in self.profileParams:
            return urllib.parse.unquote( self.profileParams['profileName'] )
        else:
            return "(none_yet_loaded)"


    def GetAvailableDisk (self, drivepath=None):
        #
        # call the TrSysState routine, but default the path
        # to the one specified in the current profile, if set.
        #
        if not drivepath:
            drivepath = self.minDiskDrive

        return self.sysinfo.GetAvailableDisk(drivepath)


    def GetNimbyStatus (self, special=None):
        #
        # nimby initialized from profile setting,
        # blade cmdline option overrides that,
        # nimby ctrl msg to the running blade overrides that
        #
        if special == "override":
            # saving chkpt, only return the override value
            # can't be None (or json null) for eval bkwd compat
            if None == self.nimbyOverride or "0"==self.nimbyOverride:
                return 0
            else:
                return self.nimbyOverride

        if self.nimbyOverride:
            nimby = self.nimbyOverride  # may be "0" meaning explicit enable

        elif self.nimbyOption:
            nimby = self.nimbyOption

        else:
            nimby = self.nimbyProfile
        
        if "1" == str(nimby) and special != "report":
            nimby = "/local/" + self.sysinfo.hostname
        elif "0" == str(nimby):
            nimby = None  # meaning no nimby restrictions

        return nimby


    def SetNimbyOverride (self, nby, reqID):
        #
        # Set the current nimby state, usually from ctrl
        # msg to the blade.  Overrides the nimby value
        # from the profile and/or cmdline options.
        #
        # Thus if the blade is started with "tractor-blade --nimby=1"
        # it will allow only local Cmds. If a wrangler uses the UI
        # to "turn nimby off" it will send an override here which
        # will override the cmdline option.  So there is no way from
        # the current UI to say "revert to whatever the cmdline or
        # the profile define" -- although the engine's URL used by
        # the UI does support sending "nimby=default" which will
        # unset the override.  A restarted blade will read the last
        # override setting from a checkpoint file and restore that,
        # unless the last setting was "nimby=0" in which case the
        # restarted blade will revert to using its cmdline or profile
        # settings.  This is a policy decision that could change if
        # we decide to support a more detailed (harder to explain)
        # set of menu options for nimby in the dashboard.
        #
        # 1.5 introduced nimby changes proxied through the engine,
        # both to allow for crew-based restrictions on who can
        # change nimby state, as well as avoiding the need for 
        # network routes from the UI (browser) to all farm machines.
        # To re-enable direct connect nimby changes from external
        # scripts, add the following to the target blade profiles:
        #   "your_profilename": { "Access": {"NimbyConnectPolicy": 1.0} }
        #
        if reqID and self.nimbyConnectPolicy >= 1.5:
            engID = self.lastProfileLMT + ":" + \
                    self.engineRPC.GetLastPeerQuad().split(':')[0]
            if reqID != engID:
                # Note - the IDs may not match under the following race
                # condition: If an admin (or cron) has caused the engine
                # to reload blade.config and then immediately sent one or
                # more nimby change directives to the engine, then the
                # engine may asynchronously send the nimby change to each
                # blade before the blade has lazily loaded the new profiles.
                # In that window, the blade.config timestamp-based ID will
                # not match. For now we workaround this case by just
                # confirming that the engine addr is the same. Ideally
                # we would force the blade update in this case, then
                # (re)send the nimby change.

                self.logger.debug("nimby id: " + reqID + " expect: " + engID)
                if reqID.split(':')[1] == engID.split(':')[1]:
                    # engine ip addrs match, so this is likely just the
                    # blade.config reload race condition described above.
                    self.logger.info("nimby, new blade.config not yet loaded?")
                else:
                    self.logger.info("NIMBY verification required, no change")
                    return '{"rc": 2, "msg": "nimby verification failed"}'

        if "default" == nby or "revert"==str(nby):
            self.nimbyOverride = None
        else:
            self.nimbyOverride = nby

        nimby = self.GetNimbyStatus()

        self.profileParams['nimby'] = nimby

        self.logger.info("NIMBY mode: " + str(nimby))

        return '{"rc": 0, "msg": "nimby '+ str(nimby) +'"}'


    def IsInService(self):
        return self.inService


    def GetExcuse (self):
        return self.lastExcuse;


    def SetExcuse (self, msg):
        xchanged = (msg != self.lastExcuse)
        if msg and msg != self.lastInfoExcuse:
            self.lastInfoExcuse = msg
            if msg.startswith("no free slots"):
                self.logger.debug("pass: " + msg)
            else:
                self.logger.info("pass: " + msg)

        if not msg and 'nimby' in self.profileParams \
            and self.profileParams['nimby']:
            msg = "nimby, (allow " + str( self.profileParams['nimby'] ) + ")"

        self.lastExcuse = msg  # may be None

        return xchanged

## --------------------------------------------------- ##
#
# TrBladeDummyRPC is a stand-in for TrHttpRPC
# used only in development with --nrmtest for "netrender unit tests"
#
class TrBladeDummyRPC (object):

    def __init__(self, host, port=80, logger=None,
                apphdrs={}, urlprefix="/Tractor/", timeout=65.0):
        h,_,p = host.partition(':')
        self.host = h
        if p:
            self.port = p
        else:
            self.port = port
    

    def Transaction (self, tractorverb, formdata, parseCtxName=None,
                     xheaders={}, preAnalyzer=None, postAnalyzer=None):

        try:
            v = tractorverb.split('?')[0].split('/')[-1]
            if v == "ipreflect":
                return (0, {'addr': '127.0.0.1'})
            elif v == "btrack":
                return (0, {'addr': '127.0.0.1'})
            elif v == "task":
                if "q=cstatus" in tractorverb+str(formdata):
                    return (0, {'rc': 0, 'msg': 'nrmtest'})
                else:
                    return (404, {'rc': 404, 'msg': 'nrmtest'})
        except:
            pass


## ------------------------------------------------------------- ##
