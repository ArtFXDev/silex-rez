#
# TrStatusFilter - a class that collects current system status
# and determines whether it is appropriate to ask the engine
# for new tasks.  Can be subclassed by site-defined override
# filters, using a local copy of TractorSiteStatusFilter.py.
#
# That's worth repeating:
# DO NOT COPY or modify this file to get site-specific
# custom filtering of blade state or custom readiness tests!
# INSTEAD:  copy TractorSiteStatusFilter.py into the location
# indicated by SiteModulesPath in blade.config, and make local
# changes to that copy.  You can implement your own filter methods
# there, or call the superclass methods (from this file) while
# also adding your own pre/post processing in the subclass.
# 
#
# ____________________________________________________________________ 
# Copyright (C) 2010-2015 Pixar Animation Studios. All rights reserved.
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
import re


## ------------------------------------------------------------- ##
class TrStatusFilter ( object ):

    def __init__(self):
        # look up the global blade logger object
        self.logger = logging.getLogger("tractor-blade")

        self.cmdLogFilterRE = None


    ## ------------------ basic state tests -------------------- ##

    def FilterBasicState(self, stateDict, now):
        """ Makes custom modifications to the stateDict.
            The inbound dict will contain basic blade
            configuration state at the time this routine
            is called.  NOTE: the stateDict may be "None"
            if the blade has not yet been able to download
            the blade.config profiles from the engine.

            These "cheap and easy to acquire" settings will be
            used for quick early-out tests prior to the more 
            expensive dynamic status-gathering and testing phase.
        """
        pass  # default filter makes no changes to curstate


    def TestBasicState (self, stateDict, now):
        #
        # by default, we don't ask for work if still waiting
        # to download a valid profile definition
        #
        if not stateDict:
            # no place to write our excuse string either
            return False

        result = False # pessimistic by nature

        try:
            if not stateDict["isInService"]:
                # When not in service, we can become relatively dormant.
                # Internally the profile will occasionally check for updates.
                stateDict["excuse"] = "profile specifies 'Not in Service'"

            elif len( stateDict["exclusiveKeys"] ) > 0:
                # a cmd using an "exclusive" service key is running,
                # so we decline to ask for more 
                stateDict["excuse"] = "exclusiveKey active: " + \
                                    ','.join(stateDict["exclusiveKeys"])

            elif stateDict["slotsAvailable"] <= 0:
                # this blade's concurrent-cmd limit reached
                stateDict["excuse"] = \
                    "no free slots (%d)" % stateDict["slotsInUse"]

            else:
                result = True  # we should do expensive checks

        except Exception:
            stateDict["excuse"] = "TestBasicState failed"
        
        return result

    ## ------------------ dynamic state tests -------------------- ##

    def FilterDynamicState (self, stateDict, now):
        pass

    def TestDynamicState (self, stateDict, now):
        #
        #
        #
        result = False
        if stateDict["cpuLoad"] > stateDict["maxLoad"]:
            stateDict["excuse"] = "MaxLoad(%.3g > %.3g) exceeded" % \
                            (stateDict["cpuLoad"], stateDict["maxLoad"])

        elif stateDict["freeRAM"] < stateDict["minRAM"]:
            stateDict["excuse"] = "MinRAM(%.3g < %.3g GB) insufficient" % \
                            (stateDict["freeRAM"], stateDict["minRAM"])

        elif stateDict["freeDisk"] < stateDict["minDisk"]:
            stateDict["excuse"] = "MinDisk(%.3g < %.3g GB) insufficient" % \
                            (stateDict["freeDisk"], stateDict["minDisk"])

        else:
            result = True  # can proceed with task request!

        return result


    ## ------- notifier methods for subprocess begin/end ------- ##

    # These methods are the default site-defined callbacks that are
    # triggered when tractor-blade launches a new command, and also when
    # each command subprocess exits.  The idea is to allow sites to
    # implement custom logging at command launch and exit events, and
    # also to allow for the collection of process information for purposes
    # external to Tractor.  It may also provide a context for custom
    # pre/post processing around a command execution on each blade.  

    def SubprocessStarted (self, cmd):
        '''
        Called after a command has been successfully launched.
        '''
        guise = ""
        if cmd.guise:
            guise = "(as %s) " % cmd.guise

        self.logger.info("+pid[%d] %s %s'%s' #%d" % \
                (cmd.pid, cmd.logref, guise, cmd.app, cmd.slots))

        if cmd.launchnote:
            self.logger.debug(" note: %s %s" % (cmd.logref, cmd.launchnote))


    # -- #
    def SubprocessFailedToStart (self, cmd):
        '''
        Called after a command exec has failed, usually due
        to path problems or mismatched platforms.
        '''
        guise = ""
        emsg = "failed to launch"
        if cmd.launchnote:
            emsg = cmd.launchnote
        if cmd.guise:
            guise = " (as %s)" % cmd.guise

        self.logger.info("launch FAILED%s: %s, [%s] %s" % \
                        (guise, emsg, cmd.argv[0], cmd.logref))


    # -- #
    def SubprocessEnded (self, cmd):
        '''
        Called after a command process has stopped running, either
        because it exitted on its own or because it was killed.
        '''
        self.logger.info("-pid[%d] %s rc=%d %.2fs #%d%s" % \
            (cmd.pid, cmd.logref, cmd.exitcode, cmd.elapsed, cmd.slots,
             (" (chkpt)" if cmd.yieldchkpt else "")))


    ## --------------- command output filtering ------------------ ##

    ## Define some class "constants" to indicate disposition.
    ## These are returned as the first part of a tuple from the
    ## filter, like (self.PROGRESS, 48)

    TR_LOG_TEXT_EMIT    = 0
    TR_LOG_PROGRESS     = 10
    TR_LOG_EXIT_CODE    = 20
    TR_LOG_FATAL_CODE   = 21
    TR_LOG_EXPAND_CHUNK = 30
    TR_LOG_ALSO_EMIT    = 16384

    def FilterSubprocessOutputLine (self, cmd, textline):
        '''
        Filter each line of command subprocess output.
        Return a (action, value) tuple.
        '''
        #
        # first set up regex matches
        #
        # looking for progress messages like this:
        #   "\nTR_PROGRESS 42%\n"
        #   "\nALF_PROGRESS 42%\n"
        #   "\nR90000  100%\r\n"                 (prman -Progress XCPT message)
        #   "\n[03] R90000  100%\r\n"            (multi-host netrender -Progress)
        #   "\n00:00:24.52 2.36GB PROG  | 17%\n" (prman v21 -Progress status)
        #   "\n42% done ...\n"                   (such as from Nuke)
        #   "\nFrame 37 (7 of 9)\n"              (progress = 7.0/9.0)
        #
        # and:
        #   "\nTR_EXIT_STATUS 0\n"
        #   "\nALF_EXIT_STATUS 0\n"
        #
        # and:
        #   '\nTR_EXPAND_CHUNK "/path/and/filename" [position]\n'
        #
        # We do this as one big regex to avoid calling re.match
        # several times on each line of app output.  The single
        # call is more efficient, but does slightly complicate
        # the output group analysis code.
        #
        if self.cmdLogFilterRE is None:
            self.cmdLogFilterRE = re.compile(
                r'(?:\[[^]]+\]:? +|^)R90000 +(\d*)'
                 '|[0-9:.]{11} \S+ +PROG +\| +(\d*)'
                 '|TR_PROGRESS +(\d*)'
                 '|ALF_PROGRESS +(\d*)'
                 '|(\d*)% done'
                 '|Frame +\d* +\((\d*) +of +(\d*)\)'
                 '|TR_EXIT_STATUS +(-?\d*)'
                 '|ALF_EXIT_STATUS +(-?\d*)'
                 '|TR_EXPAND_CHUNK +"(.+)" ?(\S+)?'
            )
        
        m = self.cmdLogFilterRE.match( textline )
        if m:
            # We matched one of the patterns. The "or" here
            # captures the value that actually matched.
            g = m.groups()
            progress = g[0] or g[1] or g[2] or g[3] or g[4]
            framepct = (g[5], g[6])  # value is tuple
            exitcode = g[7] or g[8]
            expandx  = (g[9], g[10]) # value is tuple

            if g[1] or framepct[0]:
                flags = self.TR_LOG_ALSO_EMIT
            else:
                flags = 0

            if framepct[0] and framepct[1]:
                # like Nuke's "Frame 27 (11 of 22)", we just compute
                # the progress percent and fall-through to progress handler
                try:
                    progress = 100 * float(framepct[0]) / float(framepct[1])
                    if progress < 1.0: progress = 1.0
                except:
                    pass
            
            if progress:
                p = float(progress)
                if p < 0.0:    p = 0.0
                if p > 100.0:  p = 100.0
                return (flags + self.TR_LOG_PROGRESS, p)

            elif exitcode:
                return (self.TR_LOG_EXIT_CODE, int(exitcode))

            elif expandx[0]:
                return (self.TR_LOG_EXPAND_CHUNK, expandx)

        # otherwise just pass the inbound line back out
        return (self.TR_LOG_TEXT_EMIT, textline)

## ------------------------------------------------------------- ##
