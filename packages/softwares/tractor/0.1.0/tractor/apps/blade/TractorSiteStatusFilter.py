#
# TractorSiteStatusFilter is a wrapper subclass around the built-in
# TrStatusFilter class. The StatusFilter object collects various
# status metrics from the current host and determines whether it is
# appropriate to ask the engine for new tasks. This file is intended
# to be copied, modified by local studio pipeline folks, and placed
# into the site's blade "SiteModulePaths" (see blade.config) so that
# the customized version is imported at blade start-up and profile
# reload, rather than the default one.
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

from .TrStatusFilter import TrStatusFilter


## ------------------------------------------------------------- ##
class TractorSiteStatusFilter (TrStatusFilter):
    '''
    Implement a two-step procedure to determine whether we
    should ask the engine for new tasks to launch on this
    blade.  The blade initializes a dict with some "cheap
    to acquire" static state for this host, then passes it
    to this site-defined analysis class (derived from the
    default TrStatusFilter class) which will optionally
    modify some of those state values and then do "cheap"
    early-out tests based on them.  If these easy tests
    succeed, then the blade proceeds to collect additional,
    "more expensive" dynamic state values and similarly call
    into the site-defined routines below to analyze them.
    If this second set of tests pass, then the blade proceeds
    to ask the engine for new work.
    '''

    #
    # NOTE: the calls to the "super" object in the methods
    # below is simply an example of how to inherit the stock
    # default behavior while still allowing custom behavior
    # to be added in each callback below.  If you simply want
    # to inherit the default implementation of a particular
    # method, then you can leave the "super" call wrapper here,
    # or you can simply remove those method definitions entirely
    # from this file -- in which case the base class methods
    # will be called by default.
    #

    def __init__ (self):
        self.super = super(type(self), self)  # magic proxy object
        self.super.__init__()
        
        self.logger.info("initializing site status filters")


    ## ------------------ basic state tests -------------------- ##

    def FilterBasicState (self, stateDict, now):
        """
        Makes custom modifications to the stateDict.
        The inbound dict will contain basic blade
        configuration state at the time this routine
        is called.  NOTE: the stateDict may be "None"
        if the blade has not yet been able to download
        the blade.config profiles from the engine.

        These "cheap and easy to acquire" settings will be
        used for quick early-out tests, in TestBasicState,
        below, prior to the more expensive dynamic 
        status-gathering and testing phase.
        """
        self.super.FilterBasicState(stateDict, now)


    def TestBasicState (self, stateDict, now):
        """
        Now TEST the "cheap and easy to acquire" values to 
        see if we can early-out on further status gathering
        for this pass.  Return True to indicate that we
        can accept new work based on the basic values.
        """
        return self.super.TestBasicState(stateDict, now)


    ##
    ## ------------------ dynamic state tests -------------------- ##
    ##
    # Same pattern as above, but now for the potentially more
    # expensive-to-acquire dynamic state data.  These are tests
    # that might involve contacting a production database, etc.

    def FilterDynamicState (self, stateDict, now):
        return self.super.FilterDynamicState(stateDict, now)

    def TestDynamicState (self, stateDict, now):
        return self.super.TestDynamicState(stateDict, now)


    ##
    ## -------- notifier methods for subprocess begin/end ------- ##
    ##
    # These methods are the site-defined callbacks that are triggered
    # when tractor-blade launches a new command, and also when each
    # command subprocess exits.  The idea is to allow sites to implement
    # custom logging at command launch and exit events, through self.logger
    # or via an external logging scheme or database external to Tractor.
    # They may also provide a context for custom pre/post processing
    # around a command execution on each blade.  

    def SubprocessFailedToStart (self, cmd):
        self.super.SubprocessFailedToStart( cmd )

    def SubprocessStarted (self, cmd):
        self.super.SubprocessStarted( cmd )

    def SubprocessEnded (self, cmd):
        self.super.SubprocessEnded( cmd )


    ##
    ## -------- command output log filtering methods ------- ##
    ##
    # This method is called for EVERY line of output from every
    # command launched by the blade. For example, every line of
    # output from the renderer. Typically these lines are just
    # passed back unchanged and are then written to the task log
    # files by Tractor.  There are several alternative choices
    # that the filter can make.  All responses from this method
    # should be in the form of a python TUPLE, with 2 or 3 elements.
    #  (code, value[, optional_log_text])
    #
    # - It can return the incoming line unchanged, causing it to go to the
    #   usual task output log files.
    #   return (self.TR_LOG_TEXT_EMIT, textline)
    #
    # - It can return alternate text:
    #   return (self.TR_LOG_TEXT_EMIT, "some new text")
    #
    # - It can strip the incoming line from the output stream by returning
    #   the python value None as the value part of the tuple.
    #   return (self.TR_LOG_TEXT_EMIT, None)
    #
    # - It can signal special behavior based on specific text in the 
    #   incoming line.  For example it can cause UI progress bars to 
    #   grow to a particular new value:
    #   return (self.TR_LOG_PROGRESS, .42)  # values between 0.0 and 1.0
    #
    # - It can force a particular "error status" code to be reported when
    #   the process eventually exits. All non-zero values will cause the
    #   task to be marked as an Error.
    #   return (self.TR_LOG_EXIT_CODE, 17)  # values between 0 and 255
    #
    #   A related variant will cause Tractor to immediately kill the
    #   running command subprocess:
    #   return (self.TR_LOG_FATAL_CODE, 17) # report code, kill via signal
    #
    # - It can generate a "task expand" edit on the current job,
    #   return (self.TR_LOG_EXPAND_CHUNK, "/the/subtask/file.003.alf")
    #

    def FilterSubprocessOutputLine (self, cmd, textline):
        '''
        Return a 2-tuple giving a disposition code and a value.
        The pass-through result would be:  (self.TR_LOG_TEXT_EMIT, textline)
        '''
        if "prman" == cmd.app and textline.startswith("R10007"):
            # example:  immediately fail on prman warning number "R10007"
            return (self.TR_LOG_FATAL_CODE, 10007, textline)
        else:
            # otherwise defer to the usual built-in filters
            return self.super.FilterSubprocessOutputLine( cmd, textline )

## ------------------------------------------------------------- ##
