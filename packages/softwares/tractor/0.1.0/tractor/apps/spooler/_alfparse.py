#
# alfparse.ParseAlfJob -- simple rmanpy python wrapper around tcl job parser
#
# ____________________________________________________________________ 
# Copyright (C) 2013-2015 Pixar Animation Studios. All rights reserved.
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

import os
import sys

# --- #
# Acquire and init the rmanpy embedded tcl interpreter.
# This scheme assumes that we are running in the "rmanpy" python
# "distribution" that ships with Tractor; it is a pretty standard
# python distro plus a few added modules including an embedded Tcl
# interpreter (referred to as the "prefs parser" from RMS usage).
# In Tractor usage, we want to avoid issues with loading the actual
# RMS preference files, so we temporarily redirect variables used
# during the init.
#
_vbak = {}
for v in ["HOME", "RMSPROD", "RMS_SCRIPT_PATHS"]:
    if v in os.environ:
        _vbak[v] = os.environ[v]          # save 
        os.environ[v] = "/"               # override

from rmanpy import prefs as tcl

for v in _vbak: os.environ[v] = _vbak[v]  # restore


## ------------------------------------------------------------- ##
class TrAlfParseError (Exception):
    pass


## ------------------------------------------------------------- ##
_alfparsedDidInit = False


def initializeAlfParser ():
    '''Locate the alfscript parsing library and instantiate it.'''

    tclLib = tcl.getTclLibPath()
    if tclLib == None:
        raise Exception("Can't find the required tcl library directory.")
        
    a2j = os.path.join(tclLib,"alf2json.tcl")
    
    # a2j is a path that is going to be sent into a tcl interpreter.
    # Need to avoid those back slashes.
    a2j = a2j.replace('\\', '/')

    if not os.path.exists( a2j ):
        raise Exception("can't find required job parsing script: " + a2j)
    else:
        tcl.eval("source \"" + a2j + "\"")  # establish parsing functions

        # If some script calls "exit" we want to get control back rather
        # than actually calling the C runtine exit() function and killing
        # our caller.
        tcl.eval('proc ::exit {xrc} { uplevel #0 ' \
                 '"error {job parsing interrupted} {EXIT_CALLED} $xrc" }')

        _alfparsedDidInit = True


## ------------------------------------------------------------- ##
def ParseAlfJob (options, infilename, autotext):
    #
    # Parse inbound alfred-format (tcl) job scripts, emitting a JSON format
    # job description on output. This may occur directly in the tractor-spool
    # client to catch syntax errors on the client side before sending json 
    # to the engine.
    #

    if not _alfparsedDidInit:
        initializeAlfParser()

    r = 'TrTclParseAlfScript {'
    
    if hasattr(options, "expandctx") and options.expandctx:
        r += " --expandctx {" + options.expandctx + "}"
    else:
        ## -- typical initial spooling case -- ##

        if hasattr(options, "userfile"):
            ufile = options.userfile
        else:
            ufile = infilename

        r += ' --userfile {' + ufile + '}' \
             ' --owner {'+options.jobOwner+'}' \
             ' --hname {'+options.hname+'}' \
             ' --cwd {'+(options.jobcwd or "nocwd") +'}'

        if options.priority and "default" != str(options.priority):
            r += ' --priority ' + str(options.priority)
        if options.projects and "default" != str(options.projects):
            r += ' --jprojects {' + str(options.projects) + '}'
        if options.tier and "default" != str(options.tier):
            r += ' --jtier {' + str(options.tier) + '}'
        if options.paused:
            r += " --paused 1"
        if options.svckey:
            r += " --jsvckey {"+options.svckey+"}"
        if options.envkey:
            r += " --jenvkey {"+options.envkey+"}"
        if options.aftertime:
            r += " --aftertime {"+options.aftertime+"}"
        if options.afterjid:
            r += " --afterjid {"+options.afterjid+"}"
        if options.maxactive:
            r += " --maxactive %d" % options.maxactive
        if options.remoteclean:
            r += " --jremoteclean {"+options.remoteclean+"}"
        if options.alfescape:
            r += " --alfescape " + str(options.alfescape)

    if autotext:
        r += ' --jobtext {'+autotext+'}'
    else:
        r += ' {' + infilename + '}'

    r += '}'

    try:
        #
        # Invoke the tcl interpreter getting the constructed json
        # as the return value.  On translation/traversal failure an
        # exception will be raised instead.
        #
        return tcl.evalWithErrText(r)  # run the job parser/translator

    except Exception as e:
        raise TrAlfParseError( e.message )

## ------------------------------------------------------------- ##

