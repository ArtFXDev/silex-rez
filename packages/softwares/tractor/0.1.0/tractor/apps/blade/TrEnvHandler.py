# ____________________________________________________________________ 
# TrEnvHandler - Construction of environments for apps launched by
#                tractor-blade.  Each launch request may request a
#                particular named set of environment variables from
#                this blade's profile by specifying an "envkey".
#                The profile's 'default' settings are the basis of
#                all specialized env settings.  - rdavis, 2008
#
# ____________________________________________________________________ 
# Copyright (C) 2007-2014 Pixar Animation Studios. All rights reserved.
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

import string 
import types
import logging
import platform
import os
import fnmatch
import urllib.request, urllib.parse, urllib.error
import socket

class TrEnvHandler(object):

    def __init__(self, name, envkeydict, envkeys):

        self.name = name
        self.environmentdict = {}
        self.initialEnvDict = {}
        self.rewritedict = None
        self.envkeys = envkeys
        self.logger = logging.getLogger('tractor-blade')

        self.logger.debug("initializing TrEnvHandler: %s" % (name))

        if dict != type(envkeydict):
            self.logger.error("%s envkey is not a dictionary type" % name)
            return

        if 'environment' in envkeydict:
            if dict != type(envkeydict['environment']):
                self.logger.error(name+"['environment'] must be a dictionary")
                return
            else:
                # save the "environment" dict from the profile (or shared envkeys)
                self.environmentdict = envkeydict['environment']

        if 'rewritedict' in envkeydict and \
           dict != type(envkeydict['rewritedict']):
            self.rewritedict = envkeydict['rewritedict']
        
        self.initialEnvDict = self.environmentdict.copy()

    def remapCmdArgs(self, cmdinfo, launchenv, thisHost):
        self.logger.debug("TrEnvHandler.remapCmdArgs: %s" % self.name)
        argv = []
        hh = [thisHost]

        if hasattr(cmdinfo, 'srv') and 1 < len(cmdinfo.srv):
            # by convention, srv[0] is this host, which for cmds that
            # have 'srv' items is the "clienting" (dispatcher-equiv)
            # blade, so we don't include that in the stock %H expansions.
            hh = []
            for h in cmdinfo.srv[1:]:
                # might get hostname like "pluto/123.456.789.001"
                # we want just the addr part; also, if a port is
                # given then we need to format it for prman-15's
                # new format:  -h host@port:threads
                if '/' in h:
                    nm,addr = h.split('/')
                    if ':' in addr:
                        a,port = addr.split(':')
                        addr = a + '@' + port  # need host@port
                hh.append(addr)

        tref = "-T=" + urllib.parse.quote(cmdinfo.logref)

        for a in cmdinfo.argv:
            # first expand $var references, then do % subst, since
            # the text from the $var may include additional % text
            if a.find("$") != -1:
                t = string.Template(a)
                a = t.safe_substitute(launchenv)

            if -1 == a.find('%'):
                argv.append(a)
                continue  # no subst needed

            # next try simple replacement of argv items that consist
            # ENTIRELY of "%h" or "%H"
            if a == "%h":
                argv.extend( hh )

            elif a == "%H":
                argv.append(tref)  # insert -T=/Jnnn/Tnnn/Cnnn/user
                for h in hh:
                    argv.append("-h")
                    argv.append(h)

            elif a.startswith("%(") and a.endswith(")s"):
                # subst in the stored socket fileno.
                # By pre-arrangement, the cmdline token sent from
                # our peer (e.g. netrender) is already in python's
                # dict-subst format, like: %(file)s  or  %(dspy)s 
                try:
                    s = a % cmdinfo.sfdDict
                    argv.append(s)

                except Exception:
                    self.logger.warning("parameter subst failure on " + \
                                     cmdinfo.logref + ", " + a)
            else:
                #
                # now deal with argv items that contain a % subst within
                # other text in the same token text
                #
                if a.find("%H") != -1:
                    d = " -h "
                    a = a.replace("%H", tref + d + d.join(hh))

                if a.find("%h") != -1:
                    a = a.replace("%h", " ".join(hh))

                if a.find("%s") != -1:
                    # The alfred "%s" subst was the "slot" name from
                    # the alfred.schedule file that was assigned to
                    # the given command.  Since tractor doesn't have
                    # that concept, we just use the blade name instead.
                    a = a.replace("%s", " ".join(hh))

                if a.find("%j") != -1:
                    a=a.replace("%j", str(cmdinfo.jid))

                if a.find("%J") != -1:
                    a=a.replace("%J", str(cmdinfo.jid))

                if a.find("%t") != -1:
                    a=a.replace("%t", str(cmdinfo.tid))

                if a.find("%c") != -1:
                    a=a.replace("%c", str(cmdinfo.cid))

                if a.find("%i") != -1:  # "invocation id"
                    a=a.replace("%i", "0" if cmdinfo.rev <= 1 else "1")

                if a.find("%r") != -1:
                    a=a.replace("%r", str(cmdinfo.recover))

                if a.find("%R") != -1: # resume pass count (num retries)
                    a=a.replace("%R", str(cmdinfo.rpass[0]))

                if a.find("%q") != -1: # quality 0.0-1.0, due to subtask chkpt
                    a=a.replace("%q", str(cmdinfo.rpass[1]))

                if a.find("%Y") != -1: # resume pass timeslice before yield
                    a=a.replace("%Y", str(cmdinfo.rpass[2]))

                if a.find("%n") != -1:
                    a=a.replace("%n", str(cmdinfo.slots))

                if a.find("%x") != -1:
                    a=a.replace("%x", "Active")

                if a.find("%%") != -1:
                    a=a.replace("%%", "%")

                argv.append(a)

        return argv


    def handlesEnvkey(self, inkey):
        if inkey and self.envkeys:
            for key in self.envkeys:
                if fnmatch.fnmatch(inkey, key):
                    return True
        return False

    def subclassHasMethod(self, mthd):
        return ( getattr(TrEnvHandler, mthd).__func__ != getattr(self, mthd).__func__ )


    def updateEnvironment(self, cmd, launchDict, envkeys):
        '''
        apply the default environment updates
        assumes that the incoming dict can be altered
        '''
        self.logger.debug("TrEnvHandler.updateEnvironment: %s" % self.name)
        if envkeys != None:
            self.environmentdict['TR_ENV_KEY'] = ",".join(envkeys)

        if cmd and not envkeys:
            # during the base-level pass (no envkeys given) ...

            # Add some basic facts about this command to the environment
            self.environmentdict['TR_ENV_JID'] = str(cmd.jid)
            self.environmentdict['TR_ENV_TID'] = str(cmd.tid)
            self.environmentdict['TR_ENV_CID'] = str(cmd.cid)
            self.environmentdict['TR_ENV_JOB_PROJECT'] = str(cmd.projects)

            # Resolve REMOTEHOST so that applications that want to
            # know the spooling hostname (like "it") will work.
            # The values cmd.spooladdr and cmd.cmdtype are sent
            # from post-1.3 engines.
            try:
                self.environmentdict['TR_SPOOLHOST'] = cmd.spoolhost
                self.environmentdict['TR_SPOOLADDR'] = \
                    getattr(cmd, 'spooladdr', cmd.spoolhost)
                if hasattr(cmd, 'spooluuid'):
                    self.environmentdict['TR_SPOOLUUID'] = \
                        getattr(cmd, 'spooluuid')

                if cmd.svckey == "_local_":
                    ct = "local"
                else:
                    ct = getattr(cmd, 'cmdtype', "unknown")

                if ct == "remote":
                    self.environmentdict['REMOTEHOST'] = cmd.spoolhost
                elif 'REMOTEHOST' in self.environmentdict:
                    # if the command is not running on the spooling host,
                    # then REMOVE it from the environment.
                    del( self.environmentdict['REMOTEHOST'] )
            except:
                pass

        # flatten envdict one complete pass first
        flattened = self.environmentdict.copy()
        for k,v in flattened.items():
            if v.find("$") != -1:
                t = string.Template(v)
                del flattened[k]
                # first flatten entry within defenv dictionary
                flatv = t.safe_substitute(flattened)

                # for the loop to work, the size of the dictionary cannot change.
                flattened[k] = flatv

        # now process against the launchDict (launch environment)
        for k,v in flattened.items():
            if v.find("$") != -1:
                t = string.Template(v)
                del flattened[k]  # why? avoid subst recursion? iteritems works?
                # first flatten entry within defenv dictionary
                flatv = t.safe_substitute(flattened)

                # then take that value as new template, and apply to global env
                t = string.Template(flatv)
                v = self.appendOrReplaceEntry( launchDict, k, t.safe_substitute(launchDict) )

                # restore the key for possible use in a future subst
                flattened[k] = v
            else:
                self.appendOrReplaceEntry( launchDict, k, v )

        return launchDict


    def appendOrReplaceEntry (self, ldict, k, v):
        # Tractor convention: if the new value starts with "@+" then that
        # special notation means "append this value to the existing such
        # variable, if it exists, otherwise just use this value" which is
        # to handle cases like LD_LIBRARY_PATH that may not be set at all
        # by default. For cases like extending PATH we can just use a 
        # self reference "PATH": "$PATH:new_stuff" since PATH always exists.

        if type(v) in (str, str) and v.startswith("@+"):
            try:
                if k in ldict:
                    v = ldict[k] + v[2:]
                else:
                    v = v[3:] # skip the @+ and the join character, "@+:some_path"
            except Exception:
                pass # just do the replace, below

        ldict[k] = v

        return v


    def compareDictionaries(self, d1,d2):
        # if no environment, d2 will be none.  Don't compare
        if d2 == None:
            return

        len1 = len(d1)
        len2 = len(d2)

        if len1 != len2: 
            self.logger.trace("env lengths differ: %d %d" % (len1, len2))
        compared = {}

        for e in d1:
            e1 = d1[e]
            if e in d2:
                e2 = d2[e]
                if e1 != e2:  
                    self.logger.trace("Entries different: %s" % (e))
                    self.logger.trace("%s -- %s" % (e, e1))
                    self.logger.trace("%s -- %s" % (e, e2))
            else:
                trLog.trace("dictionary 2 does not have key: %s (%s)" % (e, e1))


    def locateRMANTREE(self, version=None):
        """
        Try to locate the newest prman installation. This is only done
        if RMANTREE is not set in the default environment (of the handler),
        or in the blade inherited environment

        7/24/2012: Due to a change in Mac RPS-17 location, the rmanhandler will
        now try to locate RMANTREE of a specific version. rmshandler will still
        attempt to locate newest RMANTREE.
        """
        executables = ["prman", "prman.exe"]

        if "APPBASEDIR" in self.environmentdict:
            tmp = self.environmentdict["APPBASEDIR"]
            basedirs = tmp.split(os.pathsep)
        else:
            plat = platform.system()
            if plat == 'Darwin':
                basedirs = ["/Applications/Pixar", "/Applications/Pixar/RenderMan.app/Versions"]
            elif plat == 'Linux':
                basedirs = ["/opt/pixar"]
            else:
                basedirs = ["C:/Program Files/Pixar"]

        for basedir in basedirs:
            try:
                # looking for a specific version
                if version:
                    for executable in executables:
                        rmandir = os.path.join(basedir, "RenderManProServer-%s" % version)
                        if os.path.exists(os.path.join(rmandir, "bin", executable)):
                            return rmandir

                else:
                    dirlist = os.listdir(basedir)
                    dirlist.sort(reverse=True)
                    if len(dirlist) == 0: continue
                    for dir in dirlist:
                        if dir.find("RenderManProServer") == 0:
                            for executable in executables:
                                rmandir = os.path.join(basedir, dir)
                                if os.path.exists(os.path.join(rmandir, "bin", executable)):
                                    return rmandir

            except:
                self.logger.trace("TrEnvHandler.locateRMANTREE: " + self.logger.Xcpt())

        self.logger.debug("TrEnvHandler.locateRMANTREE=None")
        return None

    def locateMayaDirectory(self, version):
        """ 
        Simple check of the 4 variants of Maya installation
        directories.  Check through, return the first found.
        64 bit Linux has precendence of 32 bit if both installed
        """
        fmtstrings = [
            {"Linux64": "/usr/autodesk/maya%s-x64"},
            {"Linux32": "/usr/autodesk/maya%s"},
            {"Darwin": "/Applications/Autodesk/maya%s"},
            {"Windows": "C:/Program Files/Autodesk/Maya%s"}]

        try:
            for formatdict in fmtstrings:
               for p,f in formatdict.items():
                   basedir = f % version
                   if os.path.exists(basedir):
                       return basedir

        except:
            self.logger.trace("TrEnvHandler.locateMayaDirectory: " + self.logger.Xcpt())

        self.logger.debug("TrEnvHandler.locateMayaDirectory=None")
        return None



    def debug(self):
        self.logger.debug("TrEnvHandler.debug: %s" % self.name)
        self.logger.debug("TrEnvHandler.envkeys: %s" % repr(self.envkeys))
        self.logger.printDict(self.environmentdict, "environmentdict")

# ------------------------------------------------------------------ #

class default(TrEnvHandler):
    def __init__(self, name, envkeydict, envkeys):
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    # the default handler will be called with every command to allow
    # cmd specific environment variables to be put in the environment,
    # even if there is no envhandler specified.
    def updateEnvironment(self, cmd, env, envkeys):
        self.logger.debug("default.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        return TrEnvHandler.updateEnvironment(self, cmd, env, None)

    def debug(self):
        self.logger.debug("default.debug: %s" % self.name)
        TrEnvHandler.debug(self)


class rmanhandler(TrEnvHandler):
    def __init__(self, name, envkeydict, envkeys):
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    def updateEnvironment(self, cmd, env, envkeys):
        self.logger.debug("rmanhandler.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        # there should only be a single key, like "prman-20.5"
        # but we accept several, setting from the last one win
        for key in envkeys:
            tok = key.split("-")
            vers = tok[1] if len(tok) > 1 else None
            rmantree = self.locateRMANTREE(vers)
            if rmantree: 
                self.environmentdict['TR_ENV_RMANTREE'] = rmantree
            else:
                # put something in the environment to indicate obvious error
                self.environmentdict['TR_ENV_RMANTREE'] = "RMAN-%s_NOT_FOUND" % vers
        return TrEnvHandler.updateEnvironment(self, cmd, env, envkeys)

    def debug(self):
        self.logger.debug("rmanhandler.debug: %s" % self.name)
        TrEnvHandler.debug(self)


class rmshandler(TrEnvHandler):
    # Handle configs for keys like "rms-20.6-maya-2016"
    # NOTE as distinct from "rfm-21.0-maya-2016" (see rfmhandler)
    def __init__(self, name, envkeydict, envkeys):
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    def updateEnvironment(self, cmd, env, envkeys):
        self.logger.debug("rmshandler.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        if not ("RMANTREE" in env or "RMANTREE" in self.environmentdict):
            rmantree = self.locateRMANTREE()
            if rmantree: self.environmentdict['RMANTREE'] = rmantree
        for key in envkeys:
            vals = key.split("-")
            if len(vals) == 4:
                self.environmentdict['TR_ENV_RMSVER'] = vals[1]
                self.environmentdict['TR_ENV_MAYAVER'] = vals[3]
                ml = self.locateMayaDirectory(vals[3])
                if ml:
                    self.environmentdict['TR_ENV_MAYALOCATION'] = ml

        return TrEnvHandler.updateEnvironment(self, cmd, env, envkeys)

    def debug(self):
        self.logger.debug("rmshandler.debug: %s" % self.name)
        TrEnvHandler.debug(self)


class rfmhandler(TrEnvHandler):
    # Handle configs for keys like "rfm-21.0-maya-2016"
    # NOTE as distinct from "rms-20.6-maya-2016" (see rmshandler)
    def __init__(self, name, envkeydict, envkeys):
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    def updateEnvironment(self, cmd, env, envkeys):
        self.logger.debug("rfmhandler.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        if not ("RMANTREE" in env or "RMANTREE" in self.environmentdict):
            rmantree = self.locateRMANTREE()
            if rmantree: self.environmentdict['RMANTREE'] = rmantree
        for key in envkeys:
            vals = key.split("-")
            if len(vals) == 4:
                self.environmentdict['TR_ENV_RFMVER'] = vals[1]
                self.environmentdict['TR_ENV_MAYAVER'] = vals[3]
                ml = self.locateMayaDirectory(vals[3])
                if ml:
                    self.environmentdict['TR_ENV_MAYALOCATION'] = ml

        return TrEnvHandler.updateEnvironment(self, cmd, env, envkeys)

    def debug(self):
        self.logger.debug("rfmhandler.debug: %s" % self.name)
        TrEnvHandler.debug(self)

class rfmhandler2(TrEnvHandler):
    # Handle configs for keys like "rfm-22.0"
    # NOTE as distinct from "rfm-21.0-maya-2016" (see rfmhandler)
    def __init__(self, name, envkeydict, envkeys):
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    def updateEnvironment(self, cmd, env, envkeys):
        self.logger.debug("rfmhandler2.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        if not ("RMANTREE" in env or "RMANTREE" in self.environmentdict):
            rmantree = self.locateRMANTREE()
            if rmantree: self.environmentdict['RMANTREE'] = rmantree
        for key in envkeys:
            vals = key.split("-")
            if len(vals) == 2:
                self.environmentdict['TR_ENV_RFMVER'] = vals[1]

        return TrEnvHandler.updateEnvironment(self, cmd, env, envkeys)

    def debug(self):
        self.logger.debug("rfmhandler2.debug: %s" % self.name)
        TrEnvHandler.debug(self)

# ------------------------------------------------------------------ #

class rmantreehandler(TrEnvHandler):
    def __init__(self, name, envkeydict, envkeys):
        self.logger = logging.getLogger('tractor-blade')
        self.logger.debug("initializing rmantreehandler: %s" % (name))
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    def updateEnvironment(self, cmd, env, envkeys):
        self.logger.debug("rmantreehandler.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        for key in envkeys:
            v,r = key.split("=")
            self.environmentdict['TR_ENV_RMANTREE'] = r
        return TrEnvHandler.updateEnvironment(self, cmd, env, envkeys)

    def remapCmdArgs(self, cmdinfo, launchenv, thisHost):
        self.logger.debug("rmantreehandler.remapCmdArgs: %s" % self.name)
        argv = TrEnvHandler.remapCmdArgs(self, cmdinfo, launchenv, thisHost)
                
        return argv

    def debug(self):
        self.logger.debug("rmantreehandler.debug: %s" % self.name)
        TrEnvHandler.debug(self)


class rmstreehandler(TrEnvHandler):
    def __init__(self, name, envkeydict, envkeys):
        self.logger = logging.getLogger('tractor-blade')
        self.logger.debug("initializing rmstreehandler: %s" % (name))
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    def updateEnvironment(self, cmd, env, envkeys):
        self.logger.debug("rmstreehandler.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        for key in envkeys:
            v,r = key.split("=")
            self.environmentdict['TR_ENV_RMSTREE'] = r
        return TrEnvHandler.updateEnvironment(self, cmd, env, envkeys)

    def remapCmdArgs(self, cmdinfo, launchenv, thisHost):
        self.logger.debug("rmstreehandler.remapCmdArgs: %s" % self.name)
        argv = TrEnvHandler.remapCmdArgs(self, cmdinfo, launchenv, thisHost)
                
        return argv

    def debug(self):
        self.logger.debug("rmstreehandler.debug: %s" % self.name)
        TrEnvHandler.debug(self)

class rfmtreehandler(TrEnvHandler):
    def __init__(self, name, envkeydict, envkeys):
        self.logger = logging.getLogger('tractor-blade')
        self.logger.debug("initializing rfmtreehandler: %s" % (name))
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    def updateEnvironment(self, cmd, env, envkeys):
        self.logger.debug("rfmtreehandler.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        for key in envkeys:
            v,r = key.split("=")
            self.environmentdict['TR_ENV_RFMTREE'] = r
        return TrEnvHandler.updateEnvironment(self, cmd, env, envkeys)

    def remapCmdArgs(self, cmdinfo, launchenv, thisHost):
        self.logger.debug("rfmtreehandler.remapCmdArgs: %s" % self.name)
        argv = TrEnvHandler.remapCmdArgs(self, cmdinfo, launchenv, thisHost)
                
        return argv

    def debug(self):
        self.logger.debug("rfmtreehandler.debug: %s" % self.name)
        TrEnvHandler.debug(self)


# ------------------------------------------------------------------ #
class mayahandler(TrEnvHandler):
    def __init__(self, name, envkeydict, envkeys):
        self.logger = logging.getLogger('tractor-blade')
        self.logger.debug("initializing mayahandler: %s" % (name))
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    def updateEnvironment(self, cmd, env, envkeys):
        self.logger.debug("mayahandler.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        for key in envkeys:
            vals = key.split("-")
            if len(vals) == 2:
		val = vals[1]
                self.environmentdict['TR_ENV_MAYAVER'] = val 
            	ml = TrEnvHandler.locateMayaDirectory(self, val)
	    else:
            	val = key[4:]
            	self.environmentdict['TR_ENV_MAYAVER'] = val
            	ml = TrEnvHandler.locateMayaDirectory(self, val)
            if ml:
                self.environmentdict['TR_ENV_MAYALOCATION'] = ml

        if "MAYA_MODULE_PATH" not in env:
            env['MAYA_MODULE_PATH'] = "" 

        return TrEnvHandler.updateEnvironment(self, cmd, env, envkeys)

    def debug(self):
        self.logger.debug("mayahandler.debug: %s" % self.name)
        TrEnvHandler.debug(self)

# ------------------------------------------------------------------ #
class setenvhandler (TrEnvHandler):
    def __init__(self, name, envkeydict, envkeys):
        self.logger = logging.getLogger('tractor-blade')
        self.logger.debug("initializing setenvhandler: %s" % (name))
        TrEnvHandler.__init__(self, name, envkeydict, envkeys)

    def updateEnvironment(self, cmd, env, envkeys):
        #
        # expect a SINGLE key formatted as space-delimited sequence of words:
        #   "setenv MYVAR=123 YOURVAR=host:0"
        # or in alfserver.ini compatible pairs:
        #   "setenv MYVAR 123 YOURVAR host:0"
        #
        self.logger.debug("setenvhandler.updateEnvironment: %s" % self.name)
        self.environmentdict = self.initialEnvDict.copy()
        for key in envkeys:
            evars = key.split()
            nk = len(evars)
            i = 1  # skip the leading "setenv"
            while (++i < nk):
                k = evars[i]
                if "=" in k:
                    nm,_,val = k.partition("=")
                else:
                    nm = k
                    i += 1
                    if (i < nk):
                        val = evars[i]
                    else:
                        val = "1"
                i += 1
                try:
                    env[nm] = val
                except:
                    self.logger.info("setenv handler, error setting: " + nm)

        return TrEnvHandler.updateEnvironment(self, cmd, env, envkeys)

    def remapCmdArgs(self, cmdinfo, launchenv, thisHost):
        self.logger.debug("setenvhandler.remapCmdArgs: %s" % self.name)
        return TrEnvHandler.remapCmdArgs(self, cmdinfo, launchenv, thisHost)

    def debug(self):
        self.logger.debug("setenvhandler.debug: %s" % self.name)
        TrEnvHandler.debug(self)

# ------------------------------------------------------------------ #
