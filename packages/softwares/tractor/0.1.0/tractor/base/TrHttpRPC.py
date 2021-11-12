# _______________________________________________________________________
# TrHttpRPC - a tractor-blade module that makes HTTP requests of
#             tractor-engine, such as requesting configuration data
#             and commands from the job queue to execute.
#
#
#             Note, many of the functions here could be accomplished
#             using the built-in python urllib2 module.  However,
#             that module does not have "json" extraction built-in,
#             and more importantly:  urllib2 is very slow to setup
#             new connections.  Using it for obtaining new tasks and
#             reporting their results can actually reduce the overall
#             throughput of the tractor system, especailly for very
#             fast-running tasks.
#
# _______________________________________________________________________
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
# _______________________________________________________________________
#

import sys, socket, errno, urllib.request, urllib.error, urllib.parse
import struct, types


class TrHttpError(Exception):
    pass


## ------------------------- ##
class fake_json(object):
    def __init__(self):
        self.fakeJSON = 1

    def loads(self, jsonstr):
        """A stand-in for the real json.loads(), using eval() instead."""
        #
        # NOTE: In general, tractor-blade code should (and does) simply
        # "import json" and proceed from there -- which assumes that
        # the blade itself is running in a python distribution that is
        # new enough to have the json module built in.  However, this
        # one file (TrHttpRPC.py) is sometime used in other contexts in
        # which the json module is not available, hence the need for a
        # workaround.
        #
        # NOTE: python eval() will *fail* on strings ending in CRLF (\r\n),
        # they must be stripped!
        #
        # We add local variables to stand in for the three JSON
        # "native" types that aren't available in python; however,
        # these types aren't expected to appear in tractor data.
        #
        null = None
        true = True
        false = False

        return eval(jsonstr.replace("\r", ""))


## ------------------------- ##
try:
    import json
except ImportError:
    json = fake_json()

## ------------------------------------------------------------- ##


class TrHttpRPC(object):
    def __init__(
        self,
        host,
        port=80,
        logger=None,
        apphdrs={},
        urlprefix="/Tractor/",
        timeout=65.0,
    ):

        self.port = port
        self.lastPeerQuad = "0.0.0.0"
        self.engineResolved = False
        self.resolveFallbackMsgSent = False
        self.logger = logger
        self.appheaders = apphdrs
        self.urlprefix = urlprefix
        self.timeout = timeout
        self.passwdRequired = None
        self.passwordhashfunc = None

        self.host = host
        if type(port) is not int:
            raise TrHttpError("port value '%s' is not of type integer" % str(port))
        if port <= 0:
            h, c, p = host.partition(":")
            if p:
                self.host = h
                self.port = int(p)

        # embrace and extend errno values
        if not hasattr(errno, "WSAECONNRESET"):
            errno.WSAECONNRESET = 10054
        if not hasattr(errno, "WSAETIMEDOUT"):
            errno.WSAETIMEDOUT = 10060
        if not hasattr(errno, "WSAECONNREFUSED"):
            errno.WSAECONNREFUSED = 10061

    ## --------------------------------- ##
    def Transaction(
        self,
        tractorverb,
        formdata,
        parseCtxName=None,
        xheaders={},
        preAnalyzer=None,
        postAnalyzer=None,
    ):
        """
        Make an HTTP request and retrieve the reply from the server.
        An implementation using a few high-level methods from the
        urllib2 module is also possible, however it is many times
        slower than this implementation, and pulls in modules that
        are not always available (e.g. when running in maya's python).
        """
        outdata = None
        errcode = 0
        hsock = None

        try:
            # like:  http://tractor-engine:80/Tractor/task?q=nextcmd&...
            # we use POST when making changes to the destination (REST)
            req = "POST " + self.urlprefix + tractorverb + " HTTP/1.0\r\n"
            for h in self.appheaders:
                req += h + ": " + self.appheaders[h] + "\r\n"
            for h in xheaders:
                req += h + ": " + xheaders[h] + "\r\n"

            t = ""
            if formdata:
                t = formdata.strip()
                t += "\r\n"
                if t and "Content-Type: " not in req:
                    req += "Content-Type: application/x-www-form-urlencoded\r\n"

            req += "Content-Length: %d\r\n" % len(t)
            req += "\r\n"  # end of http headers
            req += t

            # error checking?  why be a pessimist?
            # that's why we have exceptions!

            errcode, outdata, hsock = self.httpConnect()

            if hsock:
                hsock.settimeout(min(55.0, self.timeout))  # 55sec *send* wait
                hsock.sendall(str.encode(req))  ## -- send the request! -- ##

                errcode, outdata = self.collectHttpReply(hsock, parseCtxName)

                if not errcode:
                    errcode, outdata = self.httpUnpackReply(
                        outdata, parseCtxName, preAnalyzer, postAnalyzer
                    )
        except Exception as e:
            raise (e)
            errcode = e
            outdata = {"msg": "http transaction: " + str(e)}

        if parseCtxName and not isinstance(outdata, dict):
            outdata = {"msg": outdata}

        if not isinstance(errcode, int):
            errcode = -1

        if hsock:
            try:
                hsock.close()
            except:
                pass

        return (errcode, outdata)

    ## --------------------------------- ##
    def httpConnect(self):
        outdata = None
        errcode = 0
        hsock = None
        try:
            # We can't use a simple socket.create_connection() here because
            # we need to protect this socket from being inherited by all of
            # the subprocesses that tractor-blade launches. Since those *may*
            # be happening in a different thread from this one, we still have
            # a race between the socket creation line and trSetNoInherit line
            # below. Python 3.2+ will finally add support for the atomic CLOEXEC
            # bit in the underlying socket create, but only for Linux.

            hsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            trSetNoInherit(hsock)
            trEnableTcpKeepAlive(hsock)
            hsock.settimeout(min(15.0, self.timeout))  # wait for *connect* step

            hsock.connect(self.resolveEngineHost())

            # if we get here with no exception thrown, then
            # the connect succeeded; save peer ip addr
            self.lastPeerQuad = hsock.getpeername()[0] + ":" + str(self.port)

        except socket.timeout:
            outdata = (
                "http connect(" + self.host + ":" + str(self.port) + "): timed out"
            )
            errcode = errno.ETIMEDOUT

        except socket.gaierror as e:
            outdata = "hostname lookup failed: " + self.host
            errcode = e

        except socket.herror as e:
            outdata = "gethostbyname lookup failed: " + self.host
            errcode = e

        except Exception as e:
            errcode = e
            outdata = "http connect(" + self.host + ":" + str(self.port) + "): "
            if e in (errno.ECONNREFUSED, errno.WSAECONNREFUSED):
                outdata += "connection refused"
            elif e in (errno.ECONNRESET, errno.WSAECONNRESET):
                outdata += "connection dropped"
            elif e in (errno.ETIMEDOUT, errno.WSAETIMEDOUT):
                outdata += "connect attempt timed-out (routing? firewall?)"
            elif e in (errno.EHOSTUNREACH, errno.ENETUNREACH, errno.ENETDOWN):
                outdata += "host or network unreachable"
            else:
                outdata += str(e)

        except KeyboardInterrupt:
            raise

        except:
            errclass, excobj = sys.exc_info()[:2]
            outdata = errclass.__name__ + " - " + str(excobj)
            errcode = 999

        if errcode and hsock:
            try:
                hsock.close()
                hsock = None
            except:
                hsock = None

        return (errcode, outdata, hsock)

    ## --------------------------------- ##
    def resolveEngineHost(self):

        if self.engineResolved:
            # use cached value
            return (self.host, self.port)

        if self.host not in ("tractor-engine", "@"):
            # caller gave a specific non-default name, always cache that
            self.engineResolved = True
            return (self.host, self.port)

        # otherwise ...
        # For the special case of the default host name,
        # check to see if it is actually resolvable,
        # and if not, try a LAN multicast search

        attemptDisco = False
        try:
            h = socket.gethostbyname(self.host)
            if h:
                self.engineResolved = True
                return (self.host, self.port)

        except socket.gaierror:
            attemptDisco = True

        except socket.herror:
            attemptDisco = True

        except:
            raise

        if attemptDisco:
            # The nameserver lookup failed, so try the EngineDiscovery
            # fallback service that the engine may be running.
            try:
                from . import TractorLocator

                found = TractorLocator.TractorLocator().Search()
                if found:
                    self.host, self.port = found
                    self.engineResolved = True
                    return (self.host, self.port)
            except:
                errclass, excobj = sys.exc_info()[:2]
                self.Debug(
                    "TractorLocator error: " + errclass.__name__ + " - " + str(excobj)
                )

        if not self.resolveFallbackMsgSent:
            self.Debug("could not resolve 'tractor-engine' -- trying localhost")
            self.resolveFallbackMsgSent = True

        return ("127.0.0.1", 80)

    ## --------------------------------- ##
    def collectHttpReply(self, hsock, parseCtxName):
        #
        # collect the reply from an http request already sent on hsock
        #
        if parseCtxName:
            errnm = str(parseCtxName)
        else:
            errnm = ""

        mustTimeWait = False

        out = ""  # build up the reply text
        err = 0

        # we rely on the poll/select timeout behavior in the "internal_select"
        # implementation (C code) of the python socket module; that is:
        # the combination of recv + settimeout gets us out of wedged recv

        hsock.settimeout(self.timeout)
        while 1:
            try:
                r = hsock.recv(4096)
                if r:
                    out += r.decode()
                else:
                    break  # end of input

            except socket.timeout:
                out = "time-out waiting for http reply " + errnm
                err = errno.ETIMEDOUT
                mustTimeWait = True
                break
            except Exception as e:
                out = "error " + str(e) + " waiting for http reply " + errnm
                err = e
                mustTimeWait = True
                break

        # Attempt to reduce descriptors held in TIME_WAIT on the
        # engine by dismantling this request socket immediately
        # if we've received an answer.  Usually the close() call
        # returns immediately (no lingering close), but the socket
        # persists in TIME_WAIT in the background for some seconds.
        # Instead, we force it to dismantle early by turning ON
        # linger-on-close() but setting the timeout to zero seconds.
        #
        if not mustTimeWait:
            hsock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )

        return (err, out)

    ## --------------------------------- ##
    def httpUnpackReply(self, t, parseCtxName, preAnalyzer, postAnalyzer):

        if t and len(t):
            n = t.find("\r\n\r\n")
            h = t[0:n]  # headers

            n += 4
            outdata = t[n:].strip()  # body, or error msg, no CRLF

            n = h.find(" ") + 1
            e = h.find(" ", n)
            errcode = int(h[n:e])

            if errcode == 200:
                errcode = 0

            # expecting a json dict?  parse it
            if outdata and parseCtxName and (0 == errcode or "{" == outdata[0]):
                # choose between pure json parse and eval
                jsonParser = json.loads
                if not preAnalyzer:
                    preAnalyzer = self.engineProtocolDetect

                jsonParser = preAnalyzer(h, errcode, jsonParser)

                try:
                    if jsonParser:
                        outdata = jsonParser(outdata)

                except Exception:
                    errcode = -1
                    self.Debug("json parse:\n" + outdata)
                    outdata = "parse %s: %s" % (parseCtxName, self.Xmsg())

            if postAnalyzer:
                postAnalyzer(h, errcode)

        else:
            outdata = "no data received"
            errcode = -1

        return (errcode, outdata)

    ## --------------------------------- ##
    def GetLastPeerQuad(self):
        return self.lastPeerQuad

    ## --------------------------------- ##
    def Debug(self, txt):
        if self.logger:
            self.logger.debug(txt)

    def Xmsg(self):
        if self.logger and hasattr(self.logger, "Xcpt"):
            return self.logger.Xcpt()
        else:
            errclass, excobj = sys.exc_info()[:2]
            return "%s - %s" % (errclass.__name__, str(excobj))

    def trStrToHex(self, str):
        s = ""
        for c in str:
            s += "%02x" % ord(c)
        return s

    def engineProtocolDetect(self, htxt, errcode, jsonParser):
        # Examine the engine's http "Server: ..." header to determine
        # whether we may be receiving pre-1.6 blade.config data which
        # is not pure json, in which case we need to use python "eval"
        # rather than json.loads().

        n = htxt.find("\nServer:")
        if n:
            n = htxt.find(" ", n) + 1
            e = htxt.find("\r\n", n)
            srvstr = htxt[n:e]
            # "Pixar_tractor/1.5.2 (build info)"
            v = srvstr.split()
            if v[0] == "Pixar":  # rather than "Pixar_tractor/1.6"
                v = ["1", "0"]
            else:
                v = v[0].split("/")[1].split(".")
            try:
                n = float(v[1])
            except:
                n = 0
            if v[0] == "1" and n < 6:
                jsonParser = eval

        return jsonParser

    def PasswordRequired(self):
        if None != self.passwdRequired:
            return self.passwdRequired  # already determined

        self.passwdRequired = False
        try:
            # get the site-defined python client functions
            err, data = self.Transaction("config?file=trSiteFunctions.py", None, None)
            if data and not err:
                exec(data)  # instantiate function defs (use imp someday)
                self.passwdRequired = trSitePasswordHash("01", "01") != None
                self.passwordhashfunc = trSitePasswordHash

            if not self.passwdRequired:
                err, data = self.Transaction("monitor?q=loginscheme", None, "chklogins")
                if data and not err:
                    if data["validation"].startswith("internal:"):
                        self.passwordhashfunc = trInternalPasswordHash
                        self.passwdRequired = True

        except Exception as err:
            # Error due to file missing or bogus functions in the file.
            # Revert back to the default password hash.
            self.passwdRequired = self.passwordhashfunc("01", "01") != None

        if self.passwdRequired and not self.passwordhashfunc:
            self.passwordhashfunc = trNoPasswordHash

        return self.passwdRequired

    def Login(self, user, passwd):
        #
        # Provides generic login support to the tractor engine/monitor
        # This module first attempts to retrieve the standard python
        # dashboard functions and executes this file to provide the
        # TrSitePasswordHash() function
        #
        # If this returns a password that is not None, then the Login module
        # requests a challenge key from the engine, then encodes the password
        # hash and challenge key into the login request
        #
        # The engine will run the "SitePasswordValidator" entry as defined in
        # the crews.config file.
        #

        loginStr = "monitor?q=login&user=%s" % user

        passwdRequired = self.PasswordRequired()
        if passwdRequired:
            if not passwd:
                raise TrHttpError("Password required, but not provided")
            else:
                # get a challenge token from the engine
                err, data = self.Transaction("monitor?q=gentoken", None, "gentoken")

                if err or not data:
                    challenge = None
                else:
                    challenge = data["challenge"]

                if err or not challenge:
                    raise TrHttpError(
                        "Failed to generate challenge token."
                        + " code="
                        + str(err)
                        + " - "
                        + str(data)
                    )

                # update the login URL to include the encoded challenge
                # and password
                challengepass = (
                    challenge + "|" + self.passwordhashfunc(passwd, challenge)
                )
                loginStr += "&c=%s" % urllib.parse.quote(self.trStrToHex(challengepass))

        err, data = self.Transaction(loginStr, None, "register")
        if err:
            raise TrHttpError(
                "Tractor login failed. code=" + str(err) + " - " + str(data)
            )

        tsid = data["tsid"]
        if tsid == None:
            raise TrHttpError(
                "Tractor login as '"
                + user
                + "' failed. code="
                + str(err)
                + " - "
                + str(data)
            )

        return data


## ------------------------------------------------------------- ##
#
# define a platform-specific routine that makes the given socket
# uninheritable, we don't want launched subprocesses to retain
# an open copy of this file descriptor
#

if "win32" == sys.platform:
    import ctypes, ctypes.wintypes

    SetHandleInformation = ctypes.windll.kernel32.SetHandleInformation
    SetHandleInformation.argtypes = (
        ctypes.wintypes.HANDLE,
        ctypes.wintypes.DWORD,
        ctypes.wintypes.DWORD,
    )
    SetHandleInformation.restype = ctypes.wintypes.BOOL
    win32_HANDLE_FLAG_INHERIT = 0x00000001

    def trSetNoInherit(sock):
        fd = int(sock.fileno())
        SetHandleInformation(fd, win32_HANDLE_FLAG_INHERIT, 0)

    def trSetInherit(sock):
        fd = int(sock.fileno())
        SetHandleInformation(fd, win32_HANDLE_FLAG_INHERIT, 1)


else:
    import fcntl

    def trSetNoInherit(sock):
        oldflags = fcntl.fcntl(sock, fcntl.F_GETFD)
        fcntl.fcntl(sock, fcntl.F_SETFD, oldflags | fcntl.FD_CLOEXEC)

    def trSetInherit(sock):
        oldflags = fcntl.fcntl(sock, fcntl.F_GETFD)
        fcntl.fcntl(sock, fcntl.F_SETFD, oldflags & ~fcntl.FD_CLOEXEC)


def trEnableTcpKeepAlive(sock):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)


## ------------------------------------------------------------- ##


def trNoPasswordHash(passwd, challenge):
    #
    # This is the default, no-op, password hash function.
    # The site-provided real one can be defined in the site's
    # tractor configuration directory, in trSiteFunctions.py,
    # or in other override config files.
    #
    return None


def trInternalPasswordHash(passwd, challenge):
    #
    # This encoding function is used for "PAM" style logins.
    # **NOTE** it assumes that your client is connected to the
    # engine over a secure connection (internal LAN or VPN)
    # because a recoverable encoding is used to deliver the
    # password to the unix PAM module on the engine.
    #
    n = len(passwd)
    k = len(challenge)
    if k < n:
        n = k

    h = "1"  # variant

    for i in range(0, n):
        k = ord(passwd[i]) ^ ord(challenge[i])
        if k > 255:
            k = 255
        h += "%02x" % k

    return h


## ------------------------------------------------------------- ##
