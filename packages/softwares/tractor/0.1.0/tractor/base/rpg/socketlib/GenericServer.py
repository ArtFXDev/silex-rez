# GenericServer.py

import os, time, random
import pickle as pickle
import socketserver, socket, select
from .Sockets import SocketError

from rpg.progutil import log as progLog, logError
from rpg.tracebackutil import printTraceback, getTraceback
from rpg.osutil import stripDomain
from rpg.osutil import getlocalhost
from rpg.socketlib import Sockets
from rpg.socketlib.ServerClientSocket import ServerClientSocket
from rpg.mailutil import sendmail

__all__ = (
        'log',
        'PickleSocketDef',
        'PickleSocket',
        'GenericServer',
        'PickleSocketError',
        'PickleSocketLoadError'
        )

# ---------------------------------------------------------------------------


def log(msg):
    progLog(msg, color='cyan')

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class PickleSocketError(SocketError):
    pass

class PickleSocketLoadError(PickleSocketError):
    """Problem loading pickled message."""
    pass

class PickleSocketDef:
    """This is a mixin class for defining the protocol of pickled
    socket communication.
    """

    def send(self, msg, *args, **kw):
        """All messages are first packed by pickle and then delimited."""
        
        pickmsg = pickle.dumps(msg) + self.delimiter
        #print 'sending %d bytes' % len(pickmsg)
        ServerClientSocket.send(self, pickmsg, *args, **kw)


    def nextMsg(self, *args, **kw):
        """Convenience function that handles any buffering required to
        read the next full message.  This is overloaded so we can always
        unpack the pickled message."""

        # get the next message delimited by the delim character
        msg = ServerClientSocket.nextMsg(self, self.delimiter, *args, **kw)
        
        try:
            data = pickle.loads(msg)
            #if random.randint(1,100) == 1:
            #    raise pickle.BadPickleGet, 'test bad pickle!'
            return data
        except EOFError as e:
            return {}
        except (ValueError, TypeError, KeyError, IndexError,
                pickle.PickleError) as e:
            err = "%s: msg='%s'" % (str(e), str(msg))
            raise PickleSocketLoadError(self.address, err)


class PickleSocket(ServerClientSocket, PickleSocketDef):

    delimiter = '\003'  # this is the default if it does not get defined

    def __init__(self, sock, address, **kw):
        """Same init.  Just grab delimiter."""
        ServerClientSocket.__init__(self, sock, address, **kw)


    def send(self, *args, **kw):
        return PickleSocketDef.send(self, *args, **kw)


    def nextMsg(self, *args, **kw):
        return PickleSocketDef.nextMsg(self, *args, **kw)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class GenericServer(object):
    """This class sets up a generic TCP server that accepts connections
    via the specified port.  Additional file descriptors outside of
    client connections can be made a part of the select() call by
    initializing the server with them under the otherfds parameter.
    Subclassed servers can then process these fds in a callback
    defined in the subclass, and specified with the otherfdsCB parameter.

    Any other processing that should happen after the select() can
    be defined in a callback method, specified by postSelectCB.
    """

    MaxConnections = 50

    def __init__(self, port=None, connectionClass=None,
                 otherfds=[], otherfdsCB=None, selectTimeout=1,
                 preSelectCB=None, postSelectCB=None, debug=False,
                 notify=[], useCStringIO=True, queueSends=False, **kwargs):

        self.servPort = port
        self.ConnectionClass = connectionClass
        self.pid = os.getpid()
        self.socket = None
        self.preSelectCB = preSelectCB
        self.postSelectCB = postSelectCB
        self.debug = debug
        self.notify = notify       # list of users to receive mail on shutdown

        # this determines whether cStringIO is used for socket buffers
        self.useCStringIO = useCStringIO
        # boolean to determine whether the server should queue send messages
        # for blocking clients
        self.queueSends = queueSends
        
        self.otherfds = otherfds       # list of input fds to check on select
        self.otherfdsCB = otherfdsCB   # callback to call if fds are selected

        self.selectTimeout = selectTimeout  
        
        self.conns = {}  # indexed by (hostname, port) yields connection obj

        self.shutdown = 0          # gets set to 0 when server should shutdown
        self.closemsg = 'unknown'  # short message to describe why server quit

        super(GenericServer, self).__init__(**kwargs)

    # ----------------------------------------------------------------------

    def startServer(self):
        """Starts the server up."""

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        msgsent = 0

        # start the server up and don't quit until we get the port.
        while 1:
            try:
                self.socket.bind(("", self.servPort))
            except socket.error as errMsg:
                if errMsg[0] != 98:
                    raise
                else:
                    if not msgsent:
                        log("waiting for port %d to be free" % \
                                 self.servPort)
                        msgsent = 1
                    time.sleep(1)
            else:
                break

        log("binding server to port: %d" % self.servPort)

        # set the server queue to be really big so we can accept
        # several connections at once.
        self.socket.listen(self.MaxConnections)

        # time the server was started
        self.starttime = time.time()


    # ----------------------------------------------------------------------

    def shutdownServer(self, exit=0):
        """Close all connections and file handlers."""

        # there are open exceptions here, which is okay, because
        # we really want to shut down the server here!

        try:
            log("shutdownServer(): %s" % self.closemsg)
        except:
            printTraceback()
            
        for conn in list(self.conns.values()):
            try:
                conn.close()
                self.removeConnection(conn)
            except:
                printTraceback()

        try:
            if self.socket:
                self.socket.close()
        except:
            printTraceback()

        if exit:
            os._exit(2)


    # ----------------------------------------------------------------------

    def getServerSocket(self):
        """Get a Socket object for a connection request to a server."""

        request,client_address = self.socket.accept()
        # try to get a hostname from an IP, otherwise just use the IP
        try:
            hostname     = socket.gethostbyaddr(client_address[0])[0]
            hostname     = stripDomain(hostname)
            if self.debug:
                log('accepted connection with %s' % hostname)
            withHostName = (hostname, client_address[1])
        except (socket.herror, socket.gaierror):
            withHostName = client_address

        # all sockets will be non-blocking

        return PickleSocket(request, withHostName, blocking=0,
                            useCStringIO=self.useCStringIO,
                            queueSends=self.queueSends)


    # ----------------------------------------------------------------------

    def getAllFileDescriptors(self):
        """Return a tuple containing the input and output lists that
        select will use."""

        input = [self.socket] + self.otherfds
        self.cleanupConnections()
        input.extend(list(self.conns.values()))

        return input,[]


    # ----------------------------------------------------------------------

    def createConnection(self):
        """A request to connect to the server has been made.  accept the
        request and add the connection to the connections list."""

        sock = self.getServerSocket()
        conn = self.ConnectionClass(sock, self)
        self.conns[sock.address] = conn


    # ----------------------------------------------------------------------

    def removeConnection(self, conn):
        """Remove a Connection object from self.conns."""

        try:
            del self.conns[conn.socket.address]
        except KeyError:
            log("attempting to remove unknown connection " + str(conn))


    # ----------------------------------------------------------------------

    def updateConnections(self, conns, now):
        """Call the update method for a list of connection objects."""

        for conn in conns:
            self.tryUpdate(conn, now)
            while conn.socket.messages:
                self.tryUpdate(conn, now)

    def tryUpdate(self, conn, now):
        try:
            conn.update(now)
        except (Sockets.WouldBlock, Sockets.Interrupted) as err:
            if self.debug:
                log(str(err))
                log('would have blocked or been interrupted')
        except (Sockets.LostConnection, Sockets.NotConnected) as err:
            conn.close()
            self.removeConnection(conn)
            log(str(err))
            log('closed connection due to error')
        except Sockets.SocketError as err:
            msg = '%s while reading socket for %s' % \
                  (str(err), str(conn))
            log(msg)
        

    # ----------------------------------------------------------------------

    def cleanupConnections(self):

        for key,conn in list(self.conns.items()):
            status = conn.healthy()
            if not status:
                # make sure connection is closed before removing it
                conn.close()
                self.removeConnection(conn)


    # ----------------------------------------------------------------------

    def mainloop(self):

        log("mainloop()...")

        self.startServer()

        input = [] # need this in case try fails before input is set
        while(1):
            try:
                if self.preSelectCB:
                    self.preSelectCB()

                input,output = self.getAllFileDescriptors()
                try:
                    # wait to see who is ready for reading
                    input,output,exc = select.select(input, output,
                                                     [], self.selectTimeout)
                except ValueError:
                    pass

                if self.debug:
                    #log('%d total %d input %d output' %
                    #    (len(sockets), len(input), len(output)))
                    if len(exc):
                        logError('%d exc' % len(exc))

                if self.postSelectCB:
                    self.postSelectCB()

                # if the server's socket is in the list of input sockets,
                # then someone is trying to connect, so accept it!
            
                if self.socket in input:
                    self.createConnection()
                    input.remove(self.socket)

                # call external handling of file descriptors, if any

                foundfds = []
                for fd in self.otherfds:
                    if fd in input:
                        foundfds.append(fd)
                        input.remove(fd)

                if self.otherfdsCB:
                    self.otherfdsCB(foundfds)

                # remaining fds are simply normal socket connections

                now = time.time()

                self.updateConnections(input, now)

                # 'nice' exit point from mainloop()

                if self.shutdown:
                    self.shutdownServer()
                    return 

            except KeyboardInterrupt as errMsg:
                log(str(errMsg))
                self.closemsg = 'Ctrl-C'
                self.shutdownServer()
                return  # this is an exit point from mainloop()
                
            except:
                self.closemsg = 'exception'
                printTraceback()
                if self.notify:
                    try:
                        log('Notify %s of shutdown' % str(self.notify))
                        body = getTraceback()
                        subject = '%s:%d died' % (getlocalhost(), self.servPort)
                        sendmail(self.notify, body, subject=subject)
                    except:
                        pass
                self.shutdownServer()
                return  # this is an exit point from mainloop()
                    
                # time.sleep(1)  # slow down errors to avoid filling drive


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

def test():

    server = GenericServer(port=2225)
    server.mainloop()
    server.shutdownServer()
    

# ---------------------------------------------------------------------------

if __name__=='__main__':
    test()

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
