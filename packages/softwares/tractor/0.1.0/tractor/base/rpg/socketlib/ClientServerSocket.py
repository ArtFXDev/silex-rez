# ClientServerSocket.py

import socket, time, errno, select

from rpg.socketlib.Sockets import SocketError, TimeOut, Socket, WouldBlock

__all__ = (
        'ClientServerSocketError',
        'OpenError',
        'AuthenticationError',
        'OpenTimeOut',
        'AuthenticationTimedOut',
        'ClientServerSocket',
        )

# ---------------------------------------------------------------------------

class ClientServerSocketError(SocketError):
    """All Client/Server socket errors."""
    pass
class OpenError(ClientServerSocketError):
    """Error while opening socket."""
    pass
class AuthenticationError(ClientServerSocketError):
    """Error related to authenticating the connection."""
    pass
class OpenTimeOut(TimeOut):
    """Opening socket timed out."""
    pass
class AuthenticationTimedOut(TimeOut):
    """Authenticating the connection timed out."""
    pass


# ----------------------------------------------------------------------------

class ClientServerSocket(Socket):
    """Any socket that connects to a server should be subclassed from
    here."""

    def __init__(self, address, **kwargs):
        """Creates a Socket object with the address used to open
        a socket connection when open() is called.  address should be a
        tuple like is sent to a socket, i.e. ('funyuns', 1234).  If the
        blocking flag is set to false then the socket will be configured
        for non-blocking operations after it is opened."""

        Socket.__init__(self, address, **kwargs)
        # if set, should give the time the connection was authenticated
        self.authenticated = 0
        # time that connect was called, not necessarily the time the
        # socket was opened.
        self.connecttime = 0
        # time that we established a connection with the remote address
        self.opentime = 0

    def _init_socket(self):
        """Initialize the socket object."""

        try:
            # create and configure the socket
            self.socket = socket.socket(self.address_family, self.socket_type)
        except socket.error as errMsg:
            raise ClientServerSocketError(self.address, str(errMsg),
                                          ecode=errMsg[0])
        self._setsockopts()

    def open(self, timeout=0, authenticate=1, blocking=1):
        """Attempt to open socket to the address set in __init__.  timeout
        is used to determine when the attempt should bail.  Any
        socket error will raise an OpenError exception and a timeout will
        raise an OpenTimeOut exception.  The authenticate() method is called
        when a valid connection is established."""

        # initialize the socket if it hasn't been done.
        if not self.socket: self._init_socket()

        # we want the timeout to be supported by blocking and nonblocking
        # sockets, so if blocking then temporarily set to nonblocking.  By
        # default opening a socket will block for no more than timeout
        # seconds, however, if your application is built robust enough you
        # might want opens to be non-blocking as well.
        if self.blocking and timeout:
            self.socket.setblocking(0)

        knownerrors = ('ECONNREFUSED', 'ETIMEDOUT',
                       'ENETUNREACH', 'EHOSTUNREACH',
                       'EHOSTDOWN', 'EBADF'', ECONNRESET')

        try:
            if not self.connecttime:
                self.connecttime = time.time()
            self.socket.connect(self.address)
        except (socket.error, socket.gaierror, socket.herror) as errMsg:
            ecode = errno.errorcode.get(errMsg[0], None)
            if ecode in knownerrors:
                raise OpenError(self.address, str(errMsg), ecode=errMsg[0])
            elif ecode != 'EINPROGRESS':
                raise OpenError(self.address, "unknown error: " +
                                str(errMsg), ecode=errMsg[0])

            # if the open is not meant to be blocking, then raise a WouldBlock
            # exception, it will be up to the caller to check on the socket
            # via select
            if not blocking:
                raise WouldBlock(self.address, str(errMsg), ecode=errMsg[0])

            # use select to wait for the socket to be ready
            try:
                if timeout:
                    input,output,err = select.select([self.socket],
                                                     [self.socket],
                                                     [], timeout)
                else:
                    input,output,err = select.select([self.socket],
                                                     [self.socket], [])
                
                if not (input or output):
                    Socket.close(self)
                    raise OpenTimeOut(self.address, "Opening socket timed out "
                                      "after %d seconds" % timeout)

                # call connect one more time so it raises an exception if
                # there is a problem
                self.socket.connect(self.address)
            except (socket.error, socket.gaierror, socket.herror) as errMsg:
                ecode = errno.errorcode.get(errMsg[0], None)
                if ecode != 'EISCONN':
                    Socket.close(self)
                    if ecode in knownerrors:
                        raise OpenError(self.address, str(errMsg), ecode=errMsg[0])
                    else:
                        raise OpenError(self.address, "unknown error: " +
                                        str(errMsg), ecode=errMsg[0])

            except select.error as errMsg:
                Socket.close(self)
                raise OpenError(self.address, "unknown error: " +
                                str(errMsg), ecode=errMsg[0])

        # set the open time now that we have definitely opened
        self.opentime = time.time()

        # set back to a blocking socket
        if self.blocking and timeout:
            self.socket.setblocking(1)
            
        if authenticate:
            newtimeout = timeout
            if timeout:
                newtimeout -= (time.time() - self.connecttime)

                if newtimeout <= 0:
                    Socket.close(self)
                    raise OpenTimeOut(self.address, "Opening socket timed "
                                      "out after %d seconds" % timeout)
                
            # try to authenticate the connection
            self.authenticate(timeout=newtimeout)

        self.readtime = time.time()
        # reset the connect time
        self.connecttime = 0
        # reset next message buffers
        self._reset()

    def authenticate(self, timeout=0):
        """Authenticates the connection with the server.  The default is
        to do nothing, but if a specific message must be sent to the server
        before continuing this is the function to overload."""
        pass

    def close(self):
        """Close the socket."""
        self.opentime = 0
        self.authenticated = 0
        self.connecttime = 0
        Socket.close(self)

# ----------------------------------------------------------------------------

def _test():
    import time
    sock = ClientServerSocket(('funyuns', 9001), blocking=0)
    sock.open()
    while 1:
        print(time.ctime())
        sock.send('0123456789'*1000, timeout=5)
        time.sleep(0.1)

if __name__=='__main__':
    _test()
