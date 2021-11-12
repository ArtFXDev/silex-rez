import re

import rpg.socketlib.Sockets as Sockets
from rpg.socketlib.ClientServerSocket import ClientServerSocketError
from rpg.socketlib.ClientServerSocket import ClientServerSocket

__all__ = (
        'ClientSocketError',
        'ClientSocket',
        'SimpleClient',
        )

# ---------------------------------------------------------------------------

# this is to make it combatible with previous verions
ClientSocketError = ClientServerSocketError
ClientSocket      = ClientServerSocket

# ----------------------------------------------------------------------------

class SimpleClient(ClientServerSocket):
    """A simple client socket class that doesn't require anything
    to be sent to the server at connect time.  Supports non-blocking
    authentication and all sends will have a newline appended."""

    endresp = re.compile('.*\n$', re.DOTALL)

    def __init__(self, address, blocking=1, timeout=15):
        """Initialize an object with an address and an optional
        blocking flag."""

        self.header  = ''
        self.timeout = timeout
        ClientServerSocket.__init__(self, address, blocking=blocking)

    def open(self, timeout=None, authenticate=1):
        """Overloaded to provide the global timeout."""
        if timeout is None:
            timeout = self.timeout
        ClientServerSocket.open(self, timeout=timeout,
                                authenticate=authenticate)

    def authenticate(self, timeout=0):
        """When a connection is made with the server it sends a header
        to identify itself.  The header is read in here and saved in
        self.header."""

        try:
            self.header = self.recv(timeout=timeout)
        except ReadTimeOut:
            raise AuthenticationTimedOut(self.address,
                                         "authenticating with server "
                                         "timed out.")

    def close(self, msg='quit'):
        """Closes the connection with the server by sending 'quit'
        to cleanly close the connection."""

        if msg:
            try:
                self.sendCmd('quit', readresp=0)
            # it's possible that we are already disconnected
            except Sockets.SocketError:
                pass

        ClientServerSocket.close(self)

    def send(self, msg, timeout=0):
        """Appends the newline character to the string before sending."""
        ClientServerSocket.send(self, msg + '\n', timeout=timeout)

    def sendCmd(self, cmd, readresp=1):
        """Send a command to the server of a SimpleClient, the 'cmd'
        argument should be the full command including any arguments.
        The response from the server will be returned."""

        self.send(cmd, timeout=self.timeout)
        if readresp:
            return self.readResponse()

    def readResponse(self):
        """Read the response from the server."""

        response = ''
        # continue to read from the socket until we get to an ending point
        while 1:
            response += self.recv(timeout=self.timeout)
            if self.endresp.match(response):
                break

        return response

