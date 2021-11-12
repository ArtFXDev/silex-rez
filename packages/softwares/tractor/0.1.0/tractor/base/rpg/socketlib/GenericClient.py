# GenericClient.py

import time

from rpg.socketlib import Sockets
from rpg.socketlib.GenericServer import PickleSocket
from rpg.socketlib.ClientServerSocket import ClientServerSocket, \
     AuthenticationTimedOut, AuthenticationError

__all__ = (
        'TimeoutInterval',
        'GenericClient',
        )

# ---------------------------------------------------------------------------

TimeoutInterval = 15

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class GenericClient(ClientServerSocket, PickleSocket):


    def __init__(self, address, timeout=TimeoutInterval, keepalive=0,
                 **kwargs):
        """Initialize the object by setting an authenticated flag to
        keep track of whether or not the connection has been authenticated
        yet or not."""

        self.authenticated = 0
        self.timeout       = timeout
        self.keepalive     = keepalive
        self.authMsgSent   = 0
        # the actual address will be decided when open is called
        ClientServerSocket.__init__(self, address, **kwargs)


    def open(self, timeout=None, authenticate=1, **kwargs):
        """Clears the authentication information before opening the
        connection."""
        self.authMsgSent   = 0
        self.authenticated = 0
        if timeout is None:
            timeout = self.timeout
        ClientServerSocket.open(self, timeout=timeout,
                                authenticate=authenticate,
                                **kwargs)

    def close(self, msg='quit'):
        """Close the connection with server."""

        # if this is a keepalive connection then make sure we tell the
        # server that we are done.
        if self.keepalive and msg:
            try:
                self.send({'close': msg}, timeout=self.timeout)
            # it's possible that we are already disconnected
            except Sockets.SocketError:
                pass

        ClientServerSocket.close(self)


    def authenticate(self, timeout=0, readresp=1, ctype=None, **kwargs):
        """Authenticate the connection.  Subclasses must overload this
        and provide the appropriate client type 'ctype'."""

        elapsed  = 0.0
        waittime = 0.25

        # first try to send the authentication message, if it hasn't
        # already been sent from a previous attempt.
        if not self.authMsgSent:
            authdict = {'type': ctype, 'keepalive': self.keepalive}
            authdict.update(kwargs)
            while not timeout or (elapsed < timeout):
                try:
                    self.send(authdict, checkAuth=0)
                # if it is going to block, wait a little bit
                except Sockets.WouldBlock:
                    if not self.blocking:
                        raise
                    time.sleep(waittime)
                    elapsed += waittime
                # if it worked then break out of the loop
                else:
                    self.authMsgSent = 1
                    break
            else:
                raise AuthenticationTimedOut(self.address,
                                             "sending authentication "
                                             "response timed out.")

        # should we read a response?
        if readresp:
            # now try to read the response
            while not timeout or elapsed < timeout:
                try:
                    response = self.nextMsg(checkAuth=0)
                except Sockets.WouldBlock:
                    if not self.blocking:
                        raise
                    time.sleep(waittime)
                    elapsed += waittime
                else:
                    break
            else:
                raise AuthenticationTimedOut(self.address,
                                             "reading authentication "
                                             "response timed out.")

            if 'error' in response or \
               response.get('result', None) != ctype:
                raise AuthenticationError(self.address,
                                          "unable to authenticate: " +
                                     response.get('error', 'unknown error'))

        self.authenticated = time.time()


    def send(self, msg, checkAuth=1, timeout=0):
        """All messages are first packed into RAMP data structures and
        then delimited."""

#        if checkAuth and not self.authenticated:
#            self.authenticate(timeout=self.timeout)

        PickleSocket.send(self, msg, timeout=timeout)


    def nextMsg(self, checkAuth=1, timeout=0):
        """Convenience function that handles any buffering required to
        read the next full message.  This is overloaded so we can always
        unpickle the message."""

#        if checkAuth and not self.authenticated:
#            self.authenticate(timeout=self.timeout)

        # get the next message delimited by the delim character
        return PickleSocket.nextMsg(self, timeout=timeout)


    def _createMsg(self, request, **kwargs):
        """Get the message dictionary that will be used during a
        transaction."""
        msg = {}
        msg.update(kwargs)
        msg['request'] = request
        msg['keepalive'] = self.keepalive
        return msg

    def transaction(self, request, **kwargs):
        """Perform a transaction and return the result, or it will
        raise an error if it reads an error from MissesD."""

        self.send(self._createMsg(request, **kwargs), timeout=self.timeout)
        response = self.nextMsg(timeout=self.timeout)

        if 'error' in response:
            print('response = ', response)
            raise Sockets.SocketError(self.address, response['error'])

        return response.get('result', None)



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
