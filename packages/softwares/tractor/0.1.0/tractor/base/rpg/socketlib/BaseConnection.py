# BaseConnection.py

import time

from rpg.progutil import log

__all__ = (
        'BaseConnection',
        )

# ---------------------------------------------------------------------------

class BaseConnection(object):
    """Every connection type to the server should be subclassed from here."""

    IdleTimeout = 30  # a keepalive connection will be dropped after # secs

    def __init__(self, sock, server, **kwargs):
        """Initialize the object with any number of additional arguments."""
        self.keepalive = 0
        # set any additional args
        for k,v in list(kwargs.items()):
            self.__dict__[k] = v

        self.socket = sock
        self.server = server   # server object may have some info we'll
        # want to pass back to a client, so make it visible to connection
        
        # the last update of this connection
        self.lastmsg = time.time()

        self._update = None

        super(BaseConnection, self).__init__()
        

    def __str__(self):
        return self.__class__.__name__ + str(self.socket.address)

    def __repr__(self):
        return self.__str__()

    def fileno(self):
        """Overloaded to work with the select method."""
        return self.socket.fileno()

    def close(self):
        self.socket.close()

    def update(self, now=0):
        """This is to be overloaded by subclasses."""
        pass

    def healthy(self, now=0, timeout=0):
        """Report if a connection is healthy.  If a connection should be
        removed then return false.  An optional now argument is provided
        to give the current time."""

        # did we lose the connection
        if self.fileno() < 0:
            log("lost connection with " + str(self))
            return 0

        # make sure we know the current time
        if not now: now = time.time()
        if not timeout: timeout = self.IdleTimeout

        # when was the last message received
        if timeout and (now - self.lastmsg) > timeout:
            log(str(self) + " was idle for more than %d seconds, " \
                  "removing" % timeout)
            return 0

        return 1
