# ServerClientSocket.py

from rpg.socketlib.Sockets import Socket, SocketError

__all__ = (
        'ServerClientSocketError',
        'ServerClientSocket',
        )

# ---------------------------------------------------------------------------

class ServerClientSocketError(SocketError):
    """All Server/Client socket errors."""
    pass

# ----------------------------------------------------------------------------

class ServerClientSocket(Socket):
    """Any server that must manage client connections should subclass
    from here."""

    def __init__(self, sock, address, blocking=1, **kw):
        """Initialize a client connection by sending the socket object
        connection to the client and the client's address.  If the
        blocking flag is set to false then the socket will be configured
        for non-blocking operations."""
        
        Socket.__init__(self, address, blocking=blocking, **kw)
        self.socket = sock
        self._setsockopts()
