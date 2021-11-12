import pickle

from rpg.socketlib.Sockets import SocketError

__all__ = (
        'PickledSocketError',
        'PickledSocket',
        )

# ----------------------------------------------------------------------------

class PickledSocketError(SocketError):
    """Any error related to pickling/unpickling messages sent/received
    from a socket."""
    pass

# ----------------------------------------------------------------------------

class PickledSocket:
    """Designed to be a mix-in class to allow messages to be pickled
    before sending them and unpickled after they are received."""

    def pickle(self, data):
        """Pickle some data."""
        return pickle.dumps(data)

    def unpickle(self, data):
        """Unpickle some data."""
        try:
            msg = pickle.loads(data)
        except (pickle.PickleError, KeyError, ValueError, IndexError) as err:
            raise PickledSocketError(self.address, 'unable to unpickle the '
                                     'data: ' + data)
        return msg
