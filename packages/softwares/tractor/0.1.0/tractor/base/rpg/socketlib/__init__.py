"""
Socket classes used to either connect to servers of various programs or
manage client connections on a server end.  All classes are meant as
wrappers around the socket calls that are made to communicate with the
servers/clients.  They are meant to catch most of the errors related to
these calls and communicate with the server/client protocol.
The classes are:

  - Socket
    base class for all socket classes that overloads the send and recv
    methods to catch the most common cases.
      - ClientServerSocket
        base class for all sockets that connect to a server
          - SimpleClient
            simple client class to send a message to a server and read a
            response
      - ServerClientSocket
        base class for all connections from clients that a server must
        manage.

"""

import re

__all__ = (
        'stripDomain',
        )

# ----------------------------------------------------------------------------

_ipre = re.compile(r'^\d+\.\d+\.\d+\.\d+$')
def stripDomain(host, convertIP=0):
    """Strip the domain name of a host off."""

    # strip off the domain name so we don't mistake
    #  host.pixar.com to be different from host, but
    #  make sure it isn't an ip address.
    if _ipre.match(host):
        if convertIP:
            hostinfo = socket.gethostbyaddr(host)
            if hostinfo:
                return hostinfo[0]
        return host

    return host.split('.')[0]
