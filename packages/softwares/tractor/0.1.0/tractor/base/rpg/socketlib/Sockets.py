# Socket.py

import socket, select, errno, io, time
import rpg
import rpg.unitutil as unitutil

__all__ = (
        'SocketError',
        'LostConnection',
        'NotConnected',
        'SendError',
        'RecvError',
        'WouldBlock',
        'Interrupted',
        'TimeOut',
        'SendTimeOut',
        'ReadTimeOut',
        'Socket',
        )

# ----------------------------------------------------------------------------

class SocketError(rpg.Error):
    """Base class of all Socket errors."""
    def __init__(self, addr, msg, ecode=-1):
        """Init a Socket exception with the address the socket
        is connected to and the error message."""
        self.addr  = addr
        self.msg   = msg
        self.ecode = ecode

    def __str__(self):
        return "%s, %s" % (str(self.addr), self.msg)

class LostConnection(SocketError):
    """Connection with remote address was lost."""
    pass
class NotConnected(SocketError):
    """No connection has been established."""
    pass
class SendError(SocketError):
    """Error when writing to socket."""
    pass
class RecvError(SocketError):
    """Error when reading from socket."""
    pass
class WouldBlock(SocketError):
    """The socket is not available for reading or writing and the
    operation would normally block.  This should only be raised
    when the socket is non-blocking."""
    pass
class Interrupted(SocketError):
    """When threads are used they often trigger EINTR exceptions which
    interrupt socket operations."""
    pass
class TimeOut(SocketError):
    """Socket call timed out."""
    pass
class SendTimeOut(TimeOut):
    """Sending of data timed out."""
    pass
class ReadTimeOut(TimeOut):
    """Reading of data timed out."""
    pass
class SendQueueLimit(SocketError):
    """The send queue has grown too large."""
    pass


# ----------------------------------------------------------------------------

class Socket(object):
    """Base class for all socket connections, server or client.  This
    defines send and recv methods that will raise the most common
    exceptions."""

    address_family = socket.AF_INET
    socket_type    = socket.SOCK_STREAM
    recvbufsize    = 1 << 13  # 8k
    sendbufsize    = 1 << 13  # 8k
    sendQueueSize  = 10<<20   # 10mb

    def __init__(self, address, blocking=1, useCStringIO=True,
                 queueSends=False):
        """Creates a Socket object and initializes self.socket to None.
        It is the responsibility of subclasses to set this value."""        
        self.socket   = None
        self.address  = address
        self.blocking = blocking

        self.useCStringIO = useCStringIO

        # these are only used if the nextMsg method is called
        self.messages = []
        if useCStringIO:
            self.recvBuf  = io.StringIO()
        else:
            self.recvBuf = ''

        # used to keep track of how many bytes of the most recent message
        # we have sent.  This is only used when send() is called with a
        # timeout option and allows for WouldBlock exceptions to be raised
        # while in _send() and still keep track of the message.
        self.__sent = 0

        self.__sendBuf  = ""
        self.queueSends = queueSends

        # the last buffer read
        #self.lastBuf  = ''
        # last time we read something from the socket
        self.readtime = 0

    def _setsockopts(self):
        """Set the appropriate socket options, this does not create a
        socket."""

        if not self.socket: return

        try:
            # configure the socket
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                                   self.recvbufsize)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,
                                   self.sendbufsize)

            # set this socket to non-blocking
            if not self.blocking: self.socket.setblocking(0)
        except socket.error as errMsg:
            raise SocketError(self.address, str(errMsg), ecode=errMsg[0])

    def _reset(self):
        """Resets the messages buffer."""
        self.__sent   = 0
        self.messages = []
        if self.useCStringIO:
            self.recvBuf.seek(0)
            self.recvBuf.truncate()
        else:
            self.recvBuf = ''
        self.__sendBuf = ""


    def checkSendQueue(self):
        """If any messages are still in our queue, then try to send them
        along now."""

        if self.__sendBuf:
            # send an empty message which will trigger the queue to be
            # sent.  Ignore would block exceptions.
            try:
                self._send("")
            except WouldBlock:
                pass


    def _send(self, msg):
        """Low level send method which does not support a timeout."""

        # make sure we have a socket
        if not self.socket:
            raise NotConnected(self.address, 'not connected')

        # keep track of how much we've sent
        sent = 0

        # prepend messages we haven't sent from previous calls
        if self.queueSends and self.__sendBuf:
            msg = self.__sendBuf + msg
            self.__sendBuf = ""

        try:
            while sent < len(msg):
                bytes = self.socket.send(msg[sent:])
                if bytes == 0:
                    raise LostConnection(self.address, "lost connection "
                                         "after sending %d bytes" % sent)
                sent += bytes
                # keep track of how many bytes have been sent so the
                # send() method knows if its full message has gone through
                self.__sent += bytes

        except socket.error as errMsg:
            code = errno.errorcode.get(errMsg[0], None)
            excp = None
            if code in ('EAGAIN', 'EWOULDBLOCK'):
                excp = WouldBlock
                if self.queueSends:
                    self.__sendBuf += msg[sent:]
                    # don't let the queue get too larger
                    if len(self.__sendBuf) >= self.sendQueueSize:
                        raise SendQueueLimit(
                            self.address, "send queue is larger than %s "
                            "bytes" % unitutil.formatBytes(self.sendQueueSize))
            elif code in ('EPIPE', 'ECONNRESET', 'ETIMEDOUT', 'EBADF'):
                excp = LostConnection
            elif code == 'ENOTCONN':
                excp = NotConnected
            elif code == 'EINTR':
                excp = Interrupted
            else:
                excp = SendError
            
            raise excp(self.address, str(errMsg), ecode=errMsg[0])

        if sent == 0:
            raise LostConnection(self.address, 'lost connection')

        #if msglen != sent:
        #    raise SendError(self.address, 'only %d bytes out of %d bytes '
        #                    'were sent.' % (sent, msglen))

        return sent

    def send(self, msg, timeout=0):
        """Sends the string 'msg' across the socket.

        @raise LostConnection: the connection with the remote address
                               was lost.
        @raise NotConnected: the socket is not currently connected to an
                             address.
        @raise WouldBlock: the socket is not ready for sending, this should
                           only be raised when the socket is non-blocking.
        @raise Interrupted: the send was interrupted by a system EINTR
        @raise SendError: an unknown error occured while sending.
        @raise SendTimeOut: msg isn't sent in 'timeout' seconds,
                            'timeout' is ignored if set to zero.
        """

        # always reset the __sent value for each send() call
        self.__sent = 0

        # if no timeout is set then just call _send
        if not timeout or not self.blocking:
            return self._send(msg)

        # make sure we have a socket
        if not self.socket:
            raise NotConnected(self.address, 'not connected')

        # if the timeout is set and the socket is set to block the
        # only way to break out of it is with an alarm, but we want to
        # avoid that approach.  Instead we will temporarily make the
        # socket non-blocking and use select to poll it.  We are not
        # using the sockopt SO_SNDTIMEO because for some reason this
        # is causing strace to interrupt and kill the process if
        # strace attaches during the send.  This is slightly more
        # complicated, but more reliable.
        if self.blocking: self.socket.setblocking(0)        
        while self.__sent < len(msg):
            try:
                input,output,exc = select.select([], [self.socket], [],
                                                 timeout)
            except (select.error, ValueError) as errMsg:
                # switch the socket back to blocking
                if self.blocking: self.socket.setblocking(1)
                raise SendError(self.address, str(errMsg))
            except:
                # switch the socket back to blocking
                if self.blocking: self.socket.setblocking(1)
                raise
            else:
                # if we don't find our socket then assume the select
                # timed out
                if self.socket not in output:
                    # switch the socket back to blocking
                    if self.blocking: self.socket.setblocking(1)
                    raise SendTimeOut(self.address, "sending message "
                                      "timed out after %s seconds" %
                                      timeout)

                # try to send the message now
                try:
                    self._send(msg[self.__sent:])
                # if it would block then go back to the top of the loop
                except WouldBlock:
                    pass
                else:
                    break

        # switch the socket back to blocking
        if self.blocking: self.socket.setblocking(1)
        return self.__sent

    def _recv(self, bufsize=None):
        """Low level recv method which does not support a timeout."""

        # make sure we have a socket
        if not self.socket:
            raise NotConnected(self.address, 'not connected')

        if not bufsize:
            bufsize = self.recvbufsize

        try:
            buffer = self.socket.recv(bufsize)
        except socket.error as errMsg:
            code = errno.errorcode.get(errMsg[0], None)
            excp = None
            
            if code in ('EAGAIN', 'EWOULDBLOCK'):
                excp = WouldBlock
            elif code in ('EPIPE', 'ECONNRESET', 'ETIMEDOUT', 'EBADF'):
                excp = LostConnection
            elif code == 'ENOTCONN':
                excp = NotConnected
            elif code == 'EINTR':
                excp = Interrupted
            else:
                excp = RecvError

            raise excp(self.address, str(errMsg), ecode=errMsg[0])
        else:
            # a closed socket is detected by returning a 0 byte string
            if not buffer:
                raise LostConnection(self.address, "lost connection")
            # take note of our last read time
            self.readtime = time.time()

        return buffer

    def recv(self, bufsize=None, timeout=0):
        """Reads the socket and returns the data read.

        @raise LostConnection: the connection with the remote address
                               was lost.
        @raise NotConnected: the socket is not currently connected to an
                             address.
        @raise WouldBlock: not data was read from the socket, this should
                           only happen when the socket is non-blocking.
        @raise Interrupted: the recv was interrupted by a system EINTR
        @raise RecvError: an unknown error occured while reading from socket.
        @raise ReadTimeOut: nothing is received in 'timeout' seconds,
                            'timeout' is ignored if set to zero.
        """

        # if no timeout is set then just call _recv
        if not timeout or not self.blocking:
            return self._recv(bufsize=bufsize)

        # make sure we have a socket
        if not self.socket:
            raise NotConnected(self.address, 'not connected')

        # if the timeout is set and the socket is set to block the
        # only way to break out of it is with an alarm, but we want to
        # avoid that approach.  Instead we will temporarily make the
        # socket non-blocking and use select to poll it.  We are not
        # using the sockopt SO_RCVTIMEO because for some reason this
        # is causing strace to interrupt and kill the process if
        # strace attaches during the recv.  This is slightly more
        # complicated, but more reliable.
        if self.blocking: self.socket.setblocking(0)
        buf = ''
        while 1:
            try:
                input,output,exc = select.select([self.socket], [], [],
                                                 timeout)
            except (select.error, ValueError) as errMsg:
                # switch the socket back to blocking
                if self.blocking: self.socket.setblocking(1)
                raise RecvError(self.address, str(errMsg))
            except:
                # switch the socket back to blocking
                if self.blocking: self.socket.setblocking(1)
                raise
            else:
                # if we don't find our socket then assume the select
                # timed out
                if self.socket not in input:
                    # switch the socket back to blocking
                    if self.blocking: self.socket.setblocking(1)
                    raise ReadTimeOut(self.address, "reading from socket "
                                      "timed out after %s seconds" %
                                      timeout)

                # try to read from the socket now
                try:
                    buf = self._recv(bufsize=bufsize)
                # if it would block then go back to the top of the loop
                except WouldBlock:
                    pass
                else:
                    break

        # switch the socket back to blocking
        if self.blocking: self.socket.setblocking(1)
        return buf

    def nextMsg(self, delim, timeout=0):
        """Convenience function that handles any buffering required to
        read the next full message.  Depending on the protocol of the
        connection messages might be delimited with a special character
        and this will always return the next complete message.  If the
        timeout variable is set then it will raise a ReadTimeOut
        exception if timeout seconds elapses with no message read.  That
        means it will continue to read until nothing is available,
        regardless of the amount of time spent."""

        # do not exit until a message is available
        while not self.messages:
            # read as much from recv as we can
            buffer = self.recv(timeout=timeout)
            
            pos = 0
            end = len(buffer)

            # each message is deliminated by whatever character 'delim'
            # is.  So the following loop will break the buffer into
            # messages and save any partial messages to self.recvBuf

            # find the first delim and loop until no more are found
            ind = buffer.find(delim, pos, end)
            while ind >= 0:
                # if the first character in 'buffer' is delim, then whatever
                # is in self.recvBuf must be a complete message.  Add the
                # message, reset self.recvBuf, and move onto the next
                # character
                if ind == 0:
                    if self.useCStringIO:
                        self.messages.append(self.recvBuf.getvalue())
                        self.recvBuf.seek(0)
                        self.recvBuf.truncate()
                    else:
                        self.messages.append(self.recvBuf)
                        self.recvBuf = ''
                    pos = ind + 1
                # if the last character in 'buffer' is delim, then the
                # complete message is 'buffer[pos:ind]'.  Note self.recvBuf
                # is prepended to the buffer subset because if pos=0 then
                # self.recvBuf could contain the beginning of the message.
                # However, if pos>0 then self.recvBuf should be None,
                # leaving the message unaffected.
                elif ind == end - 1:
                    if self.useCStringIO:
                        msg = self.recvBuf.getvalue() + buffer[pos:ind]
                        self.messages.append(msg)
                        self.recvBuf.seek(0)
                        self.recvBuf.truncate()
                    else:
                        self.messages.append(self.recvBuf + buffer[pos:ind])
                        self.recvBuf = ''
                    break
                # the delim character is found at an arbitrary location
                # within the buffer.  self.recvBuf is prepended to the
                # string for the same reasons as the previous condition.
                # add the message, reset self.recvBuf, and increment the
                # pos counter to the first character after the last delim.
                else:
                    if self.useCStringIO:
                        msg = self.recvBuf.getvalue() + buffer[pos:ind]
                        self.messages.append(msg)
                        self.recvBuf.seek(0)
                        self.recvBuf.truncate()
                    else:
                        self.messages.append(self.recvBuf + buffer[pos:ind])
                        self.recvBuf = ''
                    pos = ind + 1

                # look for the next delim
                ind = buffer.find(delim, pos, end)

            # if the loop exits without executing a break statement then
            # some (maybe all) of 'buffer' is an incomplete message and
            # should be saved for later
            else:
                # save the entire buffer
                if pos == 0:
                    if self.useCStringIO:
                        self.recvBuf.write(buffer)
                    else:
                        self.recvBuf += buffer
                # only save a slice
                else:
                    if self.useCStringIO:
                        self.recvBuf.write(buffer[pos:end])
                    else:
                        self.recvBuf += buffer[pos:end]

        # return the next valid message
        return self.messages.pop(0)

    def close(self):
        """Close the socket."""
        self.readtime = 0
        self._reset()
        if self.socket:
            # first try to shutdown the socket
            try:
                self.socket.shutdown(2)
            except socket.error:
                pass

            # now close it
            try:
                self.socket.close()
            except socket.error:
                pass
            
            self.socket = None

    def fileno(self):
        """Returns the file number of the socket.  This makes the class
        compatible with the select module."""
        if self.socket:
            return self.socket.fileno()
        return -1


