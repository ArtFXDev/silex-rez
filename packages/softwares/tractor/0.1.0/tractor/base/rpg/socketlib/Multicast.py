# Multicast.py

import sys, time, struct, errno
import pickle as pickle
import socket

import rpg.socketlib.Sockets as Sockets

__all__ = (
        'McastPort',
        'McastGroup',
        'McastSender',
        'McastReceiver',
        'PickledMcastSender',
        'PickledMcastReceiver',
        )

# ---------------------------------------------------------------------------

McastPort    = 8123
McastGroup   = '239.192.1.1'
PickledDelim = '\003'

class McastSender(object):
    def __init__(self, port=McastPort, group=McastGroup, broadcast=0, ttl=2):
        # Sender subroutine (only one per local area network)

        self.port = port
        self.group = group
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if broadcast:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.group = '<broadcast>'
        else:
            ttl = struct.pack('b', ttl)               # Time-to-live
            self.socket.setsockopt(socket.IPPROTO_IP,
                                   socket.IP_MULTICAST_TTL, ttl)


    def send(self, data):
        self.socket.sendto(data, (self.group, self.port))



class PickledMcastSender(McastSender):

    def send(self, data):
        super(PickledMcastSender, self).send(data + PickledDelim)

    def sendObj(self, obj, members):
        
        values = {}
        for member in members:
            values[member] = getattr(obj, member)
            
        msg = {
            'timestamp': int(time.time()),
            'class': obj.__class__.__name__,
            'values': values
            }
            
        self.send(pickle.dumps(msg))
            
            

class McastReceiver(Sockets.Socket):

    
    def __init__(self, group=McastGroup, port=McastPort, blocking=True):
        self.group = group
        self.port = port
        self.address = (group, port)
        super(McastReceiver, self).__init__((group, port))

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.open()
        if not blocking:
            self.socket.setblocking(0)

        
    def recv(self, timeout=0):
        try:
            data, sender = self.socket.recvfrom(self.recvbufsize)
        except socket.error as errMsg:
            code = errno.errorcode.get(errMsg[0], None)
            excp = None

            if code in ('EAGAIN', 'EWOULDBLOCK'):
                excp = Sockets.WouldBlock
            else:
                excp = Sockets.RecvError

            raise excp(self.address, str(errMsg), ecode=errMsg[0])

        return data


    # Open a UDP socket, bind it to a port and select a multicast group
    def open(self):

        # Allow multiple copies of this program on one machine
        # (not strictly needed)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind it to the port
        self.socket.bind(('', self.port))

        # Look up multicast group address in name server
        # (doesn't hurt if it is already in ddd.ddd.ddd.ddd format)
        self.group = socket.gethostbyname(self.group)

        # Construct binary group address
        bytes = list(map(int, self.group.split(".")))
        grpaddr = 0
        for byte in bytes:
            grpaddr = (grpaddr << 8) | byte

        # Construct struct mreq from grpaddr and ifaddr
        ifaddr = socket.INADDR_ANY
        mreq = struct.pack('ll', socket.htonl(grpaddr), socket.htonl(ifaddr))

        # Add group membership
        self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                               mreq)


class PickledMcastReceiver(McastReceiver):
    
    def nextMsg(self):
        # clear the buffer before each read since there is no guarantee
        # we will get the rest of a message
        self.recvBuf.seek(0)
        self.recvBuf.truncate()
        msg = super(PickledMcastReceiver, self).nextMsg(PickledDelim)

        try:
            return pickle.loads(msg)
        except EOFError as e:
            return {}
        except (ValueError, TypeError, KeyError, IndexError) as e:
            err = "%s: msg='%s'" % (str(e), str(msg))
            raise Sockets.RecvError(self.address, err)

    def recvObj(self):
        return self.nextMsg()



def test():
    # Usage:
    #   Multicast.py -s (sender)
    #   Multicast.py -b (sender, using broadcast instead multicast)
    #   Multicast.py    (receivers)

    flags = sys.argv[1:]
    #
    if flags:
        broadcast = 0
        if '-b' in flags:
            broadcast = 1
        sender = McastSender(broadcast=broadcast)
        while 1:
            msg = {'result': 'ok'}
            sender.send(pickle.dumps(msg))
            time.sleep(1)
            
    else:
        import tina
        receiver = McastReceiver(port=tina.MulticastPort)
        while 1:
            data = receiver.recv()
            print('***', len(data), 'bytes', '***')
            data = pickle.loads(data)
            keys = list(data.keys())
            keys.sort()
            for key in keys:
                print(key, ':', data[key])


if __name__ == '__main__':
    test()
