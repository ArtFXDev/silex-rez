#
# A simple SSDP-like discovery system for Tractor.
# The engine launches an "announcer" which advertises the engine's
# availability periodically (5min), then it waits for client search
# probes.  This is similar to the Alfred multicast discovery scheme,
# but has been updated to be visible to standard UPnP browser tools
# for debugging, and to abide by router rules that may be managing
# SSDP traffic in other ways such as disabling it or allowing it to
# hop subnets.  This TractorLocator scheme (as of Tractor 2.0, 2013)
# is not fully interoperable with true UPnP in terms of real URN
# schemas or accurate XML descriptions, etc.
#
import os
import sys
import time
import socket
import select
import struct


## ------------------------ ##
def main ():

    ssdp = TractorLocator()

    action = None
    addr   = None

    for arg in sys.argv[1:]:

        arg, x, val = arg.partition('=')

        if arg in ("--help","-help","-h"):
            print("options:")
            print("  --search[=tractor:engine]")
            print("  --announce[=tractor:engine]")
            print("  --addr[=svchost:port]")
            print("  --browse[=all]")
            print("  --verbose for diagnostics")
            print("  --debug for extra diagnostics")
            return

        elif arg in ("--search","-s",
                     "--browse","-b",'-B',
                     "--announce","-a","-R"):
            if not val:
                val = 'tractor:engine'
            if val == 'all':
                val = 'ssdp:all'
            if val == 'root':
                val = 'upnp:rootdevice'
            ssdp.serviceName = val

            arg = arg.lower()
            if 'b' in arg:
                action = ssdp.Browse
            elif 's' in arg:
                action = ssdp.Search
                ssdp.mainSearchOutput = 1
            else:
                action = ssdp.Advertise

        elif arg in ("-v","--verbose"):
            ssdp.verbosity += 1
        elif arg == "--debug":
            ssdp.verbosity = 2

        elif arg == "--addr":
            addr = val

        elif arg == "--mx":
            ssdp.mxtimeout = int(val)

        elif arg == "--svcvers":
            # expect string like "2.0"
            ssdp.serviceVers = val.split('.')

        elif arg in ("--dashboard", "-d"):
            action = ssdp.OpenDashboard

        else:
            print("unknown option:", arg, file=sys.stderr)
            sys.exit(1)

    if addr:
        ssdp.resolveLocalHostAddr( addr )

    if action:
        # now that all options are configured, run the selected method
        action()   #  Search(), Advertise(), or Browse()

    else:
        print("unknown TractorLocator stand-alone mode", file=sys.stderr)
        sys.exit(2)


## ------------------------ ##

class TractorLocator (object):

    def __init__ (self):

        self.SSDP_ADDR = ('239.255.255.250', 1900)
        self.TRACTOR_ENGINE_UUID = 'TRACTOR2-50A7-464A-A0E8-1DE9893AB7F3'

        self.verbosity = 0
        self.serviceName = "tractor:engine"
        self.serviceAddr = ('',80)
        self.serviceVers = (2,0) # "2.0"
        self.mxtimeout = 3
        self.bootcount = 1
        self.addrInitState = 0
        self.mainSearchOutput = 0


    ## ------------------------ ##
    def genSearchTxt (self, addedHdr=''):
        return \
            "M-SEARCH * HTTP/1.1\r\n" + \
            "ST: %s\r\n" % self.serviceName + \
            "MAN: \"ssdp:discover\"\r\n" + \
            "HOST: %s:%d\r\n" % self.SSDP_ADDR + \
            "USER-AGENT: %sTractor/2.0\r\n" % addedHdr + \
            "MX: %d\r\n" % self.mxtimeout + \
            "\r\n"


    ## ------------------------ ##
    def Search (self):
        #
        # send an SSDP M-SEARCH multicast message
        #
        if self.verbosity > 0:
            print("probing:", self.serviceName, file=sys.stderr)

        msearch = self.genSearchTxt()
        sock = self.sendUDP( msearch, skeep=1 )

        maxwait = self.mxtimeout / 2.0
        probeStart = time.time()
        probePass = 0
        found = ()

        while probePass < 2 and not found:
            dt = 0
            while dt < maxwait:
                try:
                    r,w,x = select.select([sock], [], [], maxwait - dt)
                    dt = time.time() - probeStart
                    if r:
                        reply, sender = sock.recvfrom(8192)
                        sd = self.ssdpHeadersToDict( reply )
                        st = sd['ST']

                        if self.verbosity > 0:
                            print("received", st, "from", sender, file=sys.stderr)
                            if self.verbosity > 1:
                                print(reply, file=sys.stderr)
                        try:
                            addr = sd['SEARCHADDR.PIXAR.COM']
                        except:
                            addr = sender[0]
                        try:
                            sp = int( sd['SEARCHPORT.UPNP.ORG'] )
                        except:
                            sp = 0

                        found = (addr, sp)
                        break  # first one wins

                except KeyboardInterrupt:
                    break # exit quietly
                except Exception as e:
                    if self.verbosity > 0:
                        print("discovery wait error\n", e, file=sys.stderr)

            probePass += 1
            if not found and probePass==1:
                # spec says send more than once to cover "udp unreliability"
                sock.sendto(msearch, self.SSDP_ADDR)

        sock.close()

        if not found:
            if self.verbosity > 0:
                print("no service found:", self.serviceName, file=sys.stderr)
        elif self.mainSearchOutput > 0:
            # print a result to stdout that might be useful to a script
            print("%s:%d" % found)  # like "123.456.678.9:8080"

        return found


    ## ------------------------ ##
    def Notify (self, disposition):
        #
        # send an ssdp-style response
        #
        if self.verbosity > 0 and disposition == "alive":
            print("mcast notify, advertising(" + \
                  self.serviceName + ")on %s:%d" % self.serviceAddr, file=sys.stderr)

        confID = str( self.serviceVers[0]*1000 + self.serviceVers[1] )

        notify = \
            "NOTIFY * HTTP/1.1\r\n" + \
            "HOST: %s:%d\r\n" % self.SSDP_ADDR + \
            "CACHE-CONTROL: max-age = 2345\r\n" + \
            "LOCATION: http://%s:%d" % self.serviceAddr + \
                            "/tractor/ssdp/troot.xml\r\n" + \
            "NT: %s\r\n" % self.serviceName + \
            "NTS: ssdp:%s\r\n" % disposition + \
            "SERVER: OS/1.0 UPnP/1.1 Tractor/%d.%d\r\n" % self.serviceVers + \
            "USN: uuid:"+self.TRACTOR_ENGINE_UUID + \
                  "::urn:Pixar:service:TractorEngine:2\r\n" + \
            "BOOTID.UPNP.ORG: "+str(self.bootcount)+"\r\n" + \
            "CONFIGID.UPNP.ORG: "+confID+"\r\n" + \
            "SEARCHPORT.UPNP.ORG: %d\r\n" % self.serviceAddr[1] + \
            "SEARCHADDR.PIXAR.COM: %s\r\n" % self.serviceAddr[0] + \
            "\r\n"

        self.sendUDP( notify )

        return notify


    ## ------------------------ ##
    def OpenDashboard (self):
        engineAddr = self.Search()
        if engineAddr:
            hostport = "%s:%d" % engineAddr
        else:
            hostport = os.getenv('TRACTOR_ENGINE', "tractor-engine")

        ui = "http://"+hostport+"/tractor/dashboard/"

        import urllib.request, urllib.error, urllib.parse
        try:
            ping = "http://"+hostport+"/Tractor/monitor?q=version"
            s = urllib.request.urlopen(ping)
            v = s.read()
            s.close()
            # could examine version string json 'v' here
            # but just proceed given that we got no exception
        except:
            # unable to contact an engine, so direct
            # user's browser to someplace helpful
            #ui = "file://"+installed+"/noEngineFound.html"
            ui = "https://rmanwiki.pixar.com/display/TRA/Initial+Configuration"

        import webbrowser
        webbrowser.open_new_tab( ui )

    ## ------------------------ ##
    def createMulticastListener (self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM,
                                             socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind( self.SSDP_ADDR )

        mreq = struct.pack("4sl", socket.inet_aton(self.SSDP_ADDR[0]),
                                  socket.INADDR_ANY)

        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        return sock


    ## ------------------------ ##
    def sendUDP (self, msg, destAddr=None, skeep=0):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM,
                                             socket.IPPROTO_UDP)
        if not destAddr:
            #
            # typical multicast case
            #
            destAddr = self.SSDP_ADDR
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

        sock.sendto(msg, destAddr)

        if skeep:
            return sock
        else:
            sock.close()


    ## ------------------------ ##
    def ssdpHeadersToDict (self, msg):
        sdict = {"ST": '', 'NT': '', "SEARCHPORT.UPNP.ORG": 0}
        for line in msg.split('\r\n')[1:]:
            key,_,val = line.partition(':')
            if key:
                sdict[key.upper()] = val.strip()

        return sdict


    ## ------------------------ ##
    def Browse (self):
        #
        # a simplified combination of advertise and search wait loops,
        # snooping ssdp traffic
        #
        listener = self.createMulticastListener()
        msearch = self.sendUDP( self.genSearchTxt(), skeep=1 )
        while 1:
            try:
                r,w,x = select.select([listener, msearch], [], [], 3)

            except KeyboardInterrupt:
                break # exit quietly
            except:
                raise

            for sock in r:  # readable now

                reply, sender = sock.recvfrom(8192)

                if self.verbosity > 1:
                    print(reply, file=sys.stderr)

                else:
                    sd = self.ssdpHeadersToDict( reply )
                    st = sd['ST']
                    try:
                        sp = int( sd['SEARCHPORT.UPNP.ORG'] )
                    except:
                        sp = 0

                    mtype = reply[:8]  #  'M-SEARCH' or 'NOTIFY *'

                    if mtype == "NOTIFY *":
                        if sp > 0:
                            sender = (sender[0], sp)
                        if st:
                            if sock == msearch:
                                mtype = "m-answer"
                        else:
                            st = sd['NT']
                            if 'NTS' in sd:
                                mtype = sd['NTS']
                            else:
                                mtype = "NOTIFY"

                    print(time.strftime("%H:%M:%S "), \
                          ("%s:%d" % sender).ljust(22), \
                          mtype.ljust(11), st)

        listener.close()
        msearch.close()


    ## ------------------------ ##
    def msearchResponse (self, sender, sdict, reply):

        # ssdp sends replies to M-SEARCH as *unicast* udp to the
        # sender's ORIGIN ADDR AND PORT, i.e. assume that they
        # have left that socket open to receive the reply rather
        # than opening a separate listener.
        #
        # Spec also requires msearch answers to "jitter" themselves
        # within the requester's MX interval, to lessen UDP collisions
        # on lots of replies (especially to ssdp:all). We add a simple
        # small fixed delay.

        if self.verbosity > 0:
            print("answering M-SEARCH(%s) for" % sdict['ST'], sender, file=sys.stderr)

        time.sleep(0.1)
        self.sendUDP(reply, sender)


    ## ------------------------ ##
    def chkBootstrap (self, needNotify):
        #
        # send traffic to the mcast group to do three things:
        # 1. test the mcast pathway
        # 2. generate traffic that will tell us our own IP address!
        # 3. maybe, probe for other tractor engines
        #

        if self.serviceAddr[0]:
            # have a good addr

            if self.addrInitState == 0:
                # first time through with a good addr,
                self.addrInitState = -1

                # spec suggests sending byebye at startup
                # to clear old data from clients
                #self.Notify( "byebye" )

            if needNotify:
                return 0   # cause new notify to be sent
            else:
                return 60  # just waiting for clients

        if self.addrInitState > 2:
            if self.verbosity > 0:
                print("mcast bootstrap failed, exit.", file=sys.stderr)
            return -1  # quit

        else:
            # attempt to ping ourselves via the mcast router, twice at most
            self.sendUDP( self.genSearchTxt("_TrTestIP_") )
            self.addrInitState += 1
            return 1 # timeout, expect quick test msg


    ## ------------------------ ##
    def Advertise (self):
        #
        # Begin advertising the tractor:engine service on the LAN.
        # (or other ssdp service name as given with the --announce
        # cmdline parameter, or the self.serviceName instance variable).
        # This amounts to doing a single multicast announcement of the
        # engine location (host:port) to potential clients who were
        # already running prior to the engine start; and then running
        # forever in a listener mode responding to probe requests from
        # newly started clients.  This dual approach helps to reduce
        # total discovery chatter on the network, although "browsers"
        # that make global queries to enumerate *all* ssdp services
        # on the LAN can precipitate a flood of responses.
        #
        # If the caller has not specified an explicit host:port,
        # then we attempt to figure out which address we have been
        # assigned by dhcp/mDNS and use that.  This module does NOT
        # attempt to do any mDNS-style allocation or self-assignment
        # of the actual IP addresses to the host itself.
        #

        # setup our listener before notify, so don't miss replies
        listener = self.createMulticastListener()

        sockset = [listener, sys.stdin]
        pollset = None
        if hasattr(select, "poll"):
            # if available, poll() can handle fd > 1024, useful in cases
            # where we inherit a high-numeric descriptor from a caller.
            pollset = select.poll()
            for s in sockset:
                pollset.register(s, select.POLLIN)

        notifyRefresh = 600 # seconds
        lastNotify = 0
        keepRunning = True

        while keepRunning:
            now = time.time()

            rc = self.chkBootstrap( (now - lastNotify) > notifyRefresh )
            if rc < 0:
                break  # bootstrapping failed, quit
            elif rc == 0:
                # (re)announce "tractor engine is now alive"
                notifyTxt = self.Notify( "alive" )

                notifyTxt = notifyTxt.replace("\nNT:", "\nST:")
                lastNotify = now
                timeout = 60 # seconds
            else:
                timeout = rc # brief while bootstrapping, long otherwise

            readable = []
            try:
                if pollset:
                    events = pollset.poll(timeout * 1000)
                    if events:
                        # python select.poll() returns the fd not sock object
                        fd = [x[0] for x in events]
                        readable = [s for s in sockset if s.fileno() in fd]
                else:
                    readable,w,x = select.select(sockset, [], [], timeout)

            except KeyboardInterrupt:
                break # exit quietly
            except:
                raise

            # readable set will be empty on wait timeout
            for sock in readable:

                if sock == sys.stdin:
                    request = sock.read(1)
                    if request:
                        # ignore input on stdin
                        continue
                    else:
                        # stdin was closed, parent exit, so we exit
                        keepRunning = False
                        break

                # otherwise input on the UDP listener
                # don't loop on recv! UDP dgrams are one msg per recv ONLY
                request,sender = sock.recvfrom(8192)

                if self.addrInitState > 0 and "_TrTestIP_" in request:
                    # Our msg to ourself has come back, and now we can
                    # extract the sender's IP address to learn our own
                    # IP address on an interface that is known to route
                    # to the mcast range.
                    self.addrInitState = 0  # found!
                    self.serviceAddr = (sender[0], self.serviceAddr[1])
                    if self.verbosity > 1:
                        print("IP addr bootstrap:", self.serviceAddr, file=sys.stderr)

                elif request:
                    sd = self.ssdpHeadersToDict( request )
                    if self.verbosity > 1:
                        print(request, file=sys.stderr)

                    if request.startswith("M-SEARCH") and sd['ST'] in \
                                         (self.serviceName, 'ssdp:all'):
                        self.msearchResponse( sender, sd, notifyTxt )

        #
        # service looping complete
        #
        if listener:
            listener.close()

        # on our way out send "shutting down" msg to any listening clients
        self.Notify( "byebye" )

        return None


    ## ------ ##

    def resolveLocalHostAddr (self, hostport):
        #
        # Find a "likely" ip address that this service should use for the
        # "LOCATION" value in its availability advertisement.  This is of
        # course tricky for hosts that have several interfaces, such as
        # loopback, wired, wireless, or usb connections.  Many hosts have
        # more than one of each. VPN tunnels create additional complexity.
        #
        # We want the ip address as a dotted quad so that clients are
        # not faced with a name resolution burden as well.  Ideally we
        # would have a simple call that would enumerate the "up" interfaces
        # in some order that is optimal for this service discovery use.
        # Instead we attempt to bootstrap with name resolution anyway,
        # if given a hostname to serve, otherwise a working local IP
        # address will be determined later by inspection of a working
        # roundtrip connection to ourselves through the mcast router.
        #

        hnm,x,hp = hostport.partition(':')  # "host:port"

        hport = 80
        if hp:
            try:
                p = int(hp)
                if p > 0:  hport = p
            except:
                if self.verbosity > 0:
                    print("invalid port number:", hp, "(using 80)", file=sys.stderr)

        if not hnm or hnm=='@':
            #
            # if hostname was not given, DO NOT use gethostname to
            # try to resolve it since that is fraught with issues in
            # various multi-NIC or vpn situations.  Instead, use an
            # empty hostname string to indicate that the introspection
            # technique should be applied later.
            #
            # hnm = socket.gethostname()
            #
            self.serviceAddr = ('', hport)
            return

        hquads = []
        e = None
        try:
            nm, als, hquads = socket.gethostbyname_ex( hnm )

        except socket.gaierror as e:
            # no such host, try "zeroconf" name ...
            # (or the not-local name if we got .local initially)
            if hnm.endswith(".local"):
                hnm = hnm[:-6]
            else:
                hnm += ".local"
            try:
                nm, als, hquads = socket.gethostbyname_ex( hnm )
                e = None
            except:
                # could try enumerating a few well-known interfaces,
                # but instead fallback to getsockname introspection
                hquads = []
                e = None

        except Exception as e:
            pass

        if hquads and not e:
            self.serviceAddr = (hquads[0], hport)
        else:
            self.serviceAddr = ('', hport)  # must introspect later

        if self.verbosity > 0:
            if e:
                print("lookup error '"+hnm+"'", e, \
                                    "\ntrying fallback IP discovery", file=sys.stderr)
            else:
                print("resolved addr as %s:%d" % self.serviceAddr, file=sys.stderr)



## ------------------------ ##
if __name__ == "__main__":
    main()

