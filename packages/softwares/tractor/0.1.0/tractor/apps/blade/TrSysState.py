#
# TrSysState - obtain system information and performance values
#
#
# ____________________________________________________________________ 
# Copyright (C) 2007-2016 Pixar Animation Studios. All rights reserved.
#
# The information in this file is provided for the exclusive use of the
# software licensees of Pixar.  It is UNPUBLISHED PROPRIETARY SOURCE CODE
# of Pixar Animation Studios; the contents of this file may not be disclosed
# to third parties, copied or duplicated in any form, in whole or in part,
# without the prior written permission of Pixar Animation Studios.
# Use of copyright notice is precautionary and does not imply publication.
#
# PIXAR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING
# ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT
# SHALL PIXAR BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES
# OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
# WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION,
# ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.
# ____________________________________________________________________ 
#

import os, sys, platform, time, socket, errno
import logging, subprocess, math, fnmatch
import ctypes, ctypes.util

import tractor.base.TrHostUUID as TrHostUUID

if sys.platform == 'win32':
    import winreg
    from ctypes.wintypes import HWND, HANDLE, DWORD, LPCWSTR, MAX_PATH
    DWORDLONG = ctypes.c_uint64

elif sys.platform == 'darwin':
    tr_ctypes_libc = ctypes.CDLL( ctypes.util.find_library("c") )


## --------------------------------------------------------- ##

class TrSysState(object):
    """
    The TrSysState object manages access to certain static and dynamic
    system characteristics of the blade host.  Its goal is to provide
    a bit of an abstraction for accessing these values across different
    operating systems.
    """

    def __init__(self, opts, addr):
        self.logger = logging.getLogger("tractor-blade")

        self.hostClkTck = 100
        self.nCPUs = self.countCPUs()
        self.physRAM = self.getPhysicalRAM()
        self.gpu = self.TrGPUInfo()
        self.hostUUID = TrHostUUID.GetHostUUID()
        self.boottime = self.getBootTime()

        self.processOwner = opts.processOwner
        self.hostname = "localhost"
        self.aliases = ["localhost"]
        self.addrs = ["127.0.0.1"]
        self.altname = opts.altname
        self.resolveHostname(addr)

        self.resolveOS() # sets self.osType, self.osInfo, self.osPlatform, self.osExtPlatform

        self.chkptfile = self.resolveCheckpointFilename( opts )

        self.python_vers = platform.python_version()

        self.cpuOldIdle = None
        self.cpuOldKernel = None
        self.cpuOldUser = None
        self.GetProcessorUsage()  # prime with initial snapshot


    def resolveOS (self):
        if sys.platform == 'win32':
            # Python built-in platform.platform() only knows about
            # Windows versions that existed when that python version was
            # released. We can get the actual version from the registry.
            v, p = self.getWinPlatformFromRegistry()
            self.osType = "Windows"
            self.osPlatform = p
            self.osExtPlatform = p + "-" + platform.architecture()[0]
            self.osInfo = "Windows, " + v + ", " + self.osExtPlatform
    
        elif sys.platform == 'darwin':
            self.osType = "MacOS"
            self.osPlatform = platform.platform().decode('utf8')
            self.osExtPlatform = self.osPlatform + "-" + platform.architecture()[0]
            self.osInfo = "MacOSX, " + platform.mac_ver()[0] + ", " + self.osPlatform

        else:
            self.osType = "Linux"
            self.osPlatform = platform.platform().decode('utf8')
            self.osExtPlatform = self.osPlatform + "-" + platform.architecture()[0]
            d = platform.linux_distribution()
            if ''==d[0] and os.access("/etc/system-release", os.R_OK):
                try:
                    # possibly cloud or other distro that older python doesn't know
                    f = open("/etc/system-release", "rb")
                    if f:
                        d = f.read()
                        f.close()
                        d = [v.strip() for v in d.split("release")]
                        d[1] = d[1].split()[0]
                except:
                    d = ('Linux unknown', '1.0')

            self.osInfo = d[0]+", "+d[1]+", "+ self.osPlatform


    def getWinPlatformFromRegistry (self):
        # alternative to platform.platform() for Windows because our 2.7-based
        # rmanpy has trouble resolving Windows 8+ platform info, and is weirdly
        # awkward about it, depending primarily on a 3rd party C module that
        # isn't stock, and we don't ship.

        vshort = "(unknown)"
        vkey = None
        try:
            vkey = winreg.OpenKeyEx(winreg.HKEY_LOCAL_MACHINE,
                              "SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion")
            try:
                # for win10+
                major = winreg.QueryValueEx(vkey, "CurrentMajorVersionNumber")
                minor = winreg.QueryValueEx(vkey, "CurrentMinorVersionNumber")
                vnum = str(major[0])+'.'+str(minor[0])
            except WindowsError:
                # for win7, and 8.x
                vnum = winreg.QueryValueEx(vkey, "CurrentVersion")
                vnum = str(vnum[0].decode('utf8'))

            vbuild = winreg.QueryValueEx(vkey, "CurrentBuild")
            vbuild = str(vbuild[0].decode('utf8'))
            pname = winreg.QueryValueEx(vkey, "ProductName")
            pname = str(pname[0].decode('utf8'))

            p = pname+'-'+vnum+'.'+vbuild
            p = p.replace(' ', '-')

            nm = pname.split(' ')
            if nm[0]=="Windows":
                if nm[1]=="Server":
                    # such as "Windows Server 2008 R2"
                    vshort = 'S'+nm[2]  # like S2008
                else:
                    vshort = nm[1]  # like "8.1" or "Vista"

        except Exception:
            p = platform.platform().decode('utf8')
            if fnmatch.fnmatch(p, "Windows-*-10.0.*"):
                vshort = "10.0"
            elif fnmatch.fnmatch(p, "Windows-*-6.4.*"):
                vshort = "10.0.beta"
            elif fnmatch.fnmatch(p, "Windows-*-6.3.*"):
                vshort = "8.1"
            elif fnmatch.fnmatch(p, "Windows-*-6.2.*"):
                vshort = "8"
            elif fnmatch.fnmatch(p, "Windows-*-6.1.*"):
                vshort = "7"
            elif fnmatch.fnmatch(p, "Windows-*-6.0.*"):
                vshort = "Vista"
            elif fnmatch.fnmatch(p, "Windows-*-5.*"):
                vshort = "XP"
            else:
                vshort = "Unknown"

        finally:
            if vkey:
                winreg.CloseKey(vkey)

        return (vshort, p)


    def GetProcessorUsage (self):
        if sys.platform == 'win32':
            return self.getProcessorUsage_Windows()
        else:
            # cpu load, normalized by number of CPUs (if reqd)
            # so a fully loaded machine is always "1.0" independent
            # of the number of processors.
            return( os.getloadavg()[0] / float(self.nCPUs) )


    def countCPUs(self):
        '''
        Returns the number of CPUs on the system
        '''
        num = 0
        try:
            if sys.platform == 'win32':

                try:
                    # this call returns what windows calls "cpus", if HT/SMT is
                    # on then it includes those.
                    ALL_PROCESSOR_GROUP = 65535
                    num = ctypes.windll.kernel32.GetActiveProcessorCount(ALL_PROCESSOR_GROUP)
                    #    For the hyperthreading factor info, we will
                    #    likely need to call the complicated function 
                    #    GetLogicalProcessorInformationEx 
                    #    with RelationProcessorCore.
                    #
                except (WindowsError,AttributeError) as e:
                    # for some reason can't load kernel32.dll or it doesn't
                    # have that entry then fallback to this
                    self.logger.debug("Failed to get proc count from kernel32, fall back to env: " %
                                        e.message)
                    
                    num = int(os.environ['NUMBER_OF_PROCESSORS'])

            elif sys.platform == 'darwin':
                ncpu = ctypes.c_uint(0)
                size = ctypes.c_size_t( ctypes.sizeof(ncpu) )
                try:
                    rc = tr_ctypes_libc.sysctlbyname("hw.ncpu",
                                ctypes.byref(ncpu), ctypes.byref(size),
                                None, 0)
                except:
                    rc = -1

                if 0 == rc:
                    num = ncpu.value
                else:
                    # on failure, revert to popen and execing sysctl
                    num = int(os.popen('sysctl -n hw.ncpu').read())

            else:
                # GNU/Linux
                num = os.sysconf('SC_NPROCESSORS_ONLN')
                self.hostClkTck = os.sysconf('SC_CLK_TCK')

        except Exception:
            # we can pretty much assume that since we're running,
            # there must be at least one running cpu here somewhere
            num = 1

        if num < 1:
            num = 1
            
        return int( num )


    def resolveHostname (self, reflectedAddr=None):
        lnm = "localhost";
        try:
            lnm = socket.gethostname()
            hostname,aliases,addrs = socket.gethostbyname_ex(lnm)
            aliases.append(hostname)
            hnm = lnm.split('.')[0] # matches tractor-spool.py

            # As a special case for sites with weird DNS issues, the
            # blade cmdline option --hname=. (a dot rather than a name)
            # forces the "altname" to be the name found with the above
            # call, independent of what the reverse addr lookup finds.
            if '.' == self.altname:
                self.altname = hnm

        except Exception as e:
            self.logger.error("cannot resolve network hostname info: " + \
                                lnm + " " + str(e))
            hnm = lnm
            aliases = []
            addrs = ['127.0.0.1']

            aliases.append(hnm)

            # also add alias with stripped osx off-the-grid suffix
            if hnm.endswith(".local"):
                hnm = hnm[:-6] # strip .local
                aliases.append(hnm)

            try:
                h = hnm.split('.')[0]
                if h != hnm:
                    h2,al2,ad2 = socket.gethostbyname_ex(h)
                    if h2 == hnm:
                        hnm = h
                        aliases.append(h)
                        aliases.extend(al2)
                        addrs.extend(ad2)
            except Exception:
                pass

        if reflectedAddr:
            # prefer the identity associated with our route to the engine
            # since blades.config host match criteria is defined to be
            # the "as seen by the engine" names.  So always force the
            # engine's view into the addr list.
            try:
                addrs += [reflectedAddr] # add addr as seen by engine

                # and also try to resolve that addr's name via the nameserver
                h2,al2,ad2 = socket.gethostbyaddr( reflectedAddr )
                aliases.append(h2)
                aliases.extend(al2)
                addrs.extend(ad2)
                hnm = h2.split('.')[0]
            except:
                pass

        if self.altname and self.altname != hnm:
            # When --hname=some_name is given on the cmdline, then we
            # use that name execlusively for profile matching by NAME.
            # Otherwise the blade might match a profile designed
            # specifically for the usual hostname.  However we do
            # also look up the given hname to see if there are
            # aliases defined specifically for it.
            #
            hnm = self.altname
            aliases = []
            try:
                h2,al2,ad2 = socket.gethostbyname_ex(hnm)
                aliases.append(h2)
                aliases.extend(al2)
                addrs.extend(ad2)
            except Exception:
                pass

        self.hostname = hnm

        if len(addrs) > 1:
            addrs = self.uniquifySequence(addrs)
            # remove 127.0.0.1 if there are other choices
            try:
                if len(addrs) > 1 and reflectedAddr != '127.0.0.1':
                    addrs.remove('127.0.0.1')
            except Exception:
                pass
        self.addrs = addrs

        if len(aliases) > 1:
            aliases = self.uniquifySequence(aliases)

        # also remove extraneous reverse-lookup x.y.z.inaddr-any.arpa
        # entries created by python socket.gethostbyaddr() on some platforms
        self.aliases = []
        for a in aliases:
            if not a.endswith(".arpa"):
                self.aliases.append(a)

        self.logger.info("resolved local hostname: '%s'  %s" % \
                (self.hostname, ",".join(self.addrs)))
        if len(self.aliases) > 0:
            self.logger.debug("(aliases: " + 
                    ", ".join(self.aliases) + ')')


    def getBootTime (self):
        ## FIXME: for python3 we can just use this on all platforms:
        ##   tstamp = uptime.boottime()

        tstamp = time.time()
        try:
            if sys.platform == 'win32':
                GetTickCount64 = ctypes.windll.kernel32.GetTickCount64
                GetTickCount64.restype = ctypes.c_ulonglong
                tstamp = time.time() - (GetTickCount64() / 1000.0)

            elif sys.platform == 'darwin':
                try:
                    class TIMEVAL(ctypes.Structure):
                        _fields_ = [("tv_sec", ctypes.c_long),
                                    ("tv_usec", ctypes.c_int32)]
                    tv = TIMEVAL()
                    tvsz = ctypes.c_size_t( ctypes.sizeof(tv) )
                    rc = tr_ctypes_libc.sysctlbyname("kern.boottime",
                                ctypes.byref(tv), ctypes.byref(tvsz), None, 0)

                    if rc == 0:
                        tstamp = (tv.tv_usec / 1000000.0) + tv.tv_sec
                except:
                    rc = -1

                if 0 != rc:
                    # on failure, revert to popen and execing sysctl
                    # '{ sec = 1399409948, usec = 0 } Tue May  6 13:59:08 2014\n'
                    s = os.popen('sysctl -n kern.boottime').read()
                    tstamp = float( s.split()[3].split(',')[0] )

            else:
                # GNU/Linux
                # read the "btime" entry from /proc/stat
                f = open("/proc/stat", "rb")
                stat = f.read()
                f.close()
                x = stat.find("\nbtime ")
                if x > 0:
                    x += 7  # skip btime label
                    y = stat.find('\n', x)
                    tstamp = float( stat[x:y] )

        except Exception:
            self.logger.info("get boottime failed, fallback to blade start")
        
        return tstamp;


    def GetAppTempDir (self):
        tmp = "/tmp"
        try:
            if sys.platform == 'win32':
                # ideally we'd use SHGetKnownFolderPath( FOLDERID_ProgramData )
                # if we can guarantee no pre-vista win32 issues in the site's
                # running python c code.  Instead use the older API for now.
                # Expect something like: C:\ProgramData\Pixar\TractorBlade
                CSIDL_APPDATA = 26
                CSIDL_COMMON_APPDATA = 35
                getpath = ctypes.windll.shell32.SHGetFolderPathW
                getpath.argtypes = \
                    [ HWND, ctypes.c_int, HANDLE, DWORD, LPCWSTR ]
                buf = ctypes.wintypes.create_unicode_buffer(MAX_PATH)
                getpath(0, CSIDL_COMMON_APPDATA, 0, 0, buf)
                tmp = buf.value

            elif sys.platform == 'darwin':
                tmp = os.path.expanduser("~/Library/Application Support")

            else:  # linux
                tmp = "/var/tmp"

            tmp += "/Pixar/TractorBlade"
            if not os.access(tmp, os.W_OK):
                oldumask = os.umask(0)
                os.makedirs( tmp )
                os.umask(oldumask)

        except Exception as e:
            # it is acceptable for the directory to already exist
            if e[0] not in (errno.EEXIST, errno.ERROR_ALREADY_EXISTS):
                errclass, excobj = sys.exc_info()[:2]
                self.logger.warning("tmpdir %s - %s" % \
                                    (errclass.__name__, str(excobj)))

        return tmp


    def GetCheckpointFilename (self):
        if not os.access(self.chkptfile, os.W_OK):
            self.logger.info("checkpoint file is inaccessible, retrying")
            oldumask = os.umask(0)
            os.makedirs( os.path.dirname(self.chkptfile) )
            os.umask(oldumask)

        return self.chkptfile


    def resolveCheckpointFilename (self, opts):
        #
        # The blade checkpoint file must be written someplace
        # where a restarted blade will find it again reliably.
        # The tricky bit is that more than one blade may be
        # running on each server, especially during testing,
        # and each may be connected to a different engine.
        # So the checkpoint needs to be specific to the "altname"
        # given to this blade and the engine from which it is
        # receiving jobs.
        #
        # chkpt.<enginehost_port>.<bladeAltname>.json
        # .../Pixar/TractorBlade/chkpt.tractor-engine_80.rack21A.json
        #
        out = "/tmp/trb.txt" # fallback
        try:
            out = self.GetAppTempDir()  # also performs mkdirs if necessary

            mh = opts.mtdhost.split('.')[0]
            bh = self.hostname.split('.')[0]
            pixarChkpt = "/chkpt." + \
                          mh + "_" + str(opts.mport) + \
                          "." + bh + ".json"
            out += pixarChkpt

            # test the file for i/o, only to generate a warning
            f = open( out, "a+b" )
            f.close()

        except Exception:
            self.logger.info("getCheckpointFilename: " + self.logger.Xcpt())

        return out


    def uniquifySequence(self, seq):
        # Remove duplicates from the inbound sequence.
        # Does not preserve order among retained items.
        return list({}.fromkeys(seq).keys())


    def GetAvailableDisk (self, drivepath=None):
        """
        Returns the amount of currently free disk space on
        the drive containing 'drivepath' (e.g. "/").
        Units are (float) gigabytes.  If drivepath is None,
        than a typical platform root directory is chosen.
        """

        freebytes = 0.0
        try:
            if sys.platform == 'win32':
                if not drivepath:
                    drivepath = 'c:\\'
                nbytes = ctypes.c_ulonglong(0)
                ctypes.windll.kernel32.GetDiskFreeSpaceExW (
                    ctypes.c_wchar_p( drivepath ),
                    None, None, 
                    ctypes.pointer(nbytes)
                )
                freebytes = float(nbytes.value)

            elif sys.platform == 'darwin':
                if not drivepath:
                    drivepath = '/'

                # The underlying os.statvfs system call on OSX is 32-bit,
                # (as of python 2.7.11 and 3.4).  So it incorrectly
                # truncates available space on terabyte+ file systems.
                # Instead we make the statfs system call directly,
                # which has 64-bit fields after OSX 10.6.
                osx_statfs = tr_ctypes_libc.statfs
                osx_statfs.restype = ctypes.c_int
                osx_statfs.argtypes = [ ctypes.c_char_p,
                                        ctypes.POINTER(STRUCT_STATFS) ]
                sfs = STRUCT_STATFS()
                if 0 == osx_statfs( drivepath, ctypes.byref(sfs) ):
                    freebytes = float(sfs.f_bavail) * float(sfs.f_bsize)
                else:
                    # fallback to built-in statvfs, ok for small drives
                    s = os.statvfs( drivepath )
                    freebytes = float(s.f_bavail) * float(s.f_frsize)

            else:
                # unix (recent linux and osx, with a recent python)
                if not drivepath:
                    drivepath = '/'
                s = os.statvfs( drivepath )
                freebytes = float(s.f_bavail) * float(s.f_frsize)

        except Exception:
            self.logger.debug("getAvailableDisk: " + self.logger.Xcpt())

        return freebytes / (1024.0 * 1024.0 * 1024.0)  # float gigabytes


    def getPhysicalRAM (self):
        # physical RAM installed, in gigabytes
        gbram = 0.5
        try:
            if sys.platform == 'win32':
                # sets self.physRAM as side effect!
                self.getAvailableRAM_Windows()
                gbram = self.physRAM
            elif sys.platform == 'darwin':
                ram = ctypes.c_uint64(0)
                size = ctypes.c_size_t( ctypes.sizeof(ram) )
                if 0 == tr_ctypes_libc.sysctlbyname("hw.memsize", \
                        ctypes.byref(ram), ctypes.byref(size), None, 0):
                    gbram = float(ram.value) / (1024.0 * 1024.0 * 1024.0)
            else:
                # sets self.physRAM as side effect!
                self.getAvailableRAM_Linux()
                gbram = self.physRAM

        except Exception:
            self.logger.debug("getPhysicalRAM: " + self.logger.Xcpt())

        return gbram



    def GetAvailableRAM (self):
        # estimate of free memory, in gigabytes
        gbfree = 0.1234
        try:
            if sys.platform == 'win32':
                gbfree = self.getAvailableRAM_Windows()
            elif sys.platform == 'darwin':
                gbfree = self.getAvailableRAM_OSX()
            else:
                gbfree = self.getAvailableRAM_Linux()

        except Exception:
            self.logger.debug("getAvailableRAM: " + self.logger.Xcpt())

        return gbfree


    def getAvailableRAM_OSX (self):
        # extract free+inactive pages, return "free" gigabytes
        vmstat = subprocess.Popen(["/usr/bin/vm_stat"], stdout=subprocess.PIPE).communicate()[0]
        avail = 0.0
        for t in vmstat.split('\n'):
            m = t.split()
            if 3==len(m) and m[1] in ('free:', 'inactive:', 'speculative:'):
                avail += float(m[2])

        return avail * 3.8147E-6  # (4096 / (1024.0 * 1024.0 * 1024.0))


    def getAvailableRAM_Linux (self):
        # extract meminfo items (in kb), return "free" gigabytes
        f = open("/proc/meminfo", "r")
        mi = f.read()
        f.close()

        avail = 0.0
        for k in mi.split('\n'):
            m = k.split()
            if 3==len(m) and m[0] in ('MemFree:', 'Buffers:', 'Cached:'):
                avail += float(m[1])
            elif 3==len(m) and m[0] == 'MemTotal:':
                # side effect:  save physical RAM
                self.physRAM = \
                    math.ceil( float(m[1]) / (1024.0 * 1024.0) )

        return avail / (1024.0 * 1024.0)


    ## ------------------ windows methods --------------------- ##

    def getAvailableRAM_Windows (self):
        # Gets memory usage on Windows

        memStatus = MEMORYSTATUSEX()  # ctypes class defined below
        memStatus.dwLength = ctypes.sizeof(memStatus);

        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memStatus)) == 0:
            print(ctypes.WinError())
        else:
            gbscale = (1024.0 * 1024.0 * 1024.0)

            # side effect:  save physical RAM
            self.physRAM = math.ceil( float(memStatus.ullTotalPhys) / gbscale )

            gbfree = float(memStatus.ullAvailPhys) / gbscale
            return gbfree


    def getProcessorUsage_Windows (self):
        # Gets CPU usage on Windows
        cpuUsage = 0.0

        # create a class modeled after the FILETIME struct in Winbase.h in
        # the Windows SDK.  See doc details in this msdn article:
        # http://msdn.microsoft.com/en-us/library/ms724284.aspx
        class FILETIME(ctypes.Structure):
            _fields_ = [('dwLowDateTime', DWORD),
                        ('dwHighDateTime', DWORD)]

        ftIdle = FILETIME()
        ftKernel = FILETIME()
        ftUser = FILETIME()

        if 0 == ctypes.windll.kernel32.GetSystemTimes( \
                                    ctypes.byref(ftIdle),
                                    ctypes.byref(ftKernel),
                                    ctypes.byref(ftUser) ):
            print((ctypes.WinError()))
        else:
            # the FILETIME struct stores a high order 32-bit value and a low order 32-bit value
            # we need to combine the high and low order portions to create one unsigned 64-bit integer
            uIdle = ftIdle.dwHighDateTime << 32 | ftIdle.dwLowDateTime
            uKernel = ftKernel.dwHighDateTime << 32 | ftKernel.dwLowDateTime
            uUser = ftUser.dwHighDateTime << 32 | ftUser.dwLowDateTime

            if self.cpuOldIdle != None:
                uDiffIdle = uIdle - self.cpuOldIdle
                uDiffKernel = uKernel - self.cpuOldKernel
                uDiffUser = uUser - self.cpuOldUser    
                systime = uDiffKernel + uDiffUser

                if (systime != 0):
                    cpuUsage = float(systime - uDiffIdle) / (systime)
                    #print 'CPU Usage = %g' % (cpuUsage)

            # save these for next time
            self.cpuOldIdle = uIdle
            self.cpuOldKernel = uKernel
            self.cpuOldUser = uUser

        return cpuUsage


    def ResourceProbeForPIDs (self, cmdlist):
        '''Collect runtime resource usage for a list of subprocesses.'''
        if not cmdlist:
            return
        try:
            if "darwin" == sys.platform:
                self.osxResourceProbeForPIDs( cmdlist )
            elif "win32" == sys.platform:
                #self.windowsResourceProbeForPIDs( cmdlist )
                pass
            else: # "linux" == sys.platform[:5]:
                self.linuxResourceProbeForPIDs( cmdlist )
        except Exception as e:
            self.logger.error("ResourceProbeForPIDs error: "+str(e))


    def linuxResourceProbeForPIDs (self, cmdlist):
        # examine /proc/<pid>/stat for tracking values

        now = time.time()
        MB = 1024.0 * 1024.0

        for cmd in cmdlist:
            pid = cmd.pid
            f = open("/proc/%d/stat" % pid, "rb")
            v = f.read().split()
            f.close()

            # Compute the total theoretical consumable cpu "time" in
            # seconds that was available over that last time interval,
            # it is the number of cpu cores times the interval in secs
            availableCpuTime = (now - cmd.lastStatTime) * self.nCPUs
            cmd.lastStatTime = now

            # now compute the percentage of total available "ticks"
            # that were consumed by this child process over the interval
            tusr = float(v[13]) / self.hostClkTck
            tsys = float(v[14]) / self.hostClkTck
            used = (tusr - cmd.tuser) + (tsys - cmd.tsys)
            cpu = used / availableCpuTime;

            # save for next iteration
            cmd.tuser = tusr
            cmd.tsys = tsys

            # get the memory usage stats
            vsz = float(v[22]) / MB             # bytes to mb
            rss = float(v[23]) * (4096.0 / MB)  # pages to mb

            if rss > cmd.maxRSS:
                cmd.maxRSS = rss
            if vsz > cmd.maxVSZ:
                cmd.maxVSZ = vsz
            if cpu > cmd.maxCPU:
                cmd.maxCPU = cpu

    def osxResourceProbeForPIDs (self, cmdlist):
        # for now, just exec 'ps' to get the values we need

        # handy constants, percent of one cpu -> fraction of all
        normCPU = 1.0 / (self.nCPUs * 100.0)
        kb2mb = 1.0 / 1024.0

        ps = [ "/bin/ps", "-opid,rss,vsz,%cpu", 
               "-p" + ",".join([str(c.pid) for c in cmdlist]) ]
        a = subprocess.check_output(ps)

        pdict = {}
        for line in a.split('\n')[1:]:
            v = line.split()
            if v:
                pid = int(v[0])
                rss = float(v[1]) * kb2mb
                vsz = float(v[2]) * kb2mb
                cpu = float(v[3]) * normCPU

                pdict[pid] = (rss, vsz, cpu)

        for cmd in cmdlist:
            if cmd.pid in pdict:
                rss, vsz, cpu = pdict[cmd.pid]

                if rss > cmd.maxRSS:
                    cmd.maxRSS = rss
                if vsz > cmd.maxVSZ:
                    cmd.maxVSZ = vsz
                if cpu > cmd.maxCPU:
                    cmd.maxCPU = cpu


    def windowsResourceProbeForPIDs (self, cmdlist):
        rss = 0
        vsz = 0
        cpu = 0

        for cmd in cmdlist:
            subprocessHandle = None
            # like: ctypes.windll.kernel32.GetCurrentProcess()
            # likely something in the python subprocess object
            if subprocessHandle:
                mem_struct = PROCESS_MEMORY_COUNTERS_EX()
                ret = ctypes.windll.psapi.GetProcessMemoryInfo(
                            subprocessHandle,
                            ctypes.byref(mem_struct),
                            ctypes.sizeof(mem_struct)
                        )
                if ret:
                    # units are bytes(?), we want MB
                    rss = mem_struct.PeakWorkingSetSize / (1024.0 * 1024.0)

    ## ----- ##

    class TrGPUInfo (object):
        """
        The TrGPUInfo object collects details about the GPU installed
        on the local machine, if any, and classifies it various 
        simplifying ways.
        """

        def __init__(self):
            self.logger = logging.getLogger("tractor-blade")

            self.gpuCount = 0
            self.gpuLevel = 0
            self.gpuTags = "none"
            self.gpuLabel = "none"

            self.ResolveGPUInfo()


        def ResolveGPUInfo (self, gpuEnumByPlatform={}, gpuExcludePatterns=[]):
            '''Enumerate GPUs on this host'''

            self.gpuCount = 0
            self.gpuLevel = 0
            self.gpuTags = "none"
            self.gpuLabel = "none"

            # The dict gpuEnumByPlatform can contain an enumeration command
            # to exec, for each platform "linux", "darwin", "win32". The 
            # value can be an argv list or a shell string. The list
            # gpuExcludePatterns are fnmatch style patterns to skip.

            if not gpuEnumByPlatform:
                gpuEnumByPlatform = {}

            # canonicalize the patterns:  list of strings, always *pat*
            if not gpuExcludePatterns:
                gpuExcludePatterns = []
            elif type(gpuExcludePatterns) in (str,str):
                gpuExcludePatterns = [gpuExcludePatterns]
            gpuExcludePatterns = [self.starXPat(x) for x in gpuExcludePatterns]

            try:
                if sys.platform == 'win32':
                    self.resolveGPU_Windows( gpuEnumByPlatform.get('win32'),
                                             gpuExcludePatterns )

                elif sys.platform == 'darwin':
                    self.resolveGPU_OSX( gpuEnumByPlatform.get('darwin'),
                                         gpuExcludePatterns )

                else:
                    self.resolveGPU_Linux( gpuEnumByPlatform.get('linux'),
                                           gpuExcludePatterns )

                if self.gpuCount > 0:
                    self.gpuLevel = 1  # at least
                    self.gpuTags = "basic" # software or virtualized

                    # now classify the GPU capabilities here,
                    # can get arbitrarily complex, but start simple

                    id = self.gpuLabel.upper() # canonical searching

                    if 'NVIDIA' in id or \
                       'AMD' in id or \
                       'ATI' in id or \
                       'Intel HD' in id:
                        self.gpuLevel = 2
                        self.gpuTags = "gfx"
                
            except Exception:
                self.logger.debug("resolveGPU: " + self.logger.Xcpt())


        def starXPat (self, p):
            if not p.startswith('*'): p = '*'+p
            if not p.endswith('*'): p += '*'
            return p

        def retain (self, item, exclusions):
            return not (True in [fnmatch.fnmatch(item, x) for x in exclusions])


        def resolveGPU_Linux (self, argv, exclusions):
            if not argv:
                lspci = "/sbin/lspci"             # rhel/centos
                if not os.access(lspci, os.R_OK):
                    lspci = "/usr/bin/lspci"      # debian/ubuntu
                    if not os.access(lspci, os.R_OK):
                        lspci = "lspci"

                argv = [lspci, "-mm"]

            useShell = type(argv) in (str, str)

            r = subprocess.Popen(argv, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       shell=useShell).communicate()[0]
            n = 0
            id = None
            for x in r.strip().split('\n'):
                if "VGA compatible" in x and self.retain(x,exclusions):
                    x = x.split('"')
                    n += 1
                    id = x[5]+'; '+x[3]  # of the last card only

            if n > 0 and id:
                self.gpuCount = n
                self.gpuLabel = id


        def resolveGPU_OSX (self, argv, exclusions):
            if not argv:
                argv = ["/usr/sbin/system_profiler", "SPDisplaysDataType",
                                                     "-detailLevel","mini"]

            useShell = type(argv) in (str, str)

            r = subprocess.Popen(argv, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       shell=useShell).communicate()[0]
            n = 0
            id = None
            for x in r.strip().split('\n'):
                x = x.strip()
                if x.startswith("Chipset Model:") and self.retain(x,exclusions):
                    n += 1
                    id = x[15:]  # of the last card only

            if n > 0 and id:
                self.gpuCount = n
                self.gpuLabel = id


        def resolveGPU_Windows (self, argv, exclusions):
            if not argv:
                argv = ["wmic","path","win32_VideoController","get","name"]

            useShell = type(argv) in (str, str)

            r = subprocess.Popen(argv, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       shell=useShell).communicate()[0]
            n = 0
            id = None
            for x in r.strip().split('\n'):
                x = x.strip()
                if x != "Name" and self.retain(x,exclusions):
                    n += 1
                    id = x  # of the last card only

            if n > 0 and id:
                self.gpuCount = n
                self.gpuLabel = id


## --------------------------------------------------- ##

if sys.platform == 'win32':

    # class modeled after the MEMORYSTATUSEX struct in Winbase.h in
    # the Windows SDK.  See doc details in this msdn article:
    # http://msdn.microsoft.com/en-us/library/aa366770(VS.85).aspx
    class MEMORYSTATUSEX(ctypes.Structure):
       _fields_ = [('dwLength', DWORD),
                   ('dwMemoryLoad', DWORD),
                   ('ullTotalPhys', DWORDLONG),
                   ('ullAvailPhys', DWORDLONG),
                   ('ullTotalPageFile', DWORDLONG),
                   ('ullAvailPageFile', DWORDLONG),
                   ('ullTotalVirtual', DWORDLONG),
                   ('ullAvailVirtual', DWORDLONG),
                   ('ullAvailExtendedVirtual', DWORDLONG)]

    class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
        """Used by GetProcessMemoryInfo"""
        _fields_ = [('cb', ctypes.c_ulong),
                    ('PageFaultCount', ctypes.c_ulong),
                    ('PeakWorkingSetSize', ctypes.c_size_t),
                    ('WorkingSetSize', ctypes.c_size_t),
                    ('QuotaPeakPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
                    ('PagefileUsage', ctypes.c_size_t),
                    ('PeakPagefileUsage', ctypes.c_size_t),
                    ('PrivateUsage', ctypes.c_size_t),
                   ]

elif sys.platform == 'darwin':
    class STRUCT_STATFS(ctypes.Structure):
        '''See "man statfs" on OSX'''
        _fields_ = [("f_otype",         ctypes.c_int16),
                    ("f_oflags",        ctypes.c_int16),
                    ("f_bsize",         ctypes.c_uint64),
                    ("f_iosize",        ctypes.c_int64),
                    ("f_blocks",        ctypes.c_uint64),
                    ("f_bfree",         ctypes.c_uint64),
                    ("f_bavail",        ctypes.c_uint64),
                    ("f_files",         ctypes.c_uint64),
                    ("f_ffree",         ctypes.c_uint64),
                    ("f_fsid",          2 * ctypes.c_int32),
                    ("f_owner",         ctypes.c_uint32),
                    ("f_reserved1",     ctypes.c_int16),
                    ("f_type",          ctypes.c_uint16),
                    ("f_flags",         ctypes.c_uint64),
                    ("f_reserved2",     ctypes.c_uint64),
                    ("f_fstypename",    15 * ctypes.c_char),
                    ("f_mntonname",     90 * ctypes.c_char),
                    ("f_mntfromname",   90 * ctypes.c_char),
                    ("f_reserved3",     ctypes.c_char),
                    ("f_reserved4",     4 * ctypes.c_uint64)
                  ]

    class STRUCT_STATFS_DARWIN_INODE64(ctypes.Structure):
        '''See "man statfs" on OSX, though not available to ctypes?'''
        _fields_ = [("f_bsize",         ctypes.c_uint32),
                    ("f_iosize",        ctypes.c_int32),
                    ("f_blocks",        ctypes.c_uint64),
                    ("f_bfree",         ctypes.c_uint64),
                    ("f_bavail",        ctypes.c_uint64),
                    ("f_files",         ctypes.c_uint64),
                    ("f_ffree",         ctypes.c_uint64),
                    ("f_fsid",          2 * ctypes.c_int32),
                    ("f_owner",         ctypes.c_uint32),
                    ("f_type",          ctypes.c_uint32),
                    ("f_flags",         ctypes.c_uint32),
                    ("f_fssubtype",     ctypes.c_uint32),
                    ("f_fstypename",    16 * ctypes.c_char),
                    ("f_mntonname",     1024 * ctypes.c_char),
                    ("f_mntfromname",   1024 * ctypes.c_char),
                    ("f_reserved",      8 * ctypes.c_uint)
                  ]
                    
## --------------------------------------------------------- ##
## --------------------------------------------------------- ##
import getpass

def TrGetProcessOwnerName ():
    unm = "unknown"
    try:
        # this can fail on windows service run as LocalSystem
        # or on unix if env vars are unavailable (boot of daemons?)
        unm = getpass.getuser()
    except:
        try:
            if sys.platform == 'win32':
                dll=ctypes.windll.LoadLibrary("advapi32.dll")
                buff = ctypes.create_string_buffer('\0' * 256)
                bufflen = ctypes.c_long(256)
                fun = dll.GetUserNameA
                fun(buff, ctypes.byref(bufflen))
                unm = buff.value
            else:
                unm = str( os.getuid() )
        except:
            pass

    return unm

## --------------------------------------------------------- ##

