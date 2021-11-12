import time, re, datetime
import dateutil.tz

__all__ = (
        'month2index',
        'month2index_lc',
        'hms2sec',
        'sec2hms',
        'sec2dhms',
        'dhms2sec',
        'hmsstr2sec',
        'sec2hmsString',
        'formatTime',
        'formatTimeSmall',
        'formatTimeDB',
        'mktime',
        'timestr2secs',
        'date2secs',
        'hostSleep',
        )

import rpg

class TimeUtilError(rpg.Error):
    pass

# ----------------------------------------------------------------------------

month2index = {'Jan': 1, 'Feb': 2,  'Mar': 3,  'Apr': 4,
               'May': 5, 'Jun': 6,  'Jul': 7,  'Aug': 8,
               'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
month2index_lc = {'jan': 1, 'feb': 2,  'mar': 3,  'apr': 4,
                  'may': 5, 'jun': 6,  'jul': 7,  'aug': 8,
                  'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}

def hms2sec(h, m, s):
    """
    Returns an elapsed time in the form hours, minutes, and seconds
    to the total number of seconds.

    >>> hms2sec(2,34,14)
    9254
    
    """
    
    return h*3600 + m*60 + s

def sec2hms(seconds):
    """
    Returns a tuple representing the elapsed time as
    (hours, minutes, seconds)
    
    >>> sec2hms(9254)
    (2L, 34L, 14L)
    >>> sec2hms(9254.5434)
    (2.0, 34.0, 14.543400000000474)
    
    """
    if (type(seconds) == int):
        seconds = int(seconds)

    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return (h, m, s)


def sec2dhms(seconds):
    """
    Returns a tuple representing the elapsed time as
    (days, hours, minutes, seconds)
    
    >>> sec2dhms(268454)
    (3L, 2L, 34L, 14L)
    >>> sec2dhms(268454.5434)
    (3.0, 2.0, 34.0, 14.543400000024121)
    
    """
    if (type(seconds) == int):
        seconds = int(seconds)

    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return (d, h, m, s)


def dhms2sec(d, h, m, s):
    """
    Returns an elasped time in the form days, hours, minutes, and
    seconds to the total number of seconds.

    >>> dhms2sec(3, 2, 34, 14)
    268454
    
    """

    return d*86400 + h*3600 + m*60 + s


def hmsstr2sec(hmsstr):
    """Convert elapsed time in the form [HH:]MM:SS[.mm] into total seconds.
    Calls L{hms2sec} after breaking the string into hour, minute, second
    components.

    >>> hmsstr2sec('3:04')
    184

    >>> hmsstr2sec('3:04.3')
    184.30000000000001

    """
    # convert it into total seconds
    fields = hmsstr.split(':')
    # figure out whether to cast the seconds to a float or not
    if '.' in fields[-1]:
        cast = float
    else:
        cast = int
    # there will be only two fields if no hours are provided
    if len(fields) == 2:
        return hms2sec(0, int(fields[0]), cast(fields[1]))

    return hms2sec(int(fields[0]), int(fields[1]), cast(fields[2]))


def sec2hmsString(seconds, zeroblank=True, precision=0):
    """
    Returns a string in the form 'hh:mm:ss' for an integer
    representing seconds.  Calls L{sec2hms} to break the integer
    up into hour, minute, and second components.
    
    @param seconds: 
    @type seconds: int

    @param zeroblank: if set to True, zero values will not be printed
                      (default True)
    @type zeroblank: bool

    @param percision: precision to use for the seconds, default is 0.
    @type precision: int

    >>> sec2hmsString(184)
    '3:04'

    >>> sec2hmsString(184.3)
    '3:04'

    >>> sec2hmsString(184.3, precision=1)
    '3:04.3'

    >>> sec2hmsString(184.3, zeroblank=False)
    '00:03:04'

    >>> sec2hmsString(184.3, precision=1, zeroblank=False)
    '00:03:04.3'

    >>> sec2hmsString(18434)
    '5:07:14'

    """

    # get the format for the seconds
    if precision > 0:
        sfmt = ":%%0%d.%df" % (precision + 3, precision)
    else:
        sfmt = ":%.2d"

    # add zeros for the hours even if it is 0
    if not zeroblank:
        return ("%.2d:%.2d" + sfmt) % sec2hms(seconds)

    # otherwise, check if there are hours
    (h, m, s) = sec2hms(seconds)
    if h:
        return ("%d:%.2d" + sfmt) % (h, m, s)

    return ("%d" + sfmt) % (m, s)


def formatTime(seconds, fmt=None):
    """
    Returns a string in form 'mo/dy|hh:mm' for an integer representing
    the amount of seconds since the epoch.
    
    @param seconds:
    @type seconds: integer

    >>> formatTime(0)
    '12/31|16:00'
    
    """

    if fmt is None:
        fmt = "%m/%d|%H:%M"

    return time.strftime(fmt, time.localtime(seconds))


def formatTimeSmall(seconds):
    """
    Returns a string in form 'mo/dy|hh:mm' for an integer representing
    the amount of seconds since the epoch.  If the date is today, the
    date is left off.  If the date is yesterday, 'yest.' is shown.
    
    @param seconds:
    @type seconds: integer

    >>> formatTimeSmall(0)
    '12/31|16:00'

    """
    
    buf = time.localtime(seconds)

    now = int(time.time())
    nowbuf = time.localtime(now)

    yesterday = now - 24 * 3600
    yesterdaybuf = time.localtime(yesterday)
    
    if (buf[1] == nowbuf[1] and buf[2] == nowbuf[2]):
        return "%.2d:%.2d" % (buf[3], buf[4])
    elif (buf[1] == yesterdaybuf[1] and buf[2] == yesterdaybuf[2]):
        return "yest. %.2d:%.2d" % (buf[3], buf[4])
    else:
        return formatTime(seconds)

def formatTimeDB(seconds):
    """
    Given a number of seconds after the epoch, returns a string
    compatible with an SQL DATETIME field.
    
    >>> formatTimeDB(0)
    '1969-12-31 16:00:00'

    """

    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(seconds))

def mktime(timetuple):
    """
    Return the number of seconds since the epoch based on a time
    tuple.  This is done to make an educated guess for times with no
    supplied year.  The input should be
    (year, month, day, hour, minute, second).  If the year is less then 0
    then it will be computed.

    >>> mktime((1979, 12, 31, 16, 0, 0))
    315532800L

    >>> time.ctime(mktime((1979, 12, 31, 16, 0, 0)))
    'Mon Dec 31 16:00:00 1979'
    
    """

    target = list(timetuple)
    if target[0] < 0:
        now = time.localtime()

        # if the target is the current month then assume the same year
        if target[1] == now[1]:
            target[0] = now[0]
        else:
            # this is the first way I thought of doing this.  It seems like
            # there is a better way, but it escapes me right now.
            
            # compute the dates six months from and ago from now
            sixago  = list(now)
            sixago[1] -= 6
            if sixago[1] < 0:
                sixago[0] -= 1
                sixago[1] += 12            
            sixfrom = list(now)
            sixfrom[1] += 6
            if sixfrom[1] > 11:
                sixfrom[0] += 1
                sixfrom[1] -= 12

            # if the target is equal to the sixago (then it is also equal
            # to the sixfrom), then the day and time have to be compared too
            if target[1] == sixago[1]:
                # if the days are the same, compare the hours
                if target[2] == sixago[2]:
                    # if hours equal then compare the minutes
                    if target[3] == sixago[3]:
                        # if minutes equal then compare the seconds
                        if target[4] == sixago[4]:
                            # if the seconds are equal then we have to pick
                            # a side, so make it six months ago
                            if target[5] >= sixago[5]:
                                target[0] = sixago[0]
                            else:
                                target[0] = sixfrom[0]
                        elif target[4] > sixago[4]:
                            target[0] = sixago[0]
                        else:
                            target[0] = sixfrom[0]
                    elif target[3] > sixago[3]:
                        target[0] = sixago[0]
                    else:
                        target[0] = sixfrom[0]
                elif target[2] > sixago[2]:
                    target[0] = sixago[0]
                else:
                    target[0] = sixfrom[0]
            elif (now[1] > target[1] and target[1] > sixago[1]) or \
                 (now[1] < target[1] and target[1] > sixago[1]):
                target[0] = sixago[0]
            elif (now[1] < target[1] and target[1] < sixfrom[1]) or \
                 (now[1] > target[1] and target[1] < sixfrom[1]):
                target[0] = sixfrom[0]

    # add the additional args required by time.mktime
    target.extend((-1, 0, -1))
    return int(time.mktime(target))

# ----------------------------------------------------------------------------

# we only use one format to represent the hour, minute, and second
_t2s_hms  = \
        r'(?P<hr>\d|\d\d)' \
        r'(\:' \
            r'(?P<min>\d\d)' \
            r'(\:' \
                r'(?P<sec>\d\d)' \
                r')?' \
            r')?' \
        r'(?P<n24>am|pm)?'

# but we use a few forms to represent the day, month, and year
# the first is what is commonly found in sys log
# e.g. Jan 21 16:55:06 or
#      Jan 21 16:55:06 2004 or
#      Jan 21 2004 or
#      Jan 2004 or
#      Jan
_t2s_mdy1 = \
        r'^(' \
            r'(?P<mon>\w\w\w)' \
            r'(\s+' \
                r'(?P<day>\d|\d\d)' \
                r'(\s+' \
                    r'%s' \
                    r')?' \
                r')?' \
        r'(\s+' \
            r'(?P<yr>\d\d\d\d)' \
            r')?' \
        r')$' % _t2s_hms
_t2s_reg1 = re.compile(_t2s_mdy1, re.IGNORECASE)

# a less common, but completely possible format
# e.g. Jan-21 16:55:06 or
#      Jan-21-04 or
#      Jan-21-2004 or
#      Jan 21 2004 16:55:06 or
#      01/20|16:41
_t2s_mdy2 = \
        r'(?P<mon>\w\w\w|\d\d|\d)' \
        r'([-/]|\s+)' \
        r'(?P<day>\d|\d\d)' \
        r'(' \
            r'([-/]|\s+)' \
            r'(?P<yr>\d\d|\d\d\d\d)' \
            r')?'
_t2s_reg2 = re.compile(r'^(%s((\s+|\||\.)%s)?)$' % \
                       (_t2s_mdy2, _t2s_hms), re.IGNORECASE)

_t2s_reg0 = re.compile(r'^(%s|%s)$' % (_t2s_mdy2, _t2s_hms), re.IGNORECASE)
_t2s_regs = (_t2s_reg0, _t2s_reg2, _t2s_reg1)

def timestr2secs(timestr):
    """
    Convert a time string to seconds since the epoch. Returns -1 if unable
    to convert string

    >>> time.ctime(timestr2secs('Jan 2004'))
    'Thu Jan  1 00:00:00 2004'

    >>> time.ctime(timestr2secs('Jan 21 2004'))
    'Wed Jan 21 00:00:00 2004'

    >>> time.ctime(timestr2secs('Jan 21 16:55:06 2004'))
    'Wed Jan 21 16:55:06 2004'

    >>> time.ctime(timestr2secs('Jan 21 2004 16:55:06'))
    'Wed Jan 21 16:55:06 2004'

    >>> time.ctime(timestr2secs('Jan-21-04 16:55:06'))
    'Wed Jan 21 16:55:06 2004'

    >>> time.ctime(timestr2secs('Jan-21-2004 16:55:06'))
    'Wed Jan 21 16:55:06 2004'

    >>> time.ctime(timestr2secs('01/21/04|16:55'))
    'Wed Jan 21 16:55:00 2004'
    
    >>> time.ctime(timestr2secs('01/21/04.16:55'))
    'Wed Jan 21 16:55:00 2004'
    
    """

    # look for a match
    dict = {}
    monabrv = 0

    cnt = 0
    for reg in _t2s_regs:
        match = reg.match(timestr)
        if match:
            dict = match.groupdict()
            break
        cnt += 1

    if not dict:
        return -1
    
    now    = time.localtime()
    target = list(now[:6])
    if not dict['hr']:
        target[3] = 0
    else:
        target[3] = int(dict['hr'])
        if dict['n24'] and (target[3] < 0 or target[3] > 12):
            return -1
        if dict['n24'] == 'pm' and target[3] < 12:
            target[3] += 12
        if dict['n24'] == 'am' and target[3] == 12:
            target[3] = 0
    if not dict['min']:
        target[4] = 0
    else:
        target[4] = int(dict['min'])
    if not dict['sec']:
        target[5] = 0
    else:
        target[5] = int(dict['sec'])
    if not dict['day'] and dict['mon']:
        target[2] = 1
    elif dict['day'] and dict['mon']:
        target[2] = int(dict['day'])

    # if no year was supplied then we assume it is either this year
    # or plus/minus one based on the supplied month.
    try:
        if not dict['yr'] and dict['mon']:
            monval = dict['mon']
            if len(monval) > 2:
                target[1] = month2index_lc[monval.lower()]
            else:
                target[1] = int(monval)
            target[0] = -1
        elif dict['yr'] and dict['mon']:
            target[0] = int(dict['yr'])
            monval = dict['mon']
            if len(monval) > 2:
                target[1] = month2index_lc[monval.lower()]
            else:
                target[1] = int(monval)
    except (KeyError, ValueError):
        return -1
    else:
        return mktime(target)


# ----------------------------------------------------------------------------

def rangestr2secs(rangestr):
    """Convert a range time string into a tuple containing the beginning
    and end of the range in seconds since the epoch.

    >>> a = rangestr2secs("5/14/08|4pm")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 16:00:00 2008 Wed May 14 16:00:00 2008

    >>> a = rangestr2secs("5/14|4pm 5/14|5pm")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 16:00:00 2008 Wed May 14 17:00:00 2008
    
    >>> a = rangestr2secs("5/14/08|4pm 5/14/08|5pm")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 16:00:00 2008 Wed May 14 17:00:00 2008
    
    >>> a = rangestr2secs("5/14/08|4pm May-14-2008 5pm")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 16:00:00 2008 Wed May 14 17:00:00 2008
    
    >>> a = rangestr2secs("May-14-2008 4pm 5/14/08|5pm")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 16:00:00 2008 Wed May 14 17:00:00 2008
    
    >>> a = rangestr2secs("May-14-2008 4pm May-14-2008 5pm")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 16:00:00 2008 Wed May 14 17:00:00 2008
    
    >>> a = rangestr2secs("5/14/08|4pm+1h")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 16:00:00 2008 Wed May 14 17:00:00 2008
    
    >>> a = rangestr2secs("5/14/08|4pm +1h")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 16:00:00 2008 Wed May 14 17:00:00 2008
    
    >>> a = rangestr2secs("5/14/08|4pm-+1h")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 15:00:00 2008 Wed May 14 17:00:00 2008
    
    >>> a = rangestr2secs("5/14/08|4pm+-1h")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 15:00:00 2008 Wed May 14 17:00:00 2008
    
    >>> a = rangestr2secs("5/14/08|4pm +-h")
    >>> print time.ctime(a[0]), time.ctime(a[1])
    Wed May 14 15:00:00 2008 Wed May 14 17:00:00 2008

    """


    # regexp used to search for offsets in the form:
    #  +3h
    #  -+3h
    #  +-3h
    offsetre = re.compile(r"(?P<op>(?:\+\-)|(?:\-\+)|[\-\+])"
                          r"(?P<offset>\d*[smhdw])$")

    # split on spaces and then iterate over each token until an offset is
    # found or we find a valid time
    tokens    = rangestr.split()
    t1,t2     = None,None
    offset,op = None,None
    i = 0
    currtoks  = []
    currsecs  = 0
    while i < len(tokens):
        # check if an offset is defined on this token
        match = offsetre.search(tokens[i])
        if match:
            #print match.groupdict()
            # offsets must be on the last token
            if i != len(tokens) - 1:
                raise TimeUtilError("offset not at end of '%s'" % rangestr)
            
            # all tokens preceding this one (plus the 'time' group) must
            # be a valid time str
            if match.start() > 0:
                currtoks.append(tokens[i][:match.start()])
            t1 = timestr2secs(' '.join(currtoks))
            #print ' '.join(currtoks), t1
            if t1 <= 0:
                raise TimeUtilError("invalid time format '%s'" % \
                      ' '.join(currtoks))

            # compute the offset
            offset = match.group("offset")
            op     = match.group("op")
            
            # check for implied single unit offsets (e.g. s, h, m, d, w)
            if not offset[0].isdigit():
                offset = '1' + offset

            from . import unitutil
            # convert to total seconds
            offset = unitutil.parseSeconds(offset)

            if op == '+':
                return (t1, t1 + offset)
            if op == '-':
                return (t1 - offset, t1)
            if op in ('-+', '+-'):
                return (t1 - offset, t1 + offset)

        # append to currstr until a valid time is longer found, this way
        # we can still accept two absolute times that include spaces
        secs = timestr2secs(' '.join(currtoks + [tokens[i]]))
        #print ' '.join(currtoks + [tokens[i]]), secs
        if secs <= 0 and currsecs > 0:
            t1 = currsecs
            # this means t2 must be everything that's left
            t2 = timestr2secs(' '.join(tokens[i:]))
            #print ' '.join(tokens[i:]), t2
            if t2 <= 0:
                raise TimeUtilError("invalid time format '%s'" % \
                      ' '.join(tokens[i:]))

            return (t1, t2)

        currtoks.append(tokens[i])
        currsecs = secs
        i += 1

    # if we get here, then the whole string must be one time
    t1 = timestr2secs(rangestr)
    if t1 <= 0:
        raise TimeUtilError("invalid time format '%s'" % rangestr)

    return (t1, t1)


# ----------------------------------------------------------------------------

_localos = None
def date2secs(date):
    """Convert a timezone-aware datetime object to seconds."""
    return (date - datetime.datetime(1970,1,1, tzinfo=dateutil.tz.tzutc())).total_seconds()

    # use the following block if you'd rather use strftime("%s") on mac and linux
    global _localos
    if _localos is None:
        import platform
        _localos = platform.platform().split('-')[0]
    if _localos in ("Linux", "Darwin"):
        return date.strftime("%s")
    else:
        # this commented out line is only good if the date is not timezone aware
        #return (date - datetime.datetime(1970,1,1)).total_seconds()
        return (date - datetime.datetime(1970,1,1, tzinfo=dateutil.tz.tzutc())).total_seconds()

# ----------------------------------------------------------------------------

def hostSleep(factor):
    """
    Executes a sleep command based on the last two characters
    of the hostname.  The sleep time will be the integer represented
    by the last two characters multiplied by the factor.

    If the last two characters do not form an integer, '01' is used.

    """

    try:
        seconds = int(localhost[-2:])
    except ValueError:
        seconds = 1
    
    time.sleep(seconds * factor)
