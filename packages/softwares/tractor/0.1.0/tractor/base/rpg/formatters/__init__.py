"""
Collection of formatting classes to quickly print Job, Task, and Slot
objects.  The framework of the module is designed so any object can have
a Formatter defined for it.  The module also supports the merging of
existing Formatter objects (i.e. creating a Job and Slot formatter
dynamically).
"""

from rpg import timeutil
from rpg import stringutil

__all__ = (
        'getVal',
        'getFormatTime',
        'getElapsedTime',
        'getJoinedList',
        'getDiskOrMemory',
        'getDiskOrMemory_bytes',
        )

# ----------------------------------------------------------------------------

# these are intentially left out of the Formatter class and are
# meant to be convenience functions for subclasses of Formatter.
def getVal(obj, varname):
    """
    In order to make it possible to pass a function name as a
    variable name, we have to check the type of the returned value.  If
    it is a function then execute it with no arguments. If it is indexable,
    access it
    
    >>> getVal([1,2], 1)
    2

    >>> getVal({'a': 3}, 'a')
    3

    >>> class C:
    ...   a = 4
    ...   def __init__(self):
    ...     self.b = 5
    ...     self.c = lambda: 6
    ...   def d(self):
    ...     return 7
    >>> getVal(C(), 'a')
    4
    >>> getVal(C(), 'b')
    5
    >>> getVal(C(), 'c')
    6
    >>> getVal(C(), 'd')
    7
   
    >>> getVal(5, 6)
    Traceback (most recent call last):
    ...
    TypeError: unsubscriptable object

    """

    try:
        return obj[varname]
    except AttributeError:
        # obj is not indexable, so try to access it as a member

        val = getattr(obj, varname)

        # what if the variable name is actually a function?  If so then
        # execute it with no arguments
        try:
            val = val()
        except TypeError:
            pass

        return val


def getFormatTime(obj, varname):
    """
    Returns the value obj.varname as a formatted time string using 
    L{rpg.timeutil.formatTime}, assuming of course that the value represents 
    seconds since the epoch.  Also if the time value is 0, then the string 
    'unknown' is returned.
    
    >>> getFormatTime({'a': 0}, 'a')
    'unknown'

    >>> getFormatTime({'a': None}, 'a')
    'unknown'

    >>> getFormatTime({'a': 1}, 'a')
    '12/31|16:00'

    >>> try:
    ...   getFormatTime({'a': 'blah'}, 'a')
    ... except TypeError:
    ...   pass

    """

    val = getVal(obj, varname)
    if not val:
        return 'unknown'
    return timeutil.formatTime(val)

def getElapsedTime(obj, varname):
    """
    Returns the value obj.varname as a formatted seconds s
    using L{rpg.timeutil.sec2hmsString}, as hh:mm:ss

    >>> getElapsedTime({'a': 0}, 'a')
    '00:00:00'
    
    >>> getElapsedTime({'a': None}, 'a') is None
    1

    >>> getElapsedTime({'a': 50000}, 'a')
    '13:53:20'

    >>> try:
    ...   getFormatTime({'a': 'blah'}, 'a')
    ... except TypeError:
    ...   pass
          
    
    """

    val = getVal(obj, varname)
    if val is None:
        return None
    return timeutil.sec2hmsString(val)


def getJoinedList(obj, varname):
    """
    Returns the list found in obj.varname as a space joined
    string.

    >>> getJoinedList({'a': ['1', '2', '3']}, 'a')
    '1 2 3'
    
    >>> getJoinedList({'a': []}, 'a')
    ''

    >>> getJoinedList({'a': None}, 'a')
    ''

    >>> getJoinedList({'a': 5}, 'a')
    Traceback (most recent call last):
    ...
    TypeError: sequence expected, int found

    """

    val = getVal(obj, varname)
    if not val:
        return ''
    return ' '.join(val)


def getDiskOrMemory(obj, varname):
    """
    Returns a value represented in kilobytes as megabytes and gigabytes
    where necessary.

    >>> getDiskOrMemory({'a': 50000}, 'a')
    '48.8G'

    >>> try:
    ...   getDiskOrMemory({'a': None}, 'a')
    ... except TypeError:
    ...   pass
    
    """

    val = getVal(obj, varname)
    return stringutil.formatBytes(val, base='mega', precision=2)


def getDiskOrMemory_bytes(obj, varname):
    """
    Returns a value represented in kilobytes as megabytes and gigabytes
    where necessary.

    >>> getDiskOrMemory_bytes({'a': 50000}, 'a')
    '48.8K'

    >>> try:
    ...   getDiskOrMemory_bytes({'a': None}, 'a')
    ... except TypeError:
    ...   pass

    """

    val = getVal(obj, varname)
    return stringutil.formatBytes(val, precision=2)
