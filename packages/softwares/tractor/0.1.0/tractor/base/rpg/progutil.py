import sys
import time

from rpg.terminal import TerminalColor

__all__ = (
        'getFilePosition',
        'lineno',
        'deprecated',
        'cachedproperty',
        'memoize',
        'UsageError',
        'LogColors',
        'log',
        'logWarning',
        'logError',
        )

# ---------------------------------------------------------------------------

def getFilePosition(stacklevel=0):
    """
    Returns the filename and line number 
    
    derived from warnings.py
    """

    try:
        caller = sys._getframe(stacklevel+1)
    except ValueError:
        filename = sys.__dict__.get('__file__')
        lineno = 1
    else:
        filename = caller.f_code.co_filename
        lineno = caller.f_lineno

    if filename:
        fnl = filename.lower()
        if fnl.endswith(".pyc") or fnl.endswith(".pyo"):
            filename = filename[:-1]
    else:
        module = sys.__dict__.get('__name__', '<string>')
        if module == '__main__':
            filename = sys.argv[0]
        else:
            filename = module

    return (filename, lineno)
 

def lineno(stacklevel=0):
    """
    Returns the line number of the current position in a file

    derived from warnings.py
    """
    
    try:
        return sys._getframe(stacklevel+1).f_lineno
    except ValueError:
        return 1

# ---------------------------------------------------------------------------

def deprecated(obj, msg=None, name=None, doc=None):
    """
    This is a wrapper which can be used to mark functions or objects
    as deprecated. It will result in a warning being emmitted
    when the function is used.

    @param obj: the object we wish to memoize
    @param msg: the message to print out
    @param name: if set, uses this for the __name__ instead of the
        function.__name__
    @param doc: if set, uses this for the __doc__ instead of the 
        function,__doc__

    """
    if msg is None:
        msg = 'Call to deprecated object: %s' % obj.__name__

    if name is None:
        name = obj.__name__

    if doc is None:
        doc = obj.__doc__

    # the reason why we exec this is to make sure that the object we return
    # has the same __name__ as the object passed in. While I believe __name__
    # is writable in later versions of python, it is coded this way for 
    # backwards compatibility
    exec("""def _deprecated(obj, msg, doc):
    import warnings
    __my_doc = doc
    __my_dict = obj.__dict__
    def %(name)s(*args, **kwargs):
        warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
        return obj(*args, **kwargs)

    %(name)s.__doc__ = __my_doc
    %(name)s.__dict__.update(__my_dict)

    return %(name)s
""" % {'name': name})

    return _deprecated(obj, msg, doc)

# ---------------------------------------------------------------------------

class cachedproperty(object):
    """
    a property-like method descriptor that creates a one time property. After
    the first call, the value will be cached in the instance.

    >>> class C(object):
    ...   def __init__(self):
    ...     self.counter = 0
    ...
    ...   def foo(self):
    ...     self.counter += 1
    ...     return self.counter
    ...   foo=cachedproperty(foo)
    >>> c=C()
    >>> c.foo
    1
    >>> c.foo
    1
    """

    def __init__(self, function, name=None, doc=None):
        """
        @param function: the function to cache the results from
        @param name: if set, uses this for the __name__ instead of the
            function.__name__
        @param doc: if set, uses this for the __doc__ instead of the
            function.__doc__
        """
        
        self.__dict__.update(function.__dict__)
        if doc is None:
            doc = function.__doc__
        self.__doc__ = doc
        
        if name is None:
            name = function.__name__
        self.__name__ = name

        # we need this after the update so that it's not overwritten
        self.__function = function


    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            value = self.__function(instance)
        setattr(instance, self.__name__, value)
        return value

# ---------------------------------------------------------------------------

def memoize(function, name=None, doc=None):
    """
    cache the results of a function call. Note that over time, this can
    consume a large amount of memory

    >>> counter = 0
    >>> def foo(a):
    ...   global counter
    ...   counter += 1
    ...   return a
    >>> foo = memoize(foo)
    >>> foo(5), counter
    (5, 1)
    >>> foo(5), counter
    (5, 1)
    >>> foo(6), counter
    (6, 2)

    This also works for classes.
    
    >>> class Foo(object):
    ...   def __init__(self):
    ...     self.counter = 0
    ...   def foo(self, a):
    ...     self.counter += 1
    ...     return a
    ...   foo = memoize(foo)
    >>> foo = Foo()
    >>> foo.foo(5), foo.counter
    (5, 1)
    >>> foo.foo(5), foo.counter
    (5, 1)
    >>> foo.foo(6), foo.counter
    (6, 2)

    Note that each instance of a class maintains it's own separate cache:

    >>> bar = Foo()
    >>> bar.foo(5), bar.counter
    (5, 1)
    >>> bar.foo(5), bar.counter
    (5, 1)
    >>> bar.foo(6), bar.counter
    (6, 2)

    @param function: the function we wish to memoize
    @param name: if set, uses this for the __name__ instead of the
        function.__name__
    @param doc: if set, uses this for the __doc__ instead of the 
        function,__doc__
   
    """
    
    if name is None:
        name = function.__name__

    if doc is None:
        doc = function.__doc__

    # the reason why we exec this is to make sure that the object we return
    # has the same __name__ as the object passed in. While I believe __name__
    # is writable in later versions of python, it is coded this way for 
    # backwards compatibility
    exec("""def _memoize(function, doc):
    __my_doc = doc
    __my_dict = function.__dict__
    __cache = {}
    def %(name)s(*args):
        cache = __cache
        try:
            return cache[args]
        except KeyError:
            cache[args] = value = function(*args)
            return value

    %(name)s.__doc__ = __my_doc
    %(name)s.__dict__.update(__my_dict)

    return %(name)s
""" % {'name': name})

    return _memoize(function, doc)

# ---------------------------------------------------------------------------

class UsageError(Exception):
    """
    Helper exception to print out usage errors

    """
    
    def __init__(self, msg, errno=2):
        self.msg = msg
        self.errno = errno

    def __str__(self):
        return str(self.msg)
# aliased for backwards compatibility
Usage = deprecated(UsageError, \
        msg='Usage is deprecated. Please use UsageError',
        name='Usage')

# ---------------------------------------------------------------------------

LogColors = {
    'yellow': TerminalColor('yellow'),
    'red': TerminalColor('red'),
    'blue': TerminalColor('blue'),
    'white': TerminalColor('white'),
    'cyan': TerminalColor('cyan')
    }


def log(msg, outfile=None, color=None):
    """Appends a time stamp and '==>' to a string before printing
    to stdout."""

    if not outfile:
        outfile = sys.stdout

    if color and color in LogColors:
        terminalColor = LogColors[color]
        msg = terminalColor.colorStr(msg)

    try:
        print(time.ctime() + " ==> " + msg, file=outfile)
        outfile.flush()
    except (IOError, OSError) as msg:
        pass


def logWarning(msg):
    log('WARNING: ' + msg, color='yellow')


def logError(msg):
    log('ERROR: ' + msg, color='red') 
