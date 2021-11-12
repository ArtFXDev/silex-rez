"""This module ports Python 2.6 library features back to Python 2.3+."""

import sys as _sys
from keyword import iskeyword as _iskeyword

try:
    all
except NameError:
    def all(items):
        for item in items:
            if not item:
                return False
        return True

try:
    any
except NameError:
    def any(items):
        for item in items:
            if item:
                return True
        return False

try:
    set
except NameError:
    from sets import Set as set

try:
    from operator import attrgetter
except (ImportError, AttributeError):
    def attrgetter(*items):
        if len(items) == 1:
            item = items[0]
            def g(obj):
                return getattr(obj, item)
        else:
            def g(obj):
                return tuple([getattr(obj, item) for item in items])
        return g

_attrgetter = attrgetter

try:
    from operator import itemgetter
except (ImportError, AttributeError):
    def itemgetter(*items):
        if len(items) == 1:
            item = items[0]
            def g(obj):
                return obj[item]
        else:
            def g(obj):
                return tuple([obj[item] for item in items])
        return g

_itemgetter = itemgetter

# Copied from Python2.6
def _namedtuple(typename, field_names, verbose=False):
    """Returns a new subclass of tuple with named fields.

    >>> Point = namedtuple('Point', 'x y')
    >>> Point.__doc__                   # docstring for the new class
    'Point(x, y)'
    >>> p = Point(11, y=22)             # instantiate with positional args or keywords
    >>> p[0] + p[1]                     # indexable like a plain tuple
    33
    >>> x, y = p                        # unpack like a regular tuple
    >>> x, y
    (11, 22)
    >>> p.x + p.y                       # fields also accessable by name
    33
    >>> d = p._asdict()                 # convert to a dictionary
    >>> d['x']
    11
    >>> Point(**d)                      # convert from a dictionary
    Point(x=11, y=22)
    >>> p._replace(x=100)               # _replace() is like str.replace() but targets named fields
    Point(x=100, y=22)

    """

    # Parse and validate the field names.  Validation serves two purposes,
    # generating informative error messages and preventing template injection attacks.
    if isinstance(field_names, str):
        field_names = field_names.replace(',', ' ').split() # names separated by whitespace and/or commas
    field_names = tuple(map(str, field_names))
    for name in (typename,) + field_names:
        if not all([c.isalnum() or c=='_' for c in name]):
            raise ValueError('Type names and field names can only contain alphanumeric characters and underscores: %r' % name)
        if _iskeyword(name):
            raise ValueError('Type names and field names cannot be a keyword: %r' % name)
        if name[0].isdigit():
            raise ValueError('Type names and field names cannot start with a number: %r' % name)
    seen_names = set()
    for name in field_names:
        if name.startswith('_'):
            raise ValueError('Field names cannot start with an underscore: %r' % name)
        if name in seen_names:
            raise ValueError('Encountered duplicate field name: %r' % name)
        seen_names.add(name)

    # Create and fill-in the class template
    numfields = len(field_names)
    argtxt = repr(field_names).replace("'", "")[1:-1]   # tuple repr without parens or quotes
    reprtxt = ', '.join(['%s=%%r' % name for name in field_names])
    dicttxt = ', '.join(['%r: t[%d]' % (name, pos) for pos, name in enumerate(field_names)])
    template = '''class %(typename)s(tuple):
        '%(typename)s(%(argtxt)s)' \n
        __slots__ = () \n
        _fields = %(field_names)r \n
        def __new__(cls, %(argtxt)s):
            return tuple.__new__(cls, (%(argtxt)s)) \n
        def _make(cls, iterable, new=tuple.__new__, len=len):
            'Make a new %(typename)s object from a sequence or iterable'
            result = new(cls, iterable)
            if len(result) != %(numfields)d:
                raise TypeError('Expected %(numfields)d arguments, got %%d' %% len(result))
            return result \n
        _make = classmethod(_make)
        def __repr__(self):
            return '%(typename)s(%(reprtxt)s)' %% self \n
        def _asdict(t):
            'Return a new dict which maps field names to their values'
            return {%(dicttxt)s} \n
        def _replace(self, **kwds):
            'Return a new %(typename)s object replacing specified fields with new values'
            result = self._make(map(kwds.pop, %(field_names)r, self))
            if kwds:
                raise ValueError('Got unexpected field names: %%r' %% kwds.keys())
            return result \n
        def __getnewargs__(self):
            return tuple(self) \n\n''' % locals()
    for i, name in enumerate(field_names):
        template += '        %s = property(itemgetter(%d))\n' % (name, i)
    if verbose:
        print(template)

    # Execute the template string in a temporary namespace and
    # support tracing utilities by setting a value for frame.f_globals['__name__']
    namespace = dict(itemgetter=_itemgetter, __name__='namedtuple_%s' % typename)
    try:
        exec(template, namespace)
    except SyntaxError as e:
        raise SyntaxError(e.message + ':\n' + template)
    result = namespace[typename]

    # For pickling to work, the __module__ variable needs to be set to the frame
    # where the named tuple is created.  Bypass this step in enviroments where
    # sys._getframe is not defined (Jython for example).
    if hasattr(_sys, '_getframe'):
        result.__module__ = _sys._getframe(1).f_globals.get('__name__', '__main__')

    return result


# If we got Python's namedtuple use that, otherwise use ours.
try:
    from collections import namedtuple
except (ImportError, AttributeError):
    namedtuple = _namedtuple
