import types
import re
import shlex

from rpg import listutil
from rpg import StringUtilError

__all__ = (
        'StringUtilError',
        'cStringReplace',
        'squeezeString',
        'truncString',
        'quotedSplit',
        'maxStrLen',
        'num2name',
        'name2num',
        'list2range',
        'range2list',
        'str2list',
        'file2list',
        'file2str',
        'formatHertz',
        'formatBytes',
        'Template',
        )

# ----------------------------------------------------------------------------

def cStringReplace(cStr, search, replace):
    """This function searches for all occurrences of search
    and replaces with replace.  Operations are performed on cStr
    in place.  search and replace should be of the same length.
    """

    if len(search) != len(replace):
        raise StringUtilError('cStringReplace(): search and replace strings ' \
              + 'must be the same length')

    s = cStr.getvalue()
    
    start = 0
    while (1):
        index = s.find(search, start)
        if index == -1:
            break
        cStr.seek(index)
        cStr.write(replace)
        start = index + 1


def squeezeString(myStr, length):
    """
    This subroutine shortens the specified string to the specified
    length, replacing middle characters with '...' if necessary.

    >>> squeezeString('hello', 7)
    'hello'
    >>> squeezeString('hello world', 7)
    'he...ld'
    
    """

    # pathological case
    if length <= 0:
        return ''

    strLen = len(myStr)
    
    # don't do a thing if length is okay
    if strLen <= length:
        return myStr

    # only truncate tail if desired length is too small
    if length < 6:
        return myStr[:length]

    # extract middle charactes
    strTail = int((length - 3) / 2)
    strHead = length - 3 - strTail
    strTail = strLen - strTail
    return myStr[0:strHead] + '...' + myStr[strTail:]


def truncString(s, length, loc):
    """Shorten the given string to at most length bytes.  If loc is
    'center', use squeeze string; otherwise, truncate at 'left' or 'right'
    of string.
    
    >>> truncString('hello world', 7, 'center')
    'he...ld'
    >>> truncString('hello world', 7, 'left')
    'o world'
    >>> truncString('hello world', 7, 'right')
    'hello w'
    
    """

    if len(s) <= length:
        return s
    elif loc == 'right':
        return s[:length]
    elif loc == 'left':
        return s[len(s)-length:]
    else: # assume loc == 'center'
        return squeezeString(s, length)


# tricky regexp used to split words on whitespace and matching quotes
_splitre = re.compile(r"((?:'[^\'\\]*(?:\\.[^\'\\]*)*\')|"
                      r"(?:\"[^\"\\]*(?:\\.[^\"\\]*)*\")|"
                      r"(?:\`[^\`\\]*(?:\\.[^\`\\]*)*\`)|"
                      r"(?:\S+))")
def quotedSplit(s, removeQuotes=False):
    """Split a string on whitespace and preserve quoted strings.

    >>> quotedSplit("non quoted 'single quotes'")
    ['non', 'quoted', "'single quotes'"]
    >>> quotedSplit("")
    []
    >>> quotedSplit("no quotes 'single quotes with a \\nnewline'")
    ['no', 'quotes', "'single quotes with a \\nnewline'"]

    """

    if not s: return []
    fields = _splitre.findall(s)
    if removeQuotes:
        return [f.strip("'\"") for f in fields]
    return fields
    

# ----------------------------------------------------------------------------

# aliased for backwards compatibility
maxStrLen = listutil.maxLen

# ----------------------------------------------------------------------------

def num2name(num):
    """
    Convert a number to an ASCII name with letters between A-Z. 
    
    >>> num2name(0)
    'A'

    >>> num2name(25)
    'Z'

    >>> num2name(26)
    'AA'

    >>> num2name(27)
    'AB'
    
    """

    if num < 0:
        raise StringUtilError("input to num2name must be >= to 0")

    ord_A = ord('A')
    ord_Z = ord('Z')

    name = ''
    while num >= 26:
        div  = num / 26
        name = chr(num - div*26 + ord_A) + name
        num  = div - 1
    else:
        name = chr(num + ord_A) + name
    return name


def name2num(name):
    """
    Convert an ASCII name to an equivalent number.  

    >>> name2num('A')
    0
    >>> name2num('Z')
    25
    >>> name2num('AA')
    26
    >>> name2num('AB')
    27
    
    """

    # input must be a string
    if type(name) is not bytes:
        raise StringUtilError("input to name2num must be a string")

    ord_A = ord('A')
    ord_Z = ord('Z')
    
    num = 0
    cnt = len(name) - 1
    for c in name:
        val  = ord(c)
        if val < ord_A or val > ord_Z:
            raise StringUtilError("all characters in input to name2num " \
                    "must be between A-Z")
        val -= ord_A
        if cnt:
            num += (val + 1)*26**cnt
        else:
            num += val
        cnt -= 1
    return num

# ----------------------------------------------------------------------------

def list2range(list):
    """
    This routine converts a list of numbers (1,2,3,4,5) into a range '1-5'.
    Discontinuous ranges are separated by commas. (1,2,3,5,6) -> '1-3,5-6'

    >>> list2range([1,2,3,4,5])
    '1-5'

    >>> list2range([1,2,3,5,6])
    '1-3,5-6'
    
    >>> list2range([1,3,5,7,9])
    '1,3,5,7,9'

    """

    # special thanks goes to Josh Minor who has now bequeathed this code
    # to me not once, but twice (and the first time in Perl!)

    if not list: return ''
    
    ranges = []
    listofframes = [int(x) for x in list]
    listofframes.sort()
    start = listofframes[0]
    end = start

    # append a range onto the list of ranges
    def append_range(start, end, ranges):
        if start == end:
            ranges.append(str(start))
        else:
            ranges.append("%d-%d" % (start, end))

    for frame in listofframes[1:]:
        if frame == end + 1:
            # this range continues
            end = frame
        else:
            # end of a range, append it and start a new one
            append_range(start, end, ranges)
            start = frame
            end = frame

    # append the remaining range
    append_range(start, end, ranges)

    # return a string (comma separated list of ranges)
    return ','.join(ranges)


_range2listRE = re.compile(r"([\-]?\d+)(?:\-([\-]?\d+))?")
def range2list(str, step=1):
    """
    This routine takes a range string and returns a list of numbers.
    e.g. '1-3,5-6' -> [1,2,3,5,6]

    >>> range2list('1-5')
    [1, 2, 3, 4, 5]

    >>> range2list('1-3,5-6')
    [1, 2, 3, 5, 6]

    >>> range2list('1,3,5,7,9')
    [1, 3, 5, 7, 9]
    
    >>> range2list('-14')
    [-14]
    
    >>> range2list('-14--10')
    [-14, -13, -12, -11, -10]
    
    """

    l = []
    ranges = str.split(',')

    for r in ranges:
        if r == '': continue

        # check for a match
        match = _range2listRE.match(r)
        if not match:
            raise StringUtilError("range2list: expected an integer, " \
                  "got '%s'" % r)

        first,last = match.groups()
        if not last:
            # this occurs if there was no '-'
            l.append(int(first))
        else:
            for i in range(int(first), int(last) + 1):
                l.append(i)

    if step > 1:
        lstep = []
        for i in range(0, len(l), step):
            lstep.append(l[i])
        l = lstep
                
    return l


def str2list(str, separator=r'[,\s]+'):
    """
    Break up a string of space-separated or comma-separated strings into a 
    list.
    
    >>> str2list('a, b, c')
    ['a', 'b', 'c']
    
    >>> str2list('a, b\t, c, ')
    ['a', 'b', 'c']
    
    """

    if not str:
        return []

    return [i for i in re.split(separator, str) if i]


def file2list(filename, filterComments=False):
    try:
        f = file(filename)
    except IOError:
        return None

    try:
        lines = f.readlines()
    finally:
        f.close()

    s = ''
    for line in lines:
        if filterComments:
            commentIndex = line.find('#')
            if commentIndex > -1:
                if line[-1] == '\n':
                    line = line[:commentIndex] + '\n'
                else:
                    line = line[:commentIndex]
        s += line

    return str2list(s)


def file2str(filename, doraise=1):
    try:
        f = file(filename)
    except IOError:
        if doraise:
            raise StringUtilError("file2str(): can't handle file %s" % \
                    filename)
        else:
            return None

    try:
        s = f.read()
    finally:
        f.close()

    # decompress string if it was compressed
    # TODO: add other compression schemes
    if filename[-4:] == '.bz2':
        import bz2
        s = bz2.decompress(s)
    return s

# ---------------------------------------------------------------------------

# aliased for backwards compatibility
from .unitutil import formatHertz
from .unitutil import formatBytes

# ---------------------------------------------------------------------------

import string

# see if we can just use the standard template first
try:
    Template = string.Template
except:
    # note, everything below is copy Copyright (c)
    # 2001, 2002, 2003, 2004 Python Software Foundation; All Rights Reserved
    # see http://www.python.org/2.4/license.html for more details

    class _multimap:
        """Helper class for combining multiple mappings.
    
        Used by .{safe_,}substitute() to combine the mapping and keyword
        arguments.
        """
        def __init__(self, primary, secondary):
            self._primary = primary
            self._secondary = secondary
    
        def __getitem__(self, key):
            try:
                return self._primary[key]
            except KeyError:
                return self._secondary[key]
    
    
    class _TemplateMetaclass(type):
        pattern = r"""
        %(delim)s(?:
          (?P<escaped>%(delim)s) |   # Escape sequence of two delimiters
          (?P<named>%(id)s)      |   # delimiter and a Python identifier
          {(?P<braced>%(id)s)}   |   # delimiter and a braced identifier
          (?P<invalid>)              # Other ill-formed delimiter exprs
        )
        """
    
        def __init__(cls, name, bases, dct):
            super(_TemplateMetaclass, cls).__init__(name, bases, dct)
            if 'pattern' in dct:
                pattern = cls.pattern
            else:
                pattern = _TemplateMetaclass.pattern % {
                    'delim' : re.escape(cls.delimiter),
                    'id'    : cls.idpattern,
                    }
            cls.pattern = re.compile(pattern, re.IGNORECASE | re.VERBOSE)
    
    
    class Template(metaclass=_TemplateMetaclass):
        """A string class for supporting $-substitutions."""
    
        delimiter = '$'
        idpattern = r'[_a-z][_a-z0-9]*'
    
        def __init__(self, template):
            self.template = template
    
        # Search for $$, $identifier, ${identifier}, and any bare $'s
    
        def _invalid(self, mo):
            i = mo.start('invalid')
            lines = self.template[:i].splitlines(True)
            if not lines:
                colno = 1
                lineno = 1
            else:
                colno = i - len(''.join(lines[:-1]))
                lineno = len(lines)
            raise ValueError('Invalid placeholder in string: line %d, col %d' %
                             (lineno, colno))
    
        def substitute(self, *args, **kws):
            if len(args) > 1:
                raise TypeError('Too many positional arguments')
            if not args:
                mapping = kws
            elif kws:
                mapping = _multimap(kws, args[0])
            else:
                mapping = args[0]
            # Helper function for .sub()
            def convert(mo):
                # Check the most common path first.
                named = mo.group('named') or mo.group('braced')
                if named is not None:
                    val = mapping[named]
                    # We use this idiom instead of str() because the latter will
                    # fail if val is a Unicode containing non-ASCII characters.
                    return '%s' % val
                if mo.group('escaped') is not None:
                    return self.delimiter
                if mo.group('invalid') is not None:
                    self._invalid(mo)
                raise ValueError('Unrecognized named group in pattern',
                                 self.pattern)
            return self.pattern.sub(convert, self.template)
    
        def safe_substitute(self, *args, **kws):
            if len(args) > 1:
                raise TypeError('Too many positional arguments')
            if not args:
                mapping = kws
            elif kws:
                mapping = _multimap(kws, args[0])
            else:
                mapping = args[0]
            # Helper function for .sub()
            def convert(mo):
                named = mo.group('named')
                if named is not None:
                    try:
                        # We use this idiom instead of str() because the latter
                        # will fail if val is a Unicode containing non-ASCII
                        return '%s' % mapping[named]
                    except KeyError:
                        return self.delimiter + named
                braced = mo.group('braced')
                if braced is not None:
                    try:
                        return '%s' % mapping[braced]
                    except KeyError:
                        return self.delimiter + '{' + braced + '}'
                if mo.group('escaped') is not None:
                    return self.delimiter
                if mo.group('invalid') is not None:
                    return self.delimiter
                raise ValueError('Unrecognized named group in pattern',
                                 self.pattern)
            return self.pattern.sub(convert, self.template)
