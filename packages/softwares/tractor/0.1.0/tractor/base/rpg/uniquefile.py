"""Unique Files
A collection of routines to generate a uniquely named files.

Copied the format of tempfile.py from the python distribution. According
to the 2.3.2 license, this is allowed

"""



import os 
import errno
import tempfile

__all__ = (
        'TMP_MAX',
        'Error',
        'incremental',
        'default_generator',
        'mkunique',
        'mkuniqueobj',
        'UniqueFile',
        )

# ---------------------------------------------------------------------------

_text_openflags = os.O_RDWR | os.O_CREAT | os.O_EXCL
if hasattr(os, 'O_NOINHERIT'):
    _text_openflags |= os.O_NOINHERIT
if hasattr(os, 'O_NOFOLLOW'):
    _text_openflags |= os.O_NOFOLLOW

_bin_openflags = _text_openflags
if hasattr(os, 'O_BINARY'):
    _bin_openflags |= os.O_BINARY

if hasattr(os, 'TMP_MAX'):
    TMP_MAX = os.TMP_MAX
else:
    TMP_MAX = 10000

# ---------------------------------------------------------------------------

class Error(Exception):
    pass

# ---------------------------------------------------------------------------

def incremental(prefix, suffix=''):
    """incrementally append a number to the file template until we find a
    free name"""

    for i in range(TMP_MAX):
        name = '%s%d%s' % (prefix, i, suffix)
        yield name

default_generator = incremental

def random_names(prefix, suffix=''):
    """Use the same technique that tempfile uses."""

    names = tempfile._get_candidate_names()

    for i in range(TMP_MAX):
        yield prefix + next(names) + suffix

# ---------------------------------------------------------------------------

def mkunique(
        prefix=None, 
        text=True,
        permissions=0o600,
        suffix='', 
        filename_generator=None,
        dir=None,
        ):
    """generate a unqiuely named file, using the filename_generator to 
    generate the filenames. Returns the filename and the file descriptor
    number. Note the filename_generator, if specified, should be a function
    returning an iterator of possible filenames"""

    if filename_generator is None:
        filename_generator = default_generator

    if text:
        flags = _text_openflags
    else:
        flags = _bin_openflags

    # if no prefix is given, then we need to come up with a filename
    if not prefix:
        # get a directory too
        if not dir:
            dir = tempfile.gettempdir()
        prefix = os.path.join(dir, "tmp")
    # if a directory and prefix are provided, then join them
    elif dir:
        prefix = os.path.join(dir, prefix)

    for tempname in filename_generator(prefix, suffix):
        try:
            fd = os.open(tempname, flags, permissions)
        except OSError as e:
            # only raise if it is not a "file already exists" error
            if e.errno != errno.EEXIST:
                raise e
        else:
            return (fd, tempname)
    else:
        raise Error('cannot generate temp file')

# ---------------------------------------------------------------------------

def mkuniqueobj(
        prefix=None,
        mode='w+', 
        bufsize=-1, 
        permissions=0o600,
        suffix='', 
        filename_generator=None,
        dir=None,
        ):
    """analogous to L{mkunique} but returns a file object instead of a 
    descriptor"""

    if 'b' in mode:
        text = False
    else:
        text = True

    fd, name = mkunique(
            prefix=prefix, 
            suffix=suffix, 
            text=text, 
            permissions=permissions, 
            filename_generator=filename_generator,
            dir=dir)

    # create a file object, but remove everything that tempfile setup
    # if something fails
    try:
        fobj = os.fdopen(fd, mode, bufsize)
    except:
        # don't leave anything behind if there's a problem
        os.close(fd)
        if os.path.exists(name):
            os.remove(name)
        raise

    return fobj,name
   
# ---------------------------------------------------------------------------

class _UniqueFileWrapper(object):
    """creates a uniquely named file, where it's name is specified in the 
    name attribute"""
    
    def __init__(self, name, file):
        self.name = name
        self._file = file
                   

    def __getattr__(self, name):
        """pass on all the calls to the _file object"""

        a = getattr(self._file, name)
        if type(a) != type(0):
            setattr(self, name, a)
        return a


def UniqueFile(*args, **kwds):
    file, name = mkuniqueobj(*args, **kwds)

    return _UniqueFileWrapper(name, file)   

