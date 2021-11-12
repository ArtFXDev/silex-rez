"""Locking Files
Simple helpful mechanisms for creating a lock files for processes, even
across an nfs mount. The algorithm we use to lock a file is based on the
fact that hard linking is atomic on nfs. Here's how it goes::
    
    - create a temp file on the same filesystem as the lock file
    - get the number of hard links to the temp file
    - hard link the lock file to the temp file
      - if the link errors, then someone already has the lock
    - get the number of hard links to the lock file
    - if the number of hard links of the lock file is one more than
      the temp file, we've also got the lock
      - if not, someone has already locked the file 

One thing to be careful with lock files created with these mechanisms
is that the lock file should be removed when you're done with it. This
is the only way to atomically lock across nfs. If you do need to lock
an editable file, have the lock file associated with the file you wish to
edit. For instance if you want to lock a file "blah.txt", make the lock file
"blah.txt.lock".
"""

import os
import errno
import weakref as weakref

try:
    import _thread
except ImportError:
    import dummythread as thread

import rpg.uniquefile as uniquefile

__all__ = (
        'Error',
        'FatalError',
        'mklock',
        'mklockobj',
        'LockFile',
        'RLockFile',
        )
    
# ---------------------------------------------------------------------------

class Error(Exception):
    pass

class FatalError(Exception):
    pass

# ---------------------------------------------------------------------------

def mklock(lockname, text=True):
    """
    Opens a lock file on a local or nfs mounted remote file system and 
    returns a file descriptor of the lock file. This file will be locked 
    until this file is closed. The caller is responsible for closing the 
    file.

    Raises IOError if cannot lock file
   
    @param text: if set to True, open the file in text mode. Else, set to 
                 binary
    """

    # generate tempfile
    prefix = '%s-%s.%s.' % (lockname, os.uname()[1], str(os.getpid()))

    try:
        fd, name = uniquefile.mkunique(prefix, 
                text=text, 
                permissions=0o600)
    except uniquefile.Error as e:
        raise Error(e)

    # link the fd with the lockfile

    try:
        n = os.stat(name).st_nlink

        try:
            os.link(name, lockname)
        except OSError as error:
            # the file may exist, but no one else may be using it
            raise Error('lock file already exists')
        else:
            m = os.stat(name).st_nlink
            if m == n + 1 and m == os.stat(name).st_nlink:
                return fd

        # failed to make lock
        raise IOError(errno.EEXIST, 'Cannot lock file')
    finally:
        os.remove(name)

# ---------------------------------------------------------------------------

def _mklockobj_inner(lockname, mode='w+', bufsize=-1):
    if 'b' in mode:
        text = False
    else:
        text = True
    fd = mklock(lockname, text)

    return lockname, os.fdopen(fd, mode, bufsize)


def mklockobj(*args, **kwds):
    # only grab the actual file
    return _mklockobj_inner(*args, **kwds)[1]
      
# ---------------------------------------------------------------------------

class _LockFileWrapper(uniquefile._UniqueFileWrapper):
    """Returns a file-like object that automatically unlocks the file when
    the file is closed. Note the lockfile's name can be accessed as 
    file.name
    
    Raises IOError if cannot lock file
    """

    def __init__(self, name, file):
        super(_LockFileWrapper, self).__init__(name, file)
        self._called_close = False


    # cache the unlink function because when shutting down, python
    # nulls out os
    _unlink = os.unlink

    def close(self):
        if not self._called_close:
            self._called_close = True
            self._file.close()
            self._unlink(self.name)

    def __del__(self):
        self.close()


def LockFile(*args, **kwds):
    name, file = _mklockobj_inner(*args, **kwds)
    return _LockFileWrapper(name, file)

# ---------------------------------------------------------------------------

class _RLockFileWrapper(_LockFileWrapper):
    """reentrant lock file"""
    
    def __init__(self, name, file):
        super(_RLockFileWrapper, self).__init__(name, file)
        self._mutex = _thread.allocate_lock()
        self._references = 1


    def _add_reference(self):
        self._mutex.acquire()
        try:
            self._references += 1        
        finally:
            self._mutex.release()


    def close(self):
        """ """
        self._mutex.acquire()

        try:
            if not self._called_close:
                self._references -= 1
                
                if self._references == 0:
                    # actually close the file
                    super(_RLockFileWrapper, self).close()
                
                elif self._references < 0:
                    # something bad happened, so raise exception
                    raise FatalError('closed file too many times')
        finally:
            self._mutex.release()

####

_reentrant_lockup = weakref.WeakValueDictionary()
_reentrant_lookup_mutex = _thread.allocate_lock()

def RLockFile(name, mode='w+', bufsize=-1):
    """make a re-entrant lock file that can be shared in the same file"""

    _reentrant_lookup_mutex.acquire()
    try:
        key = (name, mode, bufsize)

        # check if the lock file has been already opened
        try:
            f = _reentrant_lockup[key]
        except KeyError:
            # lock file doesn't exist, so make a new one
            pass
        else:
            # if it already is closed, return a new lock
            if f._called_close:
                del _reentrant_lockup[key]
            else:
                # we found a good version, so just return the one we found
                f._add_reference()
                return f
            
        fobj = mklockobj(name, mode, bufsize)

        # make a new copy 
        f = _RLockFileWrapper(name, fobj)

        # put it in the lookup
        _reentrant_lockup[key] = f

        return f
    finally:
        _reentrant_lookup_mutex.release()
