

import os
import re
import fnmatch
import shutil
import errno
import stat

import rpg
import rpg.uniquefile as uniquefile

__all__ = (
        'relativepath',
        'splitall',
        'splitroot',
        'ifind',
        'ifindup',
        'find',
        'findup',
        'containslink',
        'makedirs',
        'copy',
        'move',
        'FileExists',
        )

# ---------------------------------------------------------------------------

def relativepath(root, path):
    """
    Makes an absolute path relative to the specified root

    >>> relativepath('/a/b/c', '/a/b/c/d')
    'd'
    >>> relativepath('/a/b/c/', '/a/b/c/d')
    'd'
    >>> relativepath('/a/b/c', '/b/c/d')
    '/b/c/d'
            
    """
    if not path.startswith(root):
        return path

    path = path[len(root):]

    if path.startswith(os.sep):
        return path[len(os.sep):]
    return path

# ---------------------------------------------------------------------------

def commonprefix(paths):
    """
    Finds the absolute longest prefix for the given set of paths. Returns
    '' if there is no common prefix.

    >>> commonprefix(['/a/b/c', '/a/d/e'])
    '/a'

    >>> commonprefix(['/a/b1b/c', '/a/b2b/d'])
    '/a'

    >>> commonprefix(['/a/b/c', '/d/e'])
    '/'

    >>> commonprefix(['a/b/c', 'd/e'])
    ''

    """

    # because os.path.commonprefix only deals with a character at a time
    # it can mess up and for the given list ['/a/b1b', '/a/b2b'], it'll
    # return '/a/b', so we need to do it ourselves

    if not paths:
        return ''

    prefix_split = splitall(paths[0])

    for path in paths[1:]:
        path_split = splitall(path)

        for i in range(len(prefix_split)):
            # check if the prefix element is in the path. If not, cut the
            # prefix at this point and continue on to the next path

            if prefix_split[i] != path_split[i]:
                prefix_split = prefix_split[:i]
                break

    if not prefix_split:
        return ''

    return os.path.join(*prefix_split)

# ---------------------------------------------------------------------------

def splitall(path):
    """
    breaks up a path into all of it's components

    >>> splitall('/a/b/c/d')
    ['/', 'a', 'b', 'c', 'd']
    
    >>> splitall('/a/b/c/d/')
    ['/', 'a', 'b', 'c', 'd', '']

    >>> splitall('a/b/c/d')
    ['a', 'b', 'c', 'd']

    >>> os.path.join(*splitall('/a/b/c/d'))
    '/a/b/c/d'

    >>> os.path.join(*splitall('/a/b/c/d/'))
    '/a/b/c/d/'
    
    """

    paths = []

    old_path = path
        
    while True:
        path, filename = os.path.split(path)

        if path == old_path:
            if path:
                paths.append(path)
            break
        else:
            old_path = path
            paths.append(filename)

    # paths are created backwards, so just flip it around
    paths.reverse()
    return paths

# ---------------------------------------------------------------------------

def splitroot(path):
    """
    similar to os.path.split, but instead of splitting into the path and 
    filename, split off the first path element and the rest of the path

    >>> splitroot('/a/b/c/d')
    ('/', 'a/b/c/d')

    >>> splitroot('a/b/c/d')
    ('a', 'b/c/d')

    >>> splitroot('/a')
    ('/', 'a')

    >>> splitroot('a')
    ('', 'a')

    >>> splitroot('/')
    ('/', '')

    >>> os.path.join(*splitroot('/a/b/c/d'))
    '/a/b/c/d'

    """

    paths = splitall(path)
    root = paths[0]
    tail = paths[1:]
    
    if not tail and root != os.sep:
        return ('', root)
    else:
        return (root, tail and os.path.join(*tail) or '')

# ---------------------------------------------------------------------------

def _getExcludes(exclude):
   # check if a list was specified for the excludes. If not, 
    # turn it into one
    if exclude:
        if type(exclude) == type([]):
            return exclude
        else:
            return [exclude]
    else:
        return []


def _matchFiles(files, name, excludes, root, matcher):  
    if name is not None:
        files = [f for f in files if matcher(name, f)]

    # filter out any files we don't want to look at
    for exclude in excludes:
        files = [f for f in files \
                if not matcher(exclude, f)]

    # yield the remaining files
    for f in files:                   
        yield os.path.join(root, f)

########

def ifind(path, 
        name=None, 
        exclude=None, 
        mindepth=None,
        maxdepth=None,
        follow=False,
        include_dirs=True,
        only_dirs=False,
        matcher=lambda pattern, path: fnmatch.fnmatch(path, pattern),
        ):
    """
    Searches down the filesystem to find files. Note if we're matching the 
    file with fnmatch, we match with just the filename, not the file name + 
    path.
    
    @param path: search down from this path
    @param name: return only files matching using L{fnmatch}
    @param exclude: exclude paths matching using L{fnmatch}. Can be a list
    @param mindepth: do not return files less than this level depth
    @param maxdepth: desend at mount maxdepth levels below the specified path
    @param follow: desend into symlink directories
    @param include_dirs: return directories that match the name
    @param only_dirs: return only directories that match the name
    @param matcher: specify a different matching function than fnmatch. This
        function's arguments are (path, pattern)
    """

    excludes = _getExcludes(exclude)
   
    # safety check to allow the exclude to exclude the path
    for exclude in excludes:
        if matcher(exclude, path):
            return

    # adjust the depth. This is useful when we're using absolute paths
    depth = len(splitall(path))
    if mindepth is not None:
        mindepth += depth

    if maxdepth is not None:
        maxdepth += depth

    ####

    realpath = path

    # we break this into a separate function so we can potentially
    # walk down symlink paths
    def searchdownFunction(path):
        # walk the path
        for root, dirs, files in os.walk(path):
            # filter out any child directories if we don't want to walk them
            if excludes:
                # in order to guarentee that os.walk won't walk down
                # into a directory, we have to remove them from the dirs list
                for d in dirs[:]:
                    for exclude in excludes:
                        if fnmatch.fnmatch(d, exclude):
                            dirs.remove(d)

            ####

            # let exclusion occur first in case we don't want to walk down
            # those directories but we've specified a mindepth
            depth = len(splitall(root))

            # if we're at the max depth, stop recursing down branch
            if maxdepth is not None and maxdepth < depth:
                dirs[:] = []

                continue

            ####

            if only_dirs:
                files = dirs
            elif include_dirs:
                # if including directories, add them into the files to match
                files += dirs
            
            if mindepth is None or mindepth <= depth:
                # run the pattern matching
                for f in _matchFiles(files, name, excludes, root, 
                        matcher=matcher):
                    yield f

            ####
          
            # if we're following symlinks, check if each dir is a link
            # if so, call the searchdownFunction on that path
            if follow:
                for d in dirs:
                    fullpath = os.path.join(root, d)
                    if os.path.islink(fullpath):
                        searchdownFunction(fullpath)

    ####

    for item in searchdownFunction(path):
        yield item

########

def ifindup(path,
        name=None,
        exclude=None,
        mindepth=None,
        maxdepth=None,
        include_dirs=True,
        only_dirs=False,
        matcher=lambda pattern, path: fnmatch.fnmatch(path, pattern),
        ):
    """
    Searches up the filesystem to find files. Note if we're matching the file
    with fnmatch, we match with just the filename, not the file name + path.
    Note if path is a relative path, we will stop once we hit the base of the
    relative path.
    
    @param path: search up from this path
    @param name: return only files matching using L{fnmatch}
    @param exclude: exclude paths matching using L{fnmatch}. Can be a list
    @param mindepth: do not return files less than this level depth.
        if mindepth is less than or equal to zero, do nothing
    @param maxdepth: desend at mount maxdepth levels below the specified path.
        if maxdepth is zero, we only consider the current directory
    @param include_dirs: return directories that match the name
    @param only_dirs: return only directories that match the name
    @param matcher: specify a different matching function than fnmatch. This
        function's arguments are (path, pattern)
    """

    excludes = _getExcludes(exclude)

    # safety check to allow the exclude to exclude the path
    for exclude in excludes:
        if matcher(exclude, path):
            return

    ####

    depth = 0
    while True:
        # if we are ignoring certain depths, do that now
        if mindepth is None or mindepth <= depth:
            if only_dirs:
                files = [path]
            else:
                files = os.listdir(path)

            # if not including dirs, filter out directories
            if not include_dirs:
                files = [f for f in files \
                        if not os.path.isdir(os.path.join(path, f))]
            
            for f in _matchFiles(files, name, excludes, path, 
                    matcher=matcher):
                yield f

        # if we've gone deep enough, just exit
        if maxdepth is not None and maxdepth <= depth:
            break

        # break if we've reached the root directory
        # we know this by either the dirname or basename is blank
        path, basename = os.path.split(path)

        if path == '' or basename == '':
            break

        depth += 1

########

def find(*args, **kwds):
    """Non-iterator version of L{ifind}"""

    # just list-ify the ifind function
    return list(ifind(*args, **kwds))


def findup(*args, **kwds):
    """Non-iterator version of L{ifindup}"""
    
    # just list-ify the findupiter function
    return list(ifindup(*args, **kwds))

########

def containslink(path):
    """checks if a symlink is in the path. Returns the link if found, None
    if not"""

    # take shortcut if path is a symlink
    if os.path.islink(path):
        return path

    paths = splitall(path)

    for i in range(len(paths)):
        path = os.path.join(*paths[0:i + 1])
        if os.path.islink(path):
            return path

    return None



class FileExists(rpg.Error):
    pass


def makedirs(dir, mode=0o777):
    """Setup any directories that aren't created.  This doesn't simply
    call os.makedirs because if multiple processes try to create
    similar directory paths an exception might be raised before we
    can create the leaf directory.  For example, if the paths a/b/c and
    a/b/d are created by two separate processes, they both might try to
    create a/b at the same time and one would fail.  To get around this we
    basically have our own version of os.makedirs that catches EEXIST errors
    and ignores them."""

    # don't do anything if the path exists
    if not dir or os.path.exists(dir):
        return

    # split the path into head and tail to see what we need
    head,tail = os.path.split(dir)
    if not tail:
        head,tail = os.path.split(head)
    # recursively go down the path
    if head and tail:
        makedirs(head, mode=mode)
        # xxx/newdir/. exists if xxx/newdir exists
        if tail == os.curdir:
            return

    # create the leaf directory
    try:
        os.mkdir(dir, mode)
    except OSError as err:
        # make sure it didn't get created in the split second
        # after we checked on it.
        if errno.errorcode[err.errno] != "EEXIST":
            raise

def _os_stat(path):
    """Wrapper around os.stat() which catches OSError and returns None if
    'path' doesn't exist."""
    try:
        return os.stat(path)
    except OSError:
        return None

def copy(src, dst, mkdirs=True, overwrite=True, realpath=True):
    """Copy the 'src' to 'dst' and preserve permissions, timestamps, and
    make sure the dst directories are in place.  Also, copy to a
    temporary file created by the rpg.uniquefile module to prevent a partial
    data access.  Once the full file is copied, rename it with os.rename().

    @param mkdirs: effectively run mkdir -p for all directories in 'dst'
                   that aren't created.
    @type mkdirs: boolean

    @param overwrite: if 'dst' already exists, overwrite it
    @type overwrite: boolean

    @param realpath: check if 'src' and 'dst' actually point to the
                     same path by running os.path.realpath() on each path.
                     If you already know the paths are not equal, then
                     setting this to False can speed up copies since every
                     directory in the path won't be stat.
    @type realpath: boolean
    """

    # if 'dst' is a directory, then we will have to make two stats, but
    # if it isn't, then let's be efficient and keep track of whether
    # dst exists or not
    dst_stat = _os_stat(dst)

    # allow dst to be a directory
    if dst_stat and stat.S_ISDIR(dst_stat.st_mode):
        dst = os.path.join(dst, os.path.basename(src))
        # we know we don't need to bother making the directories now
        mkdirs = False
        # run stat on the new file
        dst_stat = _os_stat(dst)

    # check for the same path
    if realpath and \
       os.path.realpath(os.path.expanduser(src)) == \
       os.path.realpath(os.path.expanduser(dst)):
        return

    # check if the path exists
    if not overwrite and dst_stat:
        raise FileExists("%s already exists" % dst)

    # setup any needed directories
    if mkdirs: makedirs(os.path.dirname(dst))

    # stat the source so we can pass the same permissions bits to mkuniqueobj()
    src_stat = os.stat(src)

    # copy the stats by first copying to a tmp file, then do
    # a move.  This way the file won't be accessed with partial data.
    dir,fname = os.path.split(dst)
    # create a temporary file in the form $dst.XXXXX.
    fdst,tmpfile = uniquefile.mkuniqueobj(
        dir=dir, prefix=fname + '.',
        permissions=stat.S_IMODE(src_stat.st_mode),
        filename_generator=uniquefile.random_names)
    try:
        # open the src file
        fsrc = open(src, "rb")
        try:
            # copy the file
            shutil.copyfileobj(fsrc, fdst)
        finally:
            # close the source file
            fsrc.close()
            fdst.close()
            fdst = None

        # set the atime and mtime
        os.utime(tmpfile, (src_stat.st_atime, src_stat.st_mtime))

        # rename the temp to the dst
        os.rename(tmpfile, dst)
        tmpfile = None

    finally:
        if fdst: fdst.close()
        # never leave the tmp file behind
        if tmpfile and os.path.exists(tmpfile):
            os.remove(tmpfile)


def move(src, dst, mkdirs=True, overwrite=True, realpath=True, rename=True):
    """Move 'src' to 'dst' and preserve permissions, timestamps, and make
    sure the dst directories are in place.  If the paths live on the
    same filesystem, then do a move, otherwise we need to copy then
    remove.  We don't use shutil.move() because we want our copies to
    always be done via a tmp file to prevent partial reads.
    
    @param mkdirs: effectively run mkdir -p for all directories in 'dst'
                   that aren't created.
    @type mkdirs: boolean

    @param overwrite: if 'dst' already exists, overwrite it
    @type overwrite: boolean
    
    @param realpath: check if 'src' and 'dst' actually point to the
                     same path by running os.path.realpath() on each path.
                     If you already know the paths are not equal, then
                     setting this to False can speed up moves since every
                     directory in the path won't be stat.
    @type realpath: boolean

    @rename: by default, os.rename() is used to move 'src' to 'dst', and if
             'dst' is on a different device pathutil.copy() will be used,
             followed by os.remove(src).  If you know 'src' and 'dst' aren't
             on the same device, then it is more efficient to set this to
             False to prevent the cross-device check.
    @type rename: boolean
    """

    # call copy right away if rename if False.  This speeds things up when
    # moving to another device.
    if not rename:
        # copy the file
        copy(src, dst, mkdirs=mkdirs, overwrite=overwrite, realpath=realpath)
        # now remove the src
        os.remove(src)
        return

    # if 'dst' is a directory, then we will have to make two stats, but
    # if it isn't, then let's be efficient and keep track of whether
    # dst exists or not
    dst_stat = _os_stat(dst)

    # allow dst to be a directory
    if dst_stat and stat.S_ISDIR(dst_stat.st_mode):
        dst = os.path.join(dst, os.path.basename(src))
        # we know we don't need to bother making the directories now
        mkdirs = False
        # run stat on the new file
        dst_stat = _os_stat(dst)

    # check for the same path
    if realpath and \
       os.path.realpath(os.path.expanduser(src)) == \
       os.path.realpath(os.path.expanduser(dst)):
        return

    # check if the path exists
    if not overwrite and dst_stat:
        raise FileExists("%s already exists" % dst)

    # setup any needed directories
    if mkdirs: makedirs(os.path.dirname(dst))

    # try to do a rename
    try:
        os.rename(src, dst)
    except OSError as err:
        # re-raise if the problem isn't because of a different
        # filesystem.
        if errno.errorcode[err.errno] != "EXDEV":
            raise

        # copy the file
        copy(src, dst, mkdirs=False, realpath=realpath)
        # now remove the src
        os.remove(src)



def _test():
    #makedirs("/tmp/foo/bar")
    copy("rpgcopy.test", "/tmp/foo/bar/foobar")

    #move("rpgcopy.test", "/tmp/foo/bar/rpgcopy.test2", mkdirs=True)


if __name__ == "__main__":
    _test()
    
