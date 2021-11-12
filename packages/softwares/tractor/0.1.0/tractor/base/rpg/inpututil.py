

import os
import sys
import tempfile

class Error(Exception):
    pass

__all__ = (
        'confirm',
        'edit',
        'fedit',
        'edit_input',
        )
    
# ---------------------------------------------------------------------------

def confirm(msg, 
        responses={('y', 'yes'): True, ('n', 'no'): False}, 
        default=None,
        func=None,
        stdout=sys.stdout,
        stdin=sys.stdin):
    """
    repeatedly ask a message and read input from stdin. This input is checked
    against each of the values in the checks dictionary.

    Input is lowercased and stripped; the keys in C{responses} should take this
    into account.

    >>> confirm("Continue? [y/n] ")
    >>> if confirm("Delete file? (y/n) [n]: ", default="n"): f.delete()
    >>> confirm("(R)eply or (f)orward? [r] ", default="r",
    ...         responses={('r', 'reply'): "reply", ('f', 'forward'): "forward"})

    @param responses: dict where the key is the checked input, and the value
        is what is returned 
    @param default: when not C{None} and C{stdin} returns a blank or empty
        string, this value is used as the input instead
    @param stdout: print msg to stdout. default to sys.stdout
    @param stdin: read from the stdin. default sys.stdin
    @param func: run the function before asking for confirmation. If
        the function returns a non-None value, that value is returned by
        L{confirm} without asking for confirmation
    """
    
    while True:
        if func is not None:
            result = func()

            if result is not None:
                return result

        ####
        
        stdout.write(msg)
        stdout.flush()

        i = stdin.readline().strip().lower()
        if default is not None and not i:
            i = default

        for key, value in responses.items():
            # check if k is a tuple. if so, use "in", else just use "=="
            try:
                if i in key:
                    return value
            except TypeError:
                if i == key:
                    return value

# ---------------------------------------------------------------------------
    
def edit(filename, editor='/usr/bin/vim'):
    """edit a file using the specified editor. This is overridable via the
    environment variable EDITOR"""

    # try to use the EDITOR evironment variable first over the one 
    # passed in
    if 'EDITOR' in os.environ:
        editor = os.environ['EDITOR']

    cmd = '%s %s' % (editor, filename)
    errno = os.system(cmd)

    if errno:
        raise Error(errno, 'error running command: ' + cmd)

# ---------------------------------------------------------------------------
    
def fedit(file, **kwds):
    """edit a file object using the specified editor. This is overridable via
    the environment variable EDITOR. Note in order for this to work, the 
    file object must support read/write, seek, and truncate access"""

    # the basic algorithm for this is to copy the file contents into a 
    # named temporary file and just run L{edit} on it. Once finished, copy
    # that info back into the file

    # make sure we're at the beginning of the file before we read it
    file.seek(0)
    text = file.read()

    f = None
    try:
        f = tempfile.NamedTemporaryFile()
        f.write(text)
        f.seek(0)
        
        edit(filename=f.name, **kwds)

        f.seek(0)
        text = f.read()
    finally:
        f.close()

    # reset the size of the file before we write to it
    file.seek(0)
    file.truncate()
    file.write(text)

# ---------------------------------------------------------------------------
        
def edit_input(msg=None, **kwds):
    """input text modified via an external editor. If msg is specified, that
    text is loaded into the file before it is edited"""

    f = None
    try:
        f = tempfile.TemporaryFile()
        if msg is not None:
            f.write(msg)

        fedit(f, **kwds)
        f.seek(0)

        return f.read()
    finally:
        if f:
            f.close()

# ---------------------------------------------------------------------------

def multiline_input(msg, stdin=sys.stdin, stdout=sys.stdout, prompt="> "):
    """Ask the user for text terminated by either a lone '.' or ^D.

    The text "[terminate with a single '.' or ^D]: " is appended to the message.
    The message is then printed to C{stdout} and reads from C{stdin}. Each line
    is preceded by the prompt string. Returns a stripped string.

    >>> multiline_input("Enter change note", prompt="? ")
    Enter change note [terminate with a single '.' or ^D]
    ? testing
    ? and blah
    ? .
    'testing\nand blah'
    """
    msg = msg + " [terminate with a single '.' or ^D]\n"
    response = []

    stdout.write(msg)
    while True:
        stdout.write(prompt)
        stdout.flush()
        line = stdin.readline()
        if not line: break
        if line == ".\n": break
        response.append(line)

    return "".join(response).strip()
