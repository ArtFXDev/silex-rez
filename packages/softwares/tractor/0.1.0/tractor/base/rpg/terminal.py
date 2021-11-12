import sys
import struct

__all__ = (
        'UnknownColor',
        'StatusBar',
        'TerminalColor',
        'termwidth',
        )

# ----------------------------------------------------------------------------

class UnknownColor(Exception):
    pass

# ----------------------------------------------------------------------------

class StatusBar:
    def __init__(self, items=0, chars=76, noreturn=0):
        """Crude status bar class."""
        # number of total items the status bar will reflect
        self.items = items
        # length of the status bar in characters
        self.chars = chars
        # should we use carriage returns?
        self.noreturn = noreturn

        self.lastticks = 0

        # the item interval for each '#' mark
        self.interval = chars / float(items)

    def begin(self, extra=''):
        """begin the status bar."""

        if self.noreturn:
            print('do something')

        sys.stdout.write(self._getline(0, 0) + extra + '\r')
        sys.stdout.flush()

    def _getline(self, item, ticks):
        """get a status line."""

        perc = int(item / float(self.items) * 100.0)
        return ('#' * ticks) + (' ' * (self.chars - ticks)) + '%3d%%' % perc

    def update(self, item, extra=''):
        """update the status bar to reflect the current state."""

        ticks = int(item*self.interval)
        if ticks > self.lastticks:
            self.lastticks = ticks
            line = self._getline(item, ticks)
            sys.stdout.write(line + extra + '\r')
            sys.stdout.flush()

    def finish(self, extra=''):
        line = self._getline(self.items, self.chars)
        sys.stdout.write(line + extra + '\n')
        sys.stdout.flush()

# ----------------------------------------------------------------------------

class TerminalColor:
    """Simple class to support Terminal text coloring."""
    
    colorcodes = {'bg': {'black'  : 40,
                         'red'    : 41,
                         'green'  : 42,
                         'yellow' : 43,
                         'blue'   : 44,
                         'magenta': 45,
                         'cyan'   : 46,
                         'white'  : 47},
                  
                  'fg': {'black'  : 30,
                         'red'    : 31,
                         'green'  : 32,
                         'yellow' : 33,
                         'blue'   : 34,
                         'magenta': 35,
                         'cyan'   : 36,
                         'white'  : 37}
                  }

    def __init__(self, fg, bg=None):
        """Initialize a color, by default the background is not changed."""

        if fg not in self.colorcodes['fg']:
            raise UnknownColor("'%s' is an unknown foreground color, must " \
                  "be one of %s" % (fg, list(self.colorcodes['fg'].keys())))
        
        self.foreground = fg

        if bg and bg not in self.colorcodes['bg']:
            raise UnknownColor("'%s' is an unknown background color, must " \
                  "be one of %s" % (bg, list(self.colorcodes['bg'].keys())))
        self.background = bg

    def getEscSeq(self):
        """Return a properly formatted escape sequence that can be
        interpretted by a terminal."""

        seq = "\x1b[%.2d" % self.colorcodes['fg'][self.foreground]
        if self.background:
            seq += ";%.2d" % self.colorcodes['bg'][self.background]
        seq += "m"

        return seq

    def reset(self):
        """Reset the color back to default settings."""

        return "\x1b[0m"

    def colorStr(self, text):
        """Color a string of text this color."""

        return self.getEscSeq() + text + self.reset()

    def __eq__(self, color):
        """Tests equality with other TerminalColors."""

        return self.foreground == color.foreground \
                and self.background == color.background

# ----------------------------------------------------------------------------

def _termwidth_unix():
    """Returns the terminal width of the current terminal or -1 if there's no
       tty.  Linux/OSX implementation."""
    import fcntl, termios
    if sys.stdout.isatty():
        n = fcntl.ioctl(0, termios.TIOCGWINSZ, "XXXXXXXX")
        t = struct.unpack("hhhh", n)
        return t[1]
    else:
        return -1

def _termwidth_windows():
    """Returns the terminal width of the current terminal or -1 if there's no
       tty.  Windows implementation."""
    try:
        from ctypes import windll, create_string_buffer
        import struct
        charBuf = create_string_buffer(22)
        errHandle = windll.kernel32.GetStdHandle(-12)
        if windll.kernel32.GetConsoleScreenBufferInfo(errHandle, charbuf):
            (tmpX, tmpY, tmpX, tmpY, tmp, left, top, right, bottom, tmpX, tmpY) = struct.unpack("hhhhHhhhhhh", charBuf.raw)
            width = right - left + 1
            return width
    except:
        # generic exception catching not classy, but probably fine here
        pass
    # default to 80 chars
    return 80

def termwidth():
    """Returns the terminal width of the current terminal or -1 if there's no
       tty."""
    import platform
    this_os = platform.system()
    if this_os == "Windows":
        return _termwidth_windows()
    else:
        return _termwidth_unix()

