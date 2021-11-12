import platform
import sys
import time

__all__ = (
    'getlocalhost',
    'log',
    'logWarning',
    'logError'
    )

_localhost = None
def getlocalhost():
    global _localhost
    if _localhost is None:
        _localhost = platform.node().split('.')[0]
    return _localhost

class TerminalColor:
    """Simple class to support Terminal text coloring."""
    colorcodes = {
        'bg': {
            'black'  : 40,
            'red'    : 41,
            'green'  : 42,
            'yellow' : 43,
            'blue'   : 44,
            'magenta': 45,
            'cyan'   : 46,
            'white'  : 47},
        'fg': {
            'black'  : 30,
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
