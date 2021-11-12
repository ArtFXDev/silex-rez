import traceback
import sys

from rpg.progutil import log

__all__ = (
        'getTraceback',
        'printTraceback',
        )

# ----------------------------------------------------------------------------

def getTraceback(msg=None):
    """Returns a string of the current error stack."""
    
    tb = ''
    if msg:
        tb = msg + '\n'

    tb += ''.join(traceback.format_exception(
        sys.exc_info()[0],
        sys.exc_info()[1],
        sys.exc_info()[2]))
    return tb

# ----------------------------------------------------------------------------

def printTraceback(msg=None):
    """Prints the current error stack to stderr."""
    
    if msg:
        log(msg)
    log("printing traceback")
    traceback.print_exc()
    log("end traceback")
