#!/usr/bin/env python

#
# Launch tractor-blade - the Tractor Remote Execution Server!
#

## --------------------------------------------------- ##
# python version requirements:
# Python 2.5 moved KeyboardInterrupt and SystemExit exceptions out of
# class Exception, making for saner scripting. Some older version are
# likely to work as well, but ctrl-c won't interrupt the program.
# Python 2.6 adds the 'with' statement, which is useful for dealing with
# locks in threaded apps that throw exceptions. Recent versions also
# provide useful updates to modules like urllib2 and subprocess.
#
import sys

## --------- ##
# do a quick version check before importing version-sensitive modules
if sys.version_info < (2, 6):
    print("Error: tractor-blade requires python 2.6 (or later)\n", file=sys.stderr)
    sys.exit(26)
## --------- ##

from .TrBladeMain import TrBladeMain

if __name__ == "__main__":

    rc = TrBladeMain()

    if 0 != rc:
        sys.exit(rc)

## ------------------------------------------------------------- ##

