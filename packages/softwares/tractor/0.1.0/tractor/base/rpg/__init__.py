# $Id: __init__.py 8205 2005-12-20 19:58:38Z erickt $

"""
Handy methods that don't appear to belong with any other module end up
in here.  This is meant to be the rpg grab bag where useful little
methods are kept.

"""

__version__ = 'PACKAGING_VERSION'

__all__ = (
        'Error',
        'RPGUtilError',
        'OSUtilError',
        'StringUtilError',
        )

# ----------------------------------------------------------------------------

import exceptions, os, site

# put rpg in import path so that rpg modules will be able to import other rpg modules without
# prefixing imports with tractor.base.
site.addsitedir(os.path.split(os.path.dirname(__file__))[0]) # that just adds the parent dir of this file

class Error(exceptions.Exception):
    pass

# legacy alias for backwards compatibility
RPGUtilError = Error

class OSUtilError(Error):
    pass

class StringUtilError(Error):
    pass
