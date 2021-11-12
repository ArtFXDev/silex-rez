"""
unitutil is dedicated to unit conversions

"""

import re
import types

import rpg

__all__ = (
        'UnitConversionError',
        'formatSeconds',
        'formatHertz',
        'formatBytes',
        'parseUnits',
        'parseSeconds',
        'parseHertz',
        'parseBytes',
        )

# ----------------------------------------------------------------------------

class UnitConversionError(rpg.Error):
    pass

# ----------------------------------------------------------------------------

def _unitConversion(num, base, precision, maxwidth, zeros,
                    unitdefs, units, bases):
    """Generalized method to allow for easy unit conversion."""

    # this allows strings or decimals to be passed in for numbers
    num = float(num)

    sign = 1
    if num < 0:
        sign = -1
        num = abs(num)

    # determine where to start the conversion, find the conversion factor
    # and scale the number by it
    try:
        num *= unitdefs[bases[base]]
    except KeyError:
        raise UnitConversionError("unknown unit '%s' for converting "
                "bytes" % base)
    
    # convert the number to the most readable unit
    unit = ''
    unit_index = 0
    for i in range(len(unitdefs)):
        # if we have a match then convert it and break out of the loop
        if num >= unitdefs[i]:
            num /= unitdefs[i]
            unit = units[unit_index]
            break
        else:
            unit_index += 1
    else:
        # if it doesn't need converting then set the appropriate unit char
        unit = units[unit_index - 1]

    # adjust number by sign
    num *= sign

    # now format the the resulting string so the maximum width of the
    # string isn't greater than 'maxwidth'.  unless of course the number
    # of whole integers will exceed this

    # the maxwidth variable passed in includes the unit, so if a unit is
    # currently set then decrement the value based on the length
    if unit: maxwidth -= len(unit)
    if maxwidth < 1: maxwidth = 1

    # loop until we get a match
    while precision:
        # format the bytes based on the input parameters and see if it will
        # work
        formatted = '%.*f' % (precision, num)

        # strip trailing zeros if need be
        if not zeros:
            # split it up into the integer and the decimal
            intnum,decnum = formatted.split('.')
            decnum = decnum.rstrip('0')
            if not decnum: formatted = intnum
            else: formatted = intnum + '.' + decnum

        # if the string works then return
        if len(formatted) <= maxwidth:
            return formatted + unit        
        
        # otherwise we have to trim it down so decrement the precision
        precision -= 1
        
    return '%.0f' % num + unit

# ----------------------------------------------------------------------------

# compute these once for faster converting
_seconds_unitdefs = [31449600, 604800, 86400, 3600, 60, 1]
_seconds_units    = ['y', 'w', 'd',  'h', 'm', 's']
# lookup so we know where to start the conversion
_seconds_bases    = {'seconds': 5,
                   'minutes'  : 4,
                   'hours'    : 3,
                   'days'     : 2,
                   'weeks'    : 1,
                   'years'    : 0,
                   }

def formatSeconds(num, base='seconds', precision=6, maxwidth=5, zeros=0):
    """
    Format an integer representing seconds, minutes, or hours into whatever 
    is appropriate for human readability.  (e.g. 60min => 1hr)

    >>> formatSeconds(1234.0)
    '20.6m'
    >>> formatSeconds(1234.0, base='minutes')
    '20.6h'
    >>> formatSeconds(1234.0, base='hours')
    '7.35w'
    >>> formatSeconds(1234.0, base='days')
    '3.39y'
    >>> formatSeconds(1234.0, base='weeks')
    '23.7y'
    >>> formatSeconds(1234.0, base='years')
    '1234y'

    @param base: Set the base to be 'seconds', 'minutes', or 'hours'
                 depending on the current unit of 'num'.
                 
    @param precision: Number of decimal places when converting units
    
    @param maxwidth: Maximum width that the resulting string will be,
                     including the unit.  By default this is set to 6.

    @param zeros: Strip all trailing zeros from the string. i.e. '2.00h'
                  would be returned as '2h' if zeros is false, otherwise,
                  it would be returned as '2.00h'
   
    """

    # maxwidth has to be at least 5
    if maxwidth < 5: maxwidth = 5
    return _unitConversion(num, base, precision, maxwidth, zeros,
                           _seconds_unitdefs, _seconds_units, _seconds_bases)

# compute these once for faster converting
_hertz_unitdefs = [10.0**9, 10.0**6, 10.0**3, 1.0]
_hertz_units    = ['GHz', 'MHz', 'KHz', 'Hz']
# lookup so we know where to start the conversion
_hertz_bases    = {'hertz': 3,
                   'kilo' : 2,
                   'mega' : 1,
                   'giga' : 0}

def formatHertz(num, base='mega', precision=1, maxwidth=6, zeros=0):
    """
    Format an integer representing hertz, megahertz, or gigahertz into
    whatever is appropriate for human readability. (e.g. 1000MHz => 1GHz)

    >>> formatHertz(1234.0)
    '1.2GHz'
    >>> formatHertz(1234.0, base='hertz')
    '1.2KHz'
    >>> formatHertz(1234.0, base='kilo')
    '1.2MHz'
    >>> formatHertz(1234.0, base='mega')
    '1.2GHz'
    >>> formatHertz(1234.0, base='giga')
    '1234GHz'

    @param base: Set the base to be 'hertz', 'mega', or 'giga' depending
                 on the current unit of 'num'.
                 
    @param precision: Number of decimal places when converting units
    
    @param maxwidth: Maximum width that the resulting string will be,
                     including the unit.  By default this is set to 6.

    @param zeros: Strip all trailing zeros from the string. i.e. '2.00GHz'
                  would be returned as '2GHz' if zeros is false, otherwise,
                  it would be returned as '2.00GHz'
   
    """

    # maxwidth has to be at least 6
    if maxwidth < 6: maxwidth = 6
    return _unitConversion(num, base, precision, maxwidth, zeros,
                           _hertz_unitdefs, _hertz_units, _hertz_bases)
    

# compute these once for faster converting
_bytes_unitdefs = [1<<50, 1<<40, 1<<30, 1<<20, 1<<10, 1]
_bytes_unitdefs = [float(val) for val in _bytes_unitdefs]
_bytes_units    = ['P', 'T', 'G', 'M', 'K', 'B']
# lookup so we know where to start the conversion
_bytes_bases    = {'bytes': 5,
                   'kilo' : 4,
                   'mega' : 3,
                   'giga' : 2,
                   'tera' : 1,
                   'peta' : 0}
                   

def formatBytes(num, base='bytes', precision=2, maxwidth=5, zeros=0):
    """
    Format an integer representing bytes, kilobytes, megabytes, 
    gigabytes, terabytes, or petabytes into whatever is appropriate
    for human readability. (e.g. 1024 bytes => 1K, 1073741824 bytes => 1G).

   
    >>> formatBytes(1234.0)
    '1.21K'
    >>> formatBytes(1234.0, base='bytes')
    '1.21K'
    >>> formatBytes(1234.0, base='kilo')
    '1.21M'
    >>> formatBytes(1234.0, base='mega')
    '1.21G'
    >>> formatBytes(1234.0, base='giga')
    '1.21T'
    >>> formatBytes(1234.0, base='tera')
    '1.21P'
    >>> formatBytes(1234.0, base='peta')
    '1234P'
    >>> formatBytes(1000)
    '1000B'
    >>> formatBytes(1000, base='kilo')
    '1000K'
    >>> formatBytes(1000, base='mega')
    '1000M'

    @param base: Set the base to be 'bytes', 'kilo', 'mega', 'giga',
                 'tera', or 'peta' depending on the current unit of 'num'.
                 
    @param precision: Number of decimal places when converting units
    
    @param maxwidth: Maximum width that the resulting string will be,
                     including the unit.  By default this is set to 5,
                     and the value cannot be less than 5, otherwise
                     1000K would not be able to be displayed properly.

    @param zeros: Strip all trailing zeros from the string. i.e. '2.00G'
                  would be returned as '2G' if zeros is false, otherwise,
                  it would be returned as '2.00G'

    """

    # maxwidth has to be at least 5, otherwise we wouldn't be able to
    # display 1000M
    if maxwidth < 5: maxwidth = 5
    return _unitConversion(num, base, precision, maxwidth, zeros,
                           _bytes_unitdefs, _bytes_units, _bytes_bases)

# ---------------------------------------------------------------------------

_parser_unitsre = re.compile(r"\d+(\.\d*)?")
def parseUnits(value, factors):
    """generalized function to extract unit values from a string"""
    
    # search for the beginning of the unit
    match = _parser_unitsre.search(value)
    if match:
        # get the starting point of the unit
        end  = match.end()
        # cast the provided value to a float since it can be an int or
        # float
        val  = value[:end]
        # grab the unit
        unit = value[end:].lower()
    else:
        val  = value
        unit = ''

    # convert the value to a float
    try:
        val = float(val)
    except ValueError:
        raise UnitConversionError("format of string '%s' is invalid" % value)

    try:
        factor = factors[unit]
    except KeyError:
        raise UnitConversionError("unknown unit '%s' found in '%s'" %
                (unit, value))

    return val * factor
    

# dict to associate unit with scaling
_parse_second_factors = { \
        'w': 604800, # one week 
        'd': 86400, # one day
        'h': 3600, # one hour
        'm': 60, # one minute
        's': 1, # one second
        '': 1, # default is one second
        }

def parseSeconds(value):
    """
    Convert the input value into total seconds. The input can contain operators 
    for minutes, hours, days, or weeks

    >>> parseSeconds('123.4') == 123.4
    True
    >>> parseSeconds('123.4s') == 123.4
    True
    >>> parseSeconds('33m') == 33 * 60
    True
    >>> parseSeconds('12h') == 12 * 60 * 60
    True
    >>> parseSeconds('3d') == 3 * 60 * 60 * 24
    True
    >>> parseSeconds('4w') == 4 * 60 * 60 * 24 * 7
    True
  
    @param value: string of a range of times
    @type value: string

    @returns: string

    """
    return parseUnits(value, _parse_second_factors)


# dict to associate unit with scaling
_parse_hertz_factors = { \
        'ghz': 10**9, # one gigahertz
        'mhz': 10**6, # one megahertz
        'khz': 10**3, # one kilohertz
        'hz': 1, # one hertz
        '':  1,  # default is one hertz
        }

def parseHertz(value):
    """
    Convert the input value into total seconds. The input can contain operators 
    for minutes, hours, days, or weeks

    >>> parseHertz('123') == 123
    True
    >>> parseHertz('123Hz') == 123
    True
    >>> parseHertz('33KHz') == 33 * 10**3
    True
    >>> parseHertz('12MHz') == 12 * 10**6
    True
    >>> parseHertz('3GHz') == 3 * 10**9
    True
  
    @param value: string of a range of times
    @type value: string

    @returns: string

    """
    return parseUnits(value, _parse_hertz_factors)


# dict to associate unit with scaling
_parse_byte_factors = { \
        'p': 1<<50, # one petabyte
        't': 1<<40, # one terabyte
        'g': 1<<30, # one gigabyte
        'm': 1<<20, # one megabyte
        'k': 1<<10, # one kilobyte
        'b': 1,      # one byte
        '':  1,      # default is one byte
        }

def parseBytes(value):
    """
    Convert the input value into total seconds. The input can contain operators 
    for minutes, hours, days, or weeks

    >>> parseBytes('123') == 123
    True
    >>> parseBytes('123B') == 123
    True
    >>> parseBytes('123K') == 123 * 1 << 10L
    True
    >>> parseBytes('33M') == 33 * 1<<20L
    True
    >>> parseBytes('12G') == 12 * 1<<30L
    True
    >>> parseBytes('3T') == 3 * 1<<40L
    True
    >>> parseBytes('4P') == 4 * 1<<50L
    True
  
    @param value: string of a range of times
    @type value: string

    @returns: string

    """
    return parseUnits(value, _parse_byte_factors)

if __name__ == '__main__':
    print(formatSeconds(1234.0))
