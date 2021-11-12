import re

__all__ = (
        'ListPackerError',
        'PackError',
        'UnpackError',
        'ListPacker',
        'IntListPacker',
        )

# ----------------------------------------------------------------------------

class ListPackerError(Exception):
    pass
class PackError(ListPackerError):
    pass
class UnpackError(ListPackerError):
    pass

# ----------------------------------------------------------------------------

class ListPacker:
    """
    When iterating through a list identify ranges for compression.  For
    example an integer list [2, 3, 4, 6, 8, 10, 12] could be compressed to a
    string and read '2,3,4+2*4' because there are four numbers at the end of
    the list that are offset by two.  This class is meant to be subclassed
    to handle a custom type.
    
    """

    digit   = r'[\-]?[0-9]\d*'
    # regexp that will be used when unpacking the list
    rangere = None

    def __init__(self, sep=','):
        """Initialize a packer object with the separator character that will
        be used to distinguish each item of the list."""

        # internal variables to help identify ranges
        self.start = None
        self.last  = None
        self.step  = None
        self.num   = 0
        # separator used between each item when packed
        self.sep   = sep

    def pack(self, mylist):
        """Pack a list into a string and identify ranges."""

        mystr = ''
        # make sure we are reset
        self._reset()
        for item in mylist:
            pstr = self._next(item)
            # if something is returned, then append it with the separator,
            # otherwise don't do anything
            if pstr is not None:
                mystr += pstr + self.sep
        # finish off the compression
        pstr = self._finish()
        if pstr is not None:
            mystr += pstr

        return mystr

    def unpack(self, string, pos=0, end=0):
        """Uncompress a packed list."""

        mylist = []
        # the start and end indexes to search in can be provided
        pos    = pos
        # if the end is not provided then use the end of the string
        if not end:
            end = len(string)

        # if the packed string is empty, then return an empty list
        if not string[pos:end]:
            return mylist

        # search for the first match
        ind = string.find(self.sep, pos, end)
        while ind >= 0:
            # check if this item is a range
            if self.rangere:
                match = self.rangere.match(string, pos, ind)
            else:
                match = None
            # unpack accordingly
            if match:
                result = self.unpack_range(match)
                # make sure we got a value back
                if result is None:
                    raise UnpackError("unable to unpack range " + \
                          match.group(0))
                mylist.extend(result)
            else:
                result = self.unpack_item(string[pos:ind])
                # make sure we got a value back
                if result is None:
                    raise UnpackError("unable to unpack item " + \
                          string[pos:ind])
                mylist.append(result)
            # increment the position to start a new search
            pos = ind + 1
            ind = string.find(self.sep, pos, end)

        # handle the last item
        if self.rangere:
            match = self.rangere.match(string, pos, end)
        else:
            match = None
        # unpack accordingly
        if match:
            result = self.unpack_range(match)
            # make sure we got a value back
            if result is None:
                raise UnpackError("unable to unpack range " + \
                      match.group(0))
            mylist.extend(result)
        else:
            result = self.unpack_item(string[pos:end])
            # make sure we got a value back
            if result is None:
                raise UnpackError("unable to unpack item " + \
                      string[pos:end])
            mylist.append(result)

        return mylist

    def get_step(self, a, b):
        """Get the step size difference between item a and b.  The default
        is to assume a and be cannot be compared and return None."""
        return None

    def pack_range(self, start, step, num):
        """Get a range string based on the starting item, the step and number
        of occurrences.  The default is to assume 'start' is an integer and
        return 'start+step*num'"""
        return '%d+%d*%d' % (start, step, num)

    def unpack_range(self, match):
        """When a range regexp match is found this method is called and it
        should return a list of items that the range represents."""
        base = int(match.group(1))
        step = int(match.group(2))
        num  = int(match.group(3))
        return [base + step*i for i in range(num + 1)]

    def pack_item(self, item):
        """Convert a list item to a string.  The default is to cast
        the item with the str function."""
        return str(item)

    def unpack_item(self, item):
        """Unpack an item and return a valid item that can be added to
        the resulting list."""
        return item

    def _reset(self):
        """Internal method to reset all the variables needed to identify
        a range."""
        self.start = None
        self.last  = None
        self.step  = None
        self.num   = 0

    def _next(self, item):
        """When iterating over a list to pack it, each item is sent to
        this method to identify whether or not a range is being started,
        continued, or stopped."""
        
        # the first item
        if self.start is None:
            self.start = item
            self.last  = item
            return None

        # start a new range
        if not self.step:
            self.step = self.get_step(self.start, item)
            # if we can't compare the two, then start over
            if self.step is None:
                retval = self._finish()
                # set the current item as the start of a new range
                self.start = item
                self.last  = item
                return retval
            self.last = item
            self.num  = 1
            return None
        
        # if a range is already started, check if it can be continued
        if self.get_step(self.last, item) == self.step:
            self.num += 1
            self.last = item
            return None
        
        # if we had a range going but now it has stopped, consider it a
        # range if it is larger than 1
        if self.num > 1:
            retval     = self.pack_range(self.start, self.step, self.num)
            # reset the range so the current item is not the beginning
            self.start = item
            self.last  = item
            self.step  = None
            self.num   = 0
            return retval

        # otherwise the previous range we tried to start wasn't long enough
        self.step  = self.get_step(self.last, item)
        self.num   = 1
        # if we can't compare the current item with the last one, then
        # start over.  This means we have to now pack the last item and
        # the one before it.
        if self.step is None:
            retval     = self._finish()
            self.start = item
            self.last  = item
        # if they can be compared, then only print the item before the last
        else:
            retval     = self.pack_item(self.start)
            self.start = self.last
            self.last  = item
        return retval

    def _finish(self):
        """Called when no more items exist in the list, or we need to reset
        the data."""
        
        # handle a range larger than 1
        if self.num > 1:
            retval = self.pack_range(self.start, self.step, self.num)
            self._reset()
            return retval

        # if a range was trying to get started then just return the
        # start and last values
        if self.num:
            retval = self.pack_item(self.start) + self.sep + \
                     self.pack_item(self.last)
            self._reset()
            return retval

        # if finished was called when we had nothing set
        if not self.last:
            return None

        # otherwise assume the last value is all that is needed
        retval = self.pack_item(self.last)
        self._reset()
        return retval

# ----------------------------------------------------------------------------

class IntListPacker(ListPacker):
    """
    Pack a list of integers.
    
    >>> IntListPacker().pack([2, 3, 4, 6, 8, 10, 12])
    '2+1*2,6+2*3'

    >>> IntListPacker().unpack('2+1*2,6+2*3')
    [2, 3, 4, 6, 8, 10, 12]
    
    """

    # regexp that will be used when unpacking the list
    rangere = re.compile(r'(%s)\+(%s)\*(%s)' %
                         (ListPacker.digit,
                          ListPacker.digit,
                          ListPacker.digit))

    def get_step(self, a, b):
        """Return the step between the two values, which is just a simple
        subtraction."""
        return b - a

    def pack_range(self, start, step, num):
        """Get a range string based on the starting item, the step and number
        of occurrences.  The default is to assume 'start' is an integer and
        return 'start+step*num'"""
        return '%d+%d*%d' % (start, step, num)

    def unpack_range(self, match):
        """When a range regexp match is found this method is called and it
        should return a list of items that the range represents."""
        base = int(match.group(1))
        step = int(match.group(2))
        num  = int(match.group(3))
        return [base + step*i for i in range(num + 1)]

    def pack_item(self, item):
        """Convert an integer into a string."""
        return str(item)

    def unpack_item(self, item):
        """Unpack an integer."""
        try:
            val = int(item)
        except (ValueError, TypeError):
            return None
        return val


