"""
A generalized way to neatly format members from an object.  By itself,
the L{Formatter} class can be used to format an object, but it is best if
subclassed to create formatters specific to your class or application.

Creating Custom Formatters
==========================
  Suppose your application has a File object defined as::

    >>> class File(object):
    ...    # initialize each object with the filename
    ...    def __init__(self, name, modified, size, user):
    ...        self.name = name
    ...        self.modified = modified
    ...        self.size = size
    ...        self.user = user

  We want to define a formatter that can be used to easily print instances of
  File objects.  To do this we can subclass from L{Formatter} and create a
  member name to L{MemberFormat} mapping.  This way C{owner} will always be
  formatted with L{StringFormat}, C{size} will always be formatted with
  L{BytesFormat} so it is human readable, etc.  For example::

    >>> class FileFormatter(Formatter):
    ...    # create our mapping that this formatter will always use
    ...    memberToFormat = {'name'    : StringFormat,
    ...                      'accessed': TimeFormat,
    ...                      'modified': TimeFormat,
    ...                      'size'    : BytesFormat,
    ...                      'user'    : StringFormat,
    ...                      'perms'   : IntegerFormat}
    ...
    ...    def __init__(self, mformats):
    ...        # pass our mapping to the base class
    ...        Formatter.__init__(self, mformats,
    ...                           memberToFormat=self.memberToFormat)


Using Formatter Objects
=======================
  We can use the formatter we created in the above example to format one or
  more instances of File objects.  If we want to print the C{user}, C{size},
  C{modified}, and C{name} members of each object, then we could create a
  C{FileFormatter} with::

    >>> fileForm = FileFormatter(['user', 'size', 'modified', 'name'])

  Then we could print the objects with the formatter:

    >>> files = [
    ...   File('/blah/blee/bluu/fee', 123123, 1024, 'jag'),
    ...   File('/blah/blee/bluu/fie', 423423, 2048, 'cwalker'),
    ...   File('/blah/blee/bluu/foe', 456455, 2048, 'adamwg'),
    ...   File('/blah/blee/bluu/fum', 912332, 4096, 'jenbecker')]
    ...
    >>> for file in files:
    ...   print fileForm.format(file)
    jag 1K 01/02|02:12 /blah/blee/bluu/fee
    cwalker 2K 01/05|13:37 /blah/blee/bluu/fie
    adamwg 2K 01/05|22:47 /blah/blee/bluu/foe
    jenbecker 4K 01/11|05:25 /blah/blee/bluu/fum

  However, this is not very easy to read because the columns are not aligned.
  The C{setWidths} method can be called to set the column widths based on
  the objects that are to be printed.  Moreover, the C{formatList} does this
  for you and returns a formatted list of objects::


  will both produce::
    >>> print fileForm.formatList(files)
    user       size modified    name               
    ========= ===== =========== ===================
    jag          1K 01/02|02:12 /blah/blee/bluu/fee
    cwalker      2K 01/05|13:37 /blah/blee/bluu/fie
    adamwg       2K 01/05|22:47 /blah/blee/bluu/foe
    jenbecker    4K 01/11|05:25 /blah/blee/bluu/fum

Altering Format Attributes
==========================
  The above example is very basic and does not give the user much control
  over the output.  Suppose you wanted to restrict the file C{name} member
  to no more than 10 characters, or you wanted the C{user} member right
  justified.  The L{Formatter} class recognizes a number of operators
  for altering attributes of each member format::

    =      absolute column width
    <      maximum width a column can have
    >      minimum width a column can have
    $      justification of the member (left or right)
    @      truncate (clamp) method to use (left, center, or right)
    :      foreground color
    ;      background color

  These operators are intended to be used when a L{Formatter} object is
  created.  For example, to create another C{FileFormatter} that would
  restrict the C{name} member to no more than 10 characters:

    >>> fileForm = FileFormatter(['user', 'size', 'modified', 'name<10'])
    >>> print fileForm.formatList(files)
    user       size modified    name      
    ========= ===== =========== ==========
    jag          1K 01/02|02:12 /bla...fee
    cwalker      2K 01/05|13:37 /bla...fie
    adamwg       2K 01/05|22:47 /bla...foe
    jenbecker    4K 01/11|05:25 /bla...fum

  or restrict the C{user} member to always be 15 characters and right
  justified:

    >>> fileForm = FileFormatter(
    ...       ['user=15$right', 'size', 'modified', 'name<10'])
    >>> print fileForm.formatList(files)
               user  size modified    name      
    =============== ===== =========== ==========
                jag    1K 01/02|02:12 /bla...fee
            cwalker    2K 01/05|13:37 /bla...fie
             adamwg    2K 01/05|22:47 /bla...foe
          jenbecker    4K 01/11|05:25 /bla...fum

  The same behavior can be accomplished by explicitly creating the
  L{MemberFormat} object that will be used for a given member.  The following
  is identical to the above:

    >>> fileForm = FileFormatter([
    ...        StringFormat('user', width=15, justify='right'),
    ...        'size', 'modified', StringFormat('name', maxWidth=10)])
    >>> print fileForm.formatList(files)
               user  size modified    name      
    =============== ===== =========== ==========
                jag    1K 01/02|02:12 /bla...fee
            cwalker    2K 01/05|13:37 /bla...fie
             adamwg    2K 01/05|22:47 /bla...foe
          jenbecker    4K 01/11|05:25 /bla...fum

  Additionally, if the format used to print C{modified} is not preferred,
  then an alternate could be provided:

    >>> fileForm = FileFormatter(['user', 'size', 
    ...        TimeFormat('modified', timefmt="%c"), 'name'])
    >>> print fileForm.formatList(files)
    user       size modified                 name               
    ========= ===== ======================== ===================
    jag          1K Fri Jan  2 02:12:03 1970 /blah/blee/bluu/fee
    cwalker      2K Mon Jan  5 13:37:03 1970 /blah/blee/bluu/fie
    adamwg       2K Mon Jan  5 22:47:35 1970 /blah/blee/bluu/foe
    jenbecker    4K Sun Jan 11 05:25:32 1970 /blah/blee/bluu/fum

"""

import re, types, textwrap
import dateutil.parser

import rpg
import rpg.stringutil as stringutil
import rpg.timeutil as timeutil
import rpg.unitutil as unitutil
import rpg.terminal as terminal

__all__ = ('FormatError',
           'MemberFormatError',
           'FormatterError',
           'MemberFormat',
           'StringFormat',
           'NumberFormat',
           'IntegerFormat',
           'FloatFormat',
           'FloatRateFormat',
           'ListFormat',
           'StringListFormat',
           'DictFormat',
           'TimeFormat',
           'ElapsedSecsFormat',
           'BytesFormat',
           'KiloBytesFormat',
           'MegaBytesFormat',
           'GigaBytesFormat',
           'BytesRateFormat',
           'KiloBytesRateFormat',
           'MegaBytesRateFormat',
           'GigaBytesRateFormat',
           'PercentFormat',
           'Formatter',
           'InstanceDoc',
           'InstanceDocFormatter')

# ---------------------------------------------------------------------------

class FormatError(rpg.Error):
    """Base error type for all formatting related errors."""
    pass

class MemberFormatError(FormatError):
    """An error related to a MemberFormat object."""
    pass

class FormatterError(FormatError):
    """An error related to a Formatter object."""
    pass


class MemberFormat(object):
    """Base class for defining custom formatting classes based on a class
    member's type."""

    justifyValues  = ('left', 'right')
    truncateValues = ('left', 'center', 'right', 'wrap')

    def __init__(self, member, width=None, minWidth=None, maxWidth=None,
                 color=None, justify='left', truncate='center',
                 header=None, nocolor=False):
        """Initialize a member format with the name of the member that
        should be formatted.

        @param member: name of member that will be formatted
        @type member: string

        @param width: desired width of the formatted member value
        @type width: int

        @param minWidth: the minimum width the member can be formatted with,
          defaults to the member name length.
        @type minWidth: int

        @param maxWidth: the maximum width the member can be formatted with.
          If this is smaller than minWidth, then minWidth is changed to this
          value.
        @type maxWidth: int

        @param color: color that will be used to color the formatted value
        @type color: L{TerminalColor} instance

        @param justify: specify whether the member should be left or right
          justified.  Possible values are 'left' or 'right'.
        @type justify: string

        @param truncate: specify how the member should be truncated if its
          value exceeds the desired format width.  Possible values are
          'left', 'right', or 'center'.  'center' will effectively squeeze
          a string and put '...' in the middle.
        @type truncate: string

        @param header: name to use when a header is generated for this
          format.  By default, the member name is used.
        @type header: string
        """

        # these values are accessed through properties
        self.__fstring  = None
        self.__width    = None
        self.__hformat  = None
        self.__minWidth = None
        self.__maxWidth = None
        
        # set the member name
        self.member = member

        # is there an alternate header name?
        if header:
            self.headerStr = header
        else:
            self.headerStr = member

        self.minWidth = minWidth
        self.maxWidth = maxWidth

        # set the width now that we have set the min and max
        self.width = width
        self.color = color
        self.nocolor = nocolor
        
        # make sure justify is valid
        if justify not in self.justifyValues:
            raise MemberFormatError("'justify' must be set to one of the " \
                  "following: %s" % self.justifyValues)
        self.justify  = justify

        # make sure truncate is valid
        if truncate not in self.truncateValues:
            raise MemberFormatError("'truncate' must be set to one of " \
                    "the following: %s" % self.truncateValues)
        self.truncate = truncate


    def _setMinWidth(self, minWidth):
        """When the minWidth is set, make sure width isn't less than it."""
        # if no minimum width is provided, then use the length of the
        # member name
        if minWidth is None:
            self.__minWidth = len(self.headerStr)
        else:
            self.__minWidth = minWidth

        # make sure maxWidth is >= to us
        if self.maxWidth is not None and self.maxWidth < self.__minWidth:
            self.maxWidth = self.__minWidth

        # make sure the width isn't too small now
        if self.width and self.width < self.__minWidth:
            self.width = self.__minWidth

    def _getMinWidth(self):
        """Return the current value of the minimum width."""
        return self.__minWidth
    # make this a property so we can ensure that width is never too small
    minWidth = property(fget=_getMinWidth, fset=_setMinWidth)


    def _setMaxWidth(self, maxWidth):
        """When the maxWidth is set, make sure width isn't too large."""
        # it's okay if no maximum width is set, but make sure it isn't
        # less than our min.
        self.__maxWidth = maxWidth
        if maxWidth is not None:
            # make sure minWidth isn't too large now
            if maxWidth < self.minWidth:
                self.minWidth = maxWidth

            # make sure the width isn't too large now
            if self.width and self.width > maxWidth:
                self.width = maxWidth

    def _getMaxWidth(self):
        """Return the current value of the maximum width."""
        return self.__maxWidth
    # make this a property so we can ensure width isn't too large
    maxWidth = property(fget=_getMaxWidth, fset=_setMaxWidth)

            
    def _setWidth(self, width):
        """When the width is set we need to reset the format string."""
        #print self.member, width
        # make sure width is not None
        if width is not None:
            # ensure it is an int
            width = int(width)
            # make sure the width isn't less than our min
            if self.minWidth is not None and width < self.minWidth:
                width = self.minWidth
            # it can't be larger than our max either
            elif self.maxWidth is not None and width > self.maxWidth:
                width = self.maxWidth
        # set the value for real now
        self.__width = width
        # reset the format string
        self.__fstring = None
        # reset the header format
        self.__hformat = None

    def _getWidth(self):
        """Return the width."""
        return self.__width
    # we make a property for width so we can reset the format string if
    # the width is changed.
    width = property(fget=_getWidth, fset=_setWidth)


    def __get_fstring(self):
        """A wrapper around getFormatString() so 'fstring' can be set as
        a property, and subclasses can still overload getFormatString()."""
        if self.__fstring is None:
            self.__fstring = self.getFormatString()
        return self.__fstring
    fstring = property(fget=__get_fstring)


    def getFormatString(self, width=None):
        """Get the format string that will be used to format the values
        for a given member.  Subclasses should overload this depending
        on their type, e.g. integer types need to overload this to ensure
        an integer is properly formatted.

        @param width: optionally override self.width, useful if a subclass
                    is adding text to the final format string and the
                    overall width is not to exceed self.width.
        @type width: int

        @return: a valid format string that would normally be used with
          the '%' operator in python (ala printf)
        @rtype: string
        """

        # figure out what to do with justification
        if self.justify == 'left':
            justify = '-'
        else:
            justify = ''

        # check for an override
        if width is None:
            width = self.width

        # if a width is set, then we 
        if width:
            return '%' + justify + str(width) + 's'
        return '%' + justify + 's'


    def truncateStr(self, val):
        """Helper function to truncate a string based on the format's
        current setting.  The string is only truncated if it is longer
        than the set width.

        @param val: string to be truncated
        @type val: string

        @return: truncated string
        @rtype: string
        """

        # make sure we have a string
        if val is None: val = ''

        # check if we need to truncate
        if self.width and len(val) > self.width:
            if self.truncate == "center":
                val = stringutil.squeezeString(val, self.width)
            elif self.truncate == "right":
                val = val[:self.width]
            elif self.truncate == "wrap":
                # format each line so it is the proper width
                val = [self.fstring % line for line in
                       textwrap.wrap(val, width=self.width)]
                if len(val) == 1:
                    val = val[0]
            else:
                val = val[-self.width:]

        return val


    def getValue(self, obj):
        """Get the value from the object that will be formatted via the
        format string.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: returns a value that can be accepted by a format string
        @rtype: varying
        """
        # the default behavior is straightforward
        return getattr(obj, self.member)


    def formatValue(self, obj):
        """Format the value of our member from the provided object.  This
        only applies the format string to the value, it does not truncate
        or color the string.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: formatted value of member in object
        @rtype: string
        """

        # get the value from the object
        val = self.getValue(obj)
        # check for None
        if val is None:
            # fill the blank value in with spaces to maintain neatly aligned
            # columns
            if self.width:
                return self.width * ' '
            val = ''
        # apply the format and return the result
        return self.fstring % val


    def getColor(self, obj):
        """Color is accessed through a function to give subclasses the option
        of coloring a member based on its value.  By default, self.color
        is returned.

        @param obj: instance that contains the member to be colored
        @type obj: instance

        @return: the desired color to give the member
        @rtype: L{TerminalColor} instance
        """
        return self.color


    def format(self, obj):
        """Format the member from the provided object as a string.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: formatted member value
        @rtype: string
        """

        # format the value of the member
        fval  = self.formatValue(obj)
        # truncate the string
        mystr = self.truncateStr(fval)
        # check if we should color the value.
        if not self.nocolor:
            color = self.getColor(obj)
            # color the value if something is returned
            if color:
                return color.colorStr(mystr)
        return mystr


    def _getHFormat(self):
        """Get the header format object."""

        # create a format for the header
        if not self.__hformat:
            # create the format with all the same attributes of ourself.  
            # This is hacky, but notice the 'header' keyword is used.  This 
            # is done so that the width isn't modified incase 'headerStr' is
            # wider than self.headerStr
            self.__hformat = MemberFormat('headerStr', width=self.width,
                                          justify=self.justify,
                                          truncate=self.truncate,
                                          color=self.color,
                                          header=self.headerStr,
                                          nocolor=self.nocolor)
        return self.__hformat
    _hformat = property(fget=_getHFormat)

    def header(self):
        """
        @return: a formatted string that can be used as the header for
          this member.
        @rtype: string
        """
        return self._hformat.format(self)


    def divider(self, char='='):
        """
        @param char: character that will be used as the divider.
        @type char: string

        @return: divider string that is self.width characters wide
        @rtype: string
        """
        # make sure a width is set
        if self.width:
            div = self.width * char
        # otherwise use the header string length
        else:
            div = len(self.headerStr) * char

        # color if needed
        if self._hformat.color and not self.nocolor:
            div = self._hformat.color.colorStr(div)
        return div


    def setWidth(self, objs, force=False):
        """Set the width for this format based on a list of objects.  This
        is useful for aligning members neatly when an object is being
        formatted.

        @param objs: list of objects that will be formatted.
        @type objs: list

        @param force: Set this to True to force the width (if it is already
          set) to be set based on the objects, default is False.
        @type force: boolean
        """

        # don't do anything if the width is already set
        if self.width is not None and not force:
            return

        # reset the width so each object is formatted at its true width
        self.width = None
        # keep track of the maximum width
        width = 0
        for obj in objs:
            fval = self.formatValue(obj)
            if len(fval) > width:
                width = len(fval)
        # set the new width
        self.width = width
                

class StringFormat(MemberFormat):
    """Format string types."""
    pass


class NumberFormat(MemberFormat):
    """Base class for all number types."""

    def __init__(self, member, zeros=True, justify='right', **kwargs):
        """By default, numbers are right justified.

        @param zeros: if True, then all values will be displayed.  If
          False, then zero values will be left blank.
        """
        self.zeros = zeros
        super(NumberFormat, self).__init__(member, justify=justify, **kwargs)

    def formatValue(self, obj):
        """Format the value of our member from the provided object.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: formatted value of member in object
        @rtype: string
        """
        # get the value from the object
        val = self.getValue(obj)

        # check for None
        if val is None or (val == 0 and not self.zeros):
            # we don't want to set it to zero, because we want the result
            # to be blank.
            if self.width:
                return self.width * ' '
            return ''
        return self.fstring % val


class IntegerFormat(NumberFormat):
    """Format integer types."""

    def getFormatString(self, width=None):
        """The format string for integers is overloaded because we need to
        build it with a %d."""

        # figure out what to do with justification.
        if self.justify == 'left':
            justify = '-'
        else:
            justify = ''

        if width is None:
            width = self.width

        # if a width is set, then we
        if width:
            return '%' + justify + str(width) + 'd'
        return '%' + justify + 'd'


class FloatFormat(NumberFormat):
    """Format float types."""

    # regexp that will be used to get the desired precision from the width
    _pre = re.compile(r"^\d+(?:\.(\d+))?$")

    def __init__(self, member, precision=1, **kwargs):
        """A precision can be provided, but the default is to extract it
        from the value of 'width' if it is a a float.

        @param percision: Specify degree of precision,
                          None means to have it extracted from 'width.'
                          Setting width to a float will override this value
                          and the decimal value of the width will be used.
                          Setting width to None will also override this value.
        """
        self.precision = precision
        super(FloatFormat, self).__init__(member, **kwargs)

    def _getPrecision(self, width):
        """Strip the decimal points from the passed in value so we know
        how many digits of precision are desired."""
        if width is None:
            return None
        
        # get the precision
        match = self._pre.match(str(width))
        if match:
            pre = match.group(1)
            # if we have a match, then cast it to an int
            if pre:
                return int(pre)

        return None

    def _setWidth(self, width):
        """Overloaded so we can keep track of how many decimal places
        to put into the format string."""
        if width is not None:
            if self.precision is None or isinstance(width, float):
                self.precision = self._getPrecision(width)
            width = int(width)
        super(FloatFormat, self)._setWidth(width)
    # make a new property so we can call our own _setWidth method first
    width = property(fget=NumberFormat._getWidth, fset=_setWidth)

    def getFormatString(self, width=None):
        """The format string for floats is overloaded because we need to
        build it with a %f."""

        # figure out what to do with justification.
        if self.justify == 'left':
            justify = '-'
        else:
            justify = ''

        # figure out the precisison
        if self.precision is None:
            prec = ""
        else:
            prec = ".%d" % self.precision

        # if a width is set, then use it
        if width is None:
            width = self.width
        if width:
            return '%' + justify + str(width) + prec + 'f'
        return '%' + justify + prec + 'f'

    def getValue(self, obj):
        """Overloaded to convert strings to floats."""
        val = getattr(obj, self.member)
        if isinstance(val, str):
            try:
                return float(val)
            except ValueError:
                pass
        return val


class PercentFormat(FloatFormat):
    """Format used for displaying float fields that represent a percentage."""

    def getFormatString(self):
        """Overloaded to append a % character."""
        # our super will create a %f format string, but it needs to be one
        # character less than the total width to accommodate for the % char.
        if self.width > 0:
            width = self.width - 1
        else:
            width = self.width
        return super(PercentFormat, self).getFormatString(width=width) + "%%"


class FloatRateFormat(FloatFormat):
    """Format a float value that represents some per second rate."""

    def getFormatString(self):
        """Overloaded to append a '/s' string."""
        # our super will create a %f format string, but it needs to be two
        # characters less than the total width to accommodate for /s.
        if self.width > 1:
            width = self.width - 2
        else:
            width = self.width
        return super(FloatRateFormat, self).getFormatString(width=width) + "/s"


class ListFormat(MemberFormat):
    """Base class for all list types.  All items are converted to strings
    before they are joined."""

    def __init__(self, member, separator=' ', **kwargs):
        """
        @param member: name of member that is to be formatted.
        @type member: string

        @param separator: string that will be used to separate each list item
          when the list is joined, by default a single space is used.
        @type separator: string
        """
        self.separator = separator
        super(ListFormat, self).__init__(member, **kwargs)

    def getValue(self, obj):
        """Joins the items of the list into a string using the specified
        separator.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: joined items
        @rtype: string
        """
        # get the value from the object
        val = getattr(obj, self.member)
        # check for None
        if val is None:
            val = ''
        else:
            # join the items into one string and convert each item to a string
            val = self.separator.join([str(item) for item in val])
        return val


class StringListFormat(ListFormat):
    """Format members that are lists of strings."""

    def getValue(self, obj):
        """Joins the items of the list into a string using the specified
        separator.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: joined items
        @rtype: string
        """
        # get the value from the object
        val = getattr(obj, self.member)
        # check for None
        if val is None:
            val = ''
        else:
            # don't use str() here to allow for unicode
            val = self.separator.join(val)
        return val


class DictFormat(MemberFormat):
    """Format members that are dictionaries."""

    def __init__(self, member, separator=' ', keyval=':', **kwargs):
        """
        @param member: name of member that is to be formatted.
        @type member: string

        @param separator: string that will be used to separate each key, 
          value pair when the items are joined, by default a single space 
          is used.
        @type separator: string

        @param keyval: string that will be used to separate each key and
          value, by default a ':' is used.
        @type keyval: string
        """
        self.separator = separator
        self.keyval = keyval
        super(DictFormat, self).__init__(member, **kwargs)

    def getValue(self, obj):
        """Format the value of our member from the provided object.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: joined key:val pairs of dictionary
        @rtype: string
        """
        # get the value from the object
        val = getattr(obj, self.member)
        if val is None:
            val = ''
        else:
            # get a list of the items to join
            pairs = []
            for key,val in val.items():
                pairs.append(self.keyval.join((str(key), str(val))))
            # sort everything
            pairs.sort()
            # join the pairs
            val = self.separator.join(pairs)

        return val


class TimeFormat(MemberFormat):
    """Format members that represent seconds since the epoch into a human
    readable form."""

    def __init__(self, member, timefmt=None, **kwargs):
        """Time is formatted according to the 'timefmt' parameter.

        @param member: name of member to format
        @type member: string

        @param timefmt: format string that will be used when the time is
          formatted with L{rpg.timeutil.formatTime}.
        @type timefmt: string
        """
        # init the rest of the object, but force the width and minWidth
        super(TimeFormat, self).__init__(member, **kwargs)
        # save the time format
        self.timefmt = timefmt
        # adjust the min and max widths
        min = len(timeutil.formatTime(0, fmt=timefmt))
        if self.minWidth <= min:
            self.minWidth = self.maxWidth = min

    def getValue(self, obj):
        """Format the value of our member from the provided object.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: formatted value of member in object
        @rtype: string
        """
        # get the value from the object
        val = getattr(obj, self.member)
        if val is None:
            return val
        elif val == 0:
            val = "unknown"
        else:
            # format the time
            val = timeutil.formatTime(val, fmt=self.timefmt)

        return val


    def setWidth(self, objs, force=False):
        """Set the width for this format based on a list of objects.  This
        is useful for aligning members neatly when an object is being
        formatted.

        @param objs: list of objects that will be formatted.
        @type objs: list
        """
        # don't reset unless we are instructed to
        if self.width is not None and not force:
            return
        # we always set this based on the timefmt
        self.width = len(timeutil.formatTime(0, fmt=self.timefmt))


class StringTimeFormat(TimeFormat):
    """Format dates encoded as strings as TimeFormat, but
    convert string to epoch seconds first."""

    def getValue(self, obj):
        """Format the value of our member from the provided object.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: formatted value of member in object
        @rtype: string
        """
        # get the value from the object
        val = getattr(obj, self.member)
        if val is None:
            return val
        # convert postgres "timestamp with UTC zone" value to seconds from epoch
        val = timeutil.date2secs(dateutil.parser.parse(val))
        # cast value to float and convert to rpg-style time format
        val = timeutil.formatTime(float(val), fmt=self.timefmt)
        return val


class DatetimeFormat(TimeFormat):
    """Format datetime objects, similarly as TimeFormat, but
    convert datetime to epoch seconds first."""

    def getValue(self, obj):
        """Format the value of our member from the provided object.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: formatted value of member in object
        @rtype: string
        """
        # get the value from the object
        val = getattr(obj, self.member)
        if val is None:
            return val
        val = timeutil.date2secs(val)
        if val == 0:
            val = "unknown"
        else:
            # format the time
            val = timeutil.formatTime(val, fmt=self.timefmt)

        return val


class DatetimeListFormat(MemberFormat):
    """
    @param member: name of member that is to be formatted.
    @type member: string
    
    @param separator: string that will be used to separate each list item
    when the list is joined, by default a single space is used.
    @type separator: string
    
    @param timefmt: format string that will be used when the time is
    formatted with L{rpg.timeutil.formatTime}.
    @type timefmt: string
    """

    def __init__(self, member, separator=' ', timefmt=None, **kwargs):
        self.separator = separator
        self.timefmt = timefmt
        super(DatetimeListFormat, self).__init__(member, **kwargs)

    def getValue(self, obj):
        """Format the value of our member from the provided object.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: formatted value of member in object
        @rtype: string
        """
        # get the value from the object
        timestamps = getattr(obj, self.member)
        # check for None
        if timestamps is None:
            s = ''
        else:
            # convert each timestamp to a string
            parts = []
            for timestamp in timestamps:
                if not timestamp:
                    parts.append("unknown")
                else:
                    parts.append(timeutil.formatTime(timeutil.date2secs(timestamp), fmt=self.timefmt))
            s = self.separator.join(parts)
        return s

        # get the value from the object
        val = getattr(obj, self.member)
        if val is None:
            return val
        val = timeutil.date2secs(val)
        if val == 0:
            val = "unknown"
        else:
            # format the time
            val = timeutil.formatTime(val, fmt=self.timefmt)

        return val


class ElapsedSecsFormat(MemberFormat):
    """Format members that represent total elapsed seconds into a human
    readable form."""

    _DEFAULT_WIDTH = 10

    def __init__(self, member, zeros=False, width=None, minWidth=None,
                 maxWidth=None, justify='right', **kwargs):
        """Elapsed seconds are formatted in the form HH:MM:SS.

        @param member: name of member to format
        @type member: string

        @param zeros: by default, leading zeros are not added to the final
          formatted string.  Thus, a value of 121 seconds is formatted as
          2:01 instead of 00:02:01.  If set to True, then the latter format
          will be used.
        @type zeros: boolean
        """
        super(ElapsedSecsFormat, self).__init__(member, justify=justify,
                                                **kwargs)
        self.zeros = zeros
        # make sure the min and max are big enough
        if self.minWidth <= self._DEFAULT_WIDTH:
            self.minWidth = self.maxWidth = self._DEFAULT_WIDTH


    def getValue(self, obj):
        """Format the value of our member from the provided object.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: formatted value of member in object
        @rtype: string
        """
        # get the value from the object
        val = getattr(obj, self.member)
        if val is None:
            val = ''
        else:
            # format the seconds
            val = timeutil.sec2hmsString(val, zeroblank=(not self.zeros))
        return val


class BytesFormat(MemberFormat):
    """Format members that represent total bytes into a human readable
    form."""

    _DEFAULT_WIDTH = 5

    def __init__(self, member, base='bytes', justify='right', **kwargs):
        """
        @param member: name of member to format
        @type member: string

        @param base: the base unit for the member.  The default is 'bytes'
          because this object is intended to only format raw bytes, but
          this makes it easier for subclasses.
        @type base: string
        """
        super(BytesFormat, self).__init__(member, 
                justify=justify, 
                **kwargs)
        # save the base of the units
        self.base = base
        # make sure the width is big enough
        if self.minWidth <= self._DEFAULT_WIDTH:
            self.minWidth = self.maxWidth = self._DEFAULT_WIDTH

    def getValue(self, obj):
        """Format the value of our member from the provided object and format
        it using L{rpg.unitutil.formatBytes}.

        @param obj: instance that contains the member we need to format
        @type obj: instance

        @return: formatted value of member in object
        @rtype: string
        """
        # get the value from the object
        val = getattr(obj, self.member)
        if val is None:
            val = ''
        else:
            # format the bytes
            val = unitutil.formatBytes(val, base=self.base)
        return val


class KiloBytesFormat(BytesFormat):
    """Format members that represent total kilobytes into a human readable
    form."""

    def __init__(self, member, base=None, **kwargs):
        # hard code the base
        super(KiloBytesFormat, self).__init__(
            member, base='kilo', **kwargs)


class MegaBytesFormat(BytesFormat):
    """Format members that represent total megabytes into a human readable
    form."""

    def __init__(self, member, base=None, **kwargs):
        # hard code the base
        super(MegaBytesFormat, self).__init__(member, 
                base='mega', 
                **kwargs)


class GigaBytesFormat(BytesFormat):
    """Format members that represent total gigabytes into a human readable
    form."""

    def __init__(self, member, base=None, **kwargs):
        # hard code the base
        super(GigaBytesFormat, self).__init__(member, 
                base='giga', 
                **kwargs)


class BytesRateFormat(BytesFormat):
    """Format members that represent a rate of bytes per second."""

    # increase the default by 2 to account for '/s'
    _DEFAULT_WIDTH = 7

    def getValue(self, obj):
        """Append '/s' to the end of the string computed by the super."""
        val = super(BytesRateFormat, self).getValue(obj)
        if val: val += "/s"
        return val


class KiloBytesRateFormat(KiloBytesFormat):
    """Format members that represent a rate of kilobytes per second."""

    def getValue(self, obj):
        """Append '/s' to the end of the string computed by the super."""
        val = super(KiloBytesRateFormat, self).getValue(obj)
        if val: val += "/s"
        return val


class MegaBytesRateFormat(MegaBytesFormat):
    """Format members that represent a rate of megabytes per second."""

    def getValue(self, obj):
        """Append '/s' to the end of the string computed by the super."""
        val = super(MegaBytesRateFormat, self).getValue(obj)
        if val: val += "/s"
        return val


class GigaBytesRateFormat(GigaBytesFormat):
    """Format members that represent a rate of gigabytes per second."""

    def getValue(self, obj):
        """Append '/s' to the end of the string computed by the super."""
        val = super(GigaBytesRateFormat, self).getValue(obj)
        if val: val += "/s"
        return val


class Formatter(object):
    """Class for formatting objects.  L{MemberFormat} objects are used
    for formatting individual members, but this object is used to format
    an entire object.  By default, members are joined together with a
    space and formatted onto a single line."""

    def __init__(self, *mformats, **attrs):
        """
        @param mformats: a list of members that should be formatted.  Each
          item can be a L{MemberFormat} object or the member name as a 
          string.  When specifying via string, the following operators are 
          available for setting attributes::

            =      absolute column width
            <      maximum width a column can have
            >      minimum width a column can have
            $      justification of the member (left or right)
            @      truncate (clamp) method to use (left, center, right, wrap)
            :      foreground color
            ;      background color            

          If a specific L{MemberFormat} type should be used for a member,
          then provide a mapping in 'memberToFormat', otherwise a
          L{StringFormat} type will be used.
        @type mformats: list

        @param separator: string that will be used to separate each member
        @type separator: string

        @param memberToFormat: a dictionary mapping object member names to
          an appropriate L{MemberFormat} subclass that should be used
        """
        # the separator that will be used to join all the formatted members
        self.separator = attrs.pop("separator", ' ')

        # a mapping of member names to MemberFormat objects
        self.memberToFormat = attrs.pop("memberToFormat", {}).copy()

        # make sure the user didn't provide any others
        if attrs:
            keys = list(attrs.keys())
            keys.sort()
            raise FormatterError("unknown keywords %s found in " \
                  "Formatter.__init__" % keys)

        # it's okay if the user passes in a list, as long as there is only 
        # one
        if len(mformats) == 1 and \
           type(mformats[0]) in (tuple, list):
            mformats = mformats[0]

        # list of our member formats
        self.mformats = []

        # process all the arguments in the member formats list
        self._initMemberFormats(mformats)


    def _initMemberFormats(self, mformats):
        """Parse the list of member formats provided by the caller and
        create a list of L{MemberFormat} instances."""
        
        # iterate over each item in the list provided by the user and
        # determine if we need to create a format object
        for mf in mformats:
            # if this is already a MemberFormat instance, then add it
            if issubclass(mf.__class__, MemberFormat):
                self._handleMFormatInstance(mf)
                continue
            
            # make sure it is a string
            elif type(mf) is not bytes:
                raise FormatterError("expected a string type in member " \
                      "format list, got '%s'" % mf.__class__.__name__)
            
            # strings can contain multiple formats that are comma delimited
            for mfstr in re.split(r"\s*,\s*", mf.strip(' ,')):
                self._handleMFormatString(mfstr)


    def _handleMFormatInstance(self, mformat):
        """By default each MemberFormat instance is simply added to the
        'mformats' list."""
        self.mformats.append(mformat)


    def _handleMFormatString(self, mfstr):
        """When a string is provided, in place of a MemberFormat instance,
        we need to find a suitable MemberFormat class to create.  This
        method parses any attributes out of the string, creates the
        necessary object and adds it to the mformats list."""

        # parse the string so we can make the format objects with
        # the proper attribute settings.
        member,attrs = self._parseMemberFormatStr(mfstr)
        # now get a format object
        mfobj = self.getMemberFormat(member, attrs)
        # add it to the list
        self._handleMFormatInstance(mfobj)


    # regexp used to get the member name
    _member_re = re.compile(r"(?P<member>[a-zA-Z]\w*)")
    # regexp used to get the attributes, the basic breakdown is this
    #    =\d+
    #    >\d+
    #    <\d+
    #    $\w+
    #    @\w+
    #    :\w+
    #    ;\w+
    _attr_re   = re.compile(r"(?P<op>[<>=$@;:])"
                            r"(?P<val>(?:(?<=[<>=])\d+(?:\.\d+)?)|"
                            r"(?:(?<=[$@;:])\w+))")
    # mapping from operator to attribute name
    _attrByOp  = {'=': 'width',
                  '<': 'maxWidth',
                  '>': 'minWidth',
                  '$': 'justify',
                  '@': 'truncate'}
    def _parseMemberFormatStr(self, mformat):
        """Parses a member format string and strips out any format
        attributes that might be specified.

        @param mformat: member format string that should be parsed.
        @type mformat: string

        @return: a tuple containing the member name string and a dictionary
          containing any attributes.
        @rtype: tuple
        """

        # first try to get the member name
        match = self._member_re.match(mformat)
        if not match:
            raise FormatterError("invalid format for member string '%s', " \
                  "unable to locate member name." % mformat)

        # save the member name
        member = match.group('member')

        # now it is time to scan the remaining portion of the string for
        # attributes
        attrs = {}

        # start our search from the end of the member match
        pos = match.end()
        end = len(mformat)

        # keep track of foreground and background color, so a TerminalColor
        # object can be made after we scan the whole string.
        fg  = None
        bg  = None

        # loop until we get to the end
        while pos < end:
            # search for a match
            match = self._attr_re.match(mformat, pos)
            # make sure we found something
            if not match:
                raise FormatterError("unknown attribute value '%s' found " \
                      "in member string '%s'" % (mformat[pos:], mformat))

            # get the operator and value
            op,val   = match.group('op', 'val')

            # figure out what to do with them
            if op in '=<>':
                # cast the value to an int or float
                if val.find('.') >= 0:
                    val = float(val)
                else:
                    val = int(val)
            # make sure truncate and justify are valid
            elif op == '$' and val not in MemberFormat.justifyValues:
                raise FormatterError("unknown justification '%s', must " \
                        "be one of %s." % (val, MemberFormat.justifyValues))
            elif op == '@' and val not in MemberFormat.truncateValues:
                raise FormatterError("unknown truncate option '%s', must " \
                      "be one of %s." % (val, MemberFormat.truncateValues))
            # make some colors
            elif op == ':':
                fg = val
            elif op == ';':
                bg = val
            # just incase something slipped through our regexp
            elif op not in '@$:;':
                raise FormatterError("unknown attribute operator '%s' " \
                      "found in member string '%s'" % \
                      (mformat[pos:], mformat))

            # set the attribute if we can
            try:
                attrName = self._attrByOp[op]
                attrs[attrName] = val
            except KeyError:
                pass

            # reposition the pointer
            pos = match.end()

        # check for a color
        if fg:
            attrs['color'] = terminal.TerminalColor(fg, bg=bg)
        elif bg:
            raise FormatterError("a foreground color must be present if " \
                  "background is specified.")

        # return what we found
        return member,attrs


    def getMemberFormat(self, member, attrs={}):
        """When a member is specified as a string, a valid L{MemberFormat}
        object needs to be found.  After the string has been parsed, it is
        passed onto this method which should return a L{MemberFormat}
        subclass instance.

        @param member: name of the member that will be formatted
        @type member: string

        @param attrs: dictionary of keyword attributes that should be passed
          to the constructor of the L{MemberFormat} subclass.
        @type attrs: dictionary

        @return: the L{MemberFormat} subclass instance that will be used
          to format the member.
        @rtype: instance
        """

        # check if a format object is specified for this member
        try:
            clsobj = self.memberToFormat[member]
        except KeyError:
            clsobj = StringFormat

        # create an instance and return it
        return clsobj(member, **attrs)

    def getFormattedValues(self, obj):
        """Get a list of all the values that need to be joined together."""
        return [mf.format(obj) for mf in self.mformats]

    def format(self, obj):
        """Format the members from the provided object.

        @param obj: object instance that will have its members formatted
        @type obj: instance

        @return: a formatted version of the object that can be printed on
          a single line.
        @rtype: string
        """

        # we have to take into account that some fields might be wrapping
        # their values onto a new line.  In order to preserve our neatly
        # spaced columns, we first call format() for each member and
        # build the first line.  If anything is leftover at that point,
        # then we loop until it is all gone.

        # first get our list of formatted values
        fvalues  = self.getFormattedValues(obj)
        
        leftover = {}
        mystr = ""
        first = True
        i     = 0
        # make the first line
        for mf in self.mformats:
            val = fvalues[i]
            i += 1
            
            # if this is a list, then we know we are wrapping.  save
            # the remaining lines
            if type(val) is list:
                leftover[mf] = val
                val = val.pop(0)

            # add this to the line
            if first:
                mystr += val
                first = False
            else:
                mystr += self.separator + val

        # add the leftover text
        while leftover:
            mystr += '\n'
            first = True
            # iterate over each member again
            for mf in self.mformats:
                # check if this member has something leftover
                try:
                    vallist = leftover[mf]
                except KeyError:
                    # if not, then put in the appropriate number of spaces
                    if mf.width is None:
                        val = ''
                    else:
                        val = mf.width * ' '
                else:
                    # pop the next value off
                    val = vallist.pop(0)
                    # if we don't have anymore, than remove this member
                    if not vallist:
                        del leftover[mf]

                # add this to the line
                if first:
                    mystr += val
                    first = False
                else:
                    mystr += self.separator + val

        return mystr


    def formatList(self, objs, header=True, footer=40, widths=True):
        """Format a list of objects and put each object on its own line.
        This is essentially a wrapper around L{format}.

        @param objs: list of objects that are to be formatted.
        @type objs: list

        @param header: indicate whether a header should be included, the
          default is True.
        @type header: boolean

        @param footer: the number of objects that must be formatted
          before a footer is added to the end.  This is for asthetics,
          and if you always want a footer set this to -1.  If you never
          want a footer, then set it to 0 or False.  The default is 40.
        @type footer: int

        @param widths: call L{setWidths} before formatting the objects to
          ensure everything is neatly spaced.  The default is True.
        @type widths: boolean

        @return: the formatted list of objects
        @rtype: string
        """

        # do nothing if the list is empty
        if not objs:
            return ''

        # set the widths
        if widths:
            self.setWidths(objs)

        # append everything as we go
        mystr = ''
        # add the header
        if header:
            mystr += self.header() + '\n'

        # iterate over each object
        first = True
        for obj in objs:
            if not first:
                mystr += '\n'
            else:
                first = False
            mystr += self.format(obj)

        # add the footer
        if footer and len(objs) >= footer:
            mystr += '\n' + self.footer()

        return mystr


    def header(self, divider='='):
        """Get a header containing all the member names and a divider line.

        @param divider: the character that will be used for the divider
          line.  No divider will be added if this is None or an empty string.
        @type string
        
        @return: header string
        @rtype: string
        """
        hstrs = [mf.header() for mf in self.mformats]
        mystr = self.separator.join(hstrs)
        if divider:
            mystr += '\n' + self.divider(char=divider)
        return mystr


    def footer(self, divider='='):
        """Get a footer containing all the member names preceeded by a
        divider line.  This is the reverse of L{header}.

        @param divider: the character that will be used for the divider
          line.  No divider will be added if this is None or an empty string.
        @type string
        
        @return: footer string
        @rtype: string
        """
        if divider:
            mystr = self.divider(char=divider) + '\n'
        else:
            mystr = ''
        hstrs = [mf.header() for mf in self.mformats]
        mystr += self.separator.join(hstrs)
        return mystr


    def divider(self, char='='):
        """Get a divider that can be printed inbetween the header and a
        formatted object.

        @param char: character that will be used to create the divider
        @type char: string

        @return: formatted divider intended to be placed between the header
          and the first formatted objects.
        @rtype: string
        """
        dstrs = [mf.divider(char=char) for mf in self.mformats]
        return self.separator.join(dstrs)


    def setWidths(self, objs, force=False):
        """Set the format widths for each member based on the objects in
        the provided list.  This is useful if you do not what the width of
        each member format should be in order to neatly format all of them.

        @param objs: list of objects that will be formatted
        @type objs: list

        @param force: Set this to True to force the width (if it is already
          set) of each member to be set based on the objects, default is 
          False.
        @type force: boolean
        """

        # iterate over each member format and pass the objects on
        for mf in self.mformats:
            mf.setWidth(objs, force=force)


class InstanceDoc(object):
    """Simple container class to simplify the formatting."""
    def __init__(self, member, description):
        self.member      = member
        self.description = description


class InstanceDocFormatter(Formatter):
    """A formatter used to format the documentation strings of instance
    variables in a class (i.e. those tagged via @ivar)."""

    def __init__(self):
        # setup the MemberFormat objects that will be used

        # the desired width of each of these will be set based on the
        # class we are formatting
        member = StringFormat("member")
        desc   = StringFormat("description", truncate="wrap")

        # call the super
        super(InstanceDocFormatter, self).__init__(member, desc,
                                                   separator="  ")


    def format(self, clsobj):
        """Overloaded from the super because the instance variables need to
        be searched for in the class object."""

        # this method will be called when the InstanceDoc objects are
        # being formatted.  So check for this and redirect it to the super
        if isinstance(clsobj, InstanceDoc):
            return super(InstanceDocFormatter, self).format(clsobj)

        # search through the __doc__ string of the class and make a dictionary
        # of instance variable names to documentation string.
        descs = self.getDescriptions(clsobj)

        # make a list of InstanceDoc objects that we will pass onto
        # formatList()
        ivars = []
        keys  = list(descs.keys())
        # print them alphabetically
        keys.sort()
        for key in keys:
            ivars.append(InstanceDoc(key, descs[key]))

        # call the super and format this list ivars
        return super(InstanceDocFormatter, self).formatList(ivars,
                                                            footer=False)


    def setWidths(self, objs, force=False):
        """Set the format widths of the member name and description
        format objects.  The description will be based on the member name
        width.

        @param objs: list of objects that will be formatted
        @type objs: list

        @param force: Set this to True to force the width (if it is already
          set) of each member to be set based on the objects, default is 
          False.
        @type force: boolean
        """

        # set the member name
        self.mformats[0].setWidth(objs, force=force)
        # set the description based on the member name width
        self.mformats[1].maxWidth = 76 - self.mformats[0].width
        self.mformats[1].setWidth(objs, force=force)


    # regexp used to find detailed descriptions of each instance variable.
    _ivarre = re.compile("^[ ]+(@ivar) ([^:\s]+)\s*:\s+([^\n]+)\n",
                         re.MULTILINE)
    def getDescriptions(self, clsobj):
        """Find all the instance variable documentation strings in the
        provided class object's __doc__ string (those tagged via @ivar).

        @param clsobj: class object we need to search trough
        @type clsobj: class

        @return: a mapping of documentation strings keyed by the instance
          variable name.
        @rtype: dictionary
        """

        # parse the doc string of the class and strip out all of the
        # @ivar lines
        ivars = {}
        if not clsobj.__doc__:
            return ivars
        pos    = 0
        doc    = clsobj.__doc__.replace('\t', ' '*4)
        match  = self._ivarre.search(doc)
        while match:
            # take note of where the match starts
            start,pos = match.span()
            # save where does the @ivar start, that we know the minimum
            # indent if the description continues on a newline
            minindent = match.start(1) - start + 1
            # get the ivar and the description
            ivar,desc = match.group(2, 3)

            # check if the description continues on a newline.  the rule
            # here is that it must be indented at least one space more
            # than the @ivar declaration was.  build a regexp to check
            # for this.
            indentre  = re.compile("%s[ ]*([^\n]+)\n" % (' '*minindent))
            match     = indentre.match(doc, pos)
            while match:
                pos   = match.end()
                desc += ' ' + match.group(1)
                match = indentre.match(doc, pos)

            ivars[ivar] = desc

            # search for another ivar
            match = self._ivarre.search(doc, pos)

        return ivars


def test():
    import time

    class Test(object):
        def __init__(self, **kwargs):
            for key,val in list(kwargs.items()):
                setattr(self, key, val)

    # test the properties
    t  = Test(foo='hello')
    mf = MemberFormat('foo')
    print("minWidth=%s, maxWidth=%s, width=%s" % \
          (mf.minWidth, mf.maxWidth, mf.width))
    mf.width = 2
    print("minWidth=%s, maxWidth=%s, width=%s" % \
          (mf.minWidth, mf.maxWidth, mf.width))
    mf.minWidth = 0
    print("minWidth=%s, maxWidth=%s, width=%s" % \
          (mf.minWidth, mf.maxWidth, mf.width))
    mf.width = 2
    print("minWidth=%s, maxWidth=%s, width=%s" % \
          (mf.minWidth, mf.maxWidth, mf.width))
    mf.maxWidth = 30
    print("minWidth=%s, maxWidth=%s, width=%s" % \
          (mf.minWidth, mf.maxWidth, mf.width))
    mf.minWidth = 40
    print("minWidth=%s, maxWidth=%s, width=%s" % \
          (mf.minWidth, mf.maxWidth, mf.width))


    objs = [Test(str1="Hello World",
                 str2="Wait just one minute",
                 str3="foobar",
                 int1=57, int2=3457,
                 float1=3.780, float2=572.0,
                 list1=[1, 2, 3, 4], list2=['a', 'b', 'c', 4, 5, 6],
                 dict1={'a': 1, 'b': 2, 'c': 3},
                 time1=time.time(),
                 secs1=121, secs2=34235,
                 bytes=37239573, mbytes=3456)]

    fmats = ["str1=12", "str2$right@right=15",
             StringFormat('str3', header='my header', width=4,
                          justify='right'),
             IntegerFormat('int1', width=10),
             IntegerFormat('int2', width=5),
             'float1=7.4@left',
             FloatFormat('float2', width=4.0,
                         color=terminal.TerminalColor('red')),
             ListFormat('list1', width=15),
             ListFormat('list2', width=8),
             DictFormat('dict1', width=10),
             TimeFormat('time1'),
             ElapsedSecsFormat('secs1', header='activesecs'),
             ElapsedSecsFormat('secs2'),
             BytesFormat('bytes', header='diskspace'),
             MegaBytesFormat('mbytes')]

    form = Formatter(fmats, memberToFormat={'float1': FloatFormat})
    print(form.header())
    for obj in objs:
        print(form.format(obj))


    import os, pwd
    class File(object):
        """
        @ivar name: name of the file
        @ivar accessed: time the file was last accessed
        @ivar modified: time the file was last modified
        @ivar size: size of the file in bytes
        @ivar user: owner of the file, blah blah blah blah
          blah blah blah
        @ivar perms: permissions of the file
        """
        
        # initialize each object with the filename
        def __init__(self, name):
            self.name  = name
            # stat the file and save the info
            self.fstat = os.stat(name)

        # setup some properties so we can get attributes of the file
        def getAccessed(self):
            return self.fstat.st_atime
        accessed = property(fget=getAccessed)

        def getModified(self):
            return self.fstat.st_mtime
        modified = property(fget=getModified)

        def getSize(self):
            return self.fstat.st_size
        size = property(fget=getSize)

        def getUser(self):
            return pwd.getpwuid(self.fstat.st_uid).pw_name
        user = property(fget=getUser)

        def getPermissions(self):
            return self.fstat.st_mode
        perms = property(fget=getPermissions)


    class FileFormatter(Formatter):
        # create our mapping that this formatter will always use
        memberToFormat = {'name'    : StringFormat,
                          'accessed': TimeFormat,
                          'modified': TimeFormat,
                          'size'    : BytesFormat,
                          'user'    : StringFormat,
                          'perms'   : IntegerFormat}

        def __init__(self, *mformats):
            # pass our mapping to the base class
            Formatter.__init__(self, memberToFormat=self.memberToFormat,
                               *mformats)

    ivf = InstanceDocFormatter()
    print(ivf.format(File))

    fileForm = FileFormatter('user<6@right, size', 'modified, name')
    dir = "/shows/rpg/global"
    for fname in os.listdir(dir):
        file = File(os.path.join(dir, fname))
        print(fileForm.format(file))

    files = []
    for fname in os.listdir(dir):
        files.append(File(os.path.join(dir, fname)))
    fileForm.setWidths(files)
    print(fileForm.header())
    for file in files:
        print(fileForm.format(file))

    print(fileForm.formatList(files))


if __name__ == "__main__":
    test()
