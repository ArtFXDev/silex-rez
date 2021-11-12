import sys
import re
import types

import rpg.stringutil
from rpg.terminal import TerminalColor

__all__ = (
        'FormatterError',
        'Formatter',
        )

# ----------------------------------------------------------------------------

def _parseFormatVars(vnames, convertWidths=1):
    """
    Parses the variable name arguments passed into __init__.  Each
    name can contain optional formatting options for width and color.

    format (bracketed values are optional)::
        [prepend]I{variable}[append[append[...]]]

    prepend operators::
        +       ignored
        -       remove all instances of I{variable}
        =       modify the attribute of I{variable}

    append operators::
        =       sets the width of I{variable}
        :       sets the foreground color of I{variable}
        ;       sets the background color of I{variable}
        #       sets the lines of I{variable}

        
    Examples:
        >>> vnames = ['v0', 'v1', 'v2']
        >>> variables,widths,colors,lines = _parseFormatVars(vnames)
        >>> variables == vnames
        1
        >>> widths == {'v0': None, 'v1': None, 'v2': None}
        1
        >>> colors == {'v0': None, 'v1': None, 'v2': None}
        1
        >>> lines == {'v0': None, 'v1': None, 'v2': None}
        1


        >>> from rpg.terminal import TerminalColor
        >>> vnames = ['v0=5:blue', 'v3', 'v2=5#3', 'v0:red', '+v1=5:green;blue', '=v2=-6', 'v3', '-v3']
        >>> variables,widths,colors,lines = _parseFormatVars(vnames)
        >>> variables == ['v0', 'v2', 'v0', 'v1']
        1
        >>> widths == {'v0': -5, 'v1': -5, 'v2': 6}
        1
        >>> colors == {'v0': TerminalColor('red', None), 'v1': TerminalColor('green', 'blue'), 'v2': None}
        1
        >>> lines == {'v0': None, 'v1': None, 'v2': 3}
        1
    

    @param vnames:          a list of variable strings with embedded operators
    @param convertWidths:   if set to 1, the widths will be multiplied by
                            -1 in order to left justify the string

    """

    regex = re.compile(
            r'(?P<mode>[\+\-\=])?'     # whether adding or subtracting or replacing
            r'(?P<name>\w+(?:\.\w+)?)' # name, supports 'jid' or 'Job.jid'
            r'(?P<rest>\S*)'
            )

    width_regex = re.compile(
            r'='                                          # '=' => width
            r'(?P<value>[+-]?\d+(?:\.\d*)?)?(?=[=:;#]|$)' # accepts a signed integer or float
            )

    fg_regex = re.compile(
            r':'                           # ':' => foreground color
            r'(?P<value>\w+)?(?=[=:;#]|$)' # accepts an string color name
            )

    bg_regex = re.compile(
            r';'                           # ';' => background color
            r'(?P<value>\w+)?(?=[=:;#]|$)' # accepts an string color name
            )

    linecount_regex = re.compile(
            r'#'                                # '#' => number of lines
            r'(?P<value>[+-]?\d+)?(?=[=:;#]|$)' # accepts an integer
            )
    
    variables = []
    widths = {}
    colors = {}
    lines = {}
    removeVariables = {}
    modifiedVariables = {}

    for v in vnames:
        m = regex.match(v)

        if not m:
            raise FormatterError('syntax error in variable: ' + v)
        
        ####

        mode = m.group('mode')
        name = m.group('name')
        rest = m.group('rest')

        def get(regex):
            m = list(regex.finditer(rest))

            if not m:
                return None
            m = m[-1]

            try:
                result = m.group('value')
            except IndexError:
                raise FormatterError('syntax error in variable: ' + v)
            else:
                return result

        ####

        width = get(width_regex)
        foregroundColor = get(fg_regex)
        backgroundColor = get(bg_regex)
        linecount = get(linecount_regex)

        if width:
            try:
                if '.' in width:
                    width = float(width)
                else:
                    width = int(width)
            except ValueError:
                raise FormatterError('width must be an integer: ' + width)
            if convertWidths:
                width *= -1

        
        if foregroundColor:
            color = TerminalColor(foregroundColor, backgroundColor)
        else:
            color = None

        if linecount:
            linecount = int(linecount)

        if mode == '-':
            removeVariables[name] = None
        elif mode == '=':
            modifiedVariables[name] = None
        else:
            variables.append(name)

        if width or name not in widths:
            widths[name] = width

        if foregroundColor or name not in colors:
            colors[name] = color

        if linecount or name not in lines:
            lines[name] = linecount

    variables = [v for v in variables if v not in removeVariables]

    for v in removeVariables:
        for m in [widths, colors, lines]:
            del m[v]

    ####

    #for v in modifiedVariables:
    #    if v not in variables:
    #        del widths[v]
    #        del colors[v]
    #        del lines[v]

    ####

    return variables,widths,colors,lines

# ----------------------------------------------------------------------------

class FormatterError(Exception):
    pass

# ----------------------------------------------------------------------------

class Formatter:
    """
    Base class for all Formatting objects.  Subclasses should
    override the __init__ function to provide a valid class object
    used to determine the format strings.  Also for more unique cases
    a get_varname method can be defined and will be called each time
    a value for 'varname' is fetched.  When called the object and
    variable names are passed as arguments.  The idea behind this class
    is to simplify the process of creating arbitrary format strings
    for printing objects.
    
      - Create a Formatter object to print the variables
        ['a', 'b', 'c', 'd'] in the class Foo, the following would be used.

        f = Formatter(Foo, ['a', 'b', 'c', 'd'])

      - Create the same Formatter object but have 'a' printed left
        justified 10 characters wide and 'c' right justified 5 characters
        wide.

        f = Formatter(Foo, ['a=10', 'b', 'c=-5', 'd'])

        I{NOTE: Standard format strings in Python, Perl, C, etc. use
        negative numbers to left justify, but this Class uses negative
        numbers to 'right' justify.  This is because in general most
        output will be left justified.}

      - Optionally an arbitrary Python format string can be supplied,
        but make sure all format tokens types match properly with the
        specified variable name.

        f = Formatter(Foo, ['a', 'b', 'c', 'd'], formatStr='a=%s, b=%s, c=%s, d=%s')

        I{NOTE: any width values specified in the variable list will be
        ignored if a format string is provided.}

    Example of subclassing Formatter to create a more specific Formatter
    from a predefined object type::

      # file: format_ex.py
      
      import misterd.Formatters as Formatters
    
      class Dispatcher:
          # simple container class that has a method to print a formatted
          # dispatcher string
          def __init__(self, user, host, port=9001):
              self.user = user
              self.host = host
              self.port = port

          def getDispatcherStr(self):
              return '%s@%s:%d' % (self.user, self.host, self.port)

      class DispatcherFormatter(Formatters.Formatter):
          def __init__(self, variables, separator=' ', formatStr=None):
              # an object of the type has to be created just for
              # initialization.  This is so the variable types can be
              # determined by the Formatter class.  Since Python makes
              # a distinction between Class variables and Instance
              # variables, simply passing the Class is not enough.
              disp = Dispatcher('a', 'b')
              Formatters.Formatter.__init__(self, disp, variables,
                                            separator=separator,
                                            formatStr=formatStr)
 
          def get_dispatcher(self, obj, varname):
              # creating this method allows for a variable named
              # 'dispatcher' to be printed
              return obj.getDispatcherStr()

      disp = Dispatcher('john', 'doe')
      df   = DispatcherFormatter(['user=10', 'host=10', 'port=5',
                                  'dispatcher=30'])
      df.printHeader()
      df.printDivider()
      df.printObject(disp)

      # end file: format_ex.py

      > python format_ex.py
      user       host       port  dispatcher
      ========== ========== ===== ==============================
      john       doe        9001  john@doe:9001
      >

    This isn't very useful for printing one object, but if a list of
    objects are to be printed with the same format it can help
    tremendously.

    @ivar head: prepended to each string when printed
    @ivar tail: appended to each string when printed
    """
    
    head = ''
    tail = ''
    def __init__(self, className, variables, separator=' ',
                 formatStr=None):
        """
        Initialize a Formatter object with a valid Class reference
        and a list of variable names in the class to be printed.

        @raise FormatterError: user defined format string is not
                               correct.
        """

        # name of class where the variables are defined
        self.className = className
        # a list of the variables to be printed out.  Stored as a list
        # to preserve the desired print order.
        self.variables = []
        # dictionary used to store desired print widths of each variable,
        # keyed by variable name
        self.widths    = {}
        # dictionary used to map TerminalColor objects to each variable
        self.colors    = {}
        # dictionary used to map desired number of lines to print
        self.lines     = {}
        # internal flag used to determine if text should be colored.
        self.color     = 1

        # parse the variable names
        self.variables,self.widths,self.colors,self.lines = \
                                            _parseFormatVars(variables)

        # compute absolute widths as well
        self.abswidths = {}
        for key,val in list(self.widths.items()):
            if val is not None:
                self.abswidths[key] = abs(int(val))
            else:
                self.abswidths[key] = val

        # each variable will be separated by this string if the
        # formatStr variable is not set.
        self.separator = separator

        # use a predefined format string
        if formatStr:
            self.fstrings = self._parseFormatStr(formatStr)
            for v in self.variables:
                self.widths[v] = None
            self.separator = ''
        # create format strings for each variable based on its type and
        # desired width.
        else:
            self.fstrings = self._setFormatStrs()

    def _backup(self, variables):
        # split the variables argument to get the variable names and
        # the desired print length separated by an '=' character.
        for v in variables:
            vsplit = v.split('=')
            varLen = None
            if len(vsplit) == 2:
                try:
                    # a negative number in a format string means left
                    # justified, but the Formatter class defines left
                    # justified variables as positive numbers
                    varLen = str(int(vsplit[1]) * -1)
                except (IndexError, ValueError):
                    raise FormatterError("expected integer in format variable: " + v)
            elif len(vsplit) != 1:
                raise FormatterError("invalid format in variable list: " + v)

            self.variables.append(vsplit[0])
            self.widths[vsplit[0]] = varLen

    def _setFormatStrs(self):
        """Returns a dictionary of format strings keyed by variable
        name.  If a length is defined in self.widths for
        the respective variable it is inserted into the format
        string."""

        vtypes = {}
        # determine type of each variable
        for v in self.variables:
            varType = type(self._getValue(self.className, v))

            if varType is int or varType is int:
                varType = 'd'
            elif varType is float:
                varType = 'f'
            else:
                varType = 's'

            vtypes[v] = varType

        # create the format strings
        fstr = {}
        for (v, w) in list(self.widths.items()):
            t = vtypes[v]
            if not w:
                fstr[v] = '%' + t
            # convert the width to a string so we don't loose
            # '-' or '.'
            else:
                fstr[v] = "%%%s%s" % (str(w), t)

        return fstr

    def _parseFormatStr(self, formatStr):
        """Parses an Python format string into individual '%' tokens so
        each variable name can be processed separately.

        @raise FormatterError: unable to parse format string.
        """

        def nextToken(fstr):
            """Returns the next valid '%' token in the string and the
            remaining string after the token as a tuple."""

            end = len(fstr)
            ind = 0
            # set to 1 after the beginning of a token has been found
            beginSet = 0
            
            while 1:
                # find the next '%' character
                ind = fstr.find('%', ind)
                # if one isn't found then break and assume the whole
                # string passed is a valid token
                if ind < 0:
                    break

                # check for an invalid format string at the end
                if ind + 1 == end:
                    raise FormatterError('no type for format string token.')

                # check for '%%' for deliminating '%' characters
                if fstr[ind + 1] != '%':
                    # if the beginning has been set, then return the token
                    # and the remainder of the string
                    if beginSet:
                        return fstr[0:ind], fstr[ind:]
                    # otherwise this must be the beginning of a new token
                    else:
                        beginSet = 1
                        ind += 1
                else:
                    # skip double '%'
                    ind += 2

            return fstr, None

        # used to keep track of the current variable index
        currVar  = 0
        myfstr   = formatStr
        fstrings = {}
        
        while myfstr:
            # get the next availabe token
            (token, myfstr) = nextToken(myfstr)
            # try to add it to the fstrings dictionary
            try:
                fstrings[self.variables[currVar]] = token
            except IndexError:
                raise FormatterError('unmatched variable for token ' + token)
            else:
                currVar += 1

        if len(list(fstrings.keys())) != len(self.variables):
            raise FormatterError('more variables set than in format string.')

        return fstrings

    def _getValue(self, name, var):
        """First a get_var function is searched for in 'self'.  If one
        is not defined then the value is referenced from 'name'

        @raise FormatterError: unable to retrieve a value.
        """

        try:
            # try to get the get_var function
            func = getattr(self, 'get_' + var)
        except AttributeError:
            # if no get_var function found, then just return assume
            # a valid value exists in the object.
            try:
                return getattr(name, var)
            except AttributeError:
                raise FormatterError("no variable=%s found in the " \
                      "class %s." % (var, name.__class__))                      

        # call the get_var function and pass the called variable name
        return func(name, var)

    def getString(self, obj, fitString='squeeze'):
        """
        Returns a string based on the format string set in __init__
        and uses 'obj' as the input parameters.  If a value is a string
        and it is too large to fit in the specified width it can be
        clamped or squeezed to fit.  By default values are squeezed to
        fit in the width.  Example::

          the string
            'Python is fun for the whole family.'
          if squeezed to 25 characters would look like
            'Python is f...ole family.'

        if you want strings clamped, set fitString='clamp'.  Example::

          the string
            'Python is fun for the whole family.'
          if clamped to 25 characters would look like
            'Python is fun for the who'

        if you don't want strings altered at all and want the full string
        printed, set fitString=None.

        """

        # add the head of the string
        myStr = self.head

        # multi line values will have remaining lines saved
        remaining = {}
        # keep track of any specific colors
        colors = {}

        # iterate through each variable name
        for v in self.variables:
            # get the value of the variable
            val = self._getValue(obj, v)
            # optionally a color can be returned as well, if present
            # the text will be colored this.
            vtype = type(val)
            if vtype is tuple:
                color = val[1]
                val   = val[0]
                vtype = type(val)
            else:
                color = self.colors[v]

            colors[v] = color
            # make sure the value isn't 'None'
            if val is None:
                val   = ''

            # if the value of the variable is a string then check how many
            # lines we should print, and whether the string should be
            # sequeezed or clamped
            if vtype is bytes:
                # first see if the value has more than one line
                vlines = val.split('\n')
                # get the number of lines to be printed, None or zero means
                # print all of them.
                linenum = self.lines[v]
                # take a subset if the value is not None or zero
                if linenum:
                    if linenum > 0:
                        vlines = vlines[:linenum]
                    else:
                        vlines = vlines[linenum:]

                # pop the first line off and save the rest
                val = vlines.pop(0)
                # if anything is still there then save it
                if vlines: remaining[v] = vlines

                # get desired print length
                length = self.abswidths[v]
                # squeeze the string using the rpg function
                if length is not None:
                    if fitString == 'squeeze':
                        val = rpg.stringutil.squeezeString(val, length)
                    # clamp the string
                    elif fitString == 'clamp':
                        val = val[:length]

            # format the string and then color it if a TerminalColor object
            # is found
            try:
                fstr = self.fstrings[v] % val
            except TypeError:
                if self.widths[v]:
                    fstr = ' ' * self.abswidths[v]
                else:
                    fstr = ''
                
            if fstr and color:
                fstr = color.colorStr(fstr)

            # format the string and add the separator
            myStr += fstr + self.separator

        # remove the trailing separator and add the tail
        if self.separator:
            myStr = myStr[:-(len(self.separator))]

        myStr += self.tail

        if not remaining:
            return myStr

        # add any remaining lines
        while remaining:
            myStr += '\n' + self.head
            # iterate through each variable name
            for v in self.variables:
                color = colors[v]
                try:
                    lines = remaining[v]
                except KeyError:
                    val = ''
                else:
                    val = lines.pop(0)
                    if not lines:
                        del remaining[v]

                # get desired print length
                length = self.abswidths[v]
                # squeeze the string using the rpg function
                if length is not None:
                    if fitString == 'squeeze':
                        val = rpg.squeezeString(val, length)
                    # clamp the string
                    elif fitString == 'clamp':
                        val = val[:length]

                # format the string and then color it if a TerminalColor
                # object is found
                try:
                    fstr = self.fstrings[v] % val
                except TypeError:
                    if self.widths[v]:
                        fstr = ' ' * self.abswidths[v]
                    else:
                        fstr = ''

                if fstr and color:
                    fstr = color.colorStr(fstr)

                # format the string and add the separator
                myStr += fstr + self.separator

        # remove the trailing separator and add the tail
        if self.separator:
            myStr = myStr[:-(len(self.separator))]
            
        return myStr + self.tail

    def printObject(self, obj, fitString='squeeze', fh=sys.stdout):
        """Wrapper for getString() that prints to stdout."""
        print(self.getString(obj, fitString=fitString), file=fh)

    def _getPrintWidth(self, var, getabs=0):
        """Get the print width of a variable.  This takes into account
        any additional space needed for decimal points in float values."""

        width = self.widths[var]
        # if no width, then use the length of variable name
        if not width:
            width = len(var)
        # otherwise use the respective width
        else:
            wtype = type(width)
            if wtype is int:
                pass
##             # make sure we add one for the decimal point if needed
##             elif wtype is types.FloatType:
##                 whole = int(width)
##                 if abs(width - whole) > 0.000001:
##                     if width > 0:
##                         whole += 1
##                     else:
##                         whole -= 1
##                 width = whole
            # otherwise treat the whole thing as a string
            else:
                width = int(float(width))

        if getabs:
            return abs(width)
        return width
    
    def getDivider(self, dchar='='):
        """Returns a divider that is formatted with the same widths
        as the variable values.  By default the divider is composed of
        '=' characters.
        Example::

          var1    var2    var3
          ======= ======= ========   <---  this is returned

        calling this function will return the bottom divider line.
        """

        myStr  = ''
        # iterate through each variable name
        for v in self.variables:
            fdiv = dchar * self._getPrintWidth(v, getabs=1)

            # color the divider line
            if self.color and fdiv and self.colors[v]:
                fdiv = self.colors[v].colorStr(fdiv)
                
            myStr += fdiv + self.separator

        # truncate the trailing separator string if one is defined
        if self.separator:
            myStr = myStr[:-(len(self.separator))]

        return myStr

    def printDivider(self, dchar='=', fh=sys.stdout):
        """Wrapper function for getDivider() that prints to stdout."""
        print(self.getDivider(dchar=dchar), file=fh)

    def getHeader(self, headers={}, clamp=0):
        """Returns a header string using the variable names.  They will
        be formatted with the same widths as the variables values.
        Alternate header strings can be used and clamped to fit within
        the predefined width.  Alternates are specified using a
        dictionary where the key is the variable name and the desired
        header string is the value. Example: to print the variable name
        'foo' as 'bar' in the header.

        myFormatter.getHeader(headers={'foo': 'bar'})

        """

        myStr = ''
        # iterate through each header string
        for v in self.variables:
            # get the alternate string if one defined
            try:
                hstr = headers[v]
            except KeyError:
                hstr = v
                
            # if the header string has a length associated with it then
            # format it accordingly
            if self.widths[v]:
                width  = self._getPrintWidth(v)
                # create a format string based on its respective length
                format = "%%%ds" % width

                # check if header string should be clamped
                width = abs(width)
                if len(v) > width and clamp:
                    hstr = format % hstr[:width]
                else:
                    hstr = format % hstr

            # color the header
            if self.color and hstr and self.colors[v]:
                hstr = self.colors[v].colorStr(hstr)
                
            myStr += hstr + self.separator

        # truncate the trailing separator string if one is defined
        if self.separator:
            myStr = myStr[:-(len(self.separator))]

        return myStr

    def printHeader(self, headers={}, clamp=0, fh=sys.stdout):
        """Wrapper function for getHeader() that prints to stdout."""
        print(self.getHeader(headers=headers, clamp=clamp), file=fh)


