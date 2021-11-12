"""
The Where module implements a higher level language for querying a database.
The syntax is heavily based on Python and allows several convienences to
easily construct complex queries.  The language supports all the expected
comparison operators (=, !=, >, <, >=, <=).  In addition, 'like' is available
for regular expressions, and 'in' and 'has' are implemented for list
comparisons.  L{Where.Where} is intended to be used with L{Database.Database}
objects.

Syntax
======
  The syntax is heavily based on Python, and the primary goal was to write
  something that was flexible and human readable.  Below is a list of
  exceptable formats for queries::

    a=b
    a=b and c=d
    a=b or c=d
    a=b and (c=d or f>a)
    a=b and not (c=d or f > a)
    a in [b, c, d]
    a not in [b, c, d] or g > h

Implicit Class Member References
================================
  One of the biggest advantages to writing a separate where string parser is
  to allow implicit class member referencing, and unqoted strings.  This
  enables users to quickly type queries without worrying about quotes.
  The parser checks if an unquoted string refers to a class member, otherwise
  it assumes it is a string and puts quotes around it.  For example, suppose
  we are querying the L{TestDatabases.Music} database for all the songs with
  genre set to 'rock'.  The query would be::

    genre=rock

  which is equivalent to::

    Album.genre='rock'

List Comparisons
================
  The 'in' operator can be used to check if an item exists in a list.  The
  list can be defined in the query using brackets ([]), or it could be
  class member reference that is in the database.  For example, suppose
  we want to search for all songs in the L{TestDatabases.Music} database
  with the genre 'rock', 'alternative', or 'punk'.  We could write the
  full query with a series of 'or' expressions::

    genre=rock or genre=alternative or genre=punk

  or we could put the genres into a list::

    genre in [rock, alternative, punk]

  Furthermore, if one of the fields in the database is a list, we can
  check for unique items.  For example, the we can search for all fruit from
  the L{TestDatabases.Produce} database that has 'summer' in its seasons
  list::

    summer in seasons

  The 'has' operator can be used to check if multiple items exist in a
  a database list field.  If we are searching for fruit with 'summer' and
  'spring' in the seasons list, we could do::

    summer in seasons and spring in seasons

  or we could simplify it with::

    seasons has [summer, spring]

  The 'in' and 'has' operators are great for simplifying queries that would
  normally require parenthesis around a series of 'and' or 'or' expressions.

Regular Expressions
===================
  The 'like' operator can be used to compare a database field with a regular
  expression.  Unfortunately, regular expressions are not standard and
  should be written based on the underlying database.  However, if we were
  querying a MySQL database, we could search for all songs in the
  L{TestDatabases.Music} database with 'California' in the title (since so
  many songs are about California)::

    song like California

  However, if we only want songs that begin with 'California'::

    song like ^California

  Notice, we still don't need to quote the regular expression, but if any
  special operator characters (=, >, <, [, ], etc.) are included, the
  expression should have quotes around it.

Time Comparisons
================
  Several shortcuts exist for comparing database fields that represent time
  or elapsed seconds.  Below is a breakdown of acceptable formats::

    10am        - 10am on the current day
    5pm         - 5pm on the current day
    11:37       - 11:37am on the current day
    16:21       - 4:21pm on the current day
    4:21pm      - same thing
    3/15        - midnight on March 15 of the year closest to the current date
    3/15|4pm    - 4pm on March 15 of the year closest to the current date
    3/15.4pm    - 4pm on March 15 of the year closest to the current date
    3/15|17:37  - 5:37pm on March 15 of the year closest to the current date
    3/15/05     - midnight on March 15, 2005
    3/15/05|4am - 4am on March 15, 2005
    -1s         - 1 seconds ago
    -1m         - 1 minutes ago
    -1h         - 1 hours ago
    -1d         - 1 day ago
    -1w         - 1 week ago

  For example, to query all albums from the L{TestDatabases.Music} database
  that were released after July 1st, 2000::

    released > 7/1/00

  or all the albums released in the past 4 weeks::

    released >= -4w

Specifying Units
================
  As illustrated above, the parser understands different units for numbers
  and will scale them to a value equivalent to the field in the database.
  This way a field can be compared with 60 minutes using 60m instead of 3600.
  The following units are available::

    s         seconds
    m         minutes
    h         hours
    d         days
    w         weeks

    b|B       bytes
    K|kb|KB   kilobytes
    M|mb|MB   megabytes
    G|gb|GB   gigabytes
    T|tb|TB   terabytes
    P|pb|PB   petabytes

  For example, to query all songs that are larger than 1.5mb::

    filesize > 1.5mb

  or::

    filesize > 1.5M

Aliases
=======
  Where aliases can be defined within each L{Database.Database} object.
  They are simply a mapping from a single unquoted string to a valid
  where string that is inserted into the current where string.  When the
  parser encounters an unquoted string, it checks for an alias if it is
  positioned as a stand-alone.  For example, if the query is::

    foo and bar

  the parser will check its L{Database.Database} object if 'foo' and 'bar'
  are where aliases.  If they are, then the associated query is parsed and
  inserted into the current query.  This is done recursively with checks in
  place to prevent an infinite loop.  However, if the query is::

    foo=bar

  then neither 'foo' or 'bar' will be checked for an alias.

Executing Commands
==================
  The output from a command can be used in a query by putting the command
  in backticks (`).  The outupt will be treated differently depending on
  the placement of the command in the query.  Exceptable uses of commands
  are::

    foo=`hostname`
    foo in [`cat list_of_values.txt`]
    foo in [bar, `cat list_of_values.txt`, bowls]
    foo and `cat more_of_where_string.txt`

  Notice, that if the command is part of a comparison phrase (like the
  first two), then the output is treated only as data and syntax is not
  checked.  However, if the command is positioned as a phrase, then it is
  handled just like an alias would be.

@sort: WhereError
"""

import re, string, exceptions, types, time

from .. import sql
from .. import osutil
from .. import timeutil
from .. import WhereAst
# import using full path to module so that isinstance() works properly 
# with fields instantiated in EngineDB.py.
import tractor.base.rpg.sql.Fields as DBFields

#LikeOperator = "REGEXP" # for MySQL
#LikeOperator = "LIKE"    # for SQLite
LikeOperator = "~"    # for Postgres
NotLikeOperator = "!~"    # for Postgres


# we only advertise the following classes, the rest are considered private.
__all__ = ('Where',
           'WhereError',
           'TokenizeError',
           'SyntaxError',
           'IncorrectType',
           'InfiniteLoop',
           'MatchError',
           'NoMemberFound')

# all the regular expressions used to define the grammar of the where
# strings are based on those from the tokenize.py module of Python.
def group(*choices): return '(' + '|'.join(choices) + ')'
def maybe(*choices): return group(*choices) + '?'


class WhereError(sql.SQLError):
    """Base exception for all errors related to where strings."""
    pass

class TokenizeError(WhereError):
    """Unable to break input string into tokens."""
    pass

class SyntaxError(WhereError):
    """Input string does not have valid syntax."""
    pass

class IncorrectType(WhereError):
    """Incorrect typ for one or more operands of a comparison phrase."""
    pass

class InfiniteLoop(WhereError):
    """Referencing aliases and/or commands within each other causes an
    infinite loop."""
    pass

class MatchError(WhereError):
    """Invalid type was provided to test if it matches the where string."""
    pass

class NoMemberFound(WhereError):
    """All comparison phrases must contain at least on class member
    reference."""
    pass


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Token(object):
    """A token is a primitive block of text from the where string.  Each
    token type should define a regular expression to uniquely identify it."""

    # the regular expression used to match this token type.  This should
    # be defined by all subclasses
    regexp = None

    def __init__(self, text):
        """Initialize a token with the text that was matched."""
        self.text = text
        # the token to the left of us
        self.prev = None
        # the token to the right of us
        self.next = None

    def __str__(self):
        return self.text

    def __eq__(self, other):
        if self.text == other.text:
            return True
        return False

    def __ne__(self, other):
        return not (self == other)

    def getTree(self, indent=''):
        return str(self)

    def addToContext(self, context, stack):
        """When a list of tokens are being parsed, the parser will call
        this method for each Token object.  It is up to the subclasses to
        check for syntax errors."""
        pass

    def getNat(self):
        """Return a string representing the token in the natural language
        where format."""
        return str(self)

    def getMySQL(self, compare=None):
        """Return a string representing the token in MySQL format.

        @param compare: Token object that we are being compared against,
          if we are part of a comparison phrase.
        @type compare: L{Token} instance.
        """
        return str(self)

    def getPython(self, compare=None):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query.

        @param compare: Token object that we are being compared against,
          if we are part of a comparison phrase.
        @type compare: L{Token} instance.
        """
        return str(self)

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        return []

    def mask(self, tables, fields, keep):
        """Return a boolean indicating whether this Token should be masked."""
        return False

    def find(self, tokens):
        return []

    def getAst(self):
        """Return the abstract syntax tree for this expression node."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Data(Token):
    """Any token that can represent data (e.g. string, number, variable
    name, etc.)."""

    def addToContext(self, context, stack):
        """Make sure this Data token is in the right spot.  Data tokens can
        be in 3 different places in a query:
          - comparison:
            Data Comparison Data (e.g.  user=jag)
          - in a list
            Data ListComparison [Data, Data, ...] (e.g.  user in [jag, adamwg])
          - as a stand alone boolean phrase
            Data and Data   (e.g.  user and host)
        """

        # check if the data should be added to a list
        if context.listVal is not None:
            context.listVal.append(self)
            return

        # if no left variable (operand) has been set then set it
        if not context.left:
            #print "setting %s as left operand" % self.text
            context.left = self
            return

        # if a left variable (operand) is set then this must be a right
        # variable (operand) and if no operator is found then raise an
        # exception.
        if not context.currOp:
            raise SyntaxError("no operator found between" \
                  " %s and %s." % (context.left.text, self.text))

        # if we get here, then we must have enough info for a
        # comparison phrase
        phrase = context.currOp.getPhrase(self, context)
        # add the phrase and reset the needed variables
        context.addPhrase(phrase)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class String(Data):
    """A String token is a quoted piece of text."""

    # any text surrounded by 's or "s
    regexp = group(r"'[^\n'\\]*(?:\\.[^\n'\\]*)*'",
                   r'"[^\n"\\]*(?:\\.[^\n"\\]*)*"')

    def getAst(self):
        # strip off the quotes
        return WhereAst.Constant(self.text[1:-1])

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Number(Data):
    """A Number token is any integer or float value."""

    # accept just about every possible number format
    IntNumber   = r'[\-]?[0-9]\d*(L)?'
    Exponent    = r'[eE][-+]?\d+'
    PointFloat  = group(r'[\-]?\d+\.\d*', r'[\-]?\.\d+') + maybe(Exponent)
    ExpFloat    = r'\d+' + Exponent
    FloatNumber = group(PointFloat, ExpFloat)

    # set the final regexp
    regexp      = group(FloatNumber, IntNumber)

    def getMySQL(self, compare=None):
        """Check if 'L' is attached to the number."""
        if self.text[-1] == 'L':
            return self.text[:-1]
        return self.text

    def getAst(self):
        if re.match(self.FloatNumber, self.text):
            return WhereAst.Constant(float(self.text))
        else:
            return WhereAst.Constant(int(self.text))


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class ByteNumber(Number):
    """A Byte number token is used when comparing fields representing
    disk or memory amounts.  Creating a specific type allows for abbreviated
    values (e.g. 3G for 3 gigabytes).  The supported unit abbreviations are::

      b|B       bytes
      K|kb|KB   kilobytes
      M|mb|MB   megabytes
      G|gb|GB   gigabytes
      T|tb|TB   terabytes
      P|pb|PB   petabytes
    """

    _units = group('[kKmMGgTtPp][bB]', '[bBKMGTP]')
    regexp = Number.regexp + _units

    def __eq__(self, other):
        if self.getPython() == other.getPython():
            return True
        return False

    # regular expression used to strip the unit off the number so it can
    # be converted
    _unitsre = re.compile(_units)
    def convert(self, compare):
        """Return a converted value of this number in an integer or float format."""
        scale=self.getScale(compare)
        if isinstance(compare.field, (DBFields.MegaByteFloatField, DBFields.GigaByteFloatField)):
            return str(self.convertToFloat(scale))
        else:
            return str(self.convertToInt(scale))

    def convertToInt(self, scale):
        return int(self.convertToFloat(scale) + 0.5)
    
    def convertToFloat(self, scale):
        # search for the beginning of the unit
        match = self._unitsre.search(self.text)
        # get the starting point of the unit
        start = match.start()
        # cast the provided value to a float since it can be an int or float
        value = float(self.text[:start])
        # the unit can be more than one char, but the first char is the only
        # important one
        unit  = self.text[start].lower()

        # figure out what the factor should be, based on the unit

        # one petabyte
        if unit == 'p':
            factor = 1<<50
        # one terabyte
        elif unit == 't':
            factor = 1<<40
        # one gigabyte
        elif unit == 'g':
            factor = 1<<30
        # one megabyte
        elif unit == 'm':
            factor = 1<<20
        # one kilobyte
        elif unit == 'k':
            factor = 1<<10
        # one byte
        else:
            factor = 1

        # scale the value accordingly and cast is back to a long
        return value * factor * scale

    def getScale(self, compare):
        """Get the scale factor that will be used so that the converted
        value is in the same unit as the database field we will be comparing
        it to."""
        scale = 1
        # only muck with Member classes
        if issubclass(compare.__class__, Member):
            if compare.field.__class__ is DBFields.KiloByteField:
                scale = 1 / float(1<<10)
            elif compare.field.__class__ in (DBFields.MegaByteField, DBFields.MegaByteFloatField):
                scale = 1 / float(1<<20)
            elif compare.field.__class__ in (DBFields.GigaByteField, DBFields.GigaByteFloatField):
                scale = 1 / float(1<<30)
        return scale

    def getMySQL(self, compare=None):
        """Return a string representing the token in MySQL format."""
        # scale the value depending on the units of the token we are
        # being compared with.
        return self.convert(compare)

    def getPython(self, compare=None):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        return self.convert(compare)

    def getAst(self, compare=None):
        return WhereAst.Constant(self.convertToInt(self.getScale(compare)))


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

# class TimeNumber(Number):
#     """A Time number token is used to specify a time relative to now when
#     comparing fields representing dates or elapsed times.  Creating a
#     specific type allows for abbreviated values (e.g. 30m for 30 minutes
#     or -30m for 30 minutes ago).  The supported unit abbreviations are::

#       s    seconds
#       m    minutes
#       h    hours
#       d    days
#       w    weeks
#     """

#     # seconds, minutes, hours, days, weeks
#     _units = '[smhdw]'
#     regexp = Number.regexp + _units

#     def __eq__(self, other):
#         if self.getPython() == other.getPython():
#             return True
#         return False

#     # regular expression used to strip the unit off the number so it can
#     # be converted
#     _unitsre = re.compile(_units)
#     def convert(self):
#         """Return a converted value of this number so it is in an integer
#         format."""

#         result = self.convertToInt()

#         # check for relative times
#         if result < 0:
#             result = long(time.time()) + result
#         return str(result)

#     def convertToInt(self):
#         # search for the beginning of the unit
#         match = self._unitsre.search(self.text)
#         # get the starting point of the unit
#         start = match.start()
#         # cast the provided value to a float since it can be an int or float
#         value = float(self.text[:start])
#         # grab the unit
#         unit  = self.text[start].lower()

#         # figure out what the factor should be, based on the unit

#         # one week
#         if unit == 'w':
#             factor = 604800
#         # one day
#         elif unit == 'd':
#             factor = 86400
#         # one hour
#         elif unit == 'h':
#             factor = 3600
#         # one minute
#         elif unit == 'm':
#             factor = 60
#         # one second
#         elif unit == 's':
#             factor = 1

#         return long((value * factor) + 0.5)

#     def getMySQL(self, compare=None):
#         """Return a string representing the token in MySQL format."""
#         return self.convert()

#     def getPython(self, compare=None):
#         """Return a string that can be executed as a Python statement to
#         test whether an object from the queried table matches the query."""
#         return self.convert()

#     def getAst(self):
#         result = self.convertToInt()

#         if result < 0:
#             # Since this is relative, we need to dynamically compute it
#             # each time we do a match.
#             return WhereAst.RelativeTimeInt(result)
#         else:
#             return WhereAst.Constant(result)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

# uncomment this when you can figure out how to use it at the same time as
# TimeNumber.  There is a list of classes below that seems to only let
# one of these Time* classes work.
#
class TimeString(String):
    """A Time number token is used to specify a time relative to now when
    comparing fields representing dates or elapsed times.  Creating a
    specific type allows for abbreviated values (e.g. 30m for 30 minutes
    or -30m for 30 minutes ago).  The supported unit abbreviations are::

      s    seconds
      m    minutes
      h    hours
      d    days
      w    weeks

    This is the same as TimeNumber, but is expressed as a string for
    a TIMESTAMP field.
    """

    # seconds, minutes, hours, days, weeks
    _units = '[smhdw]'
    regexp = Number.regexp + _units

    def __eq__(self, other):
        if self.getPython() == other.getPython():
            return True
        return False

    # regular expression used to strip the unit off the number so it can
    # be converted
    _unitsre = re.compile(_units)
    def convert(self):
        """Return a converted value of this number so it is in an integer
        format."""

        result = self.convertToInt()

        # check for relative times
        if result < 0:
            result = int(time.time()) + result

        result = "'%s'" % timeutil.formatTime(result, "%Y-%m-%d %H:%M:%S")
        return result

    def convertToInt(self):
        # search for the beginning of the unit
        match = self._unitsre.search(self.text)
        # get the starting point of the unit
        start = match.start()
        # cast the provided value to a float since it can be an int or float
        value = float(self.text[:start])
        # grab the unit
        unit  = self.text[start].lower()

        # figure out what the factor should be, based on the unit

        # one week
        if unit == 'w':
            factor = 604800
        # one day
        elif unit == 'd':
            factor = 86400
        # one hour
        elif unit == 'h':
            factor = 3600
        # one minute
        elif unit == 'm':
            factor = 60
        # one second
        elif unit == 's':
            factor = 1

        return int((value * factor) + 0.5)

    def getMySQL(self, compare=None):
        """Return a string representing the token in MySQL format."""
        return self.convert()

    def getPython(self, compare=None):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        return self.convert()

    def getAst(self):
        result = self.convertToInt()

        if result < 0:
            # Since this is relative, we need to dynamically compute it
            # each time we do a match.
            return WhereAst.RelativeTimeInt(result)
        else:
            return WhereAst.Constant(result)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Date(Data):
    """A Date token is used to specify an absolute time when comparing
    fields representing dates.  Dates can be provided in several forms::
      10am       - 10am on the current day
      5pm        - 5pm on the current day
      11:37      - 11:37am on the current day
      16:21      - 4:21pm on the current day
      4:21pm     - same thing
      3/15       - midnight on March 15 of the year closest to the current date
      3/15|4pm   - 4pm on March 15 of the year closest to the current date
      3/15.4pm   - 4pm on March 15 of the year closest to the current date
      3/15|17:37 - 5:37pm on March 15 of the year closest to the current date
      3/15/05    - midnight on March 15, 2005
    """

    months = ('[Jj]an', '[Ff]eb', '[Mm]ar', '[Aa]pr', '[Mm]ay', '[Jj]un',
              '[Jj]ul', '[Aa]ug', '[Ss]ep', '[Oo]ct', '[Nn]ov', '[Dd]ec')

    # accept multiple formats for dates and times
    ampm   = r'(am|pm|AM|PM)'
    hms    = group(r'(\d\d|\d)\:(\d\d)(\:(\d\d))?' + ampm + '?',
                   r'(\d\d|\d)' + ampm)
    mdy    = r'((?:%s)|\d\d|\d)[-/](\d\d|\d)([-/](\d\d\d\d|\d\d))?' % \
             '|'.join(["(?:%s)" % mon for mon in months])
    mdyhms = mdy + '([\|\.]%s)?' % hms

    # set the final regexp
    regexp = group(mdyhms, hms)

    def __eq__(self, other):
        if self.getPython() == other.getPython():
            return True
        return False

    def convert(self):
        """Convert the string representing time into seconds since the
        epoch."""
        result = self.convertToInt()

        # check for relative times
        if result < 0:
            result = int(time.time()) + result

        result = "'%s'" % timeutil.formatTime(result, "%Y-%m-%d %H:%M:%S")
        return result

    def convertToInt(self):
        return int(timeutil.timestr2secs(self.text))

    def getMySQL(self, compare=None):
        """Return a string representing the token in MySQL format."""
        return self.convert()

    def getPython(self, compare=None):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        return self.convert()

    def getAst(self):
        return WhereAst.TimeInt(self.convertToInt())


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class UnquotedString(Data):
    """The unknown token is any piece of non-quoted text that does not
    contain any of the special operator characters."""

    regexp = r"[^!\(\)\[\],\<\>=\'\"\s]+"

    def getMySQL(self, compare=None):
        """Return a string representing the token in MySQL format."""
        return "'" + self.text + "'"

    def getPython(self, compare=None):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        return "'" + self.text + "'"

    def getAst(self):
        return WhereAst.Constant(self.text)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Member(Data):
    """Any non-quoted piece of text that is in the form of a member (field)
    name.  e.g. Job.user ...or... user.  The Where class will have to decide
    whether it is really a Member or not."""

    def __init__(self, token, field, primaryTable):
        """When a member token is found, it is initialized with the original
        'token', it's corresponding Field object, and a pointer to the primary
        table being used in the query."""
        Data.__init__(self, token)

        # the class this member belongs to
        self.cls    = field.classObject
        # the name of the member in the class
        self.member = field.member
        # the field object that describes what type of member this is
        self.field  = field
        # this is the primary table that is used in the query, and is used
        # when the getPython() method is called.
        self.primaryTable = primaryTable

    def __str__(self):
        return "%s.%s" % (self.cls.__name__, self.member)

    def __eq__(self, other):
        if self.field == other.field:
            return True
        return False

    def getMySQL(self, compare=None):
        """Return a string representing the token in MySQL format."""
        return self.field.getWhere()

    def getPython(self, compare=None):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""

        # if the field is from the primary table of the query, then the
        # reference should not include the class name.
        if self.field.table == self.primaryTable:
            return "MATCH_OBJ.%s" % self.member
        # otherwise include the class name
        return "MATCH_OBJ.%s.%s" % (self.cls.__name__, self.member)

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        return [self.field]

    def mask(self, tables, fields, keep):
        """Return a boolean indicating whether this Token should be masked."""

        # is this member in the provided lists?
        inlist = (self.field.table in tables or self.field in fields)
        # do we keep fields in the provided list or mask them?
        if (inlist and not keep) or (not inlist and keep):
            return True
        return False

    def getAst(self):
        return WhereAst.Member(self.primaryTable, self.field)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class VirtualMember(Member):
    """A virtual member refers to a virtual field in an object.  This
    distinction is needed so appropriate queries can be generated."""

    def mask(self, tables, fields, keep):
        """Return a boolean indicating whether this Token should be masked."""

        # is this member in the provided lists?
        inlist = (self.field.table in tables or self.field in fields)
        if not inlist:
            # if we still don't know, then check each dependent field
            for depfield in self.field.fields:
                if depfield in fields:
                    inlist = True
                    break
        # do we keep fields in the provided list or mask them?
        if (inlist and not keep) or (not inlist and keep):
            return True
        return False

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        return [self.field] + self.field.getDependentFields()

    def getAst(self):
        return WhereAst.VirtualMember(self.primaryTable, self.field)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Command(Data):
    """A Command token is any string surrounded by back tick quotes (`).
    The string will be treated as a command and executed.  The output from
    the command will be parsed as a where string."""

    regexp = r'`[^\n`\\]*(?:\\.[^\n`\\]*)*`'

    def __init__(self, token):
        """Overloaded so we can setup some defaults."""
        Data.__init__(self, token)

        # when the command is executed the output will be saved here
        self.output = None
        # if the output is split up into more tokens, then they will be here
        self.tokens = None

    def __str__(self):
        """Overloaded so our child tokens are returned instead of the
        command string."""

        # make sure we only add tokens to the list that have a string
        # representation.  For example, if one of our tokens is a Command
        # that produced no output, we don't want to add and empty string
        # to the toks list.
        toks = []
        for t in self.tokens:
            tstr = str(t)
            # make sure we have something before adding it
            if tstr:
                toks.append(tstr)
        return ', '.join(toks)

    def getTokens(self):
        """Called when a list comparison phrase is trying to expand."""

        # create a new list that recursively adds any tokens from other
        # Command tokens
        tokens = []

        for tok in self.tokens:
            # if we find another Command, then call its getTokens() method
            if tok.__class__ is Command:
                tokens.extend(tok.getTokens())
            else:
                tokens.append(tok)

        return tokens

    def getPython(self, compare=None):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        # make sure we only add tokens to the list that have a string
        # representation.  For example, if one of our tokens is a Command
        # that produced no output, we don't want to add and empty string
        # to the toks list.
        toks = []
        for t in self.tokens:
            python = t.getPython(compare=compare)
            # make sure we have something before adding it
            if python:
                toks.append(python)
        return ', '.join(toks)

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        members = []
        for token in self.tokens:
            members.extend(token.getMembers())
        return members

    def mask(self, tables, fields, keep):
        """Return a boolean indicating whether this Token should be masked."""

        # iterate through our tokens and check if the Token should be masked.
        # if we find one that is maskable, then we remove it from our list
        for tok in list(self.tokens):
            if tok.mask(tables, fields, keep):
                self.tokens.remove(tok)

        # if all our tokens have been removed, then we return True indicating
        # that we should be masked as well.
        if not self.tokens:
            return True

        return False


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Comparison(Token):
    """A comparison token is for any operator that compares two values
    (e.g. user=jag).  The following comparison operators are supported::
      =       equality
      IS      equality for NULL
      ==      equality
      !=      inequality
      >       greater than
      <       less than
      >=      greater than or equal
      <=      less than or equal
    """

    regexp = group(r"[=<>]=?", "!=")

    def addToContext(self, context, stack):
        """Comparison tokens must always be between two Data tokens."""

        # make sure a list isn't open
        if context.listVal is not None:
            raise SyntaxError("'%s' cannot be used in a list." % \
                  self.text)

        # make sure we don't already have an operator set
        if context.currOp:
            raise SyntaxError("'%s' operator is out of place." % \
                  self.text)

        # make sure we have a left operand set
        if not context.left:
            raise SyntaxError("no left operand found for '%s'" % \
                  self.text)

        # if we get here assume all is well
        context.currOp = self

    def getPhrase(self, right, context):
        """When tokens for a comparison phrase are found, the current
        comparison token in the context is called to request a phrase."""
        return ComparisonPhrase(context.left, context.currOp, right,
                                strict=context.strict)

    def getReversed(self):
        """Reverse the operator so that the operands can be flipped.
        This is only applies to greater/less than or equal operators."""
        if self.text == '>':
            text = '<'
        elif self.text == '<':
            text = '>'
        elif self.text == '<=':
            text = '>='
        elif self.text == '>=':
            text = '<='
        else:
            text = self.text
        return Comparison(text)

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        # equality is done with ==
        if self.text in ('=', 'IS'):
            return '=='
        return self.text

    def getAst(self, left, right):
        # We already guarenteed that the operator is one of these values.
        # So if this throws a KeyError, that should propagate through.
        return {
            '=':  WhereAst.Eq,
            'IS': WhereAst.Is,
            '==': WhereAst.Eq,
            '!=': WhereAst.Ne,
            '<':  WhereAst.Lt,
            '<=': WhereAst.Le,
            '>':  WhereAst.Gt,
            '>=': WhereAst.Ge,
        }[self.text.lower()](left, right)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class ListComparison(Comparison):
    """Operators for comparing other data values with lists."""

    # make sure our keywords are always anchored with a space or the end of
    # the string
    regexp = r"(in|like|has)(?=\s|$)"

    def getPhrase(self, right, context):
        """When tokens for a comparison phrase are found, the current
        comparison token in the context is called to request a phrase.
        We overload this from the parent so we can steel the not operators
        and easily support the syntax: user not in [...]"""

        op = context.currOp.text
        if op == 'in':
            cls = InPhrase
        elif op == 'like':
            cls = LikePhrase
        elif op == 'has':
            cls = HasPhrase

        phrase = cls(context.left, context.currOp, right,
                     notOp=context.notOp, strict=context.strict)

        context.notOp = None
        return phrase

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""
        if self.text == 'like':
            return LikeOperator
        return self.text

    def getAst(self, left, right):
        # We already guarenteed that the operator is one of these values.
        # So if this throws a KeyError, that should propagate through.
        return {
            'in':   WhereAst.In,
            'like': WhereAst.Like,
            'has':  WhereAst.Has,
        }[self.text.lower()](left, right)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Boolean(Token):
    """Boolean tokens are 'and' and 'or'."""

    # make sure our keywords are always anchored with a space or the end of
    # the string
    regexp = r"(and|or|AND|OR)(?=\s|$)"

    def addToContext(self, context, stack):
        """handle all booelan operators."""

        # make sure an operator isn't set
        if context.currOp:
            raise SyntaxError("no right operand found for '%s' " \
                  "before encountering '%s'." % \
                  (context.currOp.text, self.text))

        # make sure we have a left phrase for the operator.  It's
        # possible that a left operand is lingering, if so then
        # make that a group.
        if context.left:
            # add the Not operator to the StandAlone phrase so that
            # it is properly accounted for.
            if context.notOp:
                phrase = StandAlone(context.left, notOp=context.notOp)
                context.notOp = None
            else:
                phrase = StandAlone(context.left)
            context.addPhrase(phrase)

        if not context.lphrase:
            raise SyntaxError("no left phrase found for '%s'" % \
                  self.text)

        # assume all is good
        context.currBool = self
        #print "setting current boolean to", self.text

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        return self.text.lower()

    def getAst(self, left, right):
        # We already guarenteed that the operator is one of these values.
        # So if this throws a KeyError, that should propagate through.

        Class = {'and': WhereAst.And, 'or': WhereAst.Or}[self.text.lower()]

        # Try to fold the boolean expression if subexpressions are the same
        # type of boolean as this one is.
        if isinstance(left, Class) and isinstance(right, Class):
            return Class(left.exprs + right.exprs)
        elif isinstance(left, Class):
            left.exprs.append(right)
            return left
        elif isinstance(right, Class):
            right.exprs.append(left)
            return right
        else:
            return Class([left, right])


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Negate(Token):
    """Used for the 'not' operator to negate an expression."""

    # make sure our keywords are always anchored with a space or the end of
    # the string
    regexp = r"(not|NOT)(?=\s|$)"

    def addToContext(self, context, stack):
        """When we get a negate token, we just create a new context."""

        # make sure the operator isn't out of place
        if context.notOp or context.currOp:
            raise SyntaxError("'%s' is out of place." % self.text)

        context.notOp = self

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        return self.text.lower()

    def getAst(self, expr):
        return WhereAst.Not(expr)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Parenthesis(Token):
    """Parenthesis are used for logically grouping tokens."""

    regexp = r"[\(\)]"

    def addToContext(self, context, stack):
        """Create a new context and push it onto the stack."""

        # push a new phrase list onto the stack for an open paren
        if self.text == '(':
            stack.append(ParsingContext(**context.kwargs))
        # otherwise, pop the end and add the phrase to the next context
        else:
            try:
                # close the current context
                grp = context.end()
                # pop the context off the stack
                stack.pop()
                # set the current context
                context = stack[-1]
            except IndexError:
                raise SyntaxError("parentheses mismatch")
            context.addPhrase(Group([grp]))


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Bracket(Token):
    """Brackets are used to define lists."""

    regexp = r"[\[\]]"

    def addToContext(self, context, stack):
        """When brackets are found, then open or close a list."""

        if self.text == '[':
            # a list can only exist as a right operand
            if not context.left:
                raise SyntaxError("lists can only exist as right operands.")
            # make sure we don't already have one opened.
            if context.listVal is not None:
                raise SyntaxError("list must be closed before starting " \
                      "a new one.")
            # get a new one started
            context.listVal = []

        # check for the closing of a list
        elif self.text == ']':
            # make sure we opened one earlier
            if context.listVal is None:
                raise SyntaxError("']' is out of place. list was never " \
                      "opened.")

            # close out the list
            listPhrase = List(context.listVal)
            context.listVal = None
            # set the left or right operand accordingly
            if not context.left:
                context.left = listPhrase
            # otherwise, make a Comparison phrase
            else:
                phrase = context.currOp.getPhrase(listPhrase, context)
                # add the phrase and reset the needed variables
                context.addPhrase(phrase)

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class ListDelim(Token):
    """Commas are optionally used to deliminate items within a list,
    otherwise whitespace is used."""

    regexp = r"[,]"

    def addToContext(self, context, stack):
        """Make sure the list delimiter isn't out of place."""

        # if a list isn't open, then we have a problem.
        if context.listVal is None:
            raise SyntaxError("list delimiter '%s' is out of place" % \
                  self.text)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Alias(Token):
    """An alias token looks just like a Data token, but it must stand
    alone as a single phrase.  For example, we don't allow::

      Data Comparison Alias

    we could, but to simplify things we restrict alias to stand alone
    phrases."""

    def __init__(self, token, where):
        """Initialize an Alias token with the expanded Where object."""
        Token.__init__(self, token)
        # the Where object this alias expanded to
        self.where = where

    def addToContext(self, context, stack):
        """Make sure there is nothing in this context yet."""

        # there should be no left token
        if context.left:
            raise SyntaxError("aliases can only be used as stand-alone " \
                  "phrases.")

        # if the where already included parenthesis, then don't add them again.
        if self.where.root.__class__ is not Group or \
           not self.where.root.parens:
            parens = True
        else:
            parens = False
        phrase = Group([self], parens=parens)
        # add the phrase and reset the context
        context.addPhrase(phrase)

    def getTree(self, indent=''):
        return self.where.root.getTree(indent=indent + '  ')

    def __str__(self):
        return self.getNat()

    def getNat(self):
        """Return a string representing the token in the natural language
        where format."""
        return self.where.getNat()

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""
        return self.where.getMySQL()

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        return self.where.root.getPython()

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        return self.where.getMembers()

    def mask(self, tables, fields, keep):
        """Mask all references to the provided tables and fields.  If keep
        is True, then mask everything except the provided tables and fields."""

        self.where.root.mask(tables, fields, keep)
        self.where._resetCache()
        return False

    def find(self, tokens):
        """Return all the phrases that have the tokens in the provided list."""
        return self.where.root.find(tokens)

    def getAst(self):
        return self.where.getAst()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Phrase(object):
    """A logical grouping of one or more tokens."""

    def __init__(self):
        # the phrase in front of this one within the current context
        self.prev = None
        # the phrase after this one within the current context
        self.next = None

    def getNat(self):
        """Return a string representing the phrase in the natural language
        where format."""
        pass

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""
        pass

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        pass

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        pass

    def mask(self, tables, fields, keep):
        """Mask all references to the provided tables and fields.  If keep
        is True, then mask everything except the provided tables and fields."""
        pass

    def find(self, tokens):
        """Return all the phrases that have the tokens in the provided list."""
        return []

    def getAst(self):
        """Return the abstract syntax tree for this expression node."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Group(Phrase):
    """A group is one or more phrases enclosed within parentheses."""

    def __init__(self, phrases, parens=True):
        """Initialize a Group with a list of phrases."""
        Phrase.__init__(self)
        self.phrases = phrases
        self.parens  = parens

        # set the prev and next for each phrase passed in
        prev = None
        for phrase in self.phrases:
            if prev:
                prev.next   = phrase
                phrase.prev = prev
            prev = phrase

    def __str__(self):
        base = ' '.join([str(phrase) for phrase in self.phrases])
        if self.parens:
            return '(' + base + ')'
        return base

    def getNat(self):
        """Return a string representing the phrase in the natural language
        where format."""
        return str(self)

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""
        base = ' '.join([str(phrase.getMySQL()) for phrase in self.phrases])
        if self.parens:
            return '(' + base + ')'
        return base

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        base = ' '.join([str(p.getPython()) for p in self.phrases])
        if self.parens:
            return '(' + base + ')'
        return base

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        members = []
        for phrase in self.phrases:
            members.extend(phrase.getMembers())
        return members

    def getTree(self, indent=''):
        """Return a string representation of the phrase tree."""
        mystr = "%s " % self.__class__.__name__
        if self.parens: mystr += "(with parens)\n"
        else: mystr += "(no parens)\n"
        #print self.phrases

        # now add all the phrases in this group
        indent += '  '
        for phrase in self.phrases:
            mystr += indent + phrase.getTree(indent=indent) + '\n'

        return mystr

    def mask(self, tables, fields, keep):
        """Mask all references to the provided tables and fields.  If keep
        is True, then mask everything except the provided tables and fields."""
        # pass this onto our phrases
        for phrase in self.phrases:
            phrase.mask(tables, fields, keep)

    def find(self, tokens):
        """Return all the phrases that have the tokens in the provided list."""
        # keep a list of the phrases we find
        phrases = []
        for phrase in self.phrases:
            phrases.extend(phrase.find(tokens))
        return phrases

    def getAst(self):
        """
        We assume that Groups only contain one item, so we'll return the
        AST of that node.
        """

        if len(self.phrases) != 1:
            raise MatchError('do not know how to handle Groups with more ' \
                    'than one object')

        return self.phrases[0].getAst()

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class NegatePhrase(Group):
    """When a phrase has a Negate token infront of it, a NegatePhrase
    is created to ensure the Negate is properly handled."""

    def __init__(self, negate, phrase):
        # initialize the base class
        Group.__init__(self, [negate, phrase], parens=False)

        # save some pointers
        self.negate = negate
        self.phrase = phrase

    def getAst(self):
        return WhereAst.Not(self.phrase.getAst())


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class StandAlone(Group):
    """A Stand-alone token can be used in a query, like when saying
    'foo and bar' or something similar."""

    def __init__(self, token, notOp=None):
        # keep track of the token and notOp
        self.token = token
        self.notOp = notOp
        # call the base class
        if notOp:
            toks = [notOp, token]
        else:
            toks = [token]
        Group.__init__(self, toks, parens=False)

    def mask(self, tables, fields, keep):
        """Mask all references to the provided tables and fields.  If keep
        is True, then mask everything except the provided tables and fields."""

        # should we mask?
        if self.token.mask(tables, fields, keep):
            # reset the phrases list (note this also gets rid of any
            # Negate token that might have been infront.)
            self.token   = ComparisonPhrase(Number('0'),
                                            Comparison('='),
                                            Number('0'),
                                            strict=False)
            self.notOp   = None
            self.phrases = [self.token]

    def getMySQL(self):
        """Overloaded so we can properly handle virtual fields."""
        # pass this off to the virtual member
        if self.token.__class__ is VirtualMember:
            return self.token.field.getWhere_StandAlone(self)

        # if the member is a string, then we need to use ='' or <>''
        if self.token.__class__ is Member:
            if isinstance(self.token.field, DBFields.TimestampField):
                # get the base
                base = self.token.getMySQL()
                # the operator is based on whether the not op is present
                # while the logic looks inverted, it's supposed to be! when the where clause
                # indicates "not fieldname", then you want "fieldname *IS* NULL"
                if self.notOp:
                    op = "IS"
                else:
                    op = "IS NOT"
                return "%s %s NULL" % (base, op)

            elif isinstance(self.token.field, (DBFields.NumberField, DBFields.FloatField)):
                base = self.token.getMySQL()
                # the operator is based on whether the not op is present
                if self.notOp:
                    op = "="
                else:
                    op = "<>"
                mysql = "%s%s0" % (base, op)

                # add a second portion to the query incase the value is NULL
                if self.notOp:
                    op = "is"
                    mid = "OR"
                else:
                    op = "is NOT"
                    mid = "AND"

                return "(%s %s %s %s NULL)" % (mysql, mid, base, op)

            elif isinstance(self.token.field, DBFields.TextField):
                base = self.token.getMySQL()
                # the operator is based on whether the not op is present
                if self.notOp:
                    op = "="
                else:
                    op = "<>"
                mysql = "%s%s''" % (base, op)

                # add a second portion to the query incase the value is NULL
                if self.notOp:
                    op = "is"
                    mid = "OR"
                else:
                    op = "is NOT"
                    mid = "AND"

                return "(%s %s %s %s NULL)" % (mysql, mid, base, op)

        return Group.getMySQL(self)

    def getAst(self):
        if self.token.__class__ is VirtualMember:
            # the virtual member handles the self.notOp.
            return self.token.field.getAst_StandAlone(self)

        ast = self.token.getAst()

        if self.notOp:
            return WhereAst.Not(ast)
        else:
            return ast


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class Logic(Phrase):
    """A logic phrase is used for 'and' and 'or' boolean operators and has
    a left and right phrase."""

    def __init__(self, left, op, right):
        Phrase.__init__(self)
        self.left  = left
        self.op    = op
        self.right = right
        #print "creating Logic", self.left, self.op, self.right

    def __str__(self):
        return "%s %s %s" % (self.left, self.op, self.right)

    def getNat(self):
        """Return a string representing the phrase in the natural language
        where format."""
        return str(self)

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""
        return "%s %s %s" % (self.left.getMySQL(),
                             self.op.getMySQL(),
                             self.right.getMySQL())

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        return "%s %s %s" % (self.left.getPython(),
                             self.op.getPython(),
                             self.right.getPython())

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        members = []
        for phrase in (self.left, self.right):
            members.extend(phrase.getMembers())
        return members

    def mask(self, tables, fields, keep):
        """Mask all references to the provided tables and fields.  If keep
        is True, then mask everything except the provided tables and fields."""
        self.left.mask(tables, fields, keep)
        self.right.mask(tables, fields, keep)

    def find(self, tokens):
        """Return all the phrases that have the tokens in the provided list."""
        phrases = []
        for phrase in (self.left, self.right):
            phrases.extend(phrase.find(tokens))
        return phrases

    def getTree(self, indent=''):
        """Return a string representation of the phrase tree."""
        indent += '  '
        mystr   = "Logic (%s)\n" % self.op
        mystr  += indent + self.left.getTree(indent=indent).rstrip('\n') + '\n'
        mystr  += indent + ("*** %s ***\n" % self.op)
        mystr  += indent + self.right.getTree(indent=indent)
        return mystr

    def getAst(self):
        return self.op.getAst(self.left.getAst(), self.right.getAst())


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class ComparisonPhrase(Phrase):
    """A comparison phrase is used for the =,!=,>,<,<=,>= operators and
    has a left and right token."""

    def __init__(self, left, op, right, strict=True):
        Phrase.__init__(self)
        self.left  = left
        self.op    = op
        self.right = right

        # make sure at least one Member Token is found
        if strict and \
           not issubclass(self.left.__class__, Member) and \
           not issubclass(self.right.__class__, Member):
            raise NoMemberFound("both %s and %s don't map to valid " \
                  "member names in \"%s\"" % (self.left, self.right, self))

        # virtual fields can only be used when comparing with a non-member
        if issubclass(self.left.__class__, Member) and \
           issubclass(self.right.__class__, Member) and \
            (self.right.__class__ is VirtualMember or \
             self.left.__class__ is VirtualMember):
            raise IncorrectType("virtual fields cannot be compared with " \
                  "other members.")

    def __str__(self):
        # it's easier to read the phrases with >, <, >= and <= if there are
        # spaces between the tokens
        if self.op.text in ('=', '!=', 'IS'):
            format = "%s%s%s"
        else:
            format = "%s %s %s"
        return format % (self.left, self.op, self.right)

    def getNat(self):
        """Return a string representing the phrase in the natural language
        where format."""
        return str(self)

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""

        # virtual fields have to return a valid sql string for the query
        if self.left.__class__ is VirtualMember:
            return self.left.field.getWhere_ComparisonPhrase(self,
                                                             self.right,
                                                             left=True)
        elif self.right.__class__ is VirtualMember:
            return self.right.field.getWhere_ComparisonPhrase(self,
                                                              self.left,
                                                              left=False)

        # it's easier to read the phrases with >, <, >= and <= if there are
        # spaces between the tokens
        if self.op.text in ('=', '!=', 'IS'):
            format = "%s%s%s"
        else:
            format = "%s %s %s"
        return format % (self.left.getMySQL(self.right),
                         self.op.getMySQL(),
                         self.right.getMySQL(self.left))

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        # it's easier to read the phrases with >, <, >= and <= if there are
        # spaces between the tokens
        if self.op.text in ('=', '!=', 'IS'):
            format = "%s%s%s"
        else:
            format = "%s %s %s"
        return format % (self.left.getPython(self.right),
                         self.op.getPython(),
                         self.right.getPython(self.left))

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        # virtual fields can return a subset of their dependent fields.
        if self.left.__class__ is VirtualMember:
            return self.left.field.getDependentFields(self.right)
        elif self.right.__class__ is VirtualMember:
            return self.right.field.getDependentFields(self.left)

        members = []
        for phrase in (self.left, self.right):
            members.extend(phrase.getMembers())
        return members

    def mask(self, tables, fields, keep):
        """Mask all references to the provided tables and fields.  If keep
        is True, then mask everything except the provided tables and fields."""

        # if the left or right operands are Member tokens, then check if
        # we should mask them.
        for tok in (self.left, self.right):
            # should it be masked?
            if tok.mask(tables, fields, keep):
                # to mask, we make the phrase always be True
                self.left  = Number('0')
                self.op    = Comparison('=')
                self.right = Number('0')
                break

    def _match(self, mytoks, usertoks):
        """Compare our tokens from what the user passed in via the find()
        method.  All the tokens in 'usertoks' must be matched with 'mytoks',
        otherwise no match."""

        # make sure usertoks is <= to mytoks
        if len(usertoks) > len(mytoks):
            return False

        # we are only going to iterate based on the size of usertoks
        for i in range(len(usertoks)):
            # if the tokens are not equal, then abort
            if mytoks[i] != usertoks[i]:
                return False

        # assume we have a match
        return True

    def find(self, tokens):
        """Return all the phrases that have the tokens in the provided list."""

        # create a list of our tokens that we can compare against
        mytoks = (self.left, self.op, self.right)
        # check for a match
        if self._match(mytoks, tokens):
            return [self]

        # if nothing was found, then reverse the tokens incase they were
        # specified in the opposite manner
        mytoks = (self.right, self.op.getReversed(), self.left)
        if self._match(mytoks, tokens):
            return [self]

        # we found no match
        return []

    def getTree(self, indent=''):
        """Return a string representation of the phrase tree."""
        return "Comparison: " + str(self)

    def getAst(self):
        return self.op.getAst(self.left.getAst(), self.right.getAst())


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class List(Phrase):
    """A List phrase is a list of Data tokens."""

    def __init__(self, tokens):
        Phrase.__init__(self)
        self.tokens = tokens

    def __str__(self):
        return '[' + ', '.join([str(item) for item in self.tokens]) + ']'

    def getNat(self):
        """Return a string representing the phrase in the natural language
        where format."""
        return str(self)

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""
        return str(self)

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""
        return '[' + ', '.join([i.getPython() for i in self.tokens]) + ']'

    def getMembers(self):
        """Return a list of all the class members that are referenced."""
        members = []
        for token in self.tokens:
            members.extend(token.getMembers())
        return members

    def mask(self, tables, fields, keep):
        """Return a boolean indicating whether this Token should be masked."""

        # iterate through our tokens and check if the Token should be masked.
        # if we find one that is maskable, then we remove it from our list
        for tok in list(self.tokens):
            if tok.mask(tables, fields, keep):
                self.tokens.remove(tok)

        # if all our tokens have been removed, then we return True indicating
        # that we should be masked as well.
        if not self.tokens:
            return True

        return False

    def getAst(self):
        return WhereAst.List([t.getAst() for t in self.tokens])


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class ListComparisonPhrase(ComparisonPhrase):
    """Used for comparing a single name or value against a list of name or
    values.  The left value will be a name or value token, and the right
    value will be a List Phrase."""

    def __init__(self, left, op, right, notOp=None, strict=True):
        # we keep track of any not operators with lists so we can properly
        # support the syntax: user not in [...]
        self.notOp = notOp
        ComparisonPhrase.__init__(self, left, op, right, strict=strict)

        # unroll the List if we have one
        self.phrases = self._expandList()

    def __str__(self):
        # if we have a not operator, then slip it before the list operator
        if self.notOp:
            return "%s %s %s %s" % (self.left, self.notOp, self.op, self.right)
        return "%s %s %s" % (self.left, self.op, self.right)

    def _getComparisonPhrase(self, item):
        """This should be overloaded by subclasses and return a valid
        ComparisonPhrase with the passed in 'item'.  This is called when
        unrolling the List operands."""
        pass

    def _expandList(self):
        """A convenience method for subclasses that want to unroll the
        list phrases."""

        # make sure we are dealing with a List
        if self.right.__class__ is not List:
            return None

        phrases = []
        # iterate through each item in the list and make a new
        # ComparisonPhrase object
        for tok in self.right.tokens:
            # since Command tokens can end up having their own list of tokens
            # we generalize it
            if tok.__class__ is Command:
                items = tok.getTokens()
            else:
                items = [tok]

            # now iterate over all the items
            for item in items:
                # ask the subclass to create a ComparisonPhrase
                phrase = self._getComparisonPhrase(item)
                # add it to our list
                phrases.append(phrase)

        return phrases

    def getNat(self):
        """Return a string representing the phrase in the natural language
        where format."""
        return str(self)

    def getMySQL(self, joinwith):
        """A generalized method for returning a mysql statement for a
        list phrase.  This joins the unrolled phrases (created with
        _expandList) with one of the strings in the 'joinwith' list.
        The first item in the list is used if self.notOp is False,
        otherwise, the second is used."""

        # if we do have an empty List phrase, then always return a False
        # statement
        if len(self.phrases) == 0:
            # if the not operator is present, then we need to return a
            # true statement
            if self.notOp:
                return "0=0"
            # if the not operator is NOT present, then we must return a
            # false statement
            return "0!=0"

        # otherwise, we want to AND or OR the phrase together depending
        # on the placement of a Negate.
        if self.notOp:
            boolean = joinwith[1]
        else:
            boolean = joinwith[0]

        boolean = ' ' + boolean + ' '
        joined  = boolean.join([phrase.getMySQL() for phrase in self.phrases])

        # determine if we should put parenthesis around this expression
        if len(self.phrases) > 1:
            return "(%s)" % joined
        return joined

    def getPython(self, joinwith):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""

        # if we do have an empty List phrase, then always return a False
        # statement
        if len(self.phrases) == 0:
            # if the not operator is present, then we need to return a
            # true statement
            if self.notOp:
                return "0=0"
            # if the not operator is NOT present, then we must return a
            # false statement
            return "0!=0"

        # otherwise, we want to AND or OR the phrase together depending
        # on the placement of a Negate.
        if self.notOp:
            boolean = joinwith[1]
        else:
            boolean = joinwith[0]

        boolean = ' ' + boolean + ' '
        joined  = boolean.join([p.getPython() for p in self.phrases])

        # determine if we should put parenthesis around this expression
        if len(self.phrases) > 1:
            return "(%s)" % joined
        return joined

    def mask(self, tables, fields, keep):
        """Mask all references to the provided tables and fields.  If keep
        is True, then mask everything except the provided tables and fields."""

        # we use the same method as the base class
        ComparisonPhrase.mask(self, tables, fields, keep)
        # always regenerate the phrases incase items from our list were removed
        self.phrases = self._expandList()

    def find(self, tokens):
        """Return all the phrases that have the tokens in the provided list."""

        # if the left and right operands are not a List phrase, then let
        # the parent class handle this
        if self.phrases is None:
            return ComparisonPhrase.find(self, tokens)

        found = []
        # iterate over each of the expanded phrases
        for phrase in self.phrases:
            found.extend(phrase.find(tokens))
        return found

    def getTree(self, indent=''):
        """Return a string representation of the phrase tree."""
        return "%s: %s" % (self.__class__.__name__, str(self))


    def getAst(self):
        expr = self.op.getAst(self.left.getAst(), self.right.getAst())

        if self.notOp:
            return WhereAst.Not(expr)

        return expr


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class InPhrase(ListComparisonPhrase):
    """The 'in' operator is used to check for items within a list.  It can
    be used in the form "user in [jag, adam]" or "cars in crews"."""

    def __init__(self, left, op, right, notOp=None, strict=True):
        """Initialized no differently than a ListComparisonPhrase, but
        we make some checks to ensure the operands are the proper type."""
        ListComparisonPhrase.__init__(self, left, op, right,
                                      notOp=notOp, strict=strict)

        # we only allow the 'in' operator if the right operand is a list.
        # the list can be a Member or a List phrase.
        if self.right.__class__ not in (List, Member):
            raise IncorrectType("right operand of 'in' must be a list")
        elif self.right.__class__ is Member and \
             type(self.right.field.getDefault()) is not list:
            raise IncorrectType("right operand of 'in' must be a list")

    def _getComparisonPhrase(self, item):
        """Return a valid ComparisonPhrase."""

        # figure out which operator to use
        if self.notOp:
            op = '!='
        else:
            op = '='

        return ComparisonPhrase(self.left, Comparison(op), item)

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""

        # if the left and right are already equal, then don't bother with
        # the REGEXP operator
        if self.phrases is None and \
           self.left.getMySQL() == self.right.getMySQL():
            return "%s=%s" % (self.left.getMySQL(), self.right.getMySQL())

        # if we don't have any expanded phrases, then assume the right operand
        # is a Member.
        if self.phrases is None:
            # if one of our fields is
            # the 'in' operator is tricky since list values are actually stored
            # in the database as strings, we need to construct a regexp to
            # check for the existence of the left token.
            op = "=" if not self.notOp else "!="
            return "%s %s ANY(%s)" % (self.left.getMySQL(), op, self.right.getMySQL())

        # use the base class to do the rest and tell it our preference
        # for joining the list items
        return ListComparisonPhrase.getMySQL(self, ('OR', 'AND'))

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""

        # the 'in' operator is native to Python, so keep it in the syntax
        # if we have a not operator, then slip it before the list operator
        if self.notOp:
            op = "%s %s" % (self.notOp.getPython(), self.op.getPython())
        else:
            op = self.op.getPython()
        return "%s %s %s" % (self.left.getPython(), op, self.right.getPython())


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class LikePhrase(ListComparisonPhrase):
    """The 'like' operator is used to perform regular expressions."""

    def _getComparisonPhrase(self, item):
        """Return a valid ComparisonPhrase."""
        return LikePhrase(self.left, self.op, item, notOp=self.notOp)

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""

        # if the left and right are already equal, then don't bother with
        # the REGEXP operator
        if self.phrases is None and \
           self.left.getMySQL() == self.right.getMySQL():
            return "%s=%s" % (self.left.getMySQL(), self.right.getMySQL())

        # if the right operand was NOT a List
        if self.phrases is None:
            # virtual fields have to return a valid sql string for the query
            if self.left.__class__ is VirtualMember:
                return self.left.field.getWhere_ComparisonPhrase(self,
                                                                 self.right,
                                                                 left=True)
            elif self.right.__class__ is VirtualMember:
                return self.right.field.getWhere_ComparisonPhrase(self,
                                                                  self.left,
                                                                  left=False)

            # if the db field is a string array or json, cast it as text to use regexp
            leftSQL = self.left.getMySQL()
            if isinstance(self.left.field, (DBFields.StrArrayField, DBFields.JSONField)):
                leftSQL += "::text"
                            
            if self.notOp:
                op = " %s " % NotLikeOperator
            else:
                op = " %s " % LikeOperator
            return leftSQL + op + self.right.getMySQL()

        # otherwise, join the List
        return ListComparisonPhrase.getMySQL(self, ('OR', 'AND'))

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""

        # if the right operand was NOT a List
        if self.phrases is None:
            # if the Data token we are comparing is a list Field, then join
            # the items.  hacky, but it works.
            if self.left.__class__ is Member and \
               issubclass(self.left.field.__class__, DBFields.StrListField):
                left = "' '.join(%s)" % self.left.getPython()
            else:
                left = self.left.getPython()

            # don't forget the Negate
            if self.notOp:
                nop = "not "
            else:
                nop = ""

            if self.right.getPython() == "0" and left == "0":
                # AWG HACK: this seems to be a special case where masking is involved
                return "0==0"
            else:
                return "%sre.search(%s, %s or '')" % (nop, self.right.getPython(), left)

        # otherwise, join the List
        return ListComparisonPhrase.getPython(self, ('or', 'and'))


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class HasPhrase(ListComparisonPhrase):
    """The 'has' operator is a short-cut for phrases that include one or
    more 'in' phrases ANDed together.  For example::
      cars in crews and rat in crews
    can be replaced with::
      crews has [cars, rat]
    """

    def __init__(self, left, op, right, notOp=None, strict=True):
        """Initialized no differently than a ListComparisonPhrase, but
        we make some checks to ensure the operands are the proper type."""
        ListComparisonPhrase.__init__(self, left, op, right,
                                      notOp=notOp, strict=strict)

        # make sure we have all the proper types
        if self.left.__class__ is not Member or \
           type(self.left.field.getDefault()) is not list:
            raise IncorrectType("left operand of 'has' must be a list")
        elif self.right.__class__ is not List:
            raise IncorrectType("right operand of 'has' must be a list")

    def _getComparisonPhrase(self, item):
        """Return a valid ComparisonPhrase."""
        return InPhrase(item, ListComparison('in'), self.left,
                        notOp=self.notOp)

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""
        # nothing special for this phrase
        return ListComparisonPhrase.getMySQL(self, ('AND', 'OR'))

    def getPython(self):
        """Return a string that can be executed as a Python statement to
        test whether an object from the queried table matches the query."""

        # the 'has' operator is not supported in Python, so we unroll the list
        return ListComparisonPhrase.getPython(self, ('and', 'or'))

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

class ParsingContext(object):
    """A container object used while parsing tokens."""

    def __init__(self, **kwargs):
        # options that are passed in
        for key,val in list(kwargs.items()):
            setattr(self, key, val)
        # save the args so we can pass them onto other instances
        self.kwargs   = kwargs.copy()

        # keep track of the current left operand
        self.left     = None
        # also keep track of the current comparison operator
        self.currOp   = None

        # keep track of the current left phrase for booleans
        self.lphrase  = None
        # keep track of the current boolean
        self.currBool = None

        # keep track of whether a negate operator is present
        self.notOp    = None

        # note when a list is opened.
        self.listVal  = None

    def addPhrase(self, phrase):
        """Add a phrase to the current group stack and reset variables
        that need to be."""

        #print phrase

        # adding a phrase always resets the current left operand and operator
        self.left   = None
        self.currOp = None

        # check if this comparison phrase is out of place
        if self.lphrase and not self.currBool:
            raise SyntaxError("'%s' and '%s' must have a logical 'and' or " \
                  "'or' in-between them." % (self.lphrase, phrase))
        # add a new logic phrase
        elif self.currBool:
            # if the notOp is in front, then make a NegatePhrase
            if self.notOp:
                phrase = NegatePhrase(self.notOp, phrase)
                self.notOp = None

            logic = Logic(self.lphrase, self.currBool, phrase)
            # the entire logic phrase now becomes the left phrase
            self.lphrase  = logic
            # reset the current boolean operator
            self.currBool = None
        # set the left phrase if it isn't set
        elif not self.lphrase:
            #print "no left phrase is set"
            # if the notOp is in front, then make a NegatePhrase
            if self.notOp:
                self.lphrase = NegatePhrase(self.notOp, phrase)
                self.notOp   = None
            else:
                self.lphrase = phrase
        else:
            raise SyntaxError("no phrase was added")

    def end(self):
        """Called when the current context is finished."""

        # no lists should be opened
        if self.listVal is not None:
            raise SyntaxError("list was not closed, must be closed with a ']'")
        # make sure the operator isn't set
        if self.currOp:
            raise SyntaxError("no right operand for '%s%s'" % \
                  (self.left.text, self.currOp.text))
        # make sure the not operator isn't out of place
        if self.notOp and not (self.left or self.lphrase):
            raise SyntaxError("not operator must preceed an expression.")

        # check for a single token that will be used in a logic phrase
        phrase = None
        if self.left:
            phrase = StandAlone(self.left, notOp=self.notOp)
            # if this is the only thing for this context, then return now
            if not self.lphrase:
                return phrase

        #print "left phrase set", phrase, self.notOp
        # if the left phrase is all we got, then return it
        if self.lphrase and not self.currBool and not phrase:
            if self.notOp:
                return NegatePhrase(self.notOp, self.lphrase)
            return self.lphrase

        # check if a boolean is left dangling
        if self.lphrase and self.currBool and not phrase:
            raise SyntaxError("no expression to the right of '%s'" % \
                  self.currBool.text)

        return Logic(self.lphrase, self.currBool, phrase)

class Tokenizer(object):
    """A mix-in class to give objects the ability to scan an input string and
    split it up into Token objects."""

    # list of token types that will be searched when scanning the input string
    _Tokens = []

    def _getTokenRE(cls):
        """Get the regular expression object that will be used to scan the
        input string for tokens."""

        # return the current object if we already created one
        try:
            return cls.__dict__['_TokenRE']
        except KeyError:
            pass

        # name each token regexp and join them to form one master regexp
        tokres = []
        # define the lookup table
        cls._nameToTokenCls = {}
        for tokcls in cls._Tokens:
            # name the group after the class
            tokres.append("(?P<%s>%s)" % (tokcls.__name__, tokcls.regexp))
            # add the token class to the lookup so we can find it when
            # we get a match.
            cls._nameToTokenCls[tokcls.__name__] = tokcls

        # join all the tokens together and add a check to skip over whitespace
        cls._TokenRE = re.compile(r'\s*' + ("(?:%s)" % '|'.join(tokres)))
        return cls._TokenRE
    # make this a class method so that it will only be called once
    _getTokenRE = classmethod(_getTokenRE)

    def _getTokens(cls, str):
        """Return a list of Token objects found by scanning the passed in
        string."""

        # make sure we get rid of all the newline chars from the beg/end
        line = str.strip()
        pos  = 0
        end  = len(line)

        # get the regular expression object to parse the string
        tokenre = cls._getTokenRE()

        # keep track of the previous token
        prev = None

        # all tokens will be saved here
        tokens = []

        # iterate through the string until we get to the end
        while pos < end:
            match = tokenre.match(line, pos)
            # we should always have a match
            if not match:
                raise TokenizeError("syntax error in where string near: " \
                      "'%s'" % line[pos:])

            # get the name of the match so we know what type of token we found
            toktype = match.lastgroup
            # get the indexes of the match
            tstart,tend = match.span(toktype)
            # pull out the text that was matched
            token = line[tstart:tend]
            # move the current position to the first character after the match
            pos   = tend

            # get the class object for this token and make an instance
            tokobj = cls._nameToTokenCls[toktype](token)

            # set the previous
            if prev:
                prev.next = tokobj
                tokobj.prev = prev
            prev = tokobj

            # add it to the list
            tokens.append(tokobj)

        return tokens
    _getTokens = classmethod(_getTokens)


class Where(Tokenizer):
    """Parses a where string and provides methods to translate the string,
    search through it, and alter it.

    @type members: list
    @type mysql: string
    @type nat: string
    """

    # list the token types we will search for in each where string.
    # they should be listed in the order they need to be in the regular
    # expression.
    _Tokens = [Date, ByteNumber, TimeString, Number, Comparison,
               Parenthesis, Bracket, ListDelim, Boolean, ListComparison,
               Negate, Command, String, UnquotedString]

    def __init__(self, wstr, database, table=None, aliases=None,
                 strict=True, _prevAliases=None):
        """
        @param wstr: a natural language where string
        @type wstr:  string

        @param database: A subclass of L{Database.Database} that will be
          queried.
        @type database: L{Database.Database} subclass

        @param table: The L{Table.Table} instance that will be queried.
          Setting this helps establish a search order for evaluating class
          member names and aliases.
        @type table: L{Table.Table} instance

        @param aliases: A separate set of aliases that will completely
          override those defined in the passed in L{Database.Database}
          subclass.
        @type aliases: dictionary

        @param strict: Setting this to False relaxes some of the syntax
          checking, allowing comparison phrases to not contain at least
          one member reference.
        @type strict: boolean
        """

        # the natural language where string passed in by the user
        self.wstr     = wstr
        # the database we are quering
        self.database = database
        # the primary table used when searching
        self.table = table
        # a dictionary of all the where aliases that will be used.  If
        # nothing is provided, then use the defaults provided by the database
        self.aliases  = aliases
        # if True, then make sure all Comparison phrases have at least
        # one member referenced.  This means phrases like 'foo'='bar' are
        # not accepted
        self.strict   = strict
        # a list of all the previously referenced aliases.  This gets passed
        # around when we are recursively creating new Where objects for
        # aliases referenced within other aliases.  This is used to prevent
        # an infinite loop of alias references.
        if _prevAliases is None:
            _prevAliases = []
        self._prevAliases = _prevAliases

        # when the string is parsed, it will be broken down into a tree
        # of Phrase objects.
        self.root = None

        # parse the where string
        self._parse()

        # we cache results from the get??? methods
        self._resetCache()

    def _parse(self):
        """Iterate through the where string and handle tokens as they are
        found."""

        # get the tokens for our where string
        tokens = self._getTokens(self.wstr)

        # we keep track of all the information relevant to each group in
        # a ParsingContext object.  These are pushed/poped from the stack as
        # parenthesis are encountered.
        stack = [ParsingContext(strict=self.strict)]

        # iterate through each one and start the parsing
        for token in tokens:
            # check if we have a handle_ method defined for this token
            try:
                func = getattr(self, "_handle_" + token.__class__.__name__)
            # if not, then create it ourselves and call 'addToContext'
            except AttributeError:
                pass
            else:
                newtok = func(token, stack[-1], stack)
                # if the token was altered, then update the prev and next ptrs
                if newtok:
                    newtok.prev = token.prev
                    newtok.next = token.__next__
                    if token.prev:
                        token.prev.next = newtok
                    if token.__next__:
                        token.next.prev = newtok
                    token = newtok

            # add this token to the current context
            token.addToContext(stack[-1], stack)

        # all the tokens have been looked at, so make sure there aren't any
        # loose ends
        self._end(stack[-1], stack)

    def _end(self, context, stack):
        """Once all tokens have been handled, we close out everything with
        this function."""

        # make sure there isn't a parenthesis mismatch
        if len(stack) != 1:
            raise SyntaxError("parentheses mismatch")

        # close out the current context
        self.root = context.end()

    def _handle_Command(self, token, context, stack):
        """If a command is found, then we need to execute it and feed the
        output into another Where object."""

        # first make sure this won't cause an infinite loop
        if token.text in self._prevAliases:
            raise InfiniteLoop("calling %s again will " \
                  "cause an infinite loop." % token.text)

        # execute the command
        rcode,out,err = osutil.runCommand(token.text.strip("`"))
        # strip the string of whitespace
        out = out.strip()

        # update our _prevAliases list so we don't have an infinite loop
        prev = self._prevAliases[:]
        prev.append(token.text)

        # figure out what we should do with the output.  If the token is
        # positioned to be a stand-alone token, then treat the output is
        # another where string.
        if not (context.left or issubclass(token.next.__class__, Comparison)):
            # get another Where object, but make sure we create another
            # one with the same type as ourself
            where = self.__class__(out, self.database,
                                   table=self.table, aliases=self.aliases,
                                   _prevAliases=prev)

            # treat this is an Alias
            return Alias(token.text, where)

        # otherwise, we assume the output is supposed to be a left or right
        # operand for a comparison phrase.  If a list is open, then we want
        # to add all the tokens found in the command's output to the current
        # context's list.
        if context.listVal is not None:
            # send the output of the command through a specialized Tokenizer
            cmdout = CommandOutput(out, self.database, self.table,
                                   prevCommands=prev)
            # save the new tokens
            token.output = out
            token.tokens = cmdout.tokens
            return None

        # if we aren't part of a list, then we want the output treated as
        # a string.  However, if the output is a single token (non-quoted,
        # and no spaces), we want to check for other Data token types.
        if re.match(String.regexp, out):
            return String(out)
        elif re.search("\s", out):
            return String("'%s'" % out)

        # check for string for other data types
        cmdout = CommandOutput(out, self.database, self.table,
                               prevCommands=prev)
        # return whatever it found
        if len(cmdout.tokens) == 1:
            return cmdout.tokens[0]
        # otherwise, return the whole thing as a string
        return String("'%s'" % out)


    def _handle_UnquotedString(self, token, context, stack):
        """If an unquoted string token match is found, then call this
        method so it can be classified properly as a Member or Alias."""

        # check if the token is in a position to be an Alias.  Aliases can
        # only exist as stand-alone phrases, so the next token cannot be
        # part of a comparison phrase.
        if not (context.left or \
                issubclass(token.next.__class__, Comparison) or \
                (token.next.__class__ is Negate and \
                 issubclass(token.next.next.__class__, Comparison))):
            # Ask the database if an alias exists, if the object wasn't
            # created with a list of aliases
            aliasStr = None
            if self.aliases:
                aliasStr = self.aliases.get(token.text)
            if not aliasStr:
                aliasStr = self.database.getWhereAlias(token.text,
                                                       table=self.table)

            # if we found something, then parse it
            if aliasStr:
                # first make sure this won't cause an infinite loop
                if token.text in self._prevAliases:
                    raise InfiniteLoop("referencing '%s' in where " \
                          "statement will cause an infinite loop." % token.text)
                # update our _prevAliases list
                prev = self._prevAliases[:]
                prev.append(token.text)
                # get another Where object, but make sure we create another
                # one with the same type as ourself
                where = self.__class__(aliasStr, self.database,
                                       table=self.table, aliases=self.aliases,
                                       strict=self.strict, _prevAliases=prev)

                # create an Alias token
                return Alias(token.text, where)

            # query the Database object to see if the token refers to a class
            # member with a field
            field = self.database.fieldByMember(token.text, table=self.table)
            # if we find a match, then create a Member token and add it
            # to the current context
            if field:
                # is this virtual?
                if issubclass(field.__class__, DBFields.VirtualField):
                    return VirtualMember(token.text, field, self.table)
                return Member(token.text, field, self.table)

            # if we didn't find anything, then raise an exception if under
            # strict mode
            if self.strict:
                raise NoMemberFound("'%s' does not map to a valid member " \
                      "or alias." % token.text)


        # query the Database object to see if the token refers to a class
        # member with a field
        field = self.database.fieldByMember(token.text, table=self.table)
        # if we find a match, then create a Member token and add it
        # to the current context
        if field:
            # is this virtual?
            if issubclass(field.__class__, DBFields.VirtualField):
                return VirtualMember(token.text, field, self.table)
            return Member(token.text, field, self.table)

        # if nothing was found, then return it as a String
        return String("'%s'" % token.text)

    def _resetCache(self):
        """Reset all the cache variables for this query.  This should be
        called whenever the query is modified."""
        self.__nat     = None
        self.__mysql   = None
        self.__python  = None
        self.__members = None

    def __str__(self):
        return self.getNat()

    def getNat(self):
        """
        @return: A string representing the phrase in the natural
        language  where format.  This will be syntactically equivalent to
        what the object was initialized with.
        @rtype: string
        """
        if self.__nat is not None:
            return self.__nat
        self.__nat = self.root.getNat()
        return self.__nat
    nat = property(fget=getNat)

    def getMySQL(self):
        """
        @return: The MySQL equivalent of this where string.
        @rtype: string
        """
        if self.__mysql is not None:
            return self.__mysql
        self.__mysql = self.root.getMySQL()
        return self.__mysql
    mysql = property(fget=getMySQL)

    def getMembers(self):
        """
        @return: List of all the Field objects from class members that
        are referenced in the query.
        @rtype: list
        """
        if self.__members is not None:
            return self.__members
        # get a list of all the members
        membs = self.root.getMembers()
        # now make sure we only return unique fields
        temp = {}
        for mem in membs:
            temp[mem] = True
        self.__members = list(temp.keys())
        return self.__members
    members = property(fget=getMembers)

    def getAst(self):
        """
        @return: Construct the Ast of the expression.
        @rtype: Ast
        """
        return self.root.getAst()
    ast = property(fget=getAst)

    _shortcut_re = re.compile("^([a-zA-Z]\w*)\.\*$")
    def mask(self, members, keep=False):
        """Mask one or more member (field) references with a True statement.
        This is useful for simplifying a query to prevent a table join.

        @param members: list of member names that should be masked.
                        Example: ['song', 'Album.name', 'Artist.*']
        @type members: list

        @param keep: if True, then all members *except* those in the
                     passed in list will be masked.
        @type keep: boolean
        """

        # members can be a list or a single argument
        if type(members) not in (list, tuple):
            members = [members]

        # get a list of all the tables and fields to pass along
        tables = []
        fields = []
        for mem in members:
            # check for the shortcut syntax to specify all members in a class
            match = self._shortcut_re.match(mem)
            if match:
                # try to find a table matching the class
                table = self.database.tableByClassName(match.group(1))
                if table and table not in tables:
                    tables.append(table)

            # otherwise, search for a field
            else:
                field = self.database.fieldByMember(mem, table=self.table)
                if field and field not in fields:
                    fields.append(field)

        # pass everything onto the root phrase
        self.root.mask(tables, fields, keep)
        # we reset the cached data now that the query has potentially changed
        self._resetCache()

    def find(self, phrase, remainder=False):
        """Search the where string for all instances of the provided phrase.
        The phrase can be partial or complete.

        @param phrase: If you want to know if the query contains the phrase
          'foo=bar', you could pass in 'foo', 'foo=', or 'foo=bar'.
        @type phrase: string

        @param remainder: If 'remainder' is set to True, then the portion of
          the phrase after the match is returned.  For example, searching
          for 'foo=' would return 'bar'.
        @type remainder: boolean
        """

        # split the phrase into tokens
        tokens = self._getTokens(phrase)
        # make sure all unquoted strings are handled
        parsed = []
        for token in tokens:
            # we don't check for aliases here, so if an unquoted string is
            # found, then check if it should be a Member
            # if the type is UnquotedString, then call the handler
            if token.__class__ is UnquotedString:
                # query the Database object to see if the token refers to a
                # class member with a field
                field = self.database.fieldByMember(token.text,
                                                    table=self.table)
                # if we find a match, then create a Member token and add it
                # to the current context
                if field:
                    token = Member(token.text, field, self.table)
                # if nothing was found, then return it as a String
                else:
                    token = String("'%s'" % token.text)

            # add the token to the parsed list
            parsed.append(token)

        # search for all the phrases that contain the passed in phrase
        phrases = self.root.find(parsed)

        return phrases

    def match(self, obj):
        """Return a boolean indicating whether the passed in object matches
        this where string.  If the object does not have pointers to all
        the necessary members, it will return False.  Note that regular
        expressions should be written so the Python re module can
        understand them.

        @param obj: an instance of an object that is subclassed from the
          primary table of this query.  In other words, if this object
          was instantiated to query the Album table from
          L{TestDatabases.Music}, then only L{TestDatabases.Album}
          instances should be passed in.
        @type obj: instance

        @return: a boolean indicating whether the passed in objects matches
          this query.
        @rtype: boolean
        """

        # make sure the object we are going to check has the same base type
        # as our primary table
        if self.database.tableByClass(obj.__class__) != self.table:
            raise MatchError("expected an object of type '%s', got '%s'" % \
                  (self.table.baseClass.__name__, obj.__class__.__name__))

        # get the eval string that we will use
        if self.__python is None:
            self.__python = self.root.getPython()
        # kind of hacky, but the eval string uses all an object named
        # 'MATCH_OBJ'.  So we make a pointer to the obj with that name.
        MATCH_OBJ = obj
        try:
            # ensure that True or False is returned
            if eval(self.__python):
                return True
            return False
        except AttributeError:
            return False


class CmdUnquotedString(Data):
    """The unknown token is any piece of non-quoted text that does not
    contain any of the special operator characters."""

    regexp = r"[^\s]+"

    def getMySQL(self):
        """Return a string representing the token in MySQL format."""
        return "'" + self.text + "'"


class CommandOutput(Tokenizer):
    """When a Command token is encountered in a Where string, and it will be
    part of a Comparison phrase, we want all tokens to be treated as Data
    tokens.  So instead of sending it through the Where parser, we only
    tokenize the string and resolve any other Data tokens we might
    encounter."""

    # a list of the Token objects that we are looking for
    _Tokens = [Date, ByteNumber, TimeString, Number, Command, String,
               CmdUnquotedString]

    def __init__(self, cmdout, database, table, prevCommands):
        """We initialize this object with the command's output, a pointer
        to the Database and Table object for evaluating Members, and a
        list of all the previous commands that have been referenced
        (to avoid infinite loops)."""

        # the output from the command
        self.cmdout       = cmdout
        # the database where we can find Members
        self.database     = database
        # the primary table that is being queried (needed to establish a
        # search order when searching for Members).
        self.table        = table
        # a list of all the previous commands referenced
        self.prevCommands = prevCommands

        # list of tokens after the string is tokenized
        self.tokens = []

        # split the string into tokens
        self._tokenize()

    def _tokenize(self):
        """Split the command output string into Data tokens."""

        # get the tokens
        tokens = self._getTokens(self.cmdout)

        # final list of tokens
        self.tokens = []

        # iterate through the tokens and figure out what to do
        for token in tokens:
            # check if we have a handle_ method defined for this token
            try:
                func = getattr(self, "handle_" + token.__class__.__name__)
            # if not, then create it ourselves and call 'addToContext'
            except AttributeError:
                pass
            else:
                newtok = func(token)
                if newtok: token = newtok

            self.tokens.append(token)

    def handle_Command(self, token):
        """If a Command is encountered, then execute it and send the output
        through another CommandOutput class."""

        # first make sure this won't cause an infinite loop
        if token.text in self.prevCommands:
            raise InfiniteLoop("referencing %s in output of a command " \
                  "will cause an infinite loop." % token.text)

        # execute the command
        rcode,out,err = osutil.runCommand(token.text.strip("`"))
        # strip the string of whitespace
        out = out.strip()

        # update our prevCommands list so we don't have an infinite loop
        prev = self.prevCommands[:]
        prev.append(token.text)

        # send the output through our Tokenizer again
        cmdout = CommandOutput(out, self.database, self.table,
                               prevCommands=prev)
        # give everything to the Command token
        token.output = out
        token.tokens = cmdout.tokens

    def handle_CmdUnquotedString(self, token):
        """Check if the string should be turned into a Member or not,
        otherwise, return it as a String."""

        # search for a field in the DB
        field = self.database.fieldByMember(token.text, table=self.table)
        # if we find a match, then create a Member token and add it
        # to the current context
        if field:
            return Member(token.text, field, self.table)

        # if nothing was found, then return it as a String
        return String("'%s'" % token.text)



if __name__ == '__main__':
    import sys
    from .TestDatabase import ProduceDB, MRD, Job

    queries = [
        "user=jag",
        "user in [jag, adamwg]",
        "crews has [cars, rat]"
        ]

    while 1:
        sys.stdout.write("offramp : ")
        sys.stdout.flush()
        where = sys.stdin.readline()
        if not where: break
        w = MRD.Where(where, table=MRD.JobTable)
        j = Job()
        j.user = 'jag'
        j.host = 'pringles'
        j.port = 9001

        print("Phrase Tree:\n  %s" % w.root.getTree(indent='  '))
        print("Natural Language:\n ", w)
        print("MySQL:\n ", w.getMySQL())
        print("Python:\n ", w.root.getPython())
        print("match():\n ", w.match(j))
        print("Members:\n ", \
              ', '.join(["%s.%s" % (f.classObject.__name__, f.member)
                         for f in w.getMembers()]))
        print("find('user'):\n ", \
              ', '.join([str(p) for p in w.find('user=')]))
        w.mask("Job.user")
        print("After mask():\n ", w)
        print()

    print()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
