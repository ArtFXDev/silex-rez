"""Interface for creating command line tools to view records from a
database."""

import sys, textwrap, types
import rpg.CmdLineTool as CmdLineTool
import rpg.sql.DBFormatter as DBFormatter

__all__ = (
    "DBCmdLineToolError",
    "ColumnsOption",
    "DistinctOption",
    "SortByOption",
    "DelimiterOption",
    "NoFormatOption",
    "NoHeaderOption",
    "NoSortOption",
    "RawDataOption",
    "TimeFormatOption",
    "FullTimeOption",
    "LimitOption",
    "TableQueryCmd",
    "UnknownTable",
    "FieldsHelp",
    "DBCmdLineTool"
    )


class DBCmdLineToolError(CmdLineTool.CmdLineToolError):
    """Base error types for all errors in DBCmdLineTool."""
    pass

class UnknownMember(DBCmdLineToolError):
    """A reference to an unknown member in the database."""
    pass


class ColumnsOption(CmdLineTool.StrListOption):
    """Option used to get a list of fields that should be displayed.
    The option is hard-coded to be referenced via -c,--cols."""

    def __init__(self, default=None):
        """
        @param default: the default list of columns that should be used if
          nothing is provided by the user.
        @type default: list
        """
        # keep track of what comes in so we can append and remove items
        # from the list
        self.cols = []
        # set to true if the arguments are setting the default list of cols
        self.set  = False
        super(ColumnsOption, self).__init__(
            "-c", "--cols",
            help="list of fields that will be printed",
            default=default)

    def store(self, option, opt, value, parser):
        """Split the comma delimited list into items, convert each one, and
        store the result in the parser."""
        # split into items and call the cast method
        vals = [self.cast(item) for item in value.split(',')]
        # assume that if the first col in the list has no operator in front,
        # then we are setting the cols
        if vals[0][0] not in "+-=":
            self.cols = vals
            self.set  = True
        # otherwise, assume we are editing the list
        else:
            for val in vals:
                if not self.set:
                    self.cols.append(val)
                # if the cols have been explicitly set, then we need to
                # alter our own list.
                elif val[0] == '+':
                    self.cols.append(val[1:])
                elif val[0] == '-':
                    self.cols = [c for c in self.cols if c != val[1:]]
                elif val[0] == '=':
                    raise DBCmdLineToolError("'=' cannot be used if " \
                          "the field list is specified at the command line.")
                else:
                    self.cols.append(val)

        # save the result
        setattr(parser.values, self.dest, self.cols)


class DistinctOption(CmdLineTool.StrListOption):
    """Option used if the user only wants to display rows with distinct
    values from the provided fields.  The option is hard-coded to be
    referenced via -d,--distinct."""

    def __init__(self, default=None):
        """
        @param default: the default list of fields that should be used
          when determining whether a row is distinct or not.
        @type default: list
        """
        super(DistinctOption, self).__init__(
            "-d", "--distinct",
            help="only print rows with distinct values for "
                 "the provided fields",
            default=default)


class SortByOption(CmdLineTool.StrListOption):
    """Option used to specify which fields should be used to sort the
    result.  The option is hard-coded to be referenced via -s,--sortby."""

    def __init__(self, default=None):
        """
        @param default: the default list of fields that will be used to
          sort the result.
        @type default: list
        """
        super(SortByOption, self).__init__(
            "-s", "--sortby",
            help="sort each row by the provided list of fields",
            default=default)


class DelimiterOption(CmdLineTool.StringOption):
    """Option for specifying the delimiter that should be used between
    columns when formatting the results.  The default is to use a single
    space.  The option is also hard-coded to be referenced via --delimiter."""

    def __init__(self, default=' '):
        """
        @param default: the default delimiter
        @type default: string
        """
        super(DelimiterOption, self).__init__(
            "--delimiter", default=default,
            help="use d as the column delimiter of the output, "
                 "by default a single space is used.")


class NoFormatOption(CmdLineTool.BooleanOption):
    """Option for turning formatting off for a result.  By default, formatting
    is turned on to produce neatly spaced output.  The option is hard-coded
    to be referenced via --nf,--noformat."""

    def __init__(self):
        super(NoFormatOption, self).__init__(
            "--nf", "--noformat", dest="noformat",
            help="do not format the output in neatly spaced columns")


class NoHeaderOption(CmdLineTool.BooleanOption):
    """Option to suppress the header/footer for a result.  By default, a
    header is displayed, and a footer is displayed if at least 40 rows
    are in a result.  The option is hard-coded to be referenced via
    --nh,--noheader."""

    def __init__(self):
        super(NoHeaderOption, self).__init__(
            "--nh", "--noheader", dest="noheader",
            help="do not print a header")


class NoSortOption(CmdLineTool.BooleanOption):
    """Option to disable sorting for a result.  Results are often sorted
    by default, and for larger queries this can slow the database down.
    The option is hard-coded to be referenced as --ns,--nosort."""

    def __init__(self):
        super(NoSortOption, self).__init__(
            "--ns", "--nosort", dest="nosort",
            help="do not sort the results")


class ArchivesOption(CmdLineTool.BooleanOption):
    """Option to query archive tables.
    The option is hard-coded to be referenced as -a,--archives."""

    def __init__(self):
        super(ArchivesOption, self).__init__(
            "-a", "--archives", dest="archives",
            help="query archive tables as well")


class RawDataOption(CmdLineTool.BooleanOption):
    """Option to display data as is exists in the database.  Thus, values
    that are modified and made human readable will not be altered, but
    the output will still be neatly spaced.  This option is hard-coded to
    be referenced via --raw."""

    def __init__(self):
        super(RawDataOption, self).__init__(
            "--raw",
            help="the columns will still be neatly spaced, "
                 "but all values will be displayed as they are "
                 "in the database.  (e.g. do not format elapsed "
                 "seconds as hh:mm:ss).")


class TimeFormatOption(CmdLineTool.StringOption):
    """Option for altering how time fields will be displayed, ala strftime.
    The option is hard-coded to be referenced via --tf,--timefmt."""

    def __init__(self, default=None):
        super(TimeFormatOption, self).__init__(
            "--tf", "--timefmt", dest="timefmt", default=default,
            help="provide an alternative format for all "
                 "time fields (see man strftime)")


class FullTimeOption(CmdLineTool.BooleanOption):
    """Option for displaying all time fields with the full time."""

    def __init__(self, default=None):
        super(FullTimeOption, self).__init__(
            "--fulltime", dest="timefmt", const="%c",
            help="display all time fields with the full time, shortcut "
                 "for '--timefmt %c'")


class LimitOption(CmdLineTool.IntOption):
    """Option to specify how many rows to display from the query."""

    def __init__(self, default=None):
        super(LimitOption, self).__init__(
            "--limit",
            help="only display the first l rows")


class DBQueryCmd(CmdLineTool.BasicCmdLineTool):
    """Base object for querying any table of a database.  Simply opens
    and closes the db connection.

    @cvar Database: the L{Database.Database} class object that will be
                      queried with
    @type Database: L{Database.Database} class object
    """

    Database = None

    def __init__(self, *args, **kwargs):
        self.db = None
        super(DBQueryCmd, self).__init__(*args, **kwargs)

    def opendb(self, **kwargs):
        """Create the database instance and open a connection."""
        self.db = self.Database(debug=self.opts.debug, **kwargs)
        self.db.open()

    def closedb(self):
        """Close the database connection if it's open."""
        if self.db: self.db.close()

    def pre_execute(self):
        super(DBQueryCmd, self).pre_execute()
        self.opendb()

    def post_execute(self):
        self.closedb()
        super(DBQueryCmd, self).post_execute()
        

class TableQueryCmd(CmdLineTool.CmdLineTool):
    """Base object for querying a single table of a database from the
    command line.  This includes options for::

      - displaying specific fields (-c,--cols)
      - sorting the results (-s,--sortby)
      - only printing distinct values (-d,--distinct)
      - specifying a column delimiter (--delimiter)
      - excluding a header/footer (--nh,--noheader)
      - turning formatting off (--nf,--noformat)

    """

    # setup our options
    options = [
        ColumnsOption(),
        DistinctOption(),
        SortByOption(),
        LimitOption(),
        RawDataOption(),
        TimeFormatOption(),
        FullTimeOption(),
        DelimiterOption(),
        NoFormatOption(),
        NoHeaderOption(),
        NoSortOption(),
        ArchivesOption(),

        # add the options from the base class
        ] + CmdLineTool.CmdLineTool.options

    # we set our option style to isolated so that arguments can easily be
    # placed before or after the where string.
    optionStyle = "isolated"


    def __init__(self, database, table, formatterClass, defaultSort=None,
                 defaultDistinct=None, footer=40,
                 **kwargs):
        """
        @param database: the L{Database.Database} object that will be queried.
        @type database: L{Database.Database} object

        @param table: the L{Table.Table} object that will be queried.
        @type table: L{Table.Table} instance

        @param formatterClass: the L{DBFormatter.DBFormatter} subclass that
          will be used when displaying the result.
        @type formatterClass: L{DBFormatter.DBFormatter} class

        @param args: optional list of arguments to parse, if None, then
          sys.argv will be checked.
        @type args: list

        @param defaultSort: the default list of fields to sort the results
          by.
        @type defaultSort: list

        @param defaultDistinct: the default list of fields to use when
          checking for distinct objects.
        @type defaultDistinct: list

        @param footer: the number of objects that must be returned from
          the query before a footer is added to the end.  This is for
          asthetics, and if you always want a footer set this to -1.  If
          you never want a footer, then set it to 0 or False.
          The default is 40.
        @type footer: int
        """

        # save the important stuff
        self.database  = database
        self.table     = table
        self.formatterClass = formatterClass
        self.footer    = footer
        self.members   = None
        self.where     = None
        self.defaultSort = defaultSort
        if not defaultDistinct:
            defaultDistinct = [] 
        self.defaultDistinct = defaultDistinct

        # list of fields that we will use to check for distinct objects
        self.distinct = []

        # pointer to the database object instance when it is created.
        self.db = None

        # initialize the rest
        super(TableQueryCmd, self).__init__(**kwargs)

        # if the parent is present, then grab the command name for the
        # usage statement
        if not self.usage and self.parent:
            for name, cls in self.parent.commands.items():
                if cls == self.__class__:
                    self.usage = name


    def parseArgs(self, *args, **kwargs):
        result = super(TableQueryCmd, self).parseArgs(*args, **kwargs)

        # build the where string
        self.where = ' '.join(self.args)

        return result


    def pre_execute(self):
        """Called before the execute method is called."""
        # build a list of members that need to be selected from the db
        self.members = []
        
        # if the distinct option is set, then check if we are going to use
        # the list exclusively to sort and display
        if self.opts.distinct:
            if not self.opts.cols:
                self.opts.cols = self.opts.distinct
            if not self.opts.sortby:
                self.opts.sortby = self.opts.distinct

            distinct = self.opts.distinct
        else:
            distinct = self.defaultDistinct

        self.distinct = []
        # add these members to the select list
        for mem in distinct:
            # find the field
            field = self.database.fieldByMember(mem, table=self.table)
            if not field:
                raise UnknownMember("'%s' is not a valid field name" % mem)
            self.members.append(field.fullname)
            self.distinct.append(field)
        
        # get the formatter
        formatter = self.getFormatter()

        # get the fields we need to print
        for mf in formatter.mformats:
            # search for it's field
            field = formatter.fieldByMember[mf]
            self.members.append(field.fullname)

        # save the formatter
        self.formatter = formatter

        # check the sortby options
        if not self.opts.sortby and not self.opts.nosort:
            self.opts.sortby = self.defaultSort

        # call the super
        super(TableQueryCmd, self).pre_execute()


    def post_execute(self):
        """Called after the execute method even if 'execute' raises and
        exception."""
        # always make sure the db is closed
        self.closedb()
        # call the super
        super(TableQueryCmd, self).post_execute()


    def opendb(self, dbargs={}, openargs={}):
        """Create the appropriate L{Database.Database} object and open
        a connection."""
        dbargs.setdefault("debug", self.parent.opts.debug)
        self.db = self.database(**dbargs)
        self.db.open(**openargs)


    def closedb(self):
        """Close the connection to the database."""
        # make sure the database is closed
        if self.db:
            self.db.close()

    def getFormatter(self):
        """Create and return the Formatter instance that will be used
        to print the result."""

        # make the formatter that will be used.  This way the user can use the
        # +/-/= operators to modify the default list.

        # this nastiness is so we can use the parent's value (if present)
        # and still override it with our own.
        kwargs = {"table": self.table,
                  "separator": self.opts.delimiter}
        for var in ("raw", "timefmt", "nocolor", "zeros"):
            val = None
            # do we have a parent?
            if self.parent:
                try:
                    val = getattr(self.parent.opts, var)
                except AttributeError:
                    pass

            try:
                ourval = getattr(self.opts, var)
                if not ourval:
                    ourval = val
            except AttributeError:
                ourval = val

            if ourval:
                kwargs[var] = ourval
       
        if self.opts.cols:
            args = (self.opts.cols,)
        else:
            args = ()

        return self.formatterClass(*args, **kwargs)

    def runQuery(self):
        """Run the query provided by the caller on our table.

        @result: the result of the query.
        @rtype: L{Database.QueryResult} instance
        """
        # call the get??? object for each table, as it might be overloaded
        # from the base getObjects() call
        
        # restrict to only parent tables if --archives hasn't been specified before (self.parent.opts)
        # or after (self.opts) command keyword
        only = not self.opts.archives and not self.parent.opts.archives
        funcname = "get%ss" % self.table.baseClass.__name__
        try:
            func = getattr(self.db, funcname)
        except AttributeError:
            return self.db.getObjects(self.table,
                                      members=self.members,
                                      where=self.where,
                                      limit=self.opts.limit,
                                      orderby=self.opts.sortby,
                                      only=only)
        else:
            return func(members=self.members, where=self.where,
                        limit=self.opts.limit,
                        orderby=self.opts.sortby, only=only)

    def isDistinct(self, obj, distinct):
        """If a one-to-many table join relationship exists, then it's
        possible that duplicate rows would be printed.  This method checks if
        the object is distinct (i.e. does not have a key in the 'distinct'
        dictionary) and has not been printed yet.

        @param obj: the object needs to be checked.
        @type obj: L{DBObject.DBObject} instance

        @param distinct: mapping of all the currently processed objects,
          keyed by the fields that are being used to determine if an object
          is distinct.
        @type distinct: dictionary

        @return: True if the object is distinct
        @rtype: boolean
        """

        # build the key that will be used to check for distinct values
        key = []
        for field in self.distinct:
            if field.table is self.table:
                myobj = obj
            else:
                myobj = getattr(obj, field.table.baseClass.__name__)

            val = getattr(myobj, field.member)
            if not val:
                val = None
            elif type(val) is list:
                val = tuple(val)

            key.append(val)

        # turn the key into a tuple so it can be used as a dictionary key
        key = tuple(key)

        # check if this is distinct
        if key in distinct:
            return False

        # save this key for next time.
        distinct[key] = True
        return True

    def processResult(self, qresult, file=None):
        """Process and print all the objects returned from the query."""

        # if we had no results, then do nothing
        if len(qresult) == 0 or \
           (self.opts.limit is not None and self.opts.limit < 0):
            return

        # send everything to stdout unless otherwise stated
        if file is None:
            file = sys.stdout

        # make sure the query result doesn't cache objects after they are
        # created, as we will only need them once.
        qresult.cacheObjects = False

        # set the widths of our formatter so everything is neatly spaced
        if not self.opts.noformat:
            self.formatter.setWidths(qresult)

        # explicitly set the width of everything to None
        else:
            for mf in self.formatter.mformats:
                mf.width = None

        # print a header
        if not self.opts.noheader:
            print(self.formatter.header(), file=file)

        # keep track of which objects are distinct
        distinct = {}

        cnt = 0
        # iterate through each object and print it
        for obj in qresult:
            # check if this object is distinct
            if not self.distinct or self.isDistinct(obj, distinct):
                self.processObject(obj, file=file)
                cnt += 1
                # check if we should quit
                if self.opts.limit not in [None, 0] and cnt >= self.opts.limit:
                    break

        # if we had more than self.footer rows, then print a footer
        if not self.opts.noheader and \
           self.footer and cnt >= self.footer:
            print(self.formatter.footer(), file=file)


    def processObject(self, obj, file=None):
        """Print the object."""

        # send everything to stdout unless otherwise stated
        if file is None:
            file = sys.stdout

        print(self.formatter.format(obj), file=file)
        

    def execute(self):
        """The actual work gets done here.  Open the database, run the
        query, and then handle the result."""
        self.opendb()

        result = self.runQuery()

        self.processResult(result)

        return 0

class UnknownTable(DBCmdLineToolError):
    """Unable to find a table object."""
    pass


class FieldsHelp(CmdLineTool.CmdLineTool):
    """A help command to view a list of the fields that are available
    for searching on, viewing, and sorting."""

    usage = "fields"

    # this command has no dash options
    options = []

    description = """
    List all the data fields available for searching, sorting, and displaying.
    Most fields can be referenced from any command, but some fields will
    require the table name to be prepended to avoid name conflicts.
    """

    def __init__(self, tables=None, **kwargs):
        self.tables = tables
        super(FieldsHelp, self).__init__(**kwargs)


    def getHelpStr(self):
        """If no tables are provided at the command line, then provide
        a simple message."""

        # first get the usage string and put some newline on the end
        mystr = self.getUsage() + '\n\n'

        # get the description
        mystr += self.getDescription()
        
        # get a neatly formatted string of these tables
        tableStr = ', '.join([t.baseClass.__name__ for t in self.tables])

        # create a textwrap object to properly indent the list
        tw = textwrap.TextWrapper(subsequent_indent="    ",
                                  width=70,
                                  replace_whitespace=True,
                                  fix_sentence_endings=True)
        tableStr = tw.fill(tableStr)
        # add the list of tables that can be queried
        return mystr + "\n\n  Available arguments\n    %s\n" % tableStr


    def getMapping(self):
        """Get the mapping that will be used to determine which table
        should have its field's formatted."""

        # make a mapping from table name to table object
        mymap = {}
        for table in self.tables:
            base = table.baseClass.__name__
            mymap[base] = table
            mymap[base.lower()] = table
            mymap[base.lower() + 's'] = table

        return mymap
        

    def execute(self):
        """Overloaded from the super so we can dynamically read the database
        to figure out which tables are available."""

        # if no arguments were provided, then give a general message
        if not self.args:
            print(self.getHelpStr())
            return

        # make a mapping from table name to table object
        mymap = self.getMapping()

        # get a list of all the tables we need to print
        tables = []

        # are we are to list all the fields?
        if 'all' in self.args:
            tables = [t.baseClass.__name__ for t in self.tables]
        else:
            # check for a bad table before printing anything
            for arg in self.args:
                if arg not in list(mymap.keys()):
                    raise UnknownTable("unknown table '%s', available " \
                          "tables are %s" % \
                          (arg,
                           ', '.join([t.baseClass.__name__ for
                                      t in self.tables])))
            tables = self.args

        # create a formatter for the help
        for tname in tables:
            docf = DBFormatter.FieldDocFormatter()
            mystr = tname + '\n\n' + docf.format(mymap[tname]) + '\n'
            # put a newline inbetween each Table except the last
            if tname != tables[-1]:
                print(mystr)
            else:
                print(mystr.strip())


class DBCmdLineTool(CmdLineTool.MultiCmdLineTool):
    """Generalized object for querying any table from a database from
    the command line.

    @cvar tableByCmd: mapping of command name to the Table object that
      should be used when as an input paramater to L{TableQueryCmd}.
      This mapping is used as a backup if an entry is not found in
      the 'commands' mapping.

    @cvar sortByCmd: mapping of command name to a list of fields the
      results should be sorted by.
    """

    options = [
        CmdLineTool.BooleanOption ("--nocolor",
                                   help="do not display any output in color."),
        CmdLineTool.BooleanOption ("--zeros",
                                   help="some fields do not display a value "
                                        "if it is equal to zero, this will "
                                        "force zeros to be printed no matter "
                                        "what."),
        RawDataOption(),
        TimeFormatOption(),
        CmdLineTool.BooleanOption  ("-d", "--debug", help="print debug info."),
        
        ] + CmdLineTool.MultiCmdLineTool.options

    commands = {
        "fields": FieldsHelp,
        }
    
    tableByCmd = {}

    sortByCmd = {}

    def __init__(self, database, formatterClass, tableQueryCmd=TableQueryCmd,
            **kwargs):
        """
        @param database: the L{Database.Database} object that will be queried.
        @type database: L{Database.Database} object

        @param formatterClass: the L{DBFormatter.DBFormatter} subclass that
          will be used when displaying the result.
        @type formatterClass: L{DBFormatter.DBFormatter} class
        """

        self.database = database
        self.formatterClass = formatterClass
        self.tableQueryCmd  = tableQueryCmd

        super(DBCmdLineTool, self).__init__(**kwargs)


    def getNewCommand(self, cmd, *args, **kwargs):
        """Overloaded from the super so we can ensure that our TableQueryCmd
        objects are properly initialized."""

        cmdkwargs = kwargs.copy()
        if cmd == "fields":
            cmdkwargs['tables'] = self.database.Tables

        try:
            return super(DBCmdLineTool, self).getNewCommand(cmd, 
                    *args, **cmdkwargs)
        except CmdLineTool.UnknownCommand:
            # check for a default list to sortby
            sortby = self.sortByCmd.get(cmd)

            kwargs = kwargs.copy()
            kwargs['defaultSort'] = sortby

            try:
                tableobj = self.tableByCmd[cmd]
            except KeyError:
                raise CmdLineTool.UnknownCommand(cmd)
        
            # assume we need to make a TableQueryCmd object        
            cmdobj = self.tableQueryCmd(
                    self.database, 
                    tableobj,
                    self.formatterClass,
                    *args, **kwargs)

            if not cmdobj.usage:
                cmdobj.usage = cmd

            return cmdobj
