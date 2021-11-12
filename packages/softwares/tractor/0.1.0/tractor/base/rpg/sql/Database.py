import sys, types, re, time

from rpg.listutil import getUnion
from rpg.terminal import TerminalColor
from rpg.progutil import log, logWarning, logError

from rpg.compat import namedtuple
from rpg.sql import SQLError
from rpg.sql.Fields import AutoIncField, ObjTypeField
import rpg.sql.Where as DBWhere
import rpg.sql.Join as DBJoin
import rpg.sql.DBObject as DBObject
import rpg.sql.Fields as DBFields

__all__ = ('SQLNotOpen',
           'SQLWarning',
           'SQLQueryError',
           'SQLDuplicateInsert',
           'SQLConnectionError',
           'SQLTimeOut',
           'SQLErrorMultipleEntries',
           'SQLNotFound',
           'SQLRowsExist',
           'SQLInvalidMember',
           'SQLJoinInDelete',
           'SQLJoinInUpdate',
           'SQLInvalidObjectType',
           'QueryResult',
           'Database')

# ---------------------------------------------------------------------------
# Exceptions

class SQLNotOpen(SQLError):
    """A connection to the database has not been opened."""
    pass

class SQLWarning(SQLError):
    """A warning while updating the database."""
    pass

class SQLQueryError(SQLError):
    """The query statement contains an error."""
    pass

class SQLDuplicateInsert(SQLQueryError):
    """A entry already exists in the database for this key."""
    pass

class SQLConnectionError(SQLError):
    """Problem connecting to the database."""
    pass

class SQLTimeOut(SQLError):
    """Raised if an alarm goes off."""
    pass

class SQLErrorMultipleEntries(SQLError): # AWG: not used yet
    """A query would of resulted in multiple entries."""
    pass

class SQLNotFound(SQLError): # AWG: not used yet
    """Record not found in the database."""
    pass

class SQLRowsExist(SQLError): # AWG: not used yet
    """Rows still exist in the cursor."""
    pass

class SQLInvalidMember(SQLError):
    """A non-existent member was specified."""
    pass

class SQLJoinInDelete(SQLError):
    """A delete was attempted that would require a table join."""
    pass

class SQLJoinInUpdate(SQLError):
    """A update was attempted that would require a table join."""
    pass

class SQLInvalidObjectType(SQLError):
    """Invalid object type for converting a row."""

class SQLUnknownField(SQLError):
    """A non-existent field was found in a QueryResult."""
    pass

class QueryResultError(SQLError):
    """Base error type for all QueryResult errors."""
    pass

# ---------------------------------------------------------------------------
# Timeout Callback

def _timeoutCB(signum, frame):
    raise SQLTimeOut("Operation timed out.")


# ---------------------------------------------------------------------------

class QueryResult(object):
    """A query result object is returned to the caller after every database
    query.  This object holds the raw data returned from the database and
    is responsible for converting rows into objects.  It is intended to
    look like a list to the user."""

    def __init__(self, rows, table, database, unpack=False, objtype=None,
                 info=None):
        """
        @param rows: list of rows returned from the database.  Each row
          is returned as a dictionary.
        @type rows: list

        @param table: the primary table that was used for this query.
        @type table: L{Table.Table} instance

        @param database: the database was accessed
        @type database: L{Database.Database} instance

        @param unpack: if True, then each object will be unpacked all
          at once, rather than as members are accessed.  This can potentially
          be much slower than the default.
        @type unpack: boolean

        @param objtype: a lookup to give client applications the option of
          instantiating their own object types.
        @type objtype: dictionary
        """

        self.rows = rows      # data returned from database
        self.table = table    # the primary table that was used for this query
        self.database = database  # access to database for other info
        self.objtype = objtype # lookup table to map objtype field to a class
        self.unpack = unpack
        # additional information about each field, given to us by the cursor.
        self.fieldInfo = {}
        for item in info or ():
            # the first index in each item is the rowkey, so make that the
            # key for easy lookup
            self.fieldInfo[item[0]] = tuple(item[1:])

        # cache used to check if an object has already been created
        self.objs = {}
        # allow users to toggle caching on/off
        self.cacheObjects = True

        # the default type of the main (primary) object
        self._defaultObjName = None

        # these two members are used when the entire result is converted
        self.objects = []      # a list of all objects
        self.key2object = None # a dictionary to locate object by key

        self.rowkey2tableFieldname = {} # maps row key to (table, fieldname)

        # setup arguments for _getObject() used to create the main object.
        self._setUpMainCreateArgs()

        # this will setup a dictionary of all the external field objects
        # we need to create for each row.  The key is the name of the object
        # that is to be created, and the value is a tuple of the
        # arguments needed for _getObject() in order to create the
        # said object.
        self._setUpExtCreateArgs()

        # some external objects can be shared by muliple primary objects
        # this is most common with many-to-one table joins.  This is a
        # cache used to determine if an object has already been created
        # matching a given key.
        self._extObjCache = {}


    def _setUpMainCreateArgs(self):
        """Setup the arguments that will be passed to _getObject() when we
        are creating the objects from the primary table."""
        
        # if there are no results, then quit.
        if not self.rows:
            return

        # rows can optionally be returned as a single object, or a different
        # classMap can be provided (via the objtype paramater).
        if type(self.objtype) is dict:
            defaultObj = self.table.baseClass
            classMap   = objtype.copy()
        # no preference was provided, so use the default class map from the
        # table
        elif self.objtype is None:
            defaultObj = self.table.baseClass
            classMap   = self.table.subClasses
        # a type was explicitly specified, so all objects will be returned
        # as this type, but make sure it is a subclass of DBObject.
        elif issubclass(self.objtype, DBObject.DBObject):
            defaultObj = self.objtype
            classMap   = None
        # otherwise, the user didn't give us the right info
        else:
            raise SQLInvalidObjectType("'%s' is not a subclass of " \
                  "DBObject." % self.objtype.__class__.__name__)

        # grab the keys from the first row, we assume all rows have
        # identical keys.
        rowkeys = [col[0] for col in self.database.cursor.description]
        #rowkeys = self.rows[0].keys()
        
        # if a classMap is set, then check for the object type field name.
        # if the objtype field isn't present in the row, then we don't
        # consider it.
        if classMap and self.table.objtype and \
           self.table.objtype.fieldname in rowkeys:
            objTypeFieldName = self.table.objtype.fieldname
        else:
            objTypeFieldName = None

        # save a copy of the default object's name.  This allows external
        # objects to link back to their parent (primary)
        self._defaultObjName = defaultObj.__name__

        self._mainCreateArgs = (defaultObj, objTypeFieldName, classMap, None)


    def _setUpExtCreateArgs(self):
        """The objects returned via the __getitem__ method are always
        subclasses of the base type for the query's primary table.  However,
        fields from other tables are often included in the results via a
        table join and they need to be referenced some how.  We accomplish
        this by creating objects of the joined tables' type and make
        pointers in the main object.  For example, if table Foo was queried,
        but Bar.a was referenced via a join, Bar.a could be accessed from
        the object returned via obj.Bar.a.

        This method sets up the cache needed to make all of this work.
        Unfortunately, all fields for a given query are stored in one
        dictionary, but we want them split up by table.  Instead of moving
        pointers around and causing a bottleneck, we create all the necessary
        mappings from object space to row space here.  Then we pass this
        static mapping onto each object.  Notice this could be disasterous
        if instances started mucking with this mapping.
        """

        # if there are no results, then quit.
        if not self.rows:
            return

        # grab the keys from the first row, we assume all rows have
        # identical keys.
        rowkeys = [col[0] for col in self.database.cursor.description]
        #rowkeys = self.rows[0].keys()
        
        # a temporary holding of the row key lookups
        rowKeyByExtField = {}

        # iterate over each key and check for keys from joined tables
        for rowkey in rowkeys:
            # check for a match
            match = DBFields.Field.ExtFieldRE.match(rowkey)
            if match:
                clsName,selectName = match.groups()
                # make sure a mapping is available for this table
                mapping = rowKeyByExtField.setdefault(clsName, {})
                mapping[selectName] = rowkey

        # make sure the create args cache is clear
        self._extObjCreateArgs = {}

        # for each external table we found, setup the arguments that will
        # be used to create the objects.  Also check if the object's keys
        # are included in the row allowing for it to be shared across
        # multiple objects.
        for clsName,rowmapping in list(rowKeyByExtField.items()):
            # get the table object
            tblObj = self.database.tableByClassName(clsName)
            # the arguments for creating the object are
            #  - the default object type for this table
            #  - name of the object type field (as found in the row), if
            #    the table produces multiple object types, otherwise None
            #  - a mapping of class objects if the table produces multiple
            #    object types, otherwise None
            #  - the row key mappings

            # does this table have an objtype field?
            if tblObj.objtype:
                # lookup the objtype field name in the rowkeys dictionary,
                # if not found, then we always use the default
                objTypeFieldName = rowmapping.get(tblObj.objtype.fieldname)
                classMap         = tblObj.subClasses
                defaultObj       = tblObj.baseClass
            else:
                objTypeFieldName = None
                classMap         = None
                defaultObj       = tblObj.baseClass

            # setup the arguments
            args = (defaultObj, objTypeFieldName, classMap, rowmapping)

            # now check if the keys for this object were included allowing
            # for it to be shared across multiple objects.  First get a
            # list of the rowkey names for the table's keys.
            keys_rowkeys = [key.selectName for key in tblObj.keys]
            sharable = len(keys_rowkeys) > 0
            # now check if all the keys are included
            for key in keys_rowkeys:
                # if we don't find anything, then get out of here
                if key not in list(rowmapping.keys()):
                    sharable = False
                    break

            # if we are sharable, then we want to create a template that
            # will be used to generate a unique key for the object when
            # it is created.
            if sharable:
                # map the keys back to the actual row key
                realkeys     = [rowmapping[key] for key in keys_rowkeys]
                # now make a template
                key_template = ','.join(["%%(%s)s" % key for key in realkeys])
                # build the class name into the key so we don't have to
                # do it later
                key_template = "%s,%s" % (clsName, key_template)
            else:
                key_template = None

            # key everything with the base class name
            self._extObjCreateArgs[clsName] = (key_template, args)


    def __getitem__(self, index):
        """Overloaded to give the object the appearance of a list."""
        
        # check the cache if this object has already been created
        try:
            obj = self.objs[index]
        # if it hasn't, then create the object now
        except KeyError:
            obj = self._row2obj(self.rows[index])
            if self.cacheObjects:
                self.objs[index] = obj
        return obj


    def _row2obj(self, row):
        """Convert the provided row dictionary into the appropriate subclass
        of L{DBObject.DBObject}.

        @param row: a dictionary returned from the database representing
          a row from a table.
        @type row: dictionary

        @return: an instance of the L{DBObject.DBObject} subclass that
          corresponds to the provided row.
        @rtype: L{DBObject.DBObject} subclass instance
        """

        # create the primary object
        obj = self._getObject(row, *self._mainCreateArgs)

        # now create all the external table objects
        for objname,(key_template,args) in list(self._extObjCreateArgs.items()):
            # should we check the cache for an existing equivalent object?
            if key_template:
                # evaluate the key
                objkey = key_template % row
                # check for an existing object
                try:
                    extobj = self._extObjCache[objkey]
                except KeyError:
                    # create the object
                    extobj = self._getObject(row, *args)
                    # give the external obj a pointer to the main
                    setattr(extobj, self._defaultObjName, obj)
                    # add it to the cache
                    self._extObjCache[objkey] = extobj
                    
            # if not, then just make the object
            else:
                extobj = self._getObject(row, *args)
                # give the external obj a pointer to the main
                setattr(extobj, self._defaultObjName, obj)

            # set the the member
            setattr(obj, objname, extobj)

        return obj


    def _getObject(self, row, defaultObj, objTypeFieldName, classMap, rowkeys):
        """Get an object for the provided row.

        @param row: dictionary returned from database
        @type row: dictionary

        @param defaultObj: the default object the row will be created with
        @type defaultObj: L{DBObject.DBObject} subclass

        @param objTypeFieldName: name of the table's L{Fields.ObjTypeField}
          if it has one, otherwise it is None
        @type objTypeFieldName: string or None

        @param classMap: a mapping of class names to class objects used to
          create different object based on a table's objTypeField value
        @type classMap: dictionary

        @param rowkeys: mapping of the all joined tables so they can find
          their members in the 'rows' dictionary.
        @type rowkeys: dictionary

        @return: object instance associated with the passed in row
        @rtype: L{DBObject.DBObject} subclass instance
        """

        # check if the type varies based on a field value
        if objTypeFieldName:
            try:
                cls = classMap[row[objTypeFieldName]]
            except KeyError:
                # make sure we get something
                cls = defaultObj
        # otherwise, just use the default
        else:
            cls = defaultObj

        # make a new object of the desired type, but don't call init
        obj = cls.__new__(cls)
        obj.__dict__['indb'] = True
        obj.__dict__['__rowdata__'] = row
        obj.__dict__['__rowkeys__'] = rowkeys
        obj.__dict__['dirty'] = {}

        # call the object's init method
        obj.init()

        # optionally unpack the whole object now
        if self.unpack:
            obj.unpack()

        return obj


    def getFieldWidths(self):
        """Get the maximum character length of each field.  This is useful
        for neatly formatting the objects in a query result.

        @return: a mapping from L{Field.Field} instance to width
        @rtype: dictionary
        """

        # if there are no results, then quit.
        if not self.rows:
            return

        # first scan the description info
        widths = {}
        # iterate over each key and check for keys from joined tables
        for rowkey,info in list(self.fieldInfo.items()):
            # check for a match
            match = DBFields.Field.ExtFieldRE.match(rowkey)
            if match:
                clsName,selectName = match.groups()
                # get the table object
                tblObj = self.database.tableByClassName(clsName)
            else:
                tblObj = self.table
                selectName = rowkey
            # search for the field by rowkey
            field = tblObj._fieldByRowKey(selectName)
            if not field:
                # if this is a virtual field, then we need to check the
                # regular fieldByMember method
                field = tblObj.fieldByMember(selectName)
                if not field:
                    raise SQLUnknownField("unable to find a Field object " \
                          "for the row key '%s'" % rowkey)

            # save the width for this field
            widths[field] = info[1]

        return widths


    def __len__(self):
        return len(self.rows)


    def __str__(self):
        s = 'table = %s, # rows = %d' % (self.table.tablename, len(self.rows))
        return s


    def toObjects(self):
        """
        @return: a list of all of the rows converted to objects.
        @rtype: list
        """

        # return right away so we don't do this twice
        if self.objects:
            return self.objects

        # induce a row2obj on each row
        for i in range(len(self.rows)):
            self.objects.append(self[i]) # self[] induces row2obj conversion

        return self.objects


    def objectByKey(self, key):
        """
        @return: an object corresponding to the given key expressed as a tuple
        @rtype: L{DBObject.DBObject} subclass instance
        """

        # create the key2object LUT if it has not been
        if self.key2object is None and self.table.keys:
            self.key2object = {}
            for o in self:
                kee = tuple([getattr(o, k.member) for k in self.table.keys])
                self.key2object[kee] = o

        # express key as a tuple if it has not yet
        if type(key) is not tuple:
            key = (key,)
        return self.key2object.get(key)


    def isEmpty(self):
        """Return True if no rows were returned."""
        return len(self.rows) == 0


    def todict(self, keys=None, group=False):
        """Convert the objects of this result into a dictionary.

        @param keys: list of members that objects should be keyed by.  By
          default, objects are keyed by the primary tables keys.
        @type keys: tuple

        @param group: group objects with common keys in a list, by default
          an exception is raised if objects have the same key.
        @type group: boolean

        @return: a dictionary keyed by 'keys'
        @rtype: dict
        """

        # get the Field objects we will used to generate keys
        if not keys:
            # use the keys of the primary table
            keys = self.table.keys
        else:
            # search for the Field objects
            fields = []
            for member in keys:
                field = self.database.fieldByMember(member, table=self.table)
                if not field:
                    raise SQLInvalidMember("unknown member '%s' found " \
                          "in 'keys' parameter" % member)
                fields.append(field)
            keys = fields

        # iterate over each object and add it to our resulting dictionary
        result = {}
        for obj in self:
            # generate the key for this object
            key = []
            for field in keys:
                # if the member is not from the primary table, then we
                # have to do two lookups
                if field.table is self.table:
                    key.append(getattr(obj, field.member))
                else:
                    key.append(getattr(
                        getattr(obj, field.table.baseClass.__name__),
                        field.member))

            # if the key only has one item, then keep it as is
            if len(key) == 1:
                key = key[0]

            # make sure the key isn't a list
            if type(key) is list:
                key = tuple(key)

            # if grouping is okay, then append to the current list
            if group:
                result.setdefault(key, []).append(obj)
            elif key not in result:
                result[key] = obj
            else:
                raise QueryResultError("multiple objects with key '%s'" % \
                      key)

        return result


    def members(self, *members):
        """Makes a generator for values of C{members} as tuples from all rows.

        Best used for gathering the results of a query in bulk, when the
        features of the L{DBObjects} aren't necessary. When retrieving results
        from more than a hundred or so rows, this method is *much* faster.

        >>> widgets = db.getWidgets(where, orderby=['qty'])
        >>>
        >>> list(widgets.members('parto', 'qty'))
        [('pxy77d', 5), ('ad2525', 11), ('thx1138', 99)
        >>> for partno, qty in widgets.members('partno', 'qty'):
        ...     print partno, qty
        >>>
        >>> list(widgets.members('qty'))
        [5, 11, 99]
        >>> total_qty = sum(widgets.members('qty'))
        115

        The yielded objects are actually subclasses of C{tuple}. They offer
        similar functionality to the named tuples in Python 2.6 (and hopefully
        someday will actually be named tuples), but are backwards compatible
        with older Pythons. Note that members can only be accessed by the name
        they're specified as intiially; aliases are not resolved when accessing
        the tuple-like objects.

        >>> widgets = db.getWidgets(where).members('partno', 'qty',
        ...     'description')
        >>>
        >>> for widget in widgets:
        ...     print widget.description
        >>> sum([w.qty for w in widgets]
        >>>
        >>> # this will fail, even though quantity is a valid alias for the
        >>> # member
        >>> sum([w.quantity for w in widgets])
        """
        member2key = {}
        attrs = []
        for member in members:
            # resolve any aliases the member might refer to
            field = self.database.fieldByMember(member, table=self.table)
            if not field:
                raise SQLInvalidMember("unknown member '%s'" % member)

            if field.table != self.table:
                key = "_%s_%s" % (field.table.tablename, field.selectName)
                attr = "%s_%s" % (field.table.tablename, field.selectName)
            else:
                attr = key = field.selectName

            member2key[member] = key
            attrs.append(attr)

        if len(members) == 1:
            # if only one member was requested, we just return a generator of
            # individual values instaed of a generator of tuples
            key = member2key[members[0]]
            for row in self.rows:
                yield row[key]
        else:
            Record = namedtuple('Record', attrs)
            for row in self.rows:
                yield Record(*[row[member2key[member]] for member in members])

    def member(self, member):
        return self.members([member])

# ---------------------------------------------------------------------------

class Database(object):
    """A database object has class members to hold the definitions of all
    tables and joins in the database.  Methods are provided to open and
    close the database, as well as get/put objects from/to the database.

    @cvar Tables: subclasses should set this to the list of L{Table.Table}
      instances that are in the database.
    @type Tables: list

    @cvar Joins: a list of L{Join.Join} objects defining how tables are
      joined.  Many of the mappings are done automatically, but tables that
      do not have common key names will need to be specified explicitly.
    @type Joins: list

    @cvar SearchOrder: when a where string (intended for performing a query
      on given table) is parsed, it needs to know which members can be
      referenced.  Subclasses should define a search order so that name
      conflicts between tables can be resolved appropriately.  This should
      be a dictionary keyed by table object pointing to a list of Table
      objects that can be searched.
    @type SearchOrder: dictionary

    @cvar WhereAliases: where aliases that can be used in where strings
      for all queries.  This should be a dictionary keyed by the alias name
      which points to the expanded query.
    @type WhereAliases: dictionary

    @cvar Where: pointer to the desired L{Where.Where} used to parse where
      strings for the database.  This gives subclasses the option of
      subclassing L{Where.Where} for custom where strings.
    @type Where: L{Where.Where} subclass
    
    """

    # subclasses should set this to the list of tables that are in the
    # database.  Each item should be a subclass of a Table object
    Tables      = []

    # list of member names that can be used to automatically join tables
    CommonKeys  = []

    # subclasses should define how tables are joined.  Each item in the list
    # should be a Join object.
    Joins       = []
    JoinCls     = DBJoin.Join

    # when a where string (intended for performing a query on given table)
    # is parsed, it needs to know which members can be referenced.  Subclasses
    # should define a search order so that name conflicts between tables
    # can be resolved appropriately.  This should be a dictionary keyed by
    # table object pointing to a list of Table objects that can be searched.
    SearchOrder = {}

    # where aliases that can be used in where strings for all queries.
    WhereAliases = {}

    # save a pointer to the Where class we will use for parsing natural
    # language where strings.
    Where       = DBWhere.Where

    # save a pointer to the QueryResult class type that should be used
    # when returning results
    QueryResultCls = QueryResult



    # the following are internal values

    # for logging in color
    _bluecolor    = TerminalColor('white')
    # fieldname to use with SELECT COUNT(*) AS ...
    _rowCountName = 'rowcount'

    def __init__(self, dbhost=None, db=None, user=None, password=None,
                 port=None, debug=False, timeout=0, swap_signals=True,
                 readonly=0, unpack=False, autocommit=True, mirror=False):
        """
        @param dbhost: name of host the database server is running on
        @type dbhost: string

        @param db: name of the database
        @type db: string

        @param user: user name used to login to the server
        @type user: string

        @param password: password used to login to the server
        @type password: string

        @param port: port on which database server is listening
        @type port: int

        @param debug: set to True if debug messages should be printed
        @type debug: boolean

        @param timeout: number of seconds to wait for a response from the
          database before raising a SQLTimeOut exception.
        @type timeout: int

        @param swap_signals: Python's implementation of signals prevents us
          from immediately catching a signal during a database operation.
          Thus, queries can block the process indefinitely without giving
          the client application the option of aborting.  Setting this to
          True will ensure signals are immediately caught.
        @type swap_signals: boolean

        @param readonly: set to True if database writes should not be
          performed.
        @type readonly: boolean

        @param unpack: if True, then each object will be unpacked all
          at once, rather than as members are accessed.  This can potentially
          be much slower than the default.
        @type unpack: boolean

        @param autocommit: If True, automatically commit data. Otherwise you'll
          need to explicitly commit the data at the end of a transaction.
        """

        self.debug = debug           # print queries if set to True

        self.dbhost = dbhost         # hostname of database server
        self.db = db                 # name of database
        self.user = user             # database connection username
        self.password = password     # database connection password
        self.port = port             # database connection port

        self.readonly = readonly     # set to 1 for debugging
        self.unpack = unpack  # passed to QueryResult to unpack() objs
        
        # by default we will swap all signals when a connection is opened
        self.swap_signals = swap_signals
        # if non zero, then each call to _execute will force a SIGALRM
        # signal handler to be set.
        self.timeout = timeout
        # to avoid setting the signal handler for SIGALRM multiple times
        # while in a nested loop, some methods set this flag.
        self.reset_handler = True
        self.autocommit = autocommit
        
        self.connection = None      # database connection
        self.cursor = None          # database cursor for executing queries

        # set the database pointer in each table
        self._initTables()

    # ----------------------------------------------------------------------
    # helper methods

    def dprint(self, s):
        if self.debug:
            readonlyStr = ''
            if self.readonly:
                readonlyStr = '*read only* '
            s = '%s [%s:%s]> %s%s' % (time.ctime(), self.dbhost, self.db, readonlyStr, [s])
            print(self._bluecolor.colorStr(s))

            
    # ----------------------------------------------------------------------
    # database connectivity methods
    
    def open(self):
        raise SQLError("must define close() in sublcass.")

    def close(self):
        raise SQLError("must define close() in sublcass.")

    def _execute(self, query, multi=None):
        raise SQLError("must define execute() in subclass.")

    # ----------------------------------------------------------------------
    # database & table attribute lookup class methods

    def _initTables(cls):
        """Run through each table and make sure everything has been
        initialized."""

        # have we already done this?
        if "_tablesInit" in cls.__dict__:
            return

        # make sure each table has a pointer to us
        for table in cls.Tables:
            table.database = cls

        # don't call this anymore
        cls._tablesInit = True
    _initTables = classmethod(_initTables)
    

    def tableByName(cls, tablename):
        """
        @param tablename: name of table to search for.  Is not case sensitive.
        @type tablename: string

        @return: the L{Table.Table} instance associated with the provided
          table name.
        @rtype: L{Table.Table} instance
        """

        # set the database pointer in each table
        cls._initTables()

        # we explicitly check the class's dictionary for _nameToTable so
        # we don't accidentally pickup the member from a super class
        try:
            nameToTable = cls.__dict__['_nameToTable']
        except KeyError:
            # create it if this is the first access of this function
            nameToTable = {}
            for table in cls.Tables:
                nameToTable[table.tablename.lower()] = table
            # add it to the class dictionary so we don't do this again
            cls._nameToTable = nameToTable

        return nameToTable.get(tablename.lower())
    # make this a class method, because it is static for all instances
    tableByName = classmethod(tableByName)


    def tableByClass(cls, clsobj):
        """
        @param clsobj: class object that belongs to table that will be
          searched for.
        @type clsobj: L{DBObject.DBObject} subclass

        @return: the table object that the provided class object belongs to.
        @rtype: L{Table.Table} instance
        """

        # set the database pointer in each table
        cls._initTables()

        # we explicitly check the class's dictionary for _classToTable so
        # we don't accidentally pickup the member from a super class
        try:
            classToTable = cls.__dict__['_classToTable']
        except KeyError:
            # create it if this is the first access of this function
            classToTable = {}
            for table in cls.Tables:
                for obj in table.objects:
                    classToTable[obj] = table
            # add it to the class dictionary so we don't do this again
            cls._classToTable = classToTable

        return classToTable.get(clsobj)
    # make this a class method, because it is static for all instances
    tableByClass = classmethod(tableByClass)


    def tableByClassName(cls, className):
        """
        @param className: name of a class that object that belongs to table
          that will be searched for.
        @type className: string

        @return: the table object that the provided class name belongs to.
        @rtype: L{Table.Table} instance
        """

        # set the database pointer in each table
        cls._initTables()

        # we explicitly check the class's dictionary for _classNameToTable so
        # we don't accidentally pickup the member from a super class
        try:
            classNameToTable = cls.__dict__['_classNameToTable']
        except KeyError:
            # create it if this is the first access of this function
            classNameToTable = {}
            for table in cls.Tables:
                for obj in table.objects:
                    classNameToTable[obj.__name__] = table
            # add it to the class dictionary so we don't do this again
            cls._classNameToTable = classNameToTable

        return classNameToTable.get(className)
    # make this a class method, because it is static for all instances
    tableByClassName = classmethod(tableByClassName)


    # regexp used when a name is being checked if it has a class prepended
    # to it or not.
    __member_re = re.compile('([a-zA-Z]\w*)\.([a-zA-Z_]\w*)')
    def fieldByMember(cls, member, table=None):
        """
        @param member: name of a class member, which can be in the form
          'bar' or 'Foo.bar'.
        @type member: string

        @param table: use the search order associated with the provided
          L{Table.Table} instance.  If this is not set, then the default
          search order will be used.
        @type table: L{Table.Table} instance

        @return: L{Fields.Field} object associated with the passed in
          member name.
        @rtype: L{Fields.Field} instance
        """

        # set the database pointer in each table
        cls._initTables()

        # check if the name passed in is in the form Class.member
        match = cls.__member_re.match(member)
        if match:
            clsname,member = match.groups()
            # get the table object associated with this class
            tbl = cls.tableByClassName(clsname)
            # if we didn't find anything, then abort
            if not tbl: return None

            # get the field
            return tbl.fieldByMember(member)

        # if the name has no class associated with it, then we will search
        # through all the tables.

        # figure out our search order
        try:
            order = cls.SearchOrder[table]
        except KeyError:
            # copy the list that defines our tables and use that as the
            # default search order
            order = list(cls.Tables[:])
            # make sure the primary table is always at the front
            if table:
                order.remove(table)
                order.insert(0, table)

        # now search for a field
        for tbl in order:
            field = tbl.fieldByMember(member)
            # return if we find something
            if field: return field

        # we found nothing
        return None
    # make this a class method, because it is static for all instances
    fieldByMember = classmethod(fieldByMember)


    def join(cls, t1, t2):
        """
        Return the Join object used to join the provided table names.
        @param t1: name of table
        @type t1: string

        @param t2: name of table
        @type t2: string

        @return: the L{Join.Join} object used to join the provided table names.
        @rtype: L{Join.Join} instance
        """

        # we explicitly check the class's dictionary for _tableNamesToJoin so
        # we don't accidentally pickup the member from a super class
        try:
            tableNamesToJoin = cls.__dict__['_tableNamesToJoin']
        except KeyError:
            # create it if this is the first access of this function
            tableNamesToJoin = {}

            # step through each table and create Join objects
            for left in cls.Tables:
                for right in cls.Tables:
                    # don't join with ourselves
                    if left == right:
                        continue

                    #print left.tablename, right.tablename
                    #print left.keys, right.keys

                    # try to make a join
                    try:
                        join = cls.JoinCls(left, right,
                                           commonKeys=cls.CommonKeys)
                        key  = (left.tablename, right.tablename)
                        #print "join created", key
                        tableNamesToJoin[key] = join
                    except DBJoin.JoinError:
                        #print left.tablename, right.tablename
                        pass

            # now step through any user defined joins and overwrite our default
            # joins if need be
            for j in cls.Joins:
                tableNamesToJoin[(j.leftTable.tablename,
                                  j.rightTable.tablename)] = j

            # step through the user defined mappings again and make any reverse
            # mappings that are not defined
            for j in cls.Joins:
                key = (j.rightTable.tablename, j.leftTable.tablename)
                if j.oneway:
                    # make sure we didn't already create a mapping the
                    # other way.
                    try:
                        join = tableNamesToJoin[key]
                    except KeyError:
                        pass
                    else:
                        if join.onclause == j.onclause:
                            del tableNamesToJoin[key]
                            
                    continue

                if key not in tableNamesToJoin:
                    join = cls.JoinCls(j.rightTable, j.leftTable,
                                       onclause=j.onclause,
                                       preTables=j.preTables)
                    tableNamesToJoin[key] = join

            # add it to the class dictionary so we don't do this again
            cls._tableNamesToJoin = tableNamesToJoin

        return tableNamesToJoin.get((t1, t2))
    # make this a class method, because it is static for all instances
    join = classmethod(join)


    def getWhereAlias(cls, alias, table=None):
        """Search for a where alias named 'alias'.  If a table is provided,
        then search through it's aliases, and then the global list.  Otherwise,
        only search the global list.

        @param alias: name of alias to search for
        @type alias: string

        @param table: L{Table.Table} object that should be checked before
          searching through the global list.  If not set, then only the global
          list will be searched.
        @type table: L{Table.Table} instance

        @return: The expanded query associated with the passed in alias, if
          no match is found then return None.
        @rtype: string or None
        """

        # search for the alias in the table's list
        if table:
            try:
                return table.whereAliases[alias]
            except KeyError:
                pass

        return cls.WhereAliases.get(alias)
    getWhereAlias = classmethod(getWhereAlias)



    # ----------------------------------------------------------------------
    # high-level object storage/retrieval methods
    
    def countObjects(self, obj, where=None, table=None):
        """Return the number of rows in table that match the where clause."""

        if not table:
            table = self.tableByClass(obj.__class__)
            if not table:
                raise SQLError('A %s object does not map to any table' \
                      % obj.__class__.__name__)
        # we don't need any members when counting, so set the members to an
        # empty set
        select = self._select(table, where=where, members=[], count=True)
        self._execute(select)
        rows = self.cursor.fetchall()
        return rows[0][self._rowCountName]


    def getObjects(self, table, objtype=None,
                   members=[], notMembers=[],
                   virtual=True,
                   where=None, orderby=[],
                   limit=0, **whereargs):
        """
        @param table: L{Table.Table} object that will be used for the query.
        @type table: L{Table.Table} instance

        @param objtype: Different kinds of objects can be returned, depending
          on what objtype is set to:
            1. a dictionary mapping objtype column value to object class
               e.g. objtype={'Job': JobType, 'LumosJob': LumosJobType}
               If the column value is not found, an object of the default
               object type for the table is returned.
            2. a class, where all objects will be of that class.
               e.g. objtype=ChuuchJobType
            3. None (or not specified) where all objects will be of
               the default class for the row.
        @type objtype: dictionary

        @param members: list of member names that should be returned from
          the database.  If a member from an secondary table is desired,
          then prepend its class name (e.g. 'Foo.bar' to get member 'bar'
          from class/table 'Foo')
        @type members: list of strings

        @param notMembers: list of member names that should *not* be returned
          from the database.  This can only contain members that are from
          the primary table.
        @type notMembers: list of strings

        @param virtual: include virtual fields if no members are provided
        @type virtual: boolean

        @param where: a natural language L{sql.Where} string used to specify
          which rows to return.
        @type where: string

        @param orderby: list of member names to order the results by.  Names
          prepended with a '-' will be reverse sorted.
        @type orderby: list of strings

        @param limit: maximum number of rows to return
        @type limit: int

        @return: a list of all the rows found matching the provided query.
        @rtype: L{Database.QueryResult}

        """

        # KQ: should we specify a table, a class, or either/both
        # when using getObjects?
        

        query = self._select(table, members=members, notMembers=notMembers,
                             virtual=virtual, where=where, orderby=orderby,
                             limit=limit, **whereargs)
        self._execute(query)
        rows = self.cursor.fetchall()

        result = self.QueryResultCls(rows, table, self, objtype=objtype,
                                     unpack=self.unpack,
                                     info=self.cursor.description)
        return result
    

    def putObjects(self, objs, *args, **kw):

        # OPT: the QueryResult instantiation would slow things down if done
        # at the putObject() level; better to be outside!
        # FUTURE: want to know what table was used for QueryResult!
        # so look up by class, or by overriding table= keyword arg
        result = self.QueryResultCls([], None, self)

        for obj in objs:
            self.putObject(obj, *args, **kw)
            # TODO: there could be an accumulative query result object here?

        return result

            
    def putObject(self, obj, table=None, setAutoInc=1, members=None,
                  fields=None, equivKeys=None, execute=1):
        # FUTURE: allow the queueing of inserts of objects of the same type

        """If table is not specified, the default table for the given
        object's class is used.
        
        If autoIncField is set, when the object is inserted,
        the auto increment field is retreived and inserted into
        the object.

        The equivKeys as an equivalent way to locate the record by the
        given fields.  This is used to determine whether or not the
        record already exists.

        If fields is specified, only the given fields will be updated
        in the database if the record in UPDATEd.  All fields will be
        inserted if the record is INSERTed.
        """

        if fields:
            sys.stderr.write("'fields=' keyword argument has been deprecated" \
                             " in Database.putObject().  Use 'members='.")
            members.extend(fields)

        # make sure we have a table.
        if table is None:
            table = self.tableByClass(obj.__class__)
            if not table:
                raise SQLError('A %s object does not map to any table' \
                      % obj.__class__.__name__)

        # set to True if we only need to update, not insert the object
        update = False

        # if the table this object is being put into has an auto increment
        # key, then we want to grab it from the db if it already exists.
        autoIncKey = table.getAutoIncKey()
        if autoIncKey:
            # if the value isn't already set for the object, then check for it
            if getattr(obj, autoIncKey.member) <= 0:
                # get the id for this object and set it
                id = self.getID(obj, table=table, equivKeys=equivKeys)
                if id > 0:
                    setattr(obj, autoIncKey.member, id)
                    update = True
            # otherwise, only update the db
            else:
                update = True
        # otherwise, check if the object exits
        elif (table.keys or equivKeys) and \
             self.existsObject(obj, table=table, equivKeys=equivKeys):
            update = True

        if update:
            self.updateObject(obj, table=table, members=members)
        else:
            self.insertObject(obj, table=table, setAutoInc=setAutoInc)


    def updateObject(self, obj, table=None, members=None):
        """Update the record associated with this object.  This method
        assumes that the object has the appropriate key fields set,
        and does not check to make sure the row exists.  See putObject()
        for this behavior.

        @param table: the L{Table} object referencing the table that needs
          to be updated.  If not provided, then it is determined based on
          the input object's type.
        @type table: L{Table} instance

        @param members: list of members that should be updated, otherwise
          the objects dirty dictionary will be used.
        @type members: list

        """

        # make sure we have a table.
        if table is None:
            table = self.tableByClass(obj.__class__)
            if not table:
                raise SQLError('A %s object does not map to any table' \
                      % obj.__class__.__name__)

        # execute update statement
        update = self._update(table, obj, members=members)
        self._execute(update)
        # mark object in db
        obj.indb = True
        # clear updated members from dirty dict
        obj.clearDirty(members=members)


    def insertObject(self, obj, table=None, setAutoInc=True):
        """Insert this object into the database.  This method does not
        check for a duplicate entry before inserting.  See putObject()
        for this behavior.

        @param table: the L{Table} object referencing the table that needs
          to be inserted into.  If not provided, then it is determined based on
          the input object's type.
        @type table: L{Table} instance

        @param setAutoInc: if the table has an auto-increment field
          that wasn't previously set, then set the value in the object
          after the insert.  The defualt is True.
        @type setAutoInc: boolean
        """

        # make sure we have a table.
        if table is None:
            table = self.tableByClass(obj.__class__)
            if not table:
                raise SQLError('A %s object does not map to any table' \
                      % obj.__class__.__name__)

        # execute insert statement
        insert = self._insert(table, obj)
        self._execute(insert)
        # place the autoincrement value in the object
        if setAutoInc and table.keys and \
           isinstance(table.keys[0], AutoIncField):
            setattr(obj, table.keys[0].member, self.cursor.lastrowid)
        # mark object in db
        obj.indb = True
        # clear all members from dirty dict
        obj.clearDirty()


    _batchInsertSize = 500
    _maxQueryLength  = 1 << 16
    def insertObjects(self, objs, table=None, setAutoInc=True):
        """Insert a list of objects into the database with one query.
        This is useful when several hundred records need to be inserted
        and is far more efficient than inserting them one by one.  This
        method does not check for a duplicate entry before inserting.
        See putObject() for this behavior.

        @param table: the L{Table} object referencing the table that needs
          to be inserted into.  If not provided, then it is determined based on
          the input object's type.
        @type table: L{Table} instance
        """

        if not objs: return

        # make sure we have a table.
        if table is None:
            table = self.tableByClass(objs[0].__class__)
            if not table:
                raise SQLError('A %s object does not map to any table' \
                      % obj.__class__.__name__)

        # place the autoincrement value in the object
        if setAutoInc and table.keys and \
           isinstance(table.keys[0], AutoIncField):
            autoIncMember = table.keys[0].member
        else:
            autoIncMember = None

        cnt = len(objs)
        i   = 0
        # we insert the objects in chunks so that the query isn't too large
        # for the db to handle.  This isn't the best way to do this, as it
        # is still vulnerable to a problem.
        while i < cnt:
            start  = i
            max    = i + self._batchInsertSize
            insert = "INSERT INTO %s VALUES " % table.tablename
            while i < max and i < cnt and len(insert) < self._maxQueryLength:
                if i != start:
                    insert += ", "
                insert += self._insertValues(table, objs[i])
                i += 1

            self._execute(insert)
            currid = self.cursor.lastrowid

            # iterate over each object we just inserted and clear the dirt
            # dict
            for j in range(start, i):
                obj = objs[j]
                # add the auto inc value.  Note, this handles the case where
                # objects with preset auto inc values are interleaved with
                # those that need one assigned from the db.
                if autoIncMember:
                    objid = getattr(obj, autoIncMember)
                    #print objid, autoIncMember, obj.__class__, obj.__dict__
                    # if no value is already set in the object, then assume
                    # the db gave one to it
                    if not objid or objid <= 0:
                        setattr(obj, autoIncMember, currid)
                        currid += 1
                    # if the objid is >= to the currid, then increment
                    # the curr value.  otherwise, assume the object was
                    # succesfully inserted in a gap.
                    elif objid >= currid:
                        currid = objid + 1
                obj.indb = True
                obj.clearDirty()


    def existsObject(self, obj, table=None, equivKeys=None, fast=True):
        """Return True if object exists in the database.  Use equivKeys
        instead of an tables's standard keys, if specified.

        fast=True makes the assumption that if the object was read from the
        database that it was not deleted from the database.
        fast=False will cause the record to be explicitly SELECTed to verify
        that it exists.
        """
        
        # indb will return 1 if the object was SELECTed from the database
        # note that this doesn't protect us from another client removing record
        if fast and obj.indb:
            return True

        # make sure we have a table for this object
        if table is None:
            table = self.tableByClass(obj.__class__)
            if not table:
                raise SQLError('A %s object does not map to any table' \
                      % obj.__class__.__name__)

        # check if the table has an auto inc key and the object has it set
        if fast:
            idfield = table.getAutoIncKey()
            if idfield and getattr(obj, idfield.member) > 0:
                return True        

        # we will pass the keys and their values (from the object) as keyword
        # arguments to the select method
        kwargs = {}

        # if equivKeys are passed in then use these values to check if
        # the object exists.
        if equivKeys:
            for member in equivKeys:
                val = getattr(obj, member)
                # if any of the keys are None, then let this indicate the
                # record does not exist (when would we want NULL a valid
                # key value?)
                if val is None:
                    return False
                try:
                    field = table.member2field[member]
                except KeyError:
                    raise SQLError("member '%s' not associated with a " \
                          "field" % key)
                kwargs[member] = val
        else:
            # if the table has equivKeys defined, then use them if the
            # primary keys are AutoIncFields with no value.
            keys = table.keys
            if table.equivKeys:
                for field in table.keys:
                    # if this autoinc field is not set yet, then use our
                    # trusty equiv keys
                    if isinstance(field, AutoIncField) and \
                       getattr(obj, field.member) <= 0:
                        keys = table.equivKeys
                        break

            # set up the keyword mappings so we can pass them to select
            for field in keys:
                val = getattr(obj, field.member)
                # if any of the keys are None, then let this indicate the
                # record does not exist (when would we want NULL a valid
                # key value?)
                if val is None:
                    return False
                kwargs[field.member] = val

        # see how many records match
        select = self._select(table, count=True, **kwargs)
        self._execute(select)
        row = self.cursor.fetchone()
        if row[self._rowCountName] > 1:
            raise SQLError('(%s) must not be equivalent keys for the %s'\
                  ' table because they mapped to %d records.' \
                  % (','.join(equivKeys), table.tablename,self.cursor.rowcount))

        return row[self._rowCountName] == 1


    def deleteObjects(self, table, where=None, deleteAll=0, **whereargs):
        """deleteAll must be set to 1 to remove all objects from the
        database."""
       
        if not where and not whereargs and not deleteAll:
            raise SQLError('set deleteAll to 1 to delete all %s records'\
                  % table.tablename)

        delete = self._delete(table, where=where, **whereargs)
        self._execute(delete)


    def deleteObject(self, obj, table=None):
        """Delete a single object from the database based on it's keys."""

        # make sure we have a table for this object
        if table is None:
            table = self.tableByClass(obj.__class__)
            if not table:
                raise SQLError('A %s object does not map to any table' \
                      % obj.__class__.__name__)

        # generate a where based on the object's keys
        wargs = dict([(k.member, getattr(obj, k.member)) for k in table.keys])
        if not wargs:
            raise SQLError('no keys found for %s object' % table.tablename)

        delete = self._delete(table, **wargs)
        self._execute(delete)


    def getID(self, obj, table=None, equivKeys=None):
        """Return the unique identifier for an object if it is from a table
        with an auto increment field."""

        # make sure we have a table for this object
        if table is None:
            table = self.tableByClass(obj.__class__)
            if not table:
                raise SQLError('A %s object does not map to any table' \
                      % obj.__class__.__name__)

        # check if the table has one key, and it is an auto increment field
        idfield = table.getAutoIncKey()
        if not idfield:
            raise SQLError("Table %s does not have an auto increment " \
                  "field" % table.tablename)

        # check if the member in the object is already set
        val = getattr(obj, idfield.member)
        if val > 0: return val

        if not (table.equivKeys or equivKeys):
            # this should be no big deal if the table doesn't have equivKeys
            return -1
            #raise SQLError, "No equivalent fields defined for %s" % \
            #      table.tablename

        # otherwise, go find the value using the equivkeys
        # we will pass the keys and their values (from the object) as keyword
        # arguments to the select method
        kwargs = {}

        # use the user passed in equivkeys
        if equivKeys:
            for member in equivKeys:
                val = getattr(obj, member)
                try:
                    field = table.member2field[member]
                except KeyError:
                    raise SQLError("member '%s' not associated with a " \
                          "field" % key)
                kwargs[member] = val
        else:
            # if the table has equivKeys defined, then use them if the
            # primary keys are AutoIncFields with no value.
            for field in table.equivKeys:
                kwargs[field.member] = getattr(obj, field.member)

        # see how many records match
        select = self._select(table, members=[idfield.member], **kwargs)
        self._execute(select)
        if self.cursor.rowcount > 1:
            raise SQLErrorMultipleEntries("multiple records in %s " \
                  "matching '%s'" % \
                  (table.tablename,
                   ' AND '.join(["%s=%s" % (k, v) for k,v in list(kwargs.items())])))
        if self.cursor.rowcount <= 0:
            return -1
        
        row = self.cursor.fetchone()
        return row[idfield.fieldname]


    def rowCount(self):
        """Return the number of rows affected by most recently executed
        query."""
        
        return self.cursor.rowcount


    def fetchRow(self):
        """Return a single row."""
        return self.cursor.fetchone()
    

    # ----------------------------------------------------------------------
    # clause creation methods
    
    def getCreate(self):
        raise SQLError("must define getCreate() in sublcass.")

    def _insert(self, table, obj):
        """Return a valid INSERT query for this object."""
        return "INSERT INTO %s VALUES %s" % \
               (table.tablename, self._insertValues(table, obj))

    def _multiInserts(self, table, objs, chunksize=200):
        """Return a list valid INSERT queries for bulk insertions of
        multiple objects."""
        if not objs:
            return []
        inserts = []
        numchunks = (len(objs) - 1) / chunksize + 1
        start = 0
        end = chunksize
        for chunk in range(numchunks):
            parts = [self._insertValues(table, obj) for obj in objs[start:end]]
            insert = "INSERT INTO %s VALUES %s " % (table.tablename , ",".join(parts))
            inserts.append(insert)
            start = end
            end = start + chunksize # python takes care of end > len(objs)
        return inserts

    def _insertValues(self, table, obj):
        """Return the values from the object that will be inserted."""

        values  = []
        for field in table.fields:
            # special case for the ObjType field
            if isinstance(field, ObjTypeField):
                value = obj
            else:
                try:
                    value = getattr(obj, field.member)
                except AttributeError:
                    value = field.default
            values.append(field.pack(value))
        return '(' + ','.join(values) + ')'


    def _update(self, table, obj, members=None, where=None):
        """Get an UPDATE query for the specified object.

        @param table: primary L{Table.Table} object for the query.
        @type table: L{Table.Table} instance

        @param obj: L{DBObject.DBObject} object that is to be updated.
        @type obj: L{DBObject.DBObject} instance

        @param members: if set, then only the associated fields will be
          updated to the database.  If left unassigned, the update will
          default to all members in the dirty dictionary of the object.
        @type members: list of strings

        @param where: a natural language L{sql.Where} string used to specify
          which rows to update.
        @type where: string

        @return: a UPDATE query for the provided object
        @rtype: string
        """
        
        # if members were specified, verify they are proper fields
        if members:
            for member in members:
                # search for a field
                if not table.fieldByMember(member):
                    raise SQLInvalidMember("'%s' does no map to a field " \
                          "in the '%s' table." % (member, table.tablename))
                
        # determine which fields to update
        if not members:
            members = list(obj.dirty.keys())
            
        # create a list of assignments to follow SET
        sets = []
        for member in members:
            # do not deal with non-field members in dirty dictionary
            field = table.fieldByMember(member)
            if field is None:
                # this is just a member in the dirty dict w/o a field
                continue

            # do not update keys
            if field.key:
                continue

            # FUTURE: are we going to have an updateFormat, like in TOB.py???
            # special case for the ObjType field
            if isinstance(field, ObjTypeField):
                value = obj
            else:
                value = getattr(obj, member)
            sets.append("%s=%s" % (field.fieldname, field.pack(value)))

        if not sets:
            return ''

        setsStr = ','.join(sets)

        if where:
            extTables = []
            whereStr = self._getWhereStr(table, where, {}, extTables)
            # make sure the query isn't referencing secondary table fields
            if extTables:
                raise SQLJoinInUpdate("update queries cannot contain " \
                      "table joins")
        else:
            # key update off of table's key fields
            whereargs = {}
            for key in table.keys:
                whereargs[key.member] = getattr(obj, key.member)
            whereStr = self._getWhereStr(table, '', whereargs, [])

        update = 'UPDATE %s SET %s%s' % (table.tablename, setsStr, whereStr)
        return update


    def _select(self, table, members=None, notMembers=None, virtual=True,
                where=None, groupby=None, orderby=None, count=False, limit=0,
                offset=0,
                **whereargs):
        """Return the SELECT clause for the specified table and parameters.
        
        @param table: L{Table.Table} object that will be used for the query.
        @type table: L{Table.Table} instance

        @param members: list of member names that should included in the
          select query. If a member from an secondary table is desired,
          then prepend its class name (e.g. 'Foo.bar' to get member 'bar'
          from class/table 'Foo')
        @type members: list of strings

        @param notMembers: list of member names that should *not* be returned
          from the database.  This can only contain members that are from
          the primary table.
        @type notMembers: list of strings

        @param virtual: include virtual fields if no members are provided
        @type virtual: boolean

        @param where: a natural language L{sql.Where} string used to specify
          which rows to request in the query.
        @type where: string

        @param groupby: list of member names to group the results by.
        @type groupby: list of strings

        @param orderby: list of member names to order the results by.  Names
          prepended with a '-' will be reverse sorted.
        @type orderby: list of strings

        @param limit: maximum number of rows to return
        @type limit: int

        @param offset: the offset from the limit
        @type offset: int

        @param count: if True, then only create a query that will count the
          total number of rows matching a given query.
        @type count: boolean

        @return: a SELECT query
        @rtype: string
        """

        # get the fields we need to select for
        fields,extTables = self._getFields(table, members, notMembers,
                                            virtual=virtual)

        # get the where string.  this will add tables to 'extTables' if
        # they aren't already in the list
        whereStr = self._getWhereStr(table, where, whereargs, extTables)

        # get the groupby string.  this will add tables to 'extTables' if
        # they aren't already in the list
        groupByStr = self._getGroupBy(table, groupby, extTables)

        # get the orderby string.  this will add tables to 'extTables' if
        # they aren't already in the list
        orderByStr = self._getOrderBy(table, orderby, extTables)

        # now that we have everything we need, it is time to build the query.

        # only get the count if that's all that was requested
        if not groupby and count:
            fieldsStr = 'COUNT(*) AS %s' % self._rowCountName

        # create the field string
        else:
            # Allow a counted field to be included in the dbfields
            if count:
                dbfields = ['COUNT(*) AS %s' % self._rowCountName]
            else:
                dbfields = []

            for field in fields:
                # get the select string for this field
                dbfields.append(field.getSelect(table, extTables))
            # join everything together
            fieldsStr = ','.join(dbfields)

        # determine the table string, which includes any necessary joins
        tableStr = self._join(table, extTables=extTables)
        
        # accout for LIMIT
        limitStr = ''
        if limit > 0:
            limitStr = ' LIMIT %d' % limit

            # account for OFFSET
            if offset > 0:
                limitStr += ' OFFSET %d' % offset

        # put it all together
        select = 'SELECT %s FROM %s%s%s%s%s' % \
                 (fieldsStr, tableStr, whereStr, groupByStr, orderByStr, limitStr)

        return select


    def _delete(self, table, where=None, **whereargs):
        """Get a DELETE query for a given table.

        @param table: primary L{Table.Table} object for the query.
        @type table: L{Table.Table} instance

        @param where: optional L{sql.Where} string used to delete specific
          records.
        @type where: string

        @return: a DELETE query string
        @rtype: string
        """

        extTables = []
        whereStr  = self._getWhereStr(table, where, whereargs, extTables)
        # make sure 'extTables' comes back empty.  We can't join tables
        # during a delete.
        if extTables:
            raise SQLJoinInDelete("delete queries cannot contain table joins")
        delete = "DELETE FROM %s%s" % (table.tablename, whereStr)
        return delete

    _member_split_re = re.compile("\s*,\s*")
    def _getFields(self, table, members, notMembers, virtual=True):
        """Helper function to get a list of all the fields we need to select
        for in a 'SELECT' query.

        @param table: primary L{Table.Table} object for the query.
        @type table: L{Table.Table} instance

        @param members: list of members (or a string with comma delimited
          member names) that are being requested.  If None
          then we assume all members from the primary table are desired.
          If no members from the primary table are found, but members from
          other tables are referenced, then *all* members from the primary
          table will be selected.
        @type members: list of strings

        @param notMembers: list of members that should NOT be selected.
        @type notMembers: list of strings

        @param virtual: set to True (default) if virtual fields should be
          included if no members are selected.
        @type virtual: boolean

        @return: a two item tuple containing:
          - a list of L{Fields.Field} instances that need to be selected
          - a list of tables that will need to be joined
        @rtype: tuple
        """

        # list of fields that will be selected
        fields    = []
        # tables that we will need to join with
        extTables = []

        # if no members is set to the special value of  None, then select
        # all fields from the primary table.
        if members is None:
            fields = list(table.fields)
            # make sure the virtual fields are added
            if virtual:
                for vfield in table.vfields:
                    fields.append(vfield)
                    for etable in vfield.exttables:
                        if etable != table and etable not in extTables:
                            extTables.append(etable)
        # otherwise, grab all the fields that are specified
        else:
            # if no fields from the primary table are referenced directly,
            # then we will select all of them.  Thus, keep track of whether
            # the primary table is encountered.
            primaryRef = False
            # check if members is a string
            if type(members) is bytes:
                members = self._member_split_re.split(members)
            for member in members:
                # search for the field associated with this member name
                field = self.fieldByMember(member, table=table)
                # if we didn't find one, the raise an exception
                if not field:
                    raise SQLInvalidMember("unknown member '%s' found " \
                          "in 'members' parameter" % member)

                # is this field from the primary table?
                if field.table == table:
                    primaryRef = True
                # otherwise, keep track so we know which tables to join with
                elif field.table not in extTables:
                    extTables.append(field.table)

                # check for a virtual field
                if issubclass(field.__class__, DBFields.VirtualField):
                    for etable in field.exttables:
                        if etable != table and etable not in extTables:
                            extTables.append(etable)

                # add it to the list
                if field not in fields:
                    fields.append(field)

            # if no fields from the primary table were referenced in 'members',
            # then select all of them
            if not primaryRef:
                fields.extend(table.fields)


        # check if notMembers is a string
        if type(notMembers) is bytes:
            notMembers = self._member_split_re.split(notMembers)

        # if any members are specified in the 'notMembers' parameter, then
        # remove them from the current fields list.
        for notMember in notMembers or []:
            # search for the field
            field = self.fieldByMember(notMember, table=table)
            if not field:
                raise SQLInvalidMember("unknown member '%s' found " \
                      "in 'notMembers' parameter" % notMember)
            # make sure the field is from the primary table
            if field.table != table:
                raise SQLInvalidMember("member '%s' found in 'notMembers' " \
                      "parameter is not in the primary table." % notMember)
            # remove the field if it's in our list
            if field in fields:
                fields.remove(field)

        return fields,extTables


    def _getWhereStr(self, table, where, whereargs, extTables, aliases=None):
        """
        This method builds an SQL WHERE clause based on:
          - a passed string representing an aribrary where clause, plus
          - a set of named parameters which will be ANDed

        e.g. _getWhereStr(where='starttime > 10', user='adamwg', priority=400)
        will return 'WHERE starttime > 10 AND user='adamwg' AND priority=400'

        Any ORs necessary in a query would need to be specified in 'where'.

        @param table: primary L{Table.Table} object for the query.
        @type table: L{Table.Table} instance

        @param where: optional L{sql.Where} string used to delete specific
          records.
        @type where: string

        @param whereargs: dictionary of member,value pairs to include in
          the query.  All pairs will be ANDed together.
        @type whereargs: dictionary

        @param extTables: list of L{Table.Table} objects that will need to
          be joined with.  The list is updated if a new table is referenced in
          the 'where' parameter.
        @type extTables: list of L{Table.Table} instances

        @param aliases: dictionary of where aliases mapping a string to an expression.
          The dictionary is used by the Where parser.
        @type aliases: dictionary
        """

        # 1.  translate where clause to SQL

        whereStr = ''
        if where:
            wobj = self.Where(where, self, table=table, aliases=aliases)
            # check each field that is references
            for field in wobj.members:
                # if the field's table is not in our list already, then add it
                if field.table != table and field.table not in extTables:
                    extTables.append(field.table)
            whereStr = wobj.mysql
            
        # 2. arguments passed through whereargs are ANDed and treated as '='

        wherelist = []
        for (member, value) in list(whereargs.items()):
            field = table.fieldByMember(member)
            if field is None:
                raise SQLInvalidMember('%s is not a valid field or member of %s' \
                      % (member, table.tablename))
            val = field.pack(value)
            if val == "NULL":
                op = " is "
            else:
                op = "="
            wherelist.append("%s.%s%s%s" % (table.tablename, field.fieldname,
                                            op, val))
        
        # 3. AND results of where clause and whereargs
        
        if wherelist:
            if whereStr: whereStr = '(%s) AND ' % whereStr
            whereStr += ' AND '.join(wherelist)

        if whereStr: whereStr = ' WHERE ' + whereStr

        return whereStr


    def _getGroupBy(self, table, groupby, extTables):
        """Helper function to generate the 'GROUP BY' portion of a select
        query.

        @param table: primary L{Table.Table} object for the query.
        @type table: L{Table.Table} instance

        @param groupby: list of member strings to group the results by
        @type groupby: list of strings

        @param extTables: list of L{Table.Table} objects that will need to
          be joined with.  The list is updated if a new table is referenced in
          the 'groupby' parameter.
        @type extTables: list of L{Table.Table} instances

        @return: the 'GROUP BY' portion of a select query.
        @rtype: string
        """
        
        # check the 'groupby' parameter for any field references that will
        # require a join.  Also, create the 'ORDER BY' portion of the query.
        if not groupby:
            # nothing to order by
            return ''
            
        groupFields = []
        for member in groupby:
            # search for the field associated with this member
            field = self.fieldByMember(member, table=table)
            if not field:
                raise SQLInvalidMember("unknown member '%s' found in " \
                      "'groupby' parameter" % member)
            # if the field's table is not in our ext list already, then
            # add it
            if field.table != table and field.table not in extTables:
                extTables.append(field.table)

            # add it to our new list
            groupFields.append(field.getWhere())
        return " GROUP BY " + ','.join(groupFields)


    def _getOrderBy(self, table, orderby, extTables):
        """Helper function to generate the 'ORDER BY' portion of a select
        query.

        @param table: primary L{Table.Table} object for the query.
        @type table: L{Table.Table} instance

        @param orderby: list of member strings to order the results by
        @type orderby: list of strings

        @param extTables: list of L{Table.Table} objects that will need to
          be joined with.  The list is updated if a new table is referenced in
          the 'orderby' parameter.
        @type extTables: list of L{Table.Table} instances

        @return: the 'ORDER BY' portion of a select query.
        @rtype: string
        """
        
        # check the 'orderby' parameter for any field references that will
        # require a join.  Also, create the 'ORDER BY' portion of the query.
        if not orderby:
            # nothing to order by
            return ''
            
        orderFields = []
        for member in orderby:
            # check for an operator in front of the member name
            descending = False
            if member[0] == '-':
                descending = True
                member     = member[1:]
            elif member[0] == '+':
                member = member[1:]

            # search for the field associated with this member
            field = self.fieldByMember(member, table=table)
            if not field:
                raise SQLInvalidMember("unknown member '%s' found in " \
                      "'orderby' parameter" % member)
            # if the field's table is not in our ext list already, then
            # add it
            if field.table != table and field.table not in extTables:
                extTables.append(field.table)

            # add it to our new list
            orderFields.append(field.getOrderBy(descending=descending, table=table))
        return " ORDER BY " + ','.join(orderFields)


    def _join(self, primary, extTables=None):
        """Create a valid JOIN string based on all of the given tables."""

        # FUTURE: this is really simple right now; may be more dynamic
        # later where the user won't have to specify all table relationships

        # algorithm: first try to join the first table with all other
        # tables; if that fails, try to join other tables

        # in this method 'table' refers to 'tablename'

        # if no extTables are provided, then we don't need to join
        if not extTables:
            return primary.tablename

        # when joining we always try to establish an immediate connection
        # first, i.e. left join PRIMARY with SECONDARY.  Once that has been
        # checked, then we try to join with a SECONDARY table that has
        # already joined.  This process is repeated until a match is found
        # or all options are exhausted.

        joinStr   = primary.tablename
        currLevel = [primary]
        nextLevel = []
        added     = [primary]
        # copy the list so we can edit it
        extTables = list(extTables)


        #def names(tbls):
        #    return [t.tablename for t in tbls]

        # we loop until we go through all tables or exhaust all options
        while True:
            #print names(currLevel), names(nextLevel), names(extTables)

            # iterate over each table at the current level
            for curr in currLevel:

                remaining = []
                # now iterate over each remining ext table and check
                # for a match
                while extTables:
                    etable = extTables.pop(0)
                    join = self.join(curr.tablename, etable.tablename)
                    if join:
                        # check if this is a join requiring other joins
                        if join.preTables:
                            for p in join.preTables:
                                # check if we need to add it to the list
                                if not (p in extTables or p in added):
                                    extTables.append(p)
                            # don't add it to the list more than once
                            if etable not in remaining:
                                remaining.append(etable)
                        else:
                            # add the join string
                            joinStr += ' ' + join.joinStr()
                            # add it to the next level
                            nextLevel.append(etable)
                            added.append(etable)
                    # don't add it to the list more than once
                    elif etable not in remaining:
                        remaining.append(etable)

                extTables = remaining

            # if we ran out of tables, then get out of here
            if not extTables:
                break

            # check if we've exhausted our options
            if not nextLevel:
                raise SQLError("unable to join %s with '%s'" % \
                      (','.join([t.tablename for t in extTables]),
                       primary.tablename))

            # reset the level pointers
            currLevel = nextLevel
            nextLevel = []

        return joinStr


    # ----------------------------------------------------------------------
    # class access methods
    
    def __getattr__(self, var):
        """Overloaded so 'getOBJECT_NAME' methods do not need to be
        explicitly created.  This enables subclasses to quickly create
        a working database object."""

        # FUTURE: this precludes a table name ending in 's' that is not
        # considered plural.  Be careful! (consider 'es' plurals as well?)

        # check if this is a get or a put method
        if var[0:3] in ['get', 'put'] or var[0:6] in ['delete']:
            callback  = None
            table     = None
            if var[0:3] in ['get', 'put']:
                endIndex = 3
            else: # var[0:6] == 'delete'
                endIndex = 6
            functype  = var[0:endIndex]
            className = var[endIndex:]
            # check if the remaining portion of the name is a table name
            table  = self.tableByClassName(className)
            if not table:
                # if not, then check if it is plural
                if className[-1] == 's':
                    className = className[:-1]
                    table = self.tableByClassName(className)
                    if table:
                        if functype == 'put':
                            callback = self.putObjects
                        elif functype == 'get':
                            callback = self.getObjects
                        else: # functype == 'delete'
                            callback = self.deleteObjects
            else:
                # we only accept singular 'put' calls
                if functype == 'put':
                    callback = self.putObject
                elif functype == 'delete':
                    callback = self.deleteObjects

            # if a callback was found, then make 
            if callback and functype == 'put':
                def _putObjectsWrapper(*args, **kw):
                    return callback(*args, **kw)

                return _putObjectsWrapper

            if callback and functype == 'get':
                def _getObjectsWrapper(*args, **kw):
                    return callback(*tuple([table]+list(args)), **kw)

                return _getObjectsWrapper

            if callback and functype == 'delete':
                def _deleteObjectsWrapper(*args, **kw):
                    return callback(*tuple([table]+list(args)), **kw)

                return _deleteObjectsWrapper

        raise AttributeError("%s instance has no attribute '%s'" \
              % (self.__class__.__name__, var))
        
        

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

def testDatabase():

    from rpg.sql.TestDatabase import ProduceDB, Fruit
    from rpg.sql.DBObject import DBObjectModifyKeyError
    
    db = ProduceDB(unpack=False)
    db.open()

    print('test: try getting some objects with external fields')
    result = db.getFruits(members=['Taste.goodbad'])
    print('test passed')
    
    fruit = result[0]

    print('test: make sure you can\'t modify the key members of a read object')
    try:
        fruit.fruit = 'banana'
    except DBObjectModifyKeyError:
        print('test passed')
    else:
        raise 'test failed'

    print('test: try updating an object that was read from database')
    fruit.flavour = 'likebanana'
    db.putObjects([fruit])
    print('test passed (if displayed query was update)')
    
    print('test: allow addition of new members; should not update')
    fruit.name = 'adam'
    db.putObjects([fruit])
    print('test passed if no query was displayed')

    print('test: create new object and insert into database')
    newFruit = Fruit()
    newFruit.fruit = 'apple'
    newFruit.flavour = 'salty'
    result = db.putFruits([newFruit, newFruit])
    print('test passed if insert statement displayed')

    print('test: deletion of objects')
    #db.deleteObjects(Fruit, fruit='apple')
    db.deleteFruit(fruit='apple')
    print('test passed if delete statement displayed')

    for i in range(len(result)):
        fruit = result[i]
        print(fruit.fruit, fruit.flavour, fruit.Taste.goodbad, fruit.dirty)

    result = db.getFruits(
        members=['fruit', 'seasons', 'Taste.goodbad', 'Taste.taste',
                 'Taste.states'],
        where='taste in [sweet blah]')

    for i in range(len(result)):
        #print result.rows[i]
        x = result[i]
        x.extra = 'new value'
        print('i=', i)
        print('fruit=%s' % x.fruit)
        print('flavour=%s' % x.flavour)
        print('seasons=%s' % x.seasons)
        print('Taste.taste=%s' % x.Taste.taste)
        print('Taste.goodbad=%s' % x.Taste.goodbad)
        print('Taste.states=%s' % x.Taste.states)
        #print 'price=', x.price # must have initObjs=True for this to work
        print('extra = ', x.extra)
        #import cPickle
        #print [cPickle.dumps(x)]
        print()


    db.close()

def testQueries():
    # we don't actually connect to the db in this test, only test the
    # generation of queries.
    import rpg.sql.TestDatabase as TestDatabase
    db = TestDatabase.Music(dbhost='testdb', db='Music',
                            user ='user', password='password')

    # select cases is a tuple with:
    #   - description of test
    #   - table to select from
    #   - members to select
    #   - not members to select
    #   - query
    #   - order by
    #   - count
    #   - limit by
    #   - expected result

    cases = (('simple', db.SongTable,
              [], [], '', [], False, 0, ''),
             
             ('specific fields from primary table', db.SongTable,
              ['title', 'discnum', 'filesize'], [], '', [], False, 0, ''),

             ('specific fields from primary table with where', db.SongTable,
              ['title', 'discnum', 'filesize'], [], 'title=bar',
              [], False, 0, ''),

             ('specific fields from primary table with order', db.SongTable,
              ['title', 'discnum', 'filesize'], [], '',
              ['title'], False, 0, ''),

             ('specific fields from primary table with order', db.SongTable,
              ['title', 'discnum', 'filesize'], [], '',
              ['title', '-filesize'], False, 0, ''),

             ('specific fields from primary table with order and where',
              db.SongTable,
              ['title', 'discnum', 'filesize'], [], 'title=bar',
              ['title', '-filesize'], False, 0, ''),

             ('specific fields from primary table and limit', db.SongTable,
              ['title', 'discnum', 'filesize'], [], '', [], False, 20, ''),

             ('fields from all tables', db.SongTable,
              ['title', 'discnum', 'filesize', 'Album.name', 'tracks',
               'Artist.artistid'],
              [], '', [], False, 0, ''),

             ('fields from all tables and count', db.SongTable,
              [], [], 'tracks>10', [], True, 0, ''),

             ('secondary order by fields', db.SongTable,
              ['title', 'discnum', 'filesize'], [], '', ['-Album.name', 'title'], False, 0, ''),

             ('secondary order by fields', db.SongTable,
              ['title', 'discnum', 'filesize'], [], 'Album.name=foo', [], False, 0, ''),

             ('fields from secondar and one notMember', db.SongTable,
              ['Album.name', 'tracks', 'Artist.artistid'],
              ['title'], '', [], False, 0, ''),

             )

    for case in cases:
        print(case[0])
        print(" ", db._select(*case[1:8]))

    
if __name__ == "__main__":
    #testQueries()
    testDatabase()
