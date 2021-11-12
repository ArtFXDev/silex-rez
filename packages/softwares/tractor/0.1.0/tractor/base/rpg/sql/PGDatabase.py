# PGDatabase.py

import sys

# NOTE: while you might expect remove rpg.sql from these imports, it can break python's assumptions about
# how these modules are loaded and create odd errors such as invalid joins.  Make any such changes armed
# with deep knowledge.
import rpg.sql.Fields as DBFields
import rpg.sql.Database as Database
import rpg.sql.Join as Join
from . import Function

DEFAULT_PG_PORT = 5432

class PGJoin(Join.Join):
    """Override joinStr() method to support table inheritance "ONLY" keyword."""

    def joinStr(self, only=True):
        if self.fulljoin:
            return self.fulljoin
        leftStr = "LEFT " if self.useLeftJoin else ""
        onlyStr = "ONLY " if only else ""
        return "%sJOIN %s%s ON (%s)" % \
               (leftStr, onlyStr, self.rightTable.alias, self.onclause)


class PGQueryResult(Database.QueryResult):

    def __init__(self, *args, **kw):
        # cache for translating postgres lower-case field names to mixed case
        self.rowKey2camelCaseRowKey = {}
        # continue with init
        super(PGQueryResult, self).__init__(*args, **kw)

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

        # grab the keys from the first row, we assume all rows have identical keys.
        rowkeys = self._camelCaseRowKeys()
        
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

    def _camelCaseRowKeys(self):
        return [self._camelCaseClassNameForRowKey(col[0]) for col in self.database.cursor.description]

    def _camelCaseClassNameForRowKey(self, oldkey):
       """Convert _classname_attribute to _ClassName_attribute."""
       # first check if value is cached
       newkey = self.rowKey2camelCaseRowKey.get(oldkey)
       if newkey:
           return newkey
       # not cached; compute it
       for table in self.database.Tables:
           classname = table.tablename
           if oldkey.startswith("_" + classname.lower() + "_"):
               newkey = "_" + classname + oldkey[len(classname)+1:]
       self.rowKey2camelCaseRowKey[oldkey] = newkey or oldkey
       return newkey or oldkey

    def _row2obj(self, rowobj):
        """Overloaded in order to convert the row into a dictionary."""
        # convert the row to a dictionary
        rowdict = {}
        for objkey in list(rowobj.keys()):
            dictkey = self._camelCaseClassNameForRowKey(objkey)
            rowdict[dictkey] = rowobj[objkey]
        return super(PGQueryResult, self)._row2obj(rowdict)

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
        if self.rows:
            rowkeys = self._camelCaseRowKeys()
            #rowkeys = self.rows[0].keys()
        else:
            rowkeys = []
        widths = dict([(column, 1) for column in rowkeys])

        # iterate over each key and check for keys from joined tables
        for row in self.rows:
            for column, val in row.items():
                column = self._camelCaseClassNameForRowKey(column)
                if type(val) in [str, str]:
                    widths[column] = max(widths[column], len(val))

        # convert keys from _Table_field to Class.member or field to Class.member
        for rowkey in rowkeys:
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
                    raise Database.SQLUnknownField("unable to find a Field object " \
                          "for the row key '%s'" % rowkey)

            widths[field] = widths.pop(rowkey)
        return widths


class PGDatabase(Database.Database):
    Tables = []
    Functions = []
    Views = []
    QueryResultCls = PGQueryResult
    JoinCls = PGJoin
    # subclass overrides this with list of enumerated field classes so that they can be defined before table creation
    EnumTypes = [] 

    def __init__(self, *args, **kw):
        super(PGDatabase, self).__init__(*args, **kw)
        if self.port is None:
            self.port = DEFAULT_PG_PORT

    def getCreate(self):
        """Return create statements for creating database."""
        parts = []
        # create the database
        parts.append("CREATE DATABASE %s;\n" % self.db)
        # switch to the database before creating tables
        parts.append("\c %s;\n" % self.db)
        # first define any new field types (such as enum)
        for enumType in self.EnumTypes:
            parts.append("CREATE TYPE %s AS ENUM (%s);\n" % (enumType.FTYPE, ",".join(["'%s'" % v for v in enumType.VALUES])))
        # iterate over tables
        for table in self.Tables:
            # get table creation definition
            parts.append(table.getCreate())
            # get index creation definition 
            parts.append(table.getCreateIndexes())
            parts.append("\n")
        # iterate over functions
        parts.append(Function.FUNCTION_PREAMBLE)
        for function in self.Functions:
            # get function creation definition
            parts.append(function.getCreate())
            parts.append("\n")
        # iterate over views
        parts.append("\n")
        for view in self.Views:
            # get function creation definition
            parts.append(view.getCreate())
            parts.append("\n")

        return "".join(parts)

    def functionByName(self, fname, parameters=None):
        """Returns the function with the given name, or None if not found.

        @param fname: function name
        @type fname: string
        """
        for function in self.Functions:
            if function.name == fname:
                if parameters is None or parameters == function.parameters:
                    return function
        return None
    functionByName = classmethod(functionByName)

    def viewByName(self, vname):
        """Returns the function with the given name, or None if not found.

        @param fname: function name
        @type fname: string
        """
        for view in self.Views:
            if view.name == vname:
                return view
        return None
    viewByName = classmethod(viewByName)

    def open(self, timeout=0):
        """Open connection to database.

        @param timeout: timeout in seconds
        @type timeout: int
        """
        # NOTE: psycopg2 is not shipped with Tractor
        # database connectivity is managed through
        # the engine
        import psycopg2
        import psycopg2.extras
        
        if self.connection:
            raise Database.SQLConnectionError("connection already established, call close() first")
        # open db connection
        if not self.dbhost:
            raise Database.SQLError('No dbhost specified.')
        if not self.db:
            raise Database.SQLError('No db specified.')
        if not self.user:
            raise Database.SQLError('No user specified.')
        try:
            self.dprint('open database connection')
            connStr = "dbname=%s user=%s host=%s port=%d" \
                      % (self.db, self.user, self.dbhost, self.port)
            if self.password:
                connStr += " password=%s" % self.password
            self.connection = psycopg2.connect(connStr)
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            # password, timeout, autocommit
        except Exception as err:
            raise Database.SQLConnectionError("unable to establish connection with database %s on "\
                  "%s as user '%s': %s" % (self.db, self.dbhost, self.user, str(err)))

    def close(self):
        """Close connection with database."""

        self.dprint('close database connection')

        # close connection
        try:
            if self.connection:
                self.connection.close()
            self.connection = None
        except Exception as err:
            raise Database.SQLConnectionError("unable to close database connection with %s on %s: %s" % \
                  (self.db, self.dbhost, str(error)))

    def _execute(self, query, multi=None):
        """Execute SQL query, but add query to error message if exception."""
        import psycopg2

        if not self.connection:
            raise Database.SQLNotOpen("database connection not opened yet.")

        if not query: return

        try:
            # set alarm signal as mechanism to avoid blocking
            if self.timeout:
                if self.reset_handler:
                    self._initTimeout(self.reset_handler)
                #print 'setting alarm', self.timeout
                signal.alarm(self.timeout)

            # display debugging message
            self.dprint(query)

            # don't execute query if we're in read only mode
            if self.readonly and query[:6] != 'SELECT':
                raise Database.SQLError("database is in read only mode. query not permitted: %s" % str(query))

            # execute query or multiple queries
            if multi:
                self.cursor.executemany(query, multi)
            else:
                self.cursor.execute(query)

            # unset alarm because we didn't block
            if self.timeout:
                signal.alarm(0)

        except psycopg2.IntegrityError as err:
            # unset alarm because we didn't block
            if self.timeout:
                signal.alarm(0)
            raise Database.SQLDuplicateInsert(str(err))
        except psycopg2.Warning as err:
            # unset alarm because we didn't block
            if self.timeout:
                signal.alarm(0)
            raise Database.SQLWarning(str(err) + '\n' + query)
        except psycopg2.Error:
            # unset alarm because we didn't block
            if self.timeout:
                signal.alarm(0)
            e = str(sys.exc_info()[0]) + "\n" + str(sys.exc_info()[1]) + "\n" + \
                query
            raise Database.SQLQueryError(e)
        except:
            # unset alarm because we didn't block
            if self.timeout:
                signal.alarm(0)
            raise


    def _select(self, table, members=None, notMembers=None, virtual=True,
                where=None, groupby=None, orderby=None, count=False, limit=0,
                offset=0, only=True, aliases=None,
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

        @param only: if True, then use ONLY keyword to indicate that only 
          parent table is to be queried
        @type only: boolean

        @return: a SELECT query
        @rtype: string
        """

        # get the fields we need to select for
        fields,extTables = self._getFields(table, members, notMembers,
                                            virtual=virtual)

        # get the where string.  this will add tables to 'extTables' if
        # they aren't already in the list
        whereStr = self._getWhereStr(table, where, whereargs, extTables, aliases=aliases)

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
        tableStr = self._join(table, extTables=extTables, only=only)
        
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

    def _join(self, primary, extTables=None, only=True):
        """Create a valid JOIN string based on all of the given tables."""

        # FUTURE: this is really simple right now; may be more dynamic
        # later where the user won't have to specify all table relationships

        # algorithm: first try to join the first table with all other
        # tables; if that fails, try to join other tables

        # in this method 'table' refers to 'tablename'

        # consider whether only parent table should be used
        joinStr = "ONLY " + primary.tablename if only else primary.tablename

        # if no extTables are provided, then we don't need to join
        if not extTables:
            return joinStr

        # when joining we always try to establish an immediate connection
        # first, i.e. left join PRIMARY with SECONDARY.  Once that has been
        # checked, then we try to join with a SECONDARY table that has
        # already joined.  This process is repeated until a match is found
        # or all options are exhausted.

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
                            joinStr += ' ' + join.joinStr(only=only)
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
                raise Database.SQLError("unable to join %s with '%s'" % \
                      (','.join([t.tablename for t in extTables]),
                       primary.tablename))

            # reset the level pointers
            currLevel = nextLevel
            nextLevel = []

        return joinStr

    def getObjects(self, table, objtype=None,
                   members=[], notMembers=[],
                   virtual=True,
                   where=None, orderby=[],
                   limit=0, only=True, **whereargs):
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

        @param only: false when child tables should also be queried
        @type limit: boolean

        @return: a list of all the rows found matching the provided query.
        @rtype: L{Database.QueryResult}

        """

        query = self._select(table, members=members, notMembers=notMembers,
                             virtual=virtual, where=where, orderby=orderby,
                             limit=limit, only=only, **whereargs)
        self._execute(query)
        rows = self.cursor.fetchall()

        result = self.QueryResultCls(rows, table, self, objtype=objtype,
                                     unpack=self.unpack,
                                     info=self.cursor.description)
        return result
        
