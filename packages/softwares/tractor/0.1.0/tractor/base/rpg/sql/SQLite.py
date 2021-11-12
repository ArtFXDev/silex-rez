"""Wrapper around the base rpg.sql objects to support a SQLite database."""

import rpg.sql.Fields as DBFields
import rpg.sql.Table as Table
import rpg.sql.Database as Database

class VarCharField(DBFields.VarCharField):
    """SQLite handles values containing single quotes differently than
    MySQL, so we can't use the string_literal() method to put backslashes
    in front.  Instead we need to turn ' into ''."""

    def pack(self, data):
        if data is None:
            if self.default is None:
                return 'NULL'
            return string_literal('')
        return "'%s'" % data.replace("'", "''")


class AutoIncField(DBFields.AutoIncField):
    """SQLite declares auto increment fields as 'autoincrement' and they
    have to be the primary key."""

    def getDefault(self):
        return None

    def __str__(self):
        """Return the mysql statement that is needed to create this field."""
        return "%s INTEGER PRIMARY KEY AUTOINCREMENT" % (self.fieldname)



class SQLiteTable(Table.Table):

    def getCreate(self):
        """Get a valid create statement that can be sent to the database
        to make this table."""
        
        mystr = "CREATE TABLE %s (\n" % self.tablename
        lines = []
        
        # declare all the fields
        for field in self.fields:
            lines.append("  %s" % str(field))
        # add the keys
        if self.keys:
            # only add keys if there aren't any auto inc fields
            keys = [f.fieldname for f in self.keys
                    if not isinstance(f, AutoIncField)]
            if len(self.keys) == len(keys):
                lines.append("  PRIMARY KEY (%s)" % ','.join(keys))

        mystr += ',\n'.join(lines) + '\n);\n'

        return mystr


    def getCreateIndexes(self):
        """Get a list of the CREATE INDEX statements that need to be
        executed."""
        creates = []
        # add any indexes
        for index in self.indexes:
            fieldnames = ''.join([f.fieldname for f in index])
            indexstrings = []
            for field in index:
                indexstrings.append(field.fieldname)
            creates.append("CREATE INDEX %s_%s_index ON %s (%s)" %
                           (self.tablename, fieldnames, self.tablename,
                            ','.join(indexstrings)))
        return creates


class SQLiteQR(Database.QueryResult):

    def _row2obj(self, row):
        """Overloaded in order to convert the row into a dictionary."""
        # convert the row to a dictionary
        row = dict([(key, row[key]) for key in list(row.keys())])
        return super(SQLiteQR, self)._row2obj(row)

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
            rowkeys = list(self.rows[0].keys())
        else:
            rowkeys = []
        widths = dict([(column, 1) for column in rowkeys])

        # iterate over each key and check for keys from joined tables
        for row in self.rows:
            for column, val in row.items():
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
                    raise SQLUnknownField("unable to find a Field object " \
                          "for the row key '%s'" % rowkey)

            widths[field] = widths.pop(rowkey)
        return widths


class SQLiteDB(Database.Database):

    QueryResultCls = SQLiteQR

    def __init__(self, dbfile, autocommit=True, debug=False, unpack=False):
        self.dbfile = dbfile
        super(SQLiteDB, self).__init__(debug=debug, unpack=unpack,
                                       dbhost='file', db=self.dbfile,
                                       autocommit=autocommit,
                                       user=' ', password=' ')

    def _create(self):
        for tbl in self.Tables:
            # add the table
            self._execute(tbl.getCreate())
            # and the indexes since these are done differently in sqlite
            for index in tbl.getCreateIndexes():
                self._execute(index)


    def open(self, create=True, timeout=0, dictcursor=True):
        if self.connection:
            raise Database.SQLConnectionError("database file is already opened, call close() first")

        # make sure the directories for the dbfile are created
        if create:
            import os
            import rpg.pathutil as pathutil
            pathutil.makedirs(os.path.dirname(self.dbfile))
            # check if the dbfile exists
            if os.path.exists(self.dbfile):
                create = False

        # open db connection
        self.dprint('open database file: %s' % self.dbfile)

        try:
            import sqlite3
        except ImportError:
            from pysqlite2 import dbapi2 as sqlite3
        self.connection = sqlite3.connect(self.dbfile)

        # establish a cursor for future queries
        if dictcursor:
            self.connection.row_factory = sqlite3.Row

        self.cursor = self.connection.cursor()

        # create the tables
        if create:
            self._create()


    def commit(self):
        """Commit the most recent transaction(s)."""
        self.dprint("COMMIT")
        self.connection.commit()


    def _execute(self, query):
        super(SQLiteDB, self)._execute(query)
        # call commit for non selects
        if self.autocommit and query[:6].lower() != "select":
            self.commit()

    def rowCount(self):
        raise Database.SQLError("SQLite cursors do not provide a row count.")
