# PGDatabase.py

import sys, signal

import rpg.sql.Fields as DBFields
import rpg.sql.Database as Database

# MySQLdb_nb is a Pixar in-house modification that enables handling of Ctrl-C on the client gracefully
# If the _nb ("non-blocking") version can be loaded, this is noted in _hasSwapSignalHandlers
try:
    import MySQLdb_nb as MySQLdb
except ImportError:
    import MySQLdb
    _hasSwapSignalHandlers = False
else:
    _hasSwapSignalHandlers = True


class MySQLDatabase(Database.Database):

    #QueryResultCls = MySQLQueryResult - do this for a MySQL-specific query result class

    def __init__(self, *args, **kw):
        super(MySQLDatabase, self).__init__(*args, **kw)

    def getCreate(self):
        """Return create statements for creating database."""

        s = 'CREATE DATABASE %s;\n' % self.db
        s += 'USE %s;\n' % self.db

        for t in self.Tables:
            s += t.getCreate()

        return s


    def open(self, timeout=0, dictcursor=True):
        """Open connection to database.

        @param timeout: timeout in seconds
        @type timeout: int

        @param dictcursor: by default, all rows returned from the database
          are dictionaries.  Setting this to False will return them as tuples.
        @type dictcursor: boolean
        """

        if self.connection:
            raise Database.SQLConnectionError("connection already established, call close() first")

        # open db connection
        try:
            self.dprint('open database connection')
            self.connection = MySQLdb.connect(host=self.dbhost,
                                              user=self.user,
                                              passwd=self.password,
                                              db=self.db,
                                              connect_timeout=timeout,
                                              autocommit=self.autocommit)
        except MySQLdb.MySQLError:
            raise Database.SQLConnectionError("unable to establish connection with database %s on "\
                  "%s as user '%s'" % (self.db, self.dbhost, self.user))

        else:
            pass
            # Try to turn on/off the autocommit feature of MySQLdb
            #if hasattr(self.connection, 'autocommit'):
            #    self.connection.autocommit(self.autocommit)

            # try:
            #     # establish a cursor for future queries
            #     if dictcursor:
            #         self.cursor = \
            #                 self.connection.cursor(MySQLdb.cursors.DictCursor)
            #     else:
            #         self.cursor = \
            #                     self.connection.cursor(MySQLdb.cursors.Cursor)
            # except MySQLdb.MySQLError:
            #     raise Database.SQLConnectionError, \
            #           'unable to get cursor for database %s on %s' % \
            #           (self.db, self.dbhost)

            # if self.swap_signals:
            #     if _hasSwapSignalHandlers:
            #         MySQLdb.swapSignalHandlers()


    def close(self):
        """Close connection with database."""

        self.dprint('close database connection')

        # close connection
        try:
            if self.connection:
                self.connection.close()
            self.connection = None
        except MySQLdb.MySQLError:
            raise Database.SQLConnectionError('unable to close database connection with %s on %s' % \
                  (self.db, self.dbhost))

        # unswap all the signal handlers when we close
        if self.swap_signals:
            if _hasSwapSignalHandlers:
                MySQLdb.unswapSignalHandlers()
            self.reset_handler = True


    def _initTimeout(self, alwaysReset=1):
        """Initialize the signal handler needed to properly catch a SIGALRM
        if called while executing a query."""

        #print 'setting handler'
        signal.signal(signal.SIGALRM, _timeoutCB)
        if self.swap_signals:
            #print 'swapping handler'
            if _hasSwapSignalHandlers:
                MySQLdb.swapSignalHandlers([signal.SIGALRM])
        self.reset_handler = alwaysReset
        

    def _unsetTimeout(self):
        """Unset the reset_handler flag, so all other calls will be forced
        to update the signal handler."""
        self.reset_handler = True

    def _execute(self, query, multi=None):
        """Execute SQL query, but add query to error message if exception."""

        #if not self.cursor:
        #    raise Database.SQLNotOpen, "database connection not opened yet."

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
                return

            # execute query or multiple queries
            if multi:
                self.cursor = self.connection.executemany(query, multi)
            else:
                self.cursor = self.connection.execute(query)

            # unset alarm because we didn't block
            if self.timeout:
                signal.alarm(0)

        except MySQLdb.IntegrityError as err:
            # unset alarm because we didn't block
            if self.timeout:
                signal.alarm(0)
            raise Database.SQLDuplicateInsert(str(err))
        except MySQLdb.Warning as err:
            # unset alarm because we didn't block
            if self.timeout:
                signal.alarm(0)
            raise Database.SQLWarning(str(err) + '\n' + query)
        except MySQLdb.MySQLError:
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
