"""Starting point for ODBC Database connectivity."""

# TODO: we don't really need to call this "MySQLdb" since MySQL-specific
# code has now been separated into MySQLDatabase.py.

# had to set up a dummy exception in case pyodbc is not available
try:
    import pyodbc
    _mysqlerror = pyodbc.Error
except ImportError:
    _mysqlerror = Exception

class MySQLdb(object):
    """This class simulates the original MySQLdb class but uses pyodbc."""
    MySQLError = _mysqlerror
    
    class IntegrityError(MySQLError):
        pass

    class Warning(MySQLError):
        pass
    
    def connect(cls, host="localhost", user="root", passwd="", db="Test", connect_timeout=0,
                autocommit=True):
        dsn = "SERVER=%s;DATABASE=%s;USER=%s;PASSWORD=%s" % (host, db, user, passwd)
        connection = pyodbc.connect(dsn, autocommit=autocommit)
        return connection
    connect = classmethod(connect)
