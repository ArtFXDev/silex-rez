"""
An object oriented interface to an SQL database.  Classes correspond
to tables in the database, and the table fields (or columns) are defined
within each class.  Each row of the database is inserted and returned as
a class instance.

Classes that are to save instances in the database should subclass from
L{DBObject.DBObject} and define a list of L{Fields.Field} types at the
class level.  Then L{Database.Database} is subclassed to define the tables
and how they can be joined.  An independent querying language is defined in
L{Where} which allows for more natural language queries very similar to the
Python syntax.

Below is an example of using this module to create a database for organizing
music.  This example and others are also available in L{TestDatabases}.

Creating Class Objects
======================
  L{DBObject.DBObject} is the base class for all objects that will be saved
  in the database.  It expects each class to define a list if L{Fields.Field}
  objects that describe which instance member data will be saved.  To first
  start organizing music, we want objects for artists, albums, and songs.


Creating a Database

Querying for objects

Joining tables

Secondary object caching

Searching Database Attributes


"""

import rpg

#try:
#    from MySQLdb_nb import string_literal
#except ImportError:
#    from MySQLdb import string_literal

def string_literal(s):
    if type(s) == int:
        return s
    else:
        return "'%s'" % s.replace("'", "''")

__all__ = ('SQLError',
           'Fields',
           'Table',
           'DBObject',
           'Database',
           'Join',
           'Where',
           'DBFormatter',
           'DBCmdLineTool')


class SQLError(rpg.Error):
    """Base Class for all SQL errors."""
    def __init__(self, *args):
        self.args = args


