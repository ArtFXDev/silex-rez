"""The Index module enables SQL/plpython indexes to be defined and represented as python objects."""

class Index(object):
    def __init__(self, tablename, name, columns, where):
        self.name = name
        self.tablename = tablename
        self.columns = columns
        self.where = ""
        if where:
            self.where = " WHERE %s" % where

    def getCreate(self):
        return "CREATE INDEX %s ON %s (%s)%s;" % (self.name, self.tablename, self.columns, self.where)

