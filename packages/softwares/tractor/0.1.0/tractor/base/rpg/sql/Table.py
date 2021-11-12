# Table.py

import types
from rpg.sql import SQLError
from rpg.sql.Fields import ObjTypeField, AutoIncField

# KQ: How do we specify that fields belong to more than one index?
#     Note that their index length may be different in each index.
#     So it may not be a good idea to define a Field as being indexed,
#     and leave that to the Table definition.  By the same token, should
#     keys be defined at the Table scope as well?


class TableError(SQLError):
    pass


class FieldExists(TableError):
    """Raised when a field is added to a table and one with that name
    already exists."""
    pass

class FieldNotFound(TableError):
    """No field was found while trying to replace a field."""
    pass


class Table(object):
    """A Table object corresponds to a table in the database.  Each field
    in the table should map back to a member of the base class object or
    some subclass of it.  The fields in the table are a flattened
    representation of all the members from the class hiearchy.  Thus, a
    field might only be defined in a subclass and be empty for all
    rows that do not match that type."""

    def __init__(self, objects, tablename=None, alias=None, whereAliases=None,
                 indexes=[], engine=None):
        """Initialize a table by providing one or more objects that will
        be saved in it."""

        # the user can pass in a list of objects or just a single object
        if type(objects) not in (list, tuple):
            objects = [objects]

        # save a pointer to the all the objects
        self.objects    = objects

        # the first class in our objects list is assumed to be the root.
        self.baseClass  = objects[0]

        # name of the table
        if tablename:
            self.tablename = tablename
        else:
            self.tablename = self.baseClass.__name__

        if not alias:
            self.alias = self.tablename
        else:
            self.alias = "%s AS %s" % (alias, self.tablename)

        # store complex indexes hwere
        self.complexIndexes = indexes

        self.engine = engine

        # a list of the fields that are keyed
        self.keys = []

        # a list of equivelent keys to use if the primary key is a single
        # auto increment field and not set yet.
        self.equivKeys = []

        # each item is a list of field names that create the index
        self.indexes = []

        # a mapping for field name to Field object
        self.name2field = {}
        # a mapping for object member name to Field object
        self.member2field = {}

        # a list of the fields in the table.  This is a list because the order
        # must be preserved
        self.fields = []
        # list of virtual fields referenced by the table's objects
        self.vfields = []

        # a dictionary used to map object types specified in an ObjTypeField
        # to a valid object, make a copy of the dictionary so we gaurantee
        # nothing changes out from under us.
        self.subClasses = {}

        # pointer to the database we belong to.  This will be set by the
        # database when it is initialized.
        self.database = None

        # where aliases that are used to simplify queries on this table
        if whereAliases is None:
            self.whereAliases = {}
        else:
            self.whereAliases = whereAliases.copy()

        # a table can optionally be setup to store data from multiple object
        # types, as long as they are all subclassed from the base class.
        self.objtype = None

        # iterate through each of our objects and add the fields to the table
        for obj in objects:
            obj._table = self
            self.subClasses[obj.__name__] = obj
            # we explicitly look at the objects dictionary to avoid looking
            # at inherited fields that have already been added.
            for field in obj.__dict__.get('Fields', []):
                self.addField(field, obj)
            # handle virtual fields
            for field in obj.__dict__.get('VirtualFields', []):
                field.table = self
                field.classObject = obj
                self.vfields.append(field)
            # make any aliases defined by this object
            obj._makeProperties()

        # check if an objtype field is set yet.
        if len(objects) > 1 and not self.objtype:
            self.addField(ObjTypeField('objtype', index=True), prepend=True)
            # dynamically make a property in the base class so 'objtype'
            # can be referenced.
            funcStr = "def get_objtype(self):\n" \
                      "  return self.__class__.__name__\n"
            # exec the code so the function works
            exec(funcStr)
            # create and set the property
            setattr(self.baseClass, 'objtype',
                    property(fget=eval("get_objtype")))


    def addField(self, field, obj=None, prepend=False):
        """Add a field to this table.  This will append the provided field
        to the end of the field list, so be sure to get the order correct."""
        # take note of an object type field
        if isinstance(field, ObjTypeField):
            # we can't have more than on objtype, so complain if it has
            # already been set
            if self.objtype:
                raise TableError("an ObjTypeField was already set for " \
                      "the field '%s'" % self.objtype.fieldname)
            self.objtype = field
        # add the field to the list
        if prepend:
            self.fields.insert(0, field)
        else:
            self.fields.append(field)

        # give the field a pointer to ourself (hack!!)
        field.table = self
        field.classObject = obj

        # check if the field is keyed
        if field.key:
            self.keys.append(field)

        # check if the field is an equivelent key
        #print 'field', field.fieldname
        if field.equivKey:
            self.equivKeys.append(field)

        # check if the field is indexed
        if field.index:
            self.indexes.append([field])

        # make a mapping from field name to Field object
        if field.fieldname in self.name2field:
            raise FieldExists("a Field object for '%s' already exists." % \
                  field.fieldname)
        self.name2field[field.fieldname] = field

        # make a mapping from member name to Field object
        if field.member in self.member2field:
            raise FieldExists("a Field object for member '%s' already " \
                  "exists." % field.member)
        self.member2field[field.member] = field


    def className(self):
        """Return the name of the class of the objects the table stores."""
        return self.baseClass.__name__


    def addIndex(self, *fieldnames):
        """Add an index to this table."""

        # FUTURE: could provide a dictionary indexlen that would
        # override indexlens for fields
        # e.g. self.addIndex('seq', 'shot', indexlen={'seq':12})

        fields = [self.name2field[name] for name in fieldnames]
        self.indexes.append(fields)


    def addSubClass(self, name, cls):
        """Add a subclass to the class map for this table.  This will allow
        objects of the provided type to be inserted into this table."""
        # make sure the class is a subclass of our base class
        if not issubclass(cls, self.baseClass):
            raise TableError("%s is not a subclass of %s" % \
                  (cls.__name__, self.baseClass.__name__))
        self.subClasses[name] = cls


    def getCreate(self):
        """Get a valid create statement that can be sent to the database
        to make this table."""

        lines = []

        # declare all the fields
        for field in self.fields:
            lines.append("  %s" % str(field))
        # add the keys
        if self.keys:
            lines.append("  PRIMARY KEY (%s)" % \
                         ','.join([f.fieldname for f in self.keys]))
        if self.engine:
            # for mysql only
            tableTypeStr = " TYPE=%s" % self.engine
        else:
            tableTypeStr = ""

        s = "CREATE TABLE %s (\n%s\n)%s;\n" % (self.tablename, ",\n".join(lines), tableTypeStr)

        return s

    def getCreateIndexes(self):
        """Return a string with the required CREATE INDEX statements for this table."""
        creates = []
        # add any indexes
        for index in self.indexes:
            fieldnames = ''.join([f.fieldname for f in index])
            indexstrings = []
            for field in index:
                indexstrings.append(field.fieldname)
            creates.append("CREATE INDEX %s_%s_index ON %s (%s);" %
                           (self.tablename, fieldnames, self.tablename,
                            ','.join(indexstrings)))
        for index in self.complexIndexes:
            creates.append(index.getCreate())
        return "\n".join(creates) + "\n"

    def getAlter(self):
        """Get the appropriate alter statement that is needed to sync this
        table with the one currently configured in the database."""
        pass


    def getAutoIncKey(self):
        """Return the auto increment key field, if one exists.  If one does
        not exist, then return None."""

        if len(self.keys) == 1 and isinstance(self.keys[0], AutoIncField):
            return self.keys[0]
        return None

    def fieldByName(self, fieldname):
        """Return field as identified by fieldname."""
        return self.name2field.get(fieldname)


    def fieldByMember(self, member):
        """Return field as identified by member."""
        for obj in self.objects:
            field = obj.fieldByMember(member)
            if field: return field
        if self.objtype and self.objtype.member == member:
            return self.objtype

        return None

    def _replaceField(self, member, newfield):
        """Replace a Field instance in this table with a new one.  This
        is intended to be a temporary work around to the fact that new
        field types cannot be easily specified."""
        # find the original field
        for i in range(len(self.fields)):
            orig = self.fields[i]
            if orig.member == member:
                newfield.table = orig.table
                newfield.classObject = orig.classObject
                self.fields[i] = newfield
                # replace the field in its object too
                orig.classObject._replaceField(member, newfield)
                break
        else:
            raise FieldNotFound("the field for member '%s' was not found " \
                  "in the table '%s'" % (member, self.tablename))


    def _fieldByRowKey(self, rowkey):
        """Return field as identified by a rowkey in a QueryResult."""
        for obj in self.objects:
            # call the class objects method but with the checkTable flag
            # set to false to avoid an infinite loop if the rowkey isn't
            # found in this object.
            field = obj._fieldByRowKey(rowkey, checkTable=False)
            if field: return field
        if self.objtype and self.objtype.selectName == rowkey:
            return self.objtype


    def fieldByAlias(self, alias):
        """Return field as identified by an alias; default to member
        if no alias found."""
        for obj in self.objects:
            if alias in obj.Aliases:
                return self.fieldByMember(obj.Aliases[alias])

        return self.fieldByMember(alias)


    def fieldByWord(self, word):
        """Return field as identified by fieldname, member, or alias."""
        f = self.name2field.get(word) or \
            self.member2field.get(word) or \
            None

        return f


    def fieldnamesByMembers(self, members):
        """Return field as identified by member, or None."""

        return [self.member2field[member].fieldname for member in members]


    def membersByFieldnames(self, fieldnames):
        """Return field as identified by member, or None."""

        return [self.name2field[fieldname].member for fieldname in fieldnames]

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.tablename)

# ---------------------------------------------------------------------------

def testTable():

    from rpg.sql.Field import VarCharField, IntField
    from rpg.sql.DBObject import DBObject

    # Example 1: test simple table, with single key and single index

    class Fruit(DBObject):
        def __init__(self):
            self.fruit = ''
            self.taste = ''
            DBObject.__init__(self)

    fields = [
        VarCharField('fruit', key=True),
        VarCharField('taste', index=True)
        ]

    t = Table('fruit', Fruit, fields=fields)
    print(t.getCreate())

    # Example 2: compound key and compound index

    class Task(DBObject):
        def __init__(self):
            self.jid = 0
            self.tid = 0
            self.show = ''
            self.seq = ''
            DBObject.__init__(self)

    fields = [
        IntField('jid', key=True),
        IntField('tid', key=True),
        VarCharField('show', index=True, indexlen=8),
        VarCharField('seq', index=True),
        ]

    t = Table('Task', Task, fields=fields)
    t.addIndex('show', 'seq')
    print(t.getCreate())


if __name__=='__main__':
    testTable()

    # TODO: create a table with a compound key, and a compound index

