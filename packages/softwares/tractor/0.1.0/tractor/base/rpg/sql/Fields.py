"""
This module defines the most common field types used in the database.  Each
instance of a L{Field.Field} is meant to correspond to a member of a
L{DBObject.DBObject} subclass instance.
"""

# TODO: figure out why a DictField must specify default={}, or else
# the DBObject's member will be None on initialization

ALLOW_UNSIGNED = False # postgres does not support unsigned ints

import pickle, re, time, datetime, json
from rpg.sql import SQLError, string_literal

__all__ = ('FieldError',
           'Field',
           'ArrayField',
           'VarCharField',
           'AutoIncField',
           'SerialField',
           'TinyIntField',
           'SmallIntField',
           'IntField',
           'BigIntField',
           'BooleanField',
           'TimeIntField',
           'TimestampField',
           'ByteIntField',
           'KiloByteField',
           'MegaByteField',
           'MegaByteFloatField',
           'GigaByteField',
           'GigaByteFloatField',
           'SecsIntField',
           'FloatField',
           'SecsFloatField',
           'EnumField',
           'BlobField',
           'StrListField',
           'IntListField',
           'StrArrayField',
           'StrArrayArrayField',
           'IntArrayField',
           'TimestampArrayField',
           'DictField',
           'JSONField',
           'InetField',
           'UUIDField',
           'ObjTypeField',
           'VirtualField')

class FieldError(SQLError):
    """Base error for all errors related to L{Field.Field} objects."""
    pass


class Field(object):
    """A field is a column in a table, and it should correspond to a member of
    an object.  A subclass should be made for each data type to be stored."""

    # *** NOTE: Packed fields may require retesting since they haven't been used in tractor yet ***
    PackedPrefix = 'packed_'  
    ExternalFieldFlag = '._'
    # regexp that will be used to check for fields from a joined table
    ExtFieldRE = re.compile("^([^\.]+)\.(.+)$") # "not dots" + "dot" + "chars" e.g. Task.title

    def __init__(self, fieldname, ftype, key=False, index=False, indexlen=None,
                 member=None, default=None, equivKey=False, packed=False):
        """
        @param fieldname: name the field and the name it will have in the
          database.
        @type fieldname: string

        @param ftype: Type the field will have in the database.  This should
          be a string that the database will understand.
        @type ftype: string

        @param key: set to True if this field is part of the table key.
        @type key: boolean

        @param index: set to True if this field should be indexed.
        @type index: boolean

        @param indexlen: if the field should be indexed, and it is a string,
          then an index length can be provided.
        @type indexlen: int

        @param member: if the name of the instance member is different than
          the field name, then provide the member name here.
        @type member: string

        @param default: the default value of this field
        @type default: varying

        @param equivKey: set to True if this field is part of an equivalent
          key.  This is used if the table has an auto increment field.
        @type equivKey: boolean

        @param packed: set to True if the field packs its data when sending
          it to the database.
        @type packed: boolean
        """

        # the name of the field in the table
        self.fieldname = fieldname
        # the type that will be used in the database
        self.ftype = ftype
        # used to determine whether this field is a key or not
        self.key = key
        # the field might not be part of the primary key, but if the object
        # has not have an id set, this field will be used to determine
        # whether one exists.
        self.equivKey = equivKey
        # this is True if the field is indexed
        self.index = index
        # a non-None value will prevent a large string/blob field from
        # being indexed in its entirety
        self.indexlen = indexlen
        # by default it is assumed the field name is the same as the member
        # in the object, but if they differ then this value is used
        if member:
            self.member = member
        else:
            self.member = fieldname
        # default value for this field in the database, a value of None
        # means the default value will be NULL
        self.default = default

        # boolean indicating whether or not the field packs its data
        self.packed  = packed

        # the name that will be used when this field is selected
        self.selectName = self.fieldname

        if self.member != self.fieldname:
            self.packed = True

        # if the field is packed, then give it a prefix
        if self.packed:
            self.selectName = self.PackedPrefix + self.member

        # the table this field is a member of.  this is set when the
        # field is added to its table
        self.table = None
        # the class object this field is a member of.  this is set when the
        # field is added to its table
        self.classObject = None

    def getFullMemberName(self):
        """Return the full member name with the base class prepended."""
        return "%s.%s" % (self.table.baseClass.__name__, self.member)
    fullname = property(fget=getFullMemberName)

    def getDefault(self):
        """Return a default value for this field.  Subclasses should
        overload this incase self.default is not sufficient.  For example,
        if the default value is a list, then a new list instance needs
        to be returned."""
        return self.default

    def getSelect(self, table, extTables):
        """When this field is included in a select query, this method will
        be called to determine how the field should be selected from
        the table.

        @param table: the primary table used in the query.
        @type table: L{Table.Table} instance

        @param extTables: list of tables that will be joined in the query
        @type extTables: list of L{Table.Table} instances

        @return: portion of a select query used to select this field
        @rtype: string
        """

        # if this field is not from the primary table, then give it a
        # unique name using 'AS'
        if table != self.table:
            return '%s.%s AS "%s.%s"' % \
                   (self.table.tablename, self.fieldname,
                    self.table.baseClass.__name__, self.selectName)

        # if the query will require a join, then include the table name to
        # avoid name conflicts.
        if extTables:
            return "%s.%s AS %s" % \
                   (self.table.tablename, self.fieldname, self.selectName)

        # if the field's select name is not the same as its
        # field name, then use 'AS' to rename it
        if self.packed:
            return self.fieldname + ' AS ' + self.selectName

        # otherwise return the plain field name
        return self.fieldname

    def getOrderBy(self, descending=False, **kw):
        """When this field is included in an 'ORDER BY' portion of a query,
        this method is called to determine how the field should be included
        in the order.

        @param descending: by default the order will be ascending, set this
          to True, if rows should be in descending (reverse) order.
        @type descending: boolean

        @return: portion of the query used to order by this field
        @rtype: string
        """

        # get the base
        base = "%s.%s" % (self.table.tablename, self.fieldname)
        # should we reverse the order?
        return base + " DESC" if descending else base

    def getWhere(self):
        """When this field is referenced in a where string it needs to
        be in a form the database can identify it by.

        @return: the full name of this field, including the table name.
        @rtype: string
        """
        return "%s.%s" % (self.table.tablename, self.fieldname)

    def pack(self, data):
        """Pack the data for this field to be saved in the database.  By
        default the value is cast to a string.

        @param data: the data that needs to be packed
        @type data: varying

        @return: a packed representation of the passed in data that can
          be saved to the database
        @rtype: varying
        """
        if data is None:
            if self.default is None:
                return 'NULL'
            return string_literal('')
        return string_literal(data)

    def unpack(self, data):
        """Unpack the data for this field to be saved in the its object.  By
        default nothing is done because the database libraries generally
        return an exceptable base type (e.g. str, int, long, float)."""
        return data

    def asCSV(self, data):
        """Pack the data for representation in a CSV file."""
        return self.pack(data)
    
    def __str__(self):
        """Return the mysql statement that is needed to create this field."""
        mystr = "%s %s " % (self.fieldname, self.ftype)
        # check if the default type is NULL or not
        if self.default is not None:
            mystr += " default %s" % self.pack(self.default)
        else:
            mystr += "default NULL"
        return mystr

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.fieldname)


class ArrayField(Field):
    """A field for saving a list of values."""

    def __init__(self, fieldname, ftype, **kwargs):
        """
        @param fieldname: name of the field
        @type fieldname: string

        @param ftype: type of the values in the array
        @type ftype: string
        """
        Field.__init__(self, fieldname, ftype + '[]', **kwargs)

    def pack(self, data):
        return "'{" + ",".join([str(d) for d in data]) + "}'"

    def asCSV(self, data):
        return "{" + ",".join([str(d) for d in data]) + "}"


class CharField(Field):
    """A field for saving fixed length strings."""

    def __init__(self, fieldname, length=255, **kwargs):
        """
        @param fieldname: name of the field
        @type fieldname: string

        @param length: number of characters a string will have
        @type length: int
        """
        Field.__init__(self, fieldname, 'char(%d)' % length, **kwargs)


class VarCharField(Field):
    """A field for saving variable length strings."""

    def __init__(self, fieldname, length=255, **kwargs):
        """
        @param fieldname: name of the field
        @type fieldname: string

        @param length: maximum number of characters a string will have
        @type length: int
        """
        Field.__init__(self, fieldname, 'varchar(%d)' % length, **kwargs)


class AutoIncField(Field):
    """Autoincrement field."""

    def __init__(self, fieldname, ftype='int', **kwargs):
        """
        @param fieldname: name of the field
        @type fieldname: string

        @param ftype: the default type for all auto increment fields is
          an integer.
        @type ftype: string
        """
        self.autoinc = True
        self.unsigned = True
        self.ftype = ftype + ' unsigned'
        Field.__init__(self, fieldname, self.ftype, key=True,
                       default=0, **kwargs)

    def pack(self, data):
        """Default pack for all integers is to cast it to a string."""
        if data is None:
            return 'NULL'
        return str(data)

    def __str__(self):
        """Return the mysql statement that is needed to create this field."""
        return "%s %s auto_increment" % (self.fieldname, self.ftype)


class SerialField(Field):
    """Autoincrement field for PostgreSQL."""

    def __init__(self, fieldname, **kwargs):
        """
        @param fieldname: name of the field
        @type fieldname: string
        """
        self.ftype = "SERIAL"
        Field.__init__(self, fieldname, self.ftype, **kwargs)

    def pack(self, data):
        """Default pack for all integers is to cast it to a string."""
        if data is None:
            return 'NULL'
        return str(data)

    def __str__(self):
        """Return the mysql statement that is needed to create this field."""
        return "%s %s" % (self.fieldname, self.ftype)


class NumberField(Field):
    """Base field type for all integers."""

    def __init__(self, fieldname, ftype, unsigned=False, default=0, **kwargs):
        """
        @param fieldname: name of the field
        @type fieldname: string

        @param ftype: type of number this is.
        @type ftype: string

        @param unsigned: set to True if this number field should be unsigned
        @type unsigned: boolean

        @param default: the default value for all numbers is 0
        @type default: int
        """
        Field.__init__(self, fieldname, ftype, default=default, **kwargs)
        self.unsigned = unsigned

        if self.unsigned and ALLOW_UNSIGNED:
            self.ftype = self.ftype + " unsigned"

    def pack(self, data):
        """Default pack for all integers is to cast it to a string."""
        if data is None:
            return 'NULL'
        return str(data)


class FloatField(Field):
    """A field for saving float values."""

    def __init__(self, fieldname, ftype='float', default=0.0, **kwargs):
        Field.__init__(self, fieldname, ftype, default=default, **kwargs)

    def pack(self, data):
        return str(data)


class SecsFloatField(FloatField):
    """A field for storing elapsed time in seconds."""
    pass


class TinyIntField(NumberField):
    """A field for saving 8 bit integers which checks for values that are
    too large."""

    _MAXNUM_SIGNED   =  (2 << 6) - 1
    _MINNUM_SIGNED   = -(2 << 6)
    _MAXNUM_UNSIGNED =  (2 << 7) - 1
    _MINNUM_UNSIGNED =  0

    def __init__(self, fieldname, **kwargs):
        NumberField.__init__(self, fieldname, 'tinyint', **kwargs)

    def pack(self, data):
        try:
            data = int(data)
        except ValueError:
            raise FieldError('Cannot convert member "%s" to an integer. ' \
                    'Got %r' % (self.member, data))

        """Convert the value to a string."""
        # if we are unsigned, the max is doubled and min is 0
        if self.unsigned:
            if data < self._MINNUM_UNSIGNED:
                data = self._MINNUM_UNSIGNED
            if data > (2 * self._MAXNUM_UNSIGNED):
                data = self._MAXNUM_UNSIGNED
        else:
            if data < self._MINNUM_SIGNED:
                data = self._MINNUM_SIGNED
            elif data > self._MAXNUM_SIGNED:
                data = self._MAXNUM_SIGNED
        return str(data)


class SmallIntField(NumberField):
    """A field for saving 16 bit integers which checks for values that are
    too large."""

    _MAXNUM_SIGNED   =  (2 << 14) - 1
    _MINNUM_SIGNED   = -(2 << 14)
    _MAXNUM_UNSIGNED =  (2 << 15) - 1
    _MINNUM_UNSIGNED =  0

    def __init__(self, fieldname, **kwargs):
        NumberField.__init__(self, fieldname, 'smallint', **kwargs)

    def pack(self, data):
        """Convert the value to a string."""
        try:
            data = int(data)
        except ValueError:
            raise FieldError('Cannot convert member "%s" to an integer. ' \
                    'Got %r' % (self.member, data))

        # if we are unsigned, the max is doubled and min is 0
        if self.unsigned:
            if data < self._MINNUM_UNSIGNED:
                data = self._MINNUM_UNSIGNED
            if data > (2 * self._MAXNUM_UNSIGNED):
                data = self._MAXNUM_UNSIGNED
        else:
            if data < self._MINNUM_SIGNED:
                data = self._MINNUM_SIGNED
            elif data > self._MAXNUM_SIGNED:
                data = self._MAXNUM_SIGNED
        return str(data)


class IntField(NumberField):
    """A field for saving 32 bit integers."""

    def __init__(self, fieldname, **kwargs):
        NumberField.__init__(self, fieldname, 'int', **kwargs)


class BigIntField(NumberField):
    """A field for saving 64 bit integers."""

    def __init__(self, fieldname, **kwargs):
        NumberField.__init__(self, fieldname, 'bigint', **kwargs)


class BooleanField(Field):
    """A field for saving boolean values."""

    def __init__(self, fieldname, **kwargs):
        super(BooleanField, self).__init__(fieldname, 'boolean', **kwargs)

    def pack(self, data):
        """Default pack for all booleans is to cast it to a 't' or 'f'."""
        if data in (None, "f", False, "n", "no", 0, '0'):
            return "'f'"
        else:
            return "'t'"

    def asCSV(self, data):
        # precondition:  is coming in as a string
        if data == "True": 
            return "t"
        else:
            return "f"
        
    def unpack(self, data):
        """Unpack boolean value to True or False."""
        if data in (None, "f", False, "n", "no", 0, '0'):
            return False
        else:
            return True


class TimeIntField(IntField):
    """A field for storing time in seconds since Jan 1, 1970."""

    def __init__(self, fieldname, **kwargs):
        IntField.__init__(self, fieldname, unsigned=True, **kwargs)


class TimestampField(Field):
    """A field for storing timestamps."""
    
    def __init__(self, fieldname, **kwargs):
        Field.__init__(self, fieldname, 'timestamp', **kwargs)
        self.ftype = 'timestamptz'

    def pack(self, data):
        """Express as a timetamp mysql can understand."""
        if not data:
            return 'NULL'
        return "'%s'" % data.isoformat()

    def asCSV(self, data):
        if not data:
            return ''
        else:
            return self.pack(data)
        
class UpdateTimeField(TimeIntField):
    """A field for setting the time a record was last modified in the
    database."""

    def pack(self, data):
        """Pack the data by joining all the fields with our separator."""
        return 'UNIX_TIMESTAMP()'

    def __str__(self):
        """Return the mysql statement that is needed to create this field."""
        mystr = "%s %s default %d" % \
                (self.fieldname, self.ftype, self.default)
        return mystr

    def getDefault(self):
        return 0


class ByteIntField(BigIntField):
    """A field for storing # of bytes.  Needs to be big."""

    def __init__(self, fieldname, **kwargs):
        BigIntField.__init__(self, fieldname, unsigned=True, **kwargs)


class KiloByteField(BigIntField):
    """A field for storing # of kilobytes."""

    def __init__(self, fieldname, **kwargs):
        BigIntField.__init__(self, fieldname, unsigned=True, **kwargs)


class MegaByteField(IntField):
    """A field for storing # of megabytes."""

    def __init__(self, fieldname, **kwargs):
        IntField.__init__(self, fieldname, unsigned=True, **kwargs)


class MegaByteFloatField(FloatField):
    """A float field for storing # of megabytes."""

    def __init__(self, fieldname, ftype="real", **kwargs):
        FloatField.__init__(self, fieldname, ftype=ftype, **kwargs)


class GigaByteField(IntField):
    """A field for storing # of gigabytes."""

    def __init__(self, fieldname, **kwargs):
        IntField.__init__(self, fieldname, unsigned=True, **kwargs)


class GigaByteFloatField(FloatField):
    """A float field for storing # of gigabytes."""

    def __init__(self, fieldname, ftype="real", **kwargs):
        FloatField.__init__(self, fieldname, ftype=ftype, **kwargs)


class SecsIntField(IntField):
    """A field for storing elapsed time in seconds."""

    def __init__(self, fieldname, **kwargs):
        IntField.__init__(self, fieldname, unsigned=True, **kwargs)


class TextField(Field):
    """A field for saving large text data values."""

    def __init__(self, fieldname, **kwargs):
        """A large text field."""
        ftype = "text"
        Field.__init__(self, fieldname, ftype=ftype, **kwargs)


class BlobField(Field):
    """A field for saving large data values."""

    def __init__(self, fieldname, size=None, **kwargs):
        """By default a regular blob type is created, the size can be one
        of 'tiny', 'medium', or 'long'"""
        ftype = "blob"
        if size:
            sizes = ('tiny', 'medium', 'long')
            if size not in sizes:
                raise FieldError("blob size '%s' is not valid, use one of " \
                      "%s" % (size, sizes))
            ftype = size + ftype
        Field.__init__(self, fieldname, ftype=ftype, **kwargs)


class StrListField(Field):
    """Saves a list of strings into a field."""

    def __init__(self, fieldname, ftype='text', separator=' ', **kwargs):
        Field.__init__(self, fieldname, ftype, packed=True, **kwargs)
        # all fields in the list will be separated with this string
        self.separator = separator

        # make sure indexed text fields have specified index length
        if self.ftype == 'text' and self.index and not self.indexlen:
            raise FieldError("indexlen must be specified if indexing text")

    def getDefault(self):
        """Overloaded so we return a new list."""
        if self.default:
            return self.default[:]
        else:
            return []

    def pack(self, data):
        """Pack the data by joining all the fields with our separator."""
        if not data:
            return 'NULL'

        packed = self.separator.join(data)
        return string_literal(packed)

    def unpack(self, data):
        """Unpack the list by spliting the string with the separator."""
        # check for lists that should be empty
        #print "unpacking string list"
        if not data:
            return []
        return data.split(self.separator)

    def __str__(self):
        """Return the mysql statement that is needed to create this field."""
        mystr = "%s %s" % (self.fieldname, self.ftype)
        # don't allow defaults for blobs
        if 'blob' not in self.ftype:
            if self.default is not None:
                mystr += " default %s" % self.pack(self.default)
            else:
                mystr += " default NULL"
        return mystr


class IntListField(Field):
    """Saves a list of integers into a field."""

    def __init__(self, fieldname, ftype='text', separator=' ', **kwargs):
        Field.__init__(self, fieldname, ftype, packed=True, **kwargs)
        # all fields in the list will be separated with this string
        self.separator = separator

    def getDefault(self):
        """Overloaded so we return a new list."""
        if self.default:
            return self.default[:]
        else:
            return []

    def pack(self, data):
        """Pack the data by joining all the fields with our separator."""
        if not data:
            return 'NULL'

        packed = self.separator.join([str(item) for item in data])
        return string_literal(packed)

    def unpack(self, data):
        """Unpack the list by spliting the string with the separator."""
        # check for lists that should be empty
        if not data:
            return []
        return [int(item) for item in data.split(self.separator)]

    def __str__(self):
        """Return the mysql statement that is needed to create this field."""
        mystr = "%s %s" % (self.fieldname, self.ftype)
        # don't allow defaults for text fields
        if 'text' not in self.ftype:
            if self.default is not None:
                mystr += " default %s" % self.pack(self.default)
            else:
                mystr += " default NULL"
        return mystr


class IntArrayField(ArrayField):

    def __init__(self, fieldname, default=[], **kwargs):
        super(IntArrayField, self).__init__(fieldname, 'int', default=default, **kwargs)


class StrArrayField(ArrayField):

    def __init__(self, fieldname, default=[], **kwargs):
        super(StrArrayField, self).__init__(fieldname, 'text', default=default, **kwargs)

    def pack(self, data):
        return "'{" + ",".join(['"%s"' % d.replace('"', '\\"').replace("'", "''") for d in data]) + "}'"

    def asCSV(self, data):
        def escaped(d):
            if '"' in d or ',' in d:
                # need two double quotes around string if comma or double quote in it
                # also need to escape double quotes that is a part of data themselves
                return '""%s""' % d.replace('"', '\\""')
            else:
                return d
        needsQuotes = any(['"' in d or "'" in d or ',' in d for d in data])
        if needsQuotes:
            return '"{%s}"' % ",".join([escaped(d) for d in data])
        else:
            return "{%s}" %  ",".join(data)


class StrArrayArrayField(ArrayField):

    def __init__(self, fieldname, default=[], **kwargs):
        super(StrArrayArrayField, self).__init__(fieldname, 'text[]', default=default, **kwargs)

    def pack(self, data):
        raise "No packing has yet been defined.  Look to StrArrayField for reference."


class TimestampArrayField(ArrayField):

    def __init__(self, fieldname, default=[], **kwargs):
        super(TimestampArrayField, self).__init__(fieldname, 'timestampz', default=default, **kwargs)

    def pack(self, data):
        return "'{" + ",".join(['"%s"' % d.isoformat() for d in data]) + "}'"


class DictField(Field):
    """Saves a python dictionary into a field by pickling it."""

    def __init__(self, fieldname, ftype='text', **kwargs):
        Field.__init__(self, fieldname, ftype, packed=True, **kwargs)

    def getDefault(self):
        """Overloaded so we return a new dictionary."""
        if self.default:
            return self.default.copy()
        else:
            return {}

    def pack(self, data):
        """Pack the data by running cPickle.dumps() on the dictionary."""
        if not data:
            return 'NULL'

        packed = pickle.dumps(data)
        return string_literal(packed)

    def unpack(self, data):
        """Unpack by running cPickle.loads() on the passed in data."""
        # check for lists that should be empty
        if not data:
            return {}
        return pickle.loads(data)


class JSONField(Field):
    """Stores any python object that uses basic JSON datatypes, such as
    lists, dictionaries, strings, integers, floats, and strings.
    """

    def __init__(self, fieldname, ftype='json', packed=False, **kwargs):
        Field.__init__(self, fieldname, ftype, **kwargs)

    def pack(self, data):
        """Pack the data as serialized json."""
        if not data:
            return 'NULL'
        packed = json.dumps(data)
        return string_literal(packed)

    def unpack(self, data):
        """Unpack the data as serlialized json."""
        if not data:
            return None
        return json.loads(data)


class InetField(Field):
    """A field for saving IPv4 or IPv6 network addresses."""

    def __init__(self, fieldname, **kwargs):
        """
        @param fieldname: name of the field
        @type fieldname: string
        """
        Field.__init__(self, fieldname, "inet", **kwargs)


class UUIDField(Field):
    """A field for storing universally unique ids."""

    def __init__(self, fieldname, **kwargs):
        """
        @param fieldname: name of the field
        @type fieldname: string
        """
        Field.__init__(self, fieldname, "uuid", **kwargs)


class ObjTypeField(Field):
    """A field used to save information to distinguish object types for
    each row."""

    def __init__(self, fieldname, ftype='varchar(64)', **kwargs):
        Field.__init__(self, fieldname, ftype=ftype, **kwargs)

    def pack(self, obj):
        """Pack the fieldname of the object we are dealing with."""
        packed = obj.__class__.__name__
        return string_literal(packed)


class MultiMemberField(Field):
    # AWG: incomplete; just a stub
    def __init__(self, fieldname, ftype=None, members=[]):
        pass


class VirtualField(Field):
    """A virtual field is not part of a table, but rather depends on one
    or more fields that are.  A virtual field can be queried, sorted, and
    accessed like other fields."""

    def __init__(self, member, depMembers, packed=False):
        """Initialized with the member name that is used to reference the
        fields, and a list of the member names it depends on."""

        # name of the member
        self.member = member

        # list of the member names we depend on
        self.depMembers = depMembers

        # list of field objects we depend on
        self.__fields = None
        # list of the secondary tables we depend on (based on the fields list)
        self.__exttables = None

        Field.__init__(self, member, None, packed=packed)

    def _getFields(self):
        """Get a list of each dependent members' associated field."""
        if self.__fields is not None:
            return self.__fields

        # make the list
        self.__fields = []
        for member in self.depMembers:
            self.__fields.append(self.table.database.fieldByMember(member,
                                                          table=self.table))
        return self.__fields
    fields = property(fget=_getFields)

    def _getExtTables(self):
        """Get a list of the exttables we depend on."""
        if self.__exttables is not None:
            return self.__exttables

        # make the list
        self.__exttables = []
        for field in self.fields:
            if field.table not in self.__exttables:
                self.__exttables.append(field.table)
        return self.__exttables
    exttables = property(fget=_getExtTables)


    def getDependentFields(self, datatok=None):
        """When a Where object is queried for a list of the referenced
        members, virtual fields can optionally return a subset of its
        dependent fields.  By default, the full list of dependent members
        is returned."""
        return self.fields

    def getWhere_ComparisonPhrase(self, pharse, datatok, left=True):
        """Called when a virtual field is found in a ComparisonPhrase phrase
        of a where string.  By default, this returns a valid where that
        requires all the dependent members have a value set in the database.

        @param phrase: the phrase within the where string this field is
          referenced.
        @type phrase: L{Where.ComparisonPhrase} instance

        @return: a MySQL query equivalent to the passed in phrase
        @rtype: string
        """

        # if nothing is defined, then complain
        raise FieldError("the virtual field '%s' cannot be used in a " \
              "query." % self.member)


    def getWhere_StandAlone(self, phrase):
        """Called when a virtual field is found in a StandAlone phrase
        of a where string.  By default, this returns a valid where that
        requires all the dependent members have a value set in the database.

        @param phrase: the phrase within the where string this field is
          referenced.
        @type phrase: L{Where.StandAlone} instance

        @return: a MySQL query equivalent to the passed in phrase
        @rtype: string
        """
        mystr = "(%s)" % ' AND '.join([f.getWhere() for f in self.fields])
        if phrase.notOp:
            mystr = "NOT " + mystr
        return mystr


    def getSelect(self, table, extTables):
        """When this field is included in a select query, this method will
        be called to determine how the field should be selected from
        the table.  By default, the getSelect() methods of the dependent
        members' field object are called and joined.

        @param table: the primary table used in the query.
        @type table: L{Table.Table} instance

        @param extTables: list of tables that will be joined in the query
        @type extTables: list of L{Table.Table} instances

        @return: portion of a select query used to select the depended fields
        @rtype: string
        """

        # iterate over all our dependent fields and join the select strings
        selects = []
        for field in self.fields:
            selects.append(field.getSelect(table, extTables))
        # join everything together
        return ','.join(selects)


    def getOrderBy(self, descending=False, **kw):
        """When this field is included in an 'ORDER BY' portion of a query,
        this method is called to determine how the field should be included
        in the order.  By default, the getOrderBy() methods of the
        dependent members' field object are called and joined.

        @param descending: by default the order will be ascending, set this
          to True, if rows should be in descending (reverse) order.
        @type descending: boolean

        @return: portion of the query used to order by the depended fields
        @rtype: string
        """

        # iterate over all our dependent fields and join the select strings
        orders = []
        for field in self.fields:
            orders.append(field.getOrderBy(descending=descending))
        # join everything together
        return ','.join(orders)

class ElapsedSecsVirtualField(Field):
    def __init__(self, member, t0field, t1field, packed=False):
        self.member = member
        self.t0field = t0field
        self.t1field = t1field
        Field.__init__(self, member, None, packed=packed)

    def getSelect(self, table, extTables):
        # if this field is not from the primary table, then qualify field name with table name
        asPrefix = "" if table == self.table else self.table.tablename + "."
        return 'EXTRACT(EPOCH FROM COALESCE(%s.%s, NOW()) - %s.%s) AS "%s%s"' \
               % (self.table.tablename, self.t1field, self.table.tablename, self.t0field,
                  asPrefix, self.fieldname)
    
    def getOrderBy(self, descending=False, table=None):
        # if table is this table, we don't need to qualify with tablename
        prefix = "" if table == self.table else self.table.tablename + "."
        base = '"%s%s"' % (prefix, self.fieldname)
        # should we reverse the order?
        return base + " DESC" if descending else base
        
# ---------------------------------------------------------------------------

def testField():
    f = Field('fruit', 'varchar(20)')
    print(str(f))
    print('  pack  :', f.pack('this is data'))
    print('  unpack:', f.unpack('this is data'))

    f = VarCharField('fruit', length=20, default='a')
    print(str(f))
    print('  pack  :', f.pack('this is data'))
    print('  unpack:', f.unpack('this is data'))

    f = NumberField('age', 'int')
    print(str(f))
    print('  pack  :', f.pack(42))
    print('  unpack:', f.unpack(42))

    f = SmallIntField('age', default=0)
    print(str(f))

    f = IntField('age', default=0)
    print(str(f))

    f = BigIntField('age', default=0)
    print(str(f))

    f = TimeIntField('starttime')
    print(str(f))

    f = SecsIntField('elapsed')
    print(str(f))

    f = FloatField('pressure', default=101.1)
    print(str(f))
    print('  pack  :', f.pack(42))
    print('  unpack:', f.unpack(42))

    f = BlobField('history', default='')
    print(str(f))

    f = StrListField('slots', ftype='mediumblob')
    print(str(f))
    print('  pack  :', f.pack(['this', 'is', 'data']))
    print('  unpack:', f.unpack('this is data'))

    f = IntListField('prev', ftype='tinyblob')
    print(str(f))
    print('  pack  :', f.pack([8, 6, 7, 5, 3, 0, 9]))
    print('  unpack:', f.unpack('8 6 7 5 3 0 9'))

    f = ObjTypeField('jobtype')
    print(str(f))

    f = DictField('metadata')
    print(str(f))
    print('  pack  :', f.pack({'hello': 10, 'world': 20}))
    print('  unpack:', f.unpack(f.pack({'hello': 10, 'world': 20})))


if __name__=='__main__':
    testField()

