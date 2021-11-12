"""All objects that are going to be saved in the database should be subclassed
from L{DBObject}."""

from types import ListType, TupleType
from rpg.sql import SQLError
from rpg.sql.Fields import Field

__all__ = ('DBObjectError',
           'DBObjectModifyKeyError',
           'DBObjectUnpackError',
           'DBObject')

class DBObjectError(SQLError):
    """Base error for all DBObject errors."""
    pass

class DBObjectModifyKeyError(DBObjectError):
    """Key fields cannot be modified."""
    pass

class DBObjectUnpackError(DBObjectError):
    """Error related to unpacking the entire object."""
    pass


class DBObject(object):
    """Any object that is going to be saved in the database should be
    subclassed from here.  This implements the necessary functionality to
    have members unpacked when accessed, and provides a way to keep track
    of members that have changed since the last database update.  Subclasses
    should define a list of L{Fields.Field} objects that describe the
    member data to be stored in the database.

    @cvar Fields: list of L{Fields.Field} objects that describe the
      member data to be stored in the database.
    @type Fields: list

    @cvar Aliases: member name aliases that can be used when querying or
      when accessing the member data.
    @type Aliases: dictionary
    """

    # a list of Field objects that define each field in the database table
    Fields = []

    # a list of VirtualField objects used for defining members that are
    # not explicitly stored in the database, but depend on other fields
    VirtualFields = []

    # optional aliases used to refer to members in where strings
    Aliases = {}

    _table = None

    # not sure the benefit of this yet -jag
    #__slots__ = ['__dict__']

    def __init__(self):

        # keep track of which members have changed since the last db update
        self.dirty = {}
        
        # indb gets set to True if the object was read from or put into
        # the database.
        self.indb = False
        
        # when the object is read from the database, this points to the
        # original dictionary.
        self.__rowdata__ = {}

        # when the object is created because of a join it uses this mapping
        # to find its data in the __rowdata__ dictionary
        self.__rowkeys__ = None

        # instantiate members based on the field objects
        self._initMembers()


    def _initMembers(self, cls=None):
        """Instantiate all members in self's class, and all of self's
        base classes until we get to the root."""

        if cls is None:
            cls = self.__class__

        # if we aren't a subclass of DBObject, then don't do anything
        if not issubclass(cls, DBObject) or cls is DBObject:
            return

        # instantiate members according to fields
        for field in cls.Fields:
            self.__dict__[field.member] = field.getDefault()

        # go up the tree
        for base in cls.__bases__:
            self._initMembers(cls=base)

    def __setstate__(self, dict):
        self.__dict__.update(dict)


    def __getstate__(self):
        """Overloaded so we can ensure that all members are unpacked before
        we try to pickle the object."""

        # unpack everything
        self.unpack()
        # clear all of our row pointers.  we don't call clear() because other
        # instances might be referencing the same dictionary
        if self.__rowdata__:
            self.__rowdata__ = {}
        if self.__rowkeys__:
            self.__rowkeys__ = None
        # clear the dirty dictionary so we don't pickle more than we need to
        self.clearDirty()
        # return our dictionary
        return self.__dict__


    def __setattr__(self, member, value):
        """
        In order to keep track of each variable assignment we overload to::

         1. add the variable to the dirty dictionary.  This allows modified
            fields to be distinguished so that non-modified fields can be
            skipped when an object is updated in the database.

         2. prevent the modification of members that correspond to keys of
            an object when *and only when* the object was read from the
            database.  This prevents the modification of a key and
            a subsequent call to putObject to cause an invalid UPDATE.
            (KQ: could there be a different mechanism to deal with this?)
        """

        # these values don't need to be marked in dirty dictionary
        if member in ['dirty', 'indb']:
            self.__dict__[member] = value
            return

        # remap alias to original member
        if self._table:
            # only objects derrived from QueryResult will have a table
            field = self._table.fieldByMember(member)
        else:
            field = self.fieldByMember(member)
        if field:
            member = field.member

        # make a note of modification in dirty dictionary
        if member != "dirty":
            try:
                dirty = self.__dict__["dirty"]
            except KeyError:
                dirty = {}
                self.__dict__["dirty"] = dirty
            dirty[member] = True

        # don't allow modification of keys if this object is in database
        # queryResult won't be in objects that have been pickled

        #if self.__dict__['indb'] and self.__dict__.has_key('queryResult'):
        
        # OPT: you may want to leave this modify key check out for speed
        try:
            indb = self.__dict__["indb"]
        except KeyError:
            pass
        else:
            if indb:
                # get queryResult in order to determine keys
                # queryResult = self.__dict__['queryResult']
                # field = queryResult.table.fieldByMember(member)

                field = self.fieldByMember(member)
                if field and field.key:
                    raise DBObjectModifyKeyError("cannot modify member '%s' of %s object, because " \
                          "it is a key member and its record is in the " \
                          "database." % (member, self.__class__.__name__))

        super(DBObject, self).__setattr__(member, value)
            

    def __getattr__(self, member):
        """
        This method is overloaded so that::

          1. database values can be left in their row dictionary, and only
             need to be placed in the object when the caller requires them.
           
          2. database values can be left packed as they are pulled from the db,
             and only need to be unpacked when the caller uses them.
        """

        # check if the member is still in the raw row dictionary.
        try:
            # is deleting from one dict and adding it to another faster
            # than always referencing it from the row dictionary? -jag

            # if there is nothing in rowkeys, then don't check it
            if self.__dict__["__rowkeys__"]:
                key = self.__rowkeys__[member]
                val = self.__rowdata__.pop(key)
            else:
                val = self.__rowdata__.pop(member)
            # assign it to the objects dictionary
            self.__dict__[member] = val
            return val
        except KeyError:
            pass

        if self._table:
            # only objects derrived from QueryResult will have a table
            field = self._table.fieldByMember(member)
        else:
            field = self.fieldByMember(member)
                
        if field is not None:
            # first see if it's in the object's dict; this check is
            # necessary because we may be looking up through an alias
            try:
                return self.__dict__[field.member]
            except KeyError:
                pass

            # now resort to looking at packed row
            # locate the value with the Field's select name
            try:
                # if there is nothing in rowkeys, then don't check it
                if self.__dict__["__rowkeys__"]:
                    key = self.__rowkeys__[field.selectName]
                    val = self.__rowdata__.pop(key)
                else:
                    val = self.__rowdata__.pop(field.selectName)
            except KeyError:
                # we know this is a valid member, it just wasn't selected
                # from the database, so return its default value
                val = field.getDefault()
                self.__dict__[member] = val
                return val

            # unpack the data if it is packed
            if field.packed:
                val = field.unpack(val)

            # put the data into the object's dictionary so we don't do
            # this again
            self.__dict__[member] = val
            return val

        # check if this is a reference to a member from a subclass (hack!)
        if self.Fields:
            try:
                field = self.Fields[0].table.fieldByMember(member)
                if field is not None:
                    return field.getDefault()
            except AttributeError:
                pass # allow AttributeError to be raised

        # no external object was found, so it must be a bad attribute
        raise AttributeError('%s is not a valid member of %s; not in %s' \
              % (member, self.__class__.__name__,
                 self.__dict__.get("__rowdata__", {})))


    def init(self):
        """Subclasses should overload this method if objects need to be
        initialized when retreiving from the database.  This is only
        called by the L{Database.Database} object when a 'get' is
        performed."""
        pass


    def unpack(self):
        """Unpacks all members from the database if they haven't already
        been unpacked."""

        try:
            if self.__dict__['__isunpacked__']:
                return
        except KeyError:
            pass
        
        # if there is nothing left in the rowdata dictionary, then there
        # is nothing left to unpack
        if not self.__rowdata__:
            return

        # if rowkeys is set, then we only care about the those keys
        if self.__rowkeys__:
            rowkeys = list(self.__rowkeys__.keys())
        else:
            rowkeys = list(self.__rowdata__.keys())

        for rowkey in rowkeys:
            # if rowkeys is set, then grab the actual rowkey from it
            if self.__rowkeys__:
                realkey = self.__rowkeys__[rowkey]
                # grab the value, and delete it from the row.
                try:
                    rowval = self.__rowdata__.pop(realkey)
                except KeyError:
                    # value has already been extracted from rowdata (via getattr)
                    continue
            # if nothing is set for rowkeys, then make sure the key isn't
            # from an external table
            else:
                # if it is, then skip it
                if Field.ExtFieldRE.match(rowkey):
                    continue
                # otherwise, grab the value and delete it from the row
                rowval = self.__rowdata__.pop(rowkey)

            # get the field for this item
            field = self._fieldByRowKey(rowkey)
            # if we don't find anything, the we have a problem
            if field is None:
                raise DBObjectUnpackError("unknown field '%s' found " \
                      "in row." % rowkey)

            # unpack the field if it is packed
            if field.packed:
                rowval = field.unpack(rowval)

            # save the unpacked data
            self.__dict__[field.member] = rowval

        self.__dict__['__isunpacked__'] = True


    def isDirty(self):
        """Are any members in the object dirty?

        @return: True if one or more members are dirty, False otherwise.
        @rtype: boolean
        """
        return len(self.dirty) > 0

    
    def makeDirty(self, members=None):
        """Mark all or some members 'dirty'.  This means their values will
        be updated in the database during the next update.

        @param members: Can be 'None' to dirty all members, a list of
          members, or a single member as a string.
        """
        
        # if nothing is provided, then do everything
        if members is None:
            # dirty all the fields in this class
            for field in self.Fields:
                self.dirty[field.member] = True
            # now go up the class tree and dirty the parent fields
            base = cls.__bases__[0]
            if base is not DBObject:
                base.makeDirty(self, members=members)

        # if a list is of members is provided, then only dirty them
        elif type(members) in (ListType, TupleType):
            for member in members:
                self.dirty[member] = True

        # otherwise, assume it is a string
        else:
            self.dirty[members] = True
            

    def clearDirty(self, members=None):
        """Unmark all or some members as 'dirty'.  This means their values will
        NOT be updated in the database during the next update.

        @param members: Can be 'None' to unmark all members, a list of
          members, or a single member as a string.
        """

        # if nothing is provided, then clear everything
        if members is None:
            self.dirty.clear()

        # if a list is provided, then only dirty them
        elif type(members) in (ListType, TupleType):
            for m in members:
                # catch error if the member isn't actually dirty
                try:
                    del self.dirty[m]
                except KeyError:
                    pass

        # otherwise, assume it is a string and try to undirty it
        else:
            try:
                del self.dirty[members]
            except KeyError:
                pass


    def fieldByMember(cls, member):
        """Search for the L{Fields.Field} object associated with a provided
        member name.

        @param member: a member name to search for
        @type member: string

        @return: L{Fields.Field} object associated with provided member name.
        @rtype: L{Fields.Field} instance
        """

        # we explicitly check the class's dictionary for _member2field so
        # we don't accidentally pickup the member from a super class
        try:
            member2field = cls.__dict__['_member2field']
        except KeyError:
            # if we don't find it, then make it now.
            member2field = {}

            # always check for members in our dictionary
            try:
                fields = cls.__dict__['Fields']
            except KeyError:
                pass
            else:
                for field in fields:
                    member2field[field.member] = field

            # take care of any virtual fields
            try:
                vfields = cls.__dict__['VirtualFields']
            except KeyError:
                pass
            else:
                for field in vfields:
                    # make sure a member doesn't exist with the same name
                    if field.member in member2field:
                        raise DBObjectError("virtual member '%s' conflicts " \
                              "with actual member '%s'" % \
                              (field.member, field.member))
                    member2field[field.member] = field

            # also put the aliases in there
            try:
                aliases = cls.__dict__['Aliases']
            except KeyError:
                pass
            else:
                # make sure the alias point at a valid member
                for alias,mem in list(aliases.items()):
                    try:
                        member2field[alias] = member2field[mem]
                    except KeyError:
                        raise DBObjectError("alias '%s' points to unknown " \
                              "member '%s'" % (alias, mem))
                
            # now add it to the class object
            cls._member2field = member2field

        try:
            return member2field[member]
        except KeyError:
            # ask the base class
            for base in cls.__bases__:
                if issubclass(base, DBObject) and base is not DBObject:
                    return base.fieldByMember(member)
            # otherwise give up
            return None
    fieldByMember = classmethod(fieldByMember)


    def _fieldByRowKey(cls, rowkey, checkTable=True):
        """Search for the L{Fields.Field} object associated with the
        provided row key.  Each field has a unique name (which may differ
        from the member name) that is used when a select is performed.

        @param rowkey: a row key (select name) to search for
        @type rowkey: string

        @param checkTable: indicates whether the table (this class is a
          member of) should be checked if the rowkey isn't found in this
          object.
        @type checkTable: boolean

        @return: L{Fields.Field} object associated with provided row key.
        @rtype: L{Fields.Field} instance
        """

        # we explicitly check the class's dictionary for _rowkey2field so
        # we don't accidentally pickup the member from a super class
        try:
            rowkey2field = cls.__dict__['_rowkey2field']
        except KeyError:
            # if we don't find it, then make it now.
            rowkey2field = {}
            for field in cls.Fields + cls.VirtualFields:
                rowkey2field[field.selectName] = field
            # now add it to the class object
            cls._rowkey2field = rowkey2field

        try:
            return rowkey2field[rowkey]
        except KeyError:
            # ask the other classes in our table if they know about this
            # rowkey
            if cls.Fields and checkTable:
                return cls.Fields[0].table._fieldByRowKey(rowkey)
            # otherwise give up
            return None
    _fieldByRowKey = classmethod(_fieldByRowKey)


    def _makeProperties(cls):
        """Make property functions for all the aliases defined for this
        class.  This is kind of a hack, but this method must be called for
        each class before aliases will work.  L{Table.Table} calls this when
        it is initialized."""

        # check for the aliases in this class only
        try:
            aliases = cls.__dict__['Aliases']
        except KeyError:
            pass
        else:
            # define a function and set a property for each alias
            for alias,member in list(aliases.items()):
                # make sure the provided member exists
                field = cls.fieldByMember(member)
                if not field:
                    raise DBObjectError("alias '%s' points to unknown " \
                          "member '%s'" % (alias, member))

                # make a function to call it with
                funcStr = "def get_%s(self):\n  return self.%s\n" % \
                          (alias, member)
                # exec the code so the function works
                exec(funcStr)
                # create and set the property
                setattr(cls, alias, property(fget=eval("get_%s" % alias)))

    _makeProperties = classmethod(_makeProperties)


    def _replaceField(cls, member, newfield):
        """Replace a Field instance in this object with a new one.  This
        is intended to be a temporary work around to the fact that new
        field types cannot be easily specified."""
        # find the original field
        for i in range(len(cls.Fields)):
            orig = cls.Fields[i]
            if orig.member == member:
                cls.Fields[i] = newfield
                try:
                    del cls._member2field
                except AttributeError:
                    pass

                try:
                    del cls._rowkey2field
                except AttributeError:
                    pass
                break
        else:
            raise DBObjectError("the field for member '%s' was not found " \
                  "in the object '%s'" % (member, cls.__name__))
    _replaceField = classmethod(_replaceField)


    def __str__(self):
        """Display each and every member."""

        s = ''
        for field in self.Fields:
            aka = ''
            if field.fieldname != field.member:
                aka = ' [aka %s]' % field.fieldname
            value = getattr(self, field.member)
            
            s += '%s%s = %s\n' % (field.member, aka,
                                str(value))
            
        return s


    def getKey(self):
        """Get the key for this object represented as a tuple."""
        return tuple([getattr(self, f.member) for f in self._table.keys])


    def getEquivKey(self):
        """Get the equivalent key for this object if the auto increment
        field is not set."""
        return tuple([getattr(self, f.member) for f in self._table.equivKeys])
                   

# ---------------------------------------------------------------------------

def test():

    from .Fields import IntField, StrListField
    
    class O(DBObject):
        Fields = [
            IntField('v', default=0),
            StrListField('l', default=[])
            ]
        def __init__(self):
            DBObject.__init__(self)

            
    a = O()
    a.v = 5
    a.l.append('this')
    print(a.v, a.l)

    b = O()
    b.v = 10
    b.l.append('that')
    print(b.v, b.l)

    try:
        print(b.xxx)
    except AttributeError:
        print('successfully trapped attrbute error')
    except:
        print('failed to catch this')
        raise

def testBadAttr():
    x = DBObject()
    print('HELLO')
    print(x.foo)
    

if __name__=='__main__':
    testBadAttr()
