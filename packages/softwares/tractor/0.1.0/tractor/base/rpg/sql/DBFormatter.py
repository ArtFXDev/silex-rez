"""Aspect to neatly format L{DBObject.DBObject} instances for printing."""

import re
from .. import sql
from . import Fields
from . import Database
from .. import Formats

__all__ = (
    'DBFormatterError',
    'DBFormatter'
    )

class DBFormatterError(sql.SQLError):
    """Base error type for errors related to DBFormatters."""
    pass


class DBFormatter(Formats.Formatter):
    """A L{DBObject.DBObject} instance can be neatly formatted for printing
    very easily because most its members are L{Fields.Field} instances.
    This object takes care of the hardwork required to get a
    L{Formats.Formatter} up and running.

    @cvar fieldToFormat: a mapping from Field type to the MemberFormat
      subclass that should be used when formatting fields.  Multiple Field
      types can be grouped by their parent class.  For example, the root
      class Field uses the StringFormat, but all types subclassed from
      NumberField will use and IntegerFormat.

    @cvar defaultFormatAttrs: a mapping from full member name to a dictionary
      of attributes (keyword arguments) that will be used when creating a
      MemberFormat object for the given member.

    @cvar defaultFormatLists: a mapping from L{Table.Table} instance to
      a default list of members that should be used if non are provided,
      or if the caller is making use of the +,-,= operators to modify
      the default list.
    """

    # make the mappings for all the base types.  subclasses should copy
    # this dictionary and add any custom types.
    fieldToFormat = {
        Fields.Field            : Formats.StringFormat,
        Fields.AutoIncField     : Formats.IntegerFormat,
        Fields.NumberField      : Formats.IntegerFormat,
        Fields.FloatField       : Formats.FloatFormat,
        Fields.TimeIntField     : Formats.TimeFormat,
        Fields.TimestampField   : Formats.DatetimeFormat,
        Fields.ByteIntField     : Formats.BytesFormat,
        Fields.KiloByteField    : Formats.KiloBytesFormat,
        Fields.MegaByteField    : Formats.MegaBytesFormat,
        Fields.GigaByteField    : Formats.GigaBytesFormat,
        Fields.SecsIntField     : Formats.ElapsedSecsFormat,
        Fields.SecsFloatField   : Formats.ElapsedSecsFormat,
        Fields.StrListField     : Formats.StringListFormat,
        Fields.IntListField     : Formats.ListFormat,
        Fields.StrArrayField    : Formats.StringListFormat,
        Fields.IntArrayField    : Formats.ListFormat,
        Fields.TimestampArrayField : Formats.DatetimeListFormat,
        Fields.DictField        : Formats.DictFormat
        }

    # list of the base types that are used if the 'raw' keyword is set
    baseTypes = (Formats.StringFormat, Formats.IntegerFormat,
                 Formats.FloatFormat, Formats.ListFormat,
                 Formats.DictFormat)

    # subclasses can define a set of default attributes to be used when
    # a given member has a MemberFormat created
    defaultFormatAttrs = {}

    # subclasses can define a default list of members that will be used if
    # the formatter is initialized with a QueryResult and no list is provided.
    # the lists should be keyed by table instance.
    defaultFormatLists = {}

    def __init__(self, *mformats, **attrs):
        """
        @param mformats: a list of members that should be formatted.  See
          the L{Formats} module for additional options and help.

        @param database: the database that the instances to be formatted
          belong to.

        @param table: the primary table the instances orginated from.  This
          is needed so the proper search order can be selected when
          searching for member formats.
        """

        # set our attrs
        for attr in ("database", "table", "raw", "timefmt", "nocolor",
                     "zeros"):
            try:
                val = attrs.pop(attr)
            except KeyError:
                val = None
            setattr(self, attr, val)

        # make sure we have enough info to get started.
        if not self.database:
            raise DBFormatterError("a Database object must provided.")

        # check if we should use the default format list
        if not mformats:
            mformats = self._getDefaultList(self.table)

        # set to True if we start modifying the default
        self.modified = False            

        # keep track of the fields each member is referring to.
        self.fieldByMember = {}

        # call the base class
        Formats.Formatter.__init__(self, *mformats, **attrs)

        # setup our caches to speed up formatting
        self._setupCaches()


    def _getDefaultList(self, table):
        """Get the default format list for the given table."""

        # check for the default list
        try:
            deflist = self.defaultFormatLists[table]
        except KeyError:
            # empty default list for None
            if table is None:
                return []

            raise DBFormatterError("the table '%s' does not have a " \
                  "default format list set." % table.tablename)

        return [deflist]

    def _handleMFormatInstance(self, mformat):
        """Overloaded from the base class so we can ensure a Field
        object is found for each member."""

        # check if one already exists.  it's possible that it was added
        # by 'getMemberFormat'
        if mformat not in self.fieldByMember:
            # search for the field
            field = self.database.fieldByMember(mformat.member)
            # complain if we don't find one
            if not field:
                raise DBFormatterError("(1) no field associated with " \
                      "'%s'" % mformat.member)
            # create the mapping
            self.fieldByMember[mformat] = field

        # call the parent method
        Formats.Formatter._handleMFormatInstance(self, mformat)


    def _handleMFormatString(self, mfstr):
        """Overloaded so we can check for the +,-,= operators in front
        of each member format string.  These are used to add, subtract,
        or modify members from the default list."""

        # check for the modify operators
        if mfstr[0] in "+-=":
            # we only accept them if the default flag is True, or if this
            # is the first member format string we've parsed.
            if not self.modified and self.mformats:
                raise DBFormatterError("column lists cannot contain " \
                      "+/-/= characters if an item exists without one.")

            # if this is the first one we're parsing, then set the default
            if not self.modified:
                mformats = self._getDefaultList(self.table)                
                self._initMemberFormats(mformats)
                self.modified = True

            # strip the operator off
            op,mfstr = mfstr[0],mfstr[1:]

            # we simply add this to the existing list for the plus operator
            if op == '+':
                Formats.Formatter._handleMFormatString(self, mfstr)
                return

            # however, we need to search for members if we are removing
            # or modifying attributes
            
            # parse the string so we know which member to look for.
            member,attrs = self._parseMemberFormatStr(mfstr)
            # find a field object for this member
            field = self.database.fieldByMember(member, table=self.table)
            if not field:
                raise DBFormatterError("(2) no field associated with '%s'" % \
                      member)

            # keep track of whether we got a match
            foundMember = False
            
            # search through all the existing list and make the necessary
            # changes.  We make a copy of the list so items can be removed.
            for mf in list(self.mformats):
                # we base it all off member name
                if mf.member != field.member:
                    continue

                foundMember = True
                
                # remove it from the list
                if op == '-':
                    self.mformats.remove(mf)
                # update some attributes
                elif op == '=':
                    # if the attrs include 'width', then make sure it doesn't
                    # overrides any previously set min/max values
                    if 'width' in attrs:
                        mf.minWidth = mf.maxWidth = None
                        # also, interpret a zero as None which will force
                        # the format to use the database width hint
                        if attrs["width"] == 0:
                            attrs["width"] = None
                        
                    for key,val in list(attrs.items()):
                        setattr(mf, key, val)
                else:
                    raise DBFormatterError("unknown format modify operator " \
                          "'%s'" % op)

            # make sure we found something to modify
            if not foundMember:
                raise DBFormatterError("(3) no field associated with '%s'" % \
                      member)

        # if we don't find one, make sure we aren't expecting one
        elif self.modified:
            raise DBFormatterError("column lists cannot contain " \
                  "+/-/= characters if an item exists without one.")

        # otherwise, handle the string like normal
        else:
            Formats.Formatter._handleMFormatString(self, mfstr)


    def getFieldFormatClass(self, field):
        """
        Discover and return the format class for a field.
        
        @param field: a Field object
        @type field: L{rpg.sql.Fields.Field}
        
        @returns: L{rpg.Formats.Format} class

        """

        clsobj = None
        # check for an explicit MemberFormat for this field
        try:
            clsobj = self.memberToFormat[field.fullname]
        except KeyError:
            # search for a MemberFormat object for this field type
            try:
                clsobj = self.fieldToFormat[field.__class__]
            except KeyError:
                # if we didn't find one, then start iterating up through its
                # class hierarchy.
                cls = field.__class__
                while cls.__bases__:
                    # get the parent
                    cls = cls.__bases__[0]
                    try:
                        clsobj = self.fieldToFormat[cls]
                    except KeyError:
                        pass
                    else:
                        break
                # if we find nothing, then use the default
                else:
                    clsobj = Formats.StringFormat

        return clsobj


    def getFieldFormat(self, member, field, attrs={}):
        """
        Discover and return a formatter instance for a field.

        @param member: name of the member field was referenced with
        @type member: string
        
        @param field: a Field object
        @type field: L{rpg.sql.Fields.Field}
        
        @param attrs: dictionary of keyword attributes that should be passed
          to the constructor of the L{MemberFormat} subclass.
        @type attrs: dictionary

        @returns: L{rpg.Formats.Format} instance

        """
        clsobj = self.getFieldFormatClass(field)

        # always treat width=0 as None to allow columns to use the database
        # suggested width
        try:
            if attrs["width"] == 0:
                attrs["width"] = None
        except KeyError:
            pass

        # check for any default attributes for this memmber
        try:
            defattrs = self.defaultFormatAttrs[field.fullname].copy()
        except KeyError:
            pass
        else:
            # if the attrs include 'width', then max sure it overrides
            # any previously set min/max values
            if 'width' in attrs:
                defattrs['minWidth'] = None
                defattrs['maxWidth'] = None
            defattrs.update(attrs)
            attrs = defattrs

        # if the 'raw' keyword is present when the formatter is created
        # then, we need to force all member formats to the closest base type.
        if self.raw:
            while clsobj is not Formats.MemberFormat:
                # check for a match
                if clsobj in self.baseTypes:
                    break
                clsobj = clsobj.__bases__[0]
            # otherwise, use a string
            else:
                clsobj = Formats.StringFormat

            # get rid of any keyword args we don't recognize for the base type
            import inspect
            allowed = inspect.getargspec(clsobj.__init__)[0]
            for arg in list(attrs.keys()):
                if arg not in allowed:
                    del attrs[arg]

        # pass the timefmt keyword on to every TimeFormat subclass
        elif self.timefmt and (issubclass(clsobj, Formats.TimeFormat) or issubclass(clsobj, Formats.StringTimeFormat)):
            attrs["timefmt"] = self.timefmt
                
        # pass the zeros flag to all NumberFormats
        elif self.zeros and issubclass(clsobj, Formats.NumberFormat):
            attrs["zeros"] = self.zeros

        # let the member format know if there will be color
        attrs["nocolor"] = self.nocolor

        if "header" not in attrs:
            # the member name might contain a class (e.g. Job.title), so
            # ensure that the header displays the correct value, but we
            # tell the MemberFormat class to reference it with the
            # base member name, field.member
            attrs["header"] = member

        # create an instance of the class
        mformat = clsobj(field.member, **attrs)

        return mformat


    # regexp used to get the member name
    _member_re = re.compile('(?P<member>[a-zA-Z]\w*(?:\.[a-zA-Z_]\w*)?)')

    def getMemberFormat(self, member, attrs={}):
        """Overloaded so we can search for the appropriate L{MemberFormat}
        object via the L{fieldToFormat} lookup.

        @param member: name of the member that will be formatted
        @type member: string

        @param attrs: dictionary of keyword attributes that should be passed
          to the constructor of the L{MemberFormat} subclass.
        @type attrs: dictionary

        @return: the L{MemberFormat} subclass instance that will be used
          to format the member.
        @rtype: instance
        """

        # search for the field associated with this member
        field = self.database.fieldByMember(member, table=self.table)
        # complain if we don't find one
        if not field:
            raise DBFormatterError("(4) no field associated with '%s'" % member)

        mformat = self.getFieldFormat(member, field, attrs)
        # keep track of the field this member will be formatting
        self.fieldByMember[mformat] = field
        return mformat


    def _setupCaches(self):
        """To speed up formatting we keep a cache of which formats
        reference members from joined tables (i.e. not directly located
        int 'obj')."""

        # create the indexes we will iterate over now
        self.indexes = list(range(len(self.mformats)))
        
        # if the table of a member's field differs from the primary, then
        # we need to reference its base class before the member
        self._memberObjCache = []
        for mf in self.mformats:
            # get the field associated with this format
            field = self.fieldByMember[mf]
            if self.table is None or field.table is not self.table:
                self._memberObjCache.append(field.table.baseClass.__name__)
            else:
                self._memberObjCache.append(None)


    def getFormattedValues(self, obj):
        """Overloaded from the base class so we can support the L{rpg.sql}
        syntax for referencing members from joined tables (e.g. foo.Bar.a,
        where 'foo' points to member 'a' from the joined table 'Bar').

        @param obj: object instance that will have its members formatted
        @type obj: instance

        @return: a formatted version of the object that can be printed on
          a single line.
        @rtype: string
        """

        # This check has been commented out in order to allow alternative
        # classes to represent the row being formatted.  One such class
        # is the Row class that enables a dictionary representation of
        # a row to be accessed in an object member manner.
        # e.g. the user of row = Row({"tid": 1, "Job.user": "adamwg"}) can
        # be accessed by row.Job.user
        # ---
        # make sure we are going to format the proper object.
        #if self.table is not None and \
        #        not issubclass(obj.__class__, self.table.baseClass):
        #    raise DBFormatterError, "formatter has been configured to " \
        #          "print '%s' types, not '%s' types." % \
        #          (self.table.baseClass.__name__, obj.__class__.__name__)
        # ---

        # keep a list of all the fstrs and join them at the end
        fstrs = []
        
        # iterate through each member format and make sure the appropriate
        # object is passed along
        for i in self.indexes:
            mformat = self.mformats[i]
            mobj    = self._memberObjCache[i]
            
            # if the member's object is different than the one passed in
            # then search for it first
            if mobj:
                val = getattr(obj, mobj)
            else:
                val = obj

            # format the member
            fstrs.append(mformat.format(val))

        return fstrs


    def setWidths(self, qresult, force=False):
        """Overloaded from the base so we can take advantage of the data
        found in the query result.

        @param qresult: L{Database.QueryResult} that was returned from the
          database.
        @type objs: L{Database.QueryResult} instance

        @param force: Set this to True to force the width (if it is already
          set) of each member to be set based on the objects, default is False.
        @type force: boolean
        """

        # if this isn't a query result, then treat it as a list
        if not isinstance(qresult, Database.QueryResult):
            return self.setWidthsFromList(qresult, force=force)

        # get the widths from the database
        widths = qresult.getFieldWidths()

        # iterate over each member format and pass the objects on
        for mf in self.mformats:
            # get the field for this format
            field = self.fieldByMember[mf]
            # does this field have a callback defined?
            try:
                func = getattr(mf, "setWidthFromDB")
                func(qresult, field)
            except AttributeError:
                # look for it in the query result mapping
                try:
                    width = widths[field]
                except KeyError:
                    #print "no width found for", field.getWhere(), \
                    #      "default is", mf.width, mf.__class__
                    # well we have to set something, so use the minimum
                    if mf.width is None:
                        mf.width = mf.minWidth
                else:
                    #print field.fullname, mf.__class__, mf.width, width, mf.minWidth, mf.maxWidth
                    # use the width suggested by the database
                    if not mf.width and width:
                        mf.width = width
                    # if the database didn't have something, then use the
                    # minimum
                    elif not mf.width:
                        mf.width = mf.minWidth

            #print "final", mf.width


    def setWidthsFromList(self, objs, force=False):
        """Optional method to set the widths if the objects are not part
        of a L{Database.QueryResult}, but they are part of the database."""

        # The _memberObjCache member will return each member's parent
        # object.  Members from the non-primary object (i.e. from a join,
        # like obj.Job.user) must first have the parent object retrieved.
        # Instead of grabbing them for each member, we keep a cache
        # in case they are needed again.
        joinedObjs = {None: objs}

        # iterate through each member format and make sure the appropriate
        # object is passed along
        for i in self.indexes:
            mformat = self.mformats[i]
            mobj    = self._memberObjCache[i]

            # get the objects we want to send to the setWidth() method
            try:
                newobjs = joinedObjs[mobj]
            except KeyError:
                newobjs = [getattr(obj, mobj) for obj in objs]
                joinedObjs[mobj] = newobjs

            # call the setWidth method on the member format objects
            mformat.setWidth(newobjs)


class FieldDocFormatter(Formats.InstanceDocFormatter):
    """Formatter used to neatly display the documentation strings for
    each field of a table."""

    def getDescriptions(self, table):
        """Get all the docmentation strings for the fields of the
        provided table.

        @param table: L{Table.Table} object that will have its fields formatted
        @type clsobj: instance

        @return: a mapping of documentation strings keyed by the field
          (member) name.
        @rtype: dictionary
        """

        # iterate through each class object of this table and get its
        # descriptions
        descs = {}
        for clsobj in table.objects:
            # get all the @ivar in the __doc__ string
            clsdocs = super(FieldDocFormatter, self).getDescriptions(clsobj)

            # now make sure we only include those that are a Field or Alias
            for field in clsobj.__dict__.get("Fields", []) + \
                         clsobj.__dict__.get("VirtualFields", []):
                try:
                    doc = clsdocs[field.member]
                except KeyError:
                    doc = "*** no description ***"
                descs[field.member] = doc

            # make the alias mappings
            for alias,member in list(clsobj.Aliases.items()):
                descs[alias] = "alias for " + member

        return descs            


def test():
    import rpg.sql.TestDatabase as TestDB
    
    class ProduceFormatter(DBFormatter):
        defaultFormatLists = {
            TestDB.ProduceDB.FruitTable: "fruit,flavour"
            }
        
        def __init__(self, *mformats, **attrs):
            DBFormatter.__init__(self, database=TestDB.ProduceDB,
                                 *mformats, **attrs)

    class FruitFormatter(ProduceFormatter):
        def __init__(self, *mformats, **attrs):
            ProduceFormatter.__init__(self, table=TestDB.ProduceDB.FruitTable,
                                      *mformats, **attrs)

    db = TestDB.ProduceDB()
    db.open()

    fruits = db.getFruits()
    db.close()

    pf = ProduceFormatter("fruit,flavour", "seasons, fruit",
                          table=fruits.table)
    print(pf.formatList(fruits))

    pf = ProduceFormatter(table=fruits.table)
    print(pf.formatList(fruits))

    pf = ProduceFormatter("-fruit", table=fruits.table)
    print(pf.formatList(fruits))

    pf = ProduceFormatter("+seasons", table=fruits.table)
    print(pf.formatList(fruits))

    pf = ProduceFormatter("=flavour=30,+seasons", table=fruits.table)
    print(pf.formatList(fruits))

    pf = FruitFormatter()
    print(pf.formatList(fruits))

    pf = FruitFormatter("=flavour=30,+seasons")
    print(pf.formatList(fruits))

    fdf = FieldDocFormatter()
    print(fdf.format(TestDB.ProduceDB.FruitTable))

    


if __name__ == "__main__":
    test()
