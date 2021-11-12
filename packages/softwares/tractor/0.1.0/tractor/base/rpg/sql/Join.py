# Join.py

class JoinError(Exception):
    pass

class Join:
    def __init__(self, leftTable, rightTable, onclause=None,
                 fields=None, preTables=None, oneway=False,
                 commonKeys=None, fulljoin=None, leftJoin=True):

        self.leftTable  = leftTable
        self.rightTable = rightTable
        self.preTables  = None
        self.onclause   = None
        self.oneway     = oneway
        self.fulljoin   = None
        self.useLeftJoin = leftJoin

        # if the fulljoin is provided, then we won't do anything but
        # return this value when called.
        if fulljoin:
            self.fulljoin = fulljoin
            # hack because some code assume onclause will be set
            self.onclause = fulljoin

        # if preTables is set, then we do nothing else
        elif onclause:
            self.onclause = onclause

        elif preTables:
            self.preTables = preTables

        # if common fields are provided, then try to do that
        elif fields:
            phrases = []
            for key in fields:
                phrases.append("%s.%s=%s.%s" % (leftTable.tablename, key,
                                                rightTable.tablename, key))
            self.onclause = ' AND '.join(phrases)

        # if no onclause is provided, then try to figure out what to join
        # on based on the primary keys of the two tables.
        elif commonKeys:
            # save a list of the common keys
            common = []

            # search for common keys in the following order
            order  = [(leftTable.keys, rightTable.keys),
                      (leftTable.keys, rightTable.equivKeys),
                      (leftTable.equivKeys, rightTable.keys),
                      (leftTable.equivKeys, rightTable.equivKeys)]

            #print leftTable.keys

            # if we don't find any common fields after searching through all
            # the list pairs, then throw an error.
            for pair in order:
                #print pair
                common = self._findCommon(pair[0], pair[1],
                                          commonKeys=commonKeys)
                # if we find some common fields, then break out of the loop
                if common: break
            else:
                raise JoinError("no common keys found between %s and %s" % \
                      (leftTable.tablename, rightTable.tablename))

            # make the onclause
            phrases = []
            for key in common:
                phrases.append("%s.%s=%s.%s" % (leftTable.tablename, key,
                                                rightTable.tablename, key))
            self.onclause = ' AND '.join(phrases)

        else:
            raise JoinError("no common keys found between %s and %s" % \
                  (leftTable.tablename, rightTable.tablename))


    def __str__(self):
        if self.preTables:
            return "(%s, %s): requires %s" % \
                   (self.leftTable.tablename, self.rightTable.tablename,
                    ', '.join([t.tablename for t in self.preTables]))
        return "(%s, %s): %s" % (self.leftTable.tablename,
                                 self.rightTable.tablename,
                                 self.joinStr())
                                 

    def _findCommon(self, lfields, rfields, commonKeys=None):
        """Return the common field names found between the two lists."""

        common = []
        #print lfields, rfields
        for left in lfields:
            for right in rfields:
                if left.fieldname == right.fieldname and \
                   left.fieldname in commonKeys:
                    common.append(left.fieldname)
                    break
        return common

    def joinStr(self):
        if self.fulljoin:
            return self.fulljoin
        if self.useLeftJoin:
            leftStr = "LEFT "
        else:
            leftStr = ""
        return "%sJOIN %s ON (%s)" % \
               (leftStr, self.rightTable.alias, self.onclause)

        
def testJoin():
    from Field import VarCharField
    from .Table import Table
    from .DBObject import DBObject
    
    class Fruit(DBObject):
        def __init__(self):
            self.fruit = ''
            self.taste = ''
            DBObject.__init__(self)

    class Taste(DBObject):
        def __init__(self):
            self.taste = ''
            self.goodbad = ''
            DBObject.__init__(self)
            
    FruitFields = [
        VarCharField('fruit', length=16, key=True),
        VarCharField('taste', length=16, index=True)
        ]
    FruitTable = Table('Fruit', Fruit, fields=FruitFields)
    
    TasteFields = [
        VarCharField('taste', length=16, key=True),
        VarCharField('goodbad', length=16, index=True)
        ]
    TasteTable = Table('Taste', Taste, fields=TasteFields)
    
    join = Join(FruitTable, TasteTable, 'Fruit.taste=Taste.taste')
    print(join.joinStr())


if __name__=='__main__':
    testJoin()
    
