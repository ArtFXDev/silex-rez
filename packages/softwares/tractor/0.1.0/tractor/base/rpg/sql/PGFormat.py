"""Functions to express L{DBObject.DBObject} instances in a format conducive to COPY FROM

e.g.
>>> job = DBEngine.Job()
>>> pgsql.cursor.copy_from(formatObjsForCOPY([job]), 'job')
"""

import sys, time

from . import Fields

SEPARATOR = "\t"
NULL = "\N"

def formatTimestampForCOPY(timestamp):
    """Return timestamp as a string compatible in COPY FROM format."""
    if timestamp:
        return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        return NULL

def formatIntArrayForCOPY(array):
    """Return array of values compatible in COPY FROM format."""
    if array:
        return "{%s}" % ",".join([str(i) for i in array])
    else:
        return  "{}"

def formatOtherForCOPY(v):
    """Return the value in a format compatible in COPY FROM format."""
    if v is not None:
        return str(v)
    else:
        return NULL
    
def formatStrForCOPY(s):
    """Return the string in a format compatible in COPY FROM format."""
    if s is None:
        return NULL
    else:
        return s
    
def formatStrArrayForCOPY(array):
    """Return array of values compatible in COPY FROM format."""
    if array:
        return "{%s}" % ",".join([formatStrForCOPY(el) for el in array])
    else:
        return "{}"

def formatBoolForCOPY(b):
    """Express boolean value in a COPY FROM compatible format."""
    if b:
        return "t"
    else:
        return "f"

fieldToFormatFunc = {
    "TimestampField"   : formatTimestampForCOPY,
    "IntArrayField"    : formatIntArrayForCOPY,
    "StrArrayField"    : formatStrArrayForCOPY,
    "BooleanField"     : formatBoolForCOPY,
    "TextField"        : formatStrForCOPY,
    "SmallIntField"    : formatOtherForCOPY,
    "IntField"         : formatOtherForCOPY,
    "BigIntField"      : formatOtherForCOPY,
    "FloatField"       : formatOtherForCOPY,
    "GigaByteFloatField" : formatOtherForCOPY,
}

def formatObjsForCOPY(objs):
    """Return the given object in a COPY FROM format."""
    parts = []
    fields = objs[0].getFields() if objs else None
    for obj in objs:
        for field in fields:
            formatFunc = fieldToFormatFunc.get(field.__class__.__name__, formatStrForCOPY)
            try:
                parts.append(formatFunc(getattr(obj, field.fieldname)))
            except:
                sys.stderr.write("obj=%s\nfield=%s\nclass=%s\nfunc=%s\n" %
                                 (str(obj), field.fieldname, field.__class__.__name__, str(formatFunc)))
                raise
            if field is not fields[-1]:
                parts.append("\t")
        parts.append("\n")
    return "".join(parts)
