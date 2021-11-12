import types

from rpg.listutil import getUnion
    
__all__ = (
        'mergeDict',
        'fromkeys',
        'pop',
        'popitem',
        )

# ----------------------------------------------------------------------------

def mergeDict(dict1, dict2, unionLists=0):
    """
    Merge two dictionaries

    >>> d = mergeDict({'a': 1, 'b': 2}, {'c': 3, 'd': 4})
    >>> d == {'a': 1, 'b': 2, 'c': 3, 'd': 4}
    1

    >>> d = mergeDict(
    ...     {'a': [1,2], 'b': {1:2, 3:4}}, 
    ...     {'a': [3,4], 'b': {3:4, 5:6}, 'c': 7})
    >>> d == {'a': [3,4], 'b': {1:2, 3:4, 5:6}, 'c': 7}
    1

    >>> d = mergeDict(
    ...     {'a': [1,2], 'b': {1:2, 3:4}}, 
    ...     {'a': [3,4], 'b': {3:4, 5:6}, 'c': 7}, unionLists=1)
    >>> d == {'a': [1,2,3,4], 'b': {1:2, 3:4, 5:6}, 'c': 7}
    1
    
    """

    for key,val2 in dict2.items():
        try:
            val1 = dict1[key]
        except KeyError:
            # make a deep copy of the value is a dictionary
            if type(val2) is dict:
                dict1[key] = {}
                mergeDict(dict1[key], val2)
            # make a new list
            elif type(val2) is list:
                dict1[key] = list(val2)
            else:
                dict1[key] = val2
        else:
            # recursively merge the two dictionaries
            if type(val1) is dict and \
               type(val2) is dict:
                mergeDict(val1, val2)
            # take the union of the two lists
            elif unionLists and \
                 type(val1) is list and \
                 type(val2) is list:
                dict1[key] = getUnion(val1, val2)
            # replace the value in dict1 with dict2's
            else:
                dict1[key] = val2
    return dict1

# ---------------------------------------------------------------------------

def _fromkeys(items, value=None):
    """
    create a new dictionary with keys from seq and values set to value

    >>> _fromkeys([1, 2, 3]) == {1: None, 2: None, 3: None}
    True
    >>> _fromkeys([1, 2, 3], 5) == {1: 5, 2: 5, 3: 5}
    True
    
    """
    
    return dict([(item, value) for item in items])

try:
    fromkeys = dict.fromkeys
except AttributeError:
    fromkeys = _fromkeys

# ---------------------------------------------------------------------------

def _pop(dictionary, key, *args):
    """
    _pop(d, k[, x]) 
    d[k] if k in d, else x (and remove k)

    >>> d = {1: 2, 3: 4}
    >>> _pop(d, 3)
    4
    >>> 3 in d
    False
    >>> _pop(d, 5)
    Traceback (most recent call last):
    ...
    KeyError: 5
    >>> _pop(d, 5, 6)
    6

    """
    
    if not args:
        val = dictionary[key]
        del dictionary[key]
        return val
    else:
        num = len(args)
        if num > 1:
            raise TypeError('pop expected at most 2 arguments, got ' + \
                    str(num))

        try:
            val = dictionary[key]
            del dictionary[key]
            return val
        except KeyError:
            return args[0]

try:
    pop = dict.pop
except AttributeError:
    pop = _pop

# ---------------------------------------------------------------------------

def _popitem(dictionary):
    """
    remove and return an arbitrary (key, value) pair

    >>> d = {1: 2}
    >>> _popitem(d)
    (1, 2)
    >>> len(d)
    0

    >>> d = {1: 2, 3: 4}
    >>> copy = d.copy()
    >>> k, v = _popitem(d)
    >>> k not in d and k in copy and copy[k] == v
    True
    >>> len(d)
    1
    >>> k, v = _popitem(d)
    >>> k not in d and k in copy and copy[k] == v
    True
    >>> len(d)
    0
    >>> k, v = _popitem(d)
    Traceback (most recent call last):
    ...
    KeyError: 'popitem(): dictionary is empty'

    """
    
    try:
        key, value = next(iter(dictionary.items()))
    except StopIteration:
        raise KeyError('popitem(): dictionary is empty')

    del dictionary[key]
    return (key, value)

try:
    popitem = dict.popitem
except AttributeError:
    popitem = _popitem

