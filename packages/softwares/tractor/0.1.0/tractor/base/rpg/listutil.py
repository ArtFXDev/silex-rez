

__all__ = (
        'maxLen',
        'getUnion',
        'segmentize',
        'iterSegments',
        'iunique',
        'unique',
        'uniquesort',
        )

# ---------------------------------------------------------------------------

def maxLen(myLists):
    """
    Returns the length of the longest item in the list
    
    >>> maxLen(['123', '1', '12'])
    3

    """

    maxLen = 0
    for l in myLists:
        maxLen = max(maxLen, len(l))

    return maxLen

# ----------------------------------------------------------------------------

def getUnion(*mylists):
    """
    Return a union of all the lists given as arguments.  This resulting
    list is sorted.
    
    >>> getUnion([1,2,3], [5,4,3], [4,5,6], [8])
    [1, 2, 3, 4, 5, 6, 8]
    
    """

    # use a hash to mark unique list items
    keys = list(dict([(entry, None) for l in mylists for entry in l]).keys())
    keys.sort()
    return keys

# ----------------------------------------------------------------------------

def segmentize(items, step):
    """
    Split a list, or a list-like object, into a list of list segments, 
    sliced at every step 
 
    >>> segmentize([1,2,3,4,5,6,7,8,9,10], 3)
    [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]
    
    """
    return [items[index: index + step] \
            for index in range(0, len(items), step)]

# ----------------------------------------------------------------------------

def iterSegments(items, step):
    """
    Split a list, or a list-like object, into a list of list segments, 
    sliced at every step 

    >>> list(iterSegments([1,2,3,4,5,6,7,8,9,10], 3))
    [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]
    
    """

    for index in range(0, len(items), step):
        yield items[index: index + step]

# ----------------------------------------------------------------------------

def iunique(items):
    """
    Removes repeated items from a list. In order for it to work, the items in
    the list must be hashable. Returns an iterator.
    
    >>> list(iunique([1,5,3,1,2,5,2,7]))
    [1, 5, 3, 2, 7]
    
    """

    d = {}
    for item in items:
        if item in d:
            pass
        else:
            d[item] = None
            yield item

# ----------------------------------------------------------------------------

def unique(items):
    """
    Removes repeated items from a list. In order for it to work, the items in
    the list must be hashable. Returns a list.

    >>> unique([1,5,3,1,2,5,2,7])
    [1, 5, 3, 2, 7]

    """
    
    return list(iunique(items))

# ----------------------------------------------------------------------------

def uniquesort(items):
    """
    Copies the items list and returns a sorted version with the duplicates 
    removed. Two items equal each other if they have the same hash code

    >>> uniquesort([5,3,4,5,3,1,2])
    [1, 2, 3, 4, 5]

    """
    from . import dictutil

    items = list(dictutil.fromkeys(items).keys())
    items.sort()

    return items
