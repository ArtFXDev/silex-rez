

import random as _random

__all__ = (
        'repeatshuffle',
        )

# ---------------------------------------------------------------------------

def repeatshuffle(sequence, times=None, random=_random.random):
    """return an iterator to return a unique item from the list without 
    repeating until all items in the list have been returned 

    >>> import random

    >>> random.seed(0)
    >>> g = repeatshuffle([1,2,3,4])
    >>> [g.next() for i in xrange(5*4)]
    [2, 1, 3, 4, 3, 2, 4, 1, 2, 4, 3, 1, 2, 4, 1, 3, 2, 3, 1, 4]

    >>> random.seed(0)
    >>> list(repeatshuffle([1,2,3,4], 5))
    [2, 1, 3, 4, 3, 2, 4, 1, 2, 4, 3, 1, 2, 4, 1, 3, 2, 3, 1, 4]
    >>> random.seed(None)
    
    """
    sequence = list(sequence)

    if times is None:
        while True:
            _random.shuffle(sequence, random)
            for item in sequence:
                yield item
    else:
        for i in range(times):
            _random.shuffle(sequence, random)
            for item in sequence:
                yield item
