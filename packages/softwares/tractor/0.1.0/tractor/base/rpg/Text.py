"""Manages a list of regular expressions to use while parsing a piece of text.
The regular expressions are merged into one for efficiency, and when a match
is found the cooresponding callback is run.  The Text class manages all the
expressions and keeps track of the current location in the string being
parsed.

Text objects are instantiated with a list of RegExp objects, and text is
parsed by calling the parse() method.  Below is an example of using
the Text class to pase a data file.

Data File::

                         count               bytes
--------------------------------------------------
Depth List Stats:
  Pool size         =      10000            640000          0.610
  Max pool size     =      10000            640000          0.610
  Pool reclaims     =     286513
  Pool free         =      10000
  # depth lists     =      10000            640000          0.610
  # cache bins      =      28474           1138960          1.086
  average initCount =         29


We want to extract each piece of data from this file, and give it a unique
name in a dictionary.  So create a regexp that will be used for each line
with data::

    class Data(RegExp):
        # Capture lines that have data.

        def __init__(self):
            # Bake our regular expression in for this class.  Notice that
            # it is written to accept lines with 1 to 3 values.
            super(Data, self).__init__("^\s+([^=\n]+?)\s+=\s+(\d+)"
                                       "(?:\s+(\d+)(?:\s+(\S+))?)?\n")


        def match(self, match):
            # each line captured can have 1 to 3 values.  The first value
            # is a 'count', second 'bytes', third a fraction.  Notice
            # that group numbers are referenced as offsets from match.lastindex
            # because there could be groups from other RegExp objects.
            key = match.group(match.lastindex + 1).strip()
            # set the count
            val1 = long(match.group(match.lastindex + 2))
            self.parent.data[key + "/count"] = val1

            # check for bytes
            val2 = match.group(match.lastindex + 3)
            if val2 is not None:
                val2 = long(val2)
                self.parent.data[key + "/bytes"] = val2

            # check for a fraction
            val3 = match.group(match.lastindex + 4)
            if val3 is not None:
                val3 = float(val3)
                self.parent.data[key + "/fraction"] = val3


    # create a Text object
    text = Text([Data()])
    # add a dictionary for the callback to write to
    text.data = {}
    # parse the text
    text.parse(datastr)

"""

import re

class RegExp(object):
    """Defines a callback that is called when the regular expression is
    matched."""

    def __init__(self, regexp):
        # regular expression that defines this section
        self.regexp = regexp
        # the Text object governing the list of expressions
        self.parent = None

    def match(self, match):
        """Called when the regular expression is matched."""
        pass

    
class TextError(Exception):
    """Base class for all Text object errors."""
    pass

class Text(object):
    """Runs a string of text through a set of regular expressions.  Each
    expression is defined in a Section object, and when a match is found,
    the match() method is run."""

    def __init__(self, regexps, debug=False):
        """A Text is initialized with the a string of text that will be
        parsed."""
        self.debug = debug

        # when the main regexp is built, a pointer to each sub regexp is
        # saved for quick access.  The class and method name are saved in
        # tuple
        self.group2data = {}

        # a list of regular expressions used to find all the matches
        self.reobjs = self._buildRegExps(regexps)

    def _buildRegExps(self, regexps):
        """Based on the RegExps provided, build a list of regular
        expressions that will be used to find all the matches.  Ideally all
        objects will be merged into one regexp, but python has a limit of
        the number of groups, so we have multiple merges."""

        # we will uniquely identify each one based on this counter
        cnt   = 0
        # make a list of all the unique cases that will be merged to form
        # the final set of regular expressions
        groups = []

        for regexp in regexps:
            # set the parent
            regexp.parent = self
            # make a unique group name that the re module will accept
            groupname = 'grp_%d' % cnt
            # increment cnt for the next pass
            cnt      += 1
            # uniquely name each regexp so we know which class and method
            # to call if a match is found
            groups.append('(?P<%s>%s)' % (groupname, regexp.regexp))

            # make a lookup
            self.group2data[groupname] = regexp

        # now we need to join all the groups we collected above into as few
        # regular expressions as possible with the | operator.  Ideally,
        # all groups would be joined into one regexp, but python has a limit
        # of the number of groups.  Thus, if an AssertionError is raised,
        # then adjust the number of groups we are joining.  To accomplish
        # this, we put an anchor case at the end of each regexp we create
        # to know when we should move onto the next one in the list

        # this will be placed at the end to ensure we don't go to far when
        # searching for a match.
        anchor  = "(?P<A>(^[^\n]+\n)|(^\n))"
        # first try to join everything
        num     = len(groups)
        # save all resulting reobjs in here
        reobjs  = []

        while groups:
            # now join all the reobjs with an 'or'
            reobjstr = '|'.join(groups[:num]) + '|' + anchor
            try:
                rexp = re.compile(reobjstr, re.MULTILINE | re.DOTALL)
            except AssertionError:
                # if we are only trying to add 1 regexp then abort
                if num == 1:
                    raise

                # if we are less than 10, then start decrementing by 1
                if num <= 10:
                    num -= 1
                else:
                    num -= 10
            else:
                # add the regexp to the list
                reobjs.append(rexp)
                # now try with whatever is left
                groups = groups[num:]

        return reobjs

    def parse(self, text, start=0, end=None):
        """Parse a string of text."""

        # make sure the sections have been set
        if not self.reobjs:
            raise TextError("no regexps have been set for the " \
                  "interpreter.")

        # to avoid an unneeded level of name lookup, we fetch values that we
        # know we will need during each iteration of the loop.
        group2data = self.group2data
        regexps    = [getattr(rexp, 'search') for rexp in self.reobjs]
        nextpos    = start

        if end is None:
            end = len(text)

        # search until we have nothing more to look at            
        while nextpos < end:
            # start searching for matches
            for rexp in regexps:
                match = rexp(text, nextpos, end)
                # a valid match was found, so break out of the loop
                if match and match.lastgroup != 'A':
                    break
            else:
                # if no matches were found, then the string must be empty
                # or have no newlines, either way, quit
                if not match:
                    break
                
                # keep going until we get to the end of the string.
                nextpos = match.end()
                continue
            
            # we assume each match will have a name associated with it, this
            # way we can quickly lookup the class object and method name.
            group2data[match.lastgroup].match(match)

            # get a new next position
            nextpos = match.end()


def test():

    datastr = """
                         count               bytes
--------------------------------------------------
Depth List Stats:
  Pool size         =      10000            640000          0.610
  Max pool size     =      10000            640000          0.610
  Pool reclaims     =     286513
  Pool free         =      10000
  # depth lists     =      10000            640000          0.610
  # cache bins      =      28474           1138960          1.086
  average initCount =         29
"""

    class Data(RegExp):
        # Capture lines that have data.

        def __init__(self):
            # Bake our regular expression in for this class.  Notice that
            # it is written to accept lines with 1 to 3 values.
            super(Data, self).__init__("^\s+([^=\n]+?)\s+=\s+(\d+)"
                                       "(?:\s+(\d+)(?:\s+(\S+))?)?\n")


        def match(self, match):
            # each line captured can have 1 to 3 values.  The first value
            # is a 'count', second 'bytes', third a fraction.  Notice
            # that group numbers are referenced as offsets from match.lastindex
            # because there could be groups from other RegExp objects.
            key = match.group(match.lastindex + 1).strip()
            # set the count
            val1 = int(match.group(match.lastindex + 2))
            self.parent.data[key + "/count"] = val1

            # check for bytes
            val2 = match.group(match.lastindex + 3)
            if val2 is not None:
                val2 = int(val2)
                self.parent.data[key + "/bytes"] = val2

            # check for a fraction
            val3 = match.group(match.lastindex + 4)
            if val3 is not None:
                val3 = float(val3)
                self.parent.data[key + "/fraction"] = val3


    # create a Text object
    text = Text([Data()])
    # add a dictionary for the callback to write to
    text.data = {}
    # parse the text
    text.parse(datastr)

    for key,val in list(text.data.items()):
        print(key,val)
    

if __name__ == "__main__":
    test()
