import os, types, re
import xml.dom, xml.sax
from xml.dom import Node
from xml.dom.minidom import parse
from xml.dom.minidom import getDOMImplementation

__all__ = (
        'PreferencesError',
        'Preferences',
        )

# ---------------------------------------------------------------------------

class PreferencesError(Exception):
    pass


class Preferences:
    """Designed to be a mix-in class to easily save preferences of a class
    or application.  The preferences are saved to a file in XML format
    and read/written using the Python xml modules."""

    __PREFS_VERSION = '1.1'

    def __init__(self, variables, root=None, basename=None, filename=None):
        """Initialize the Preferences object with a list of variables that
        will be saved when C{writePrefs} is called."""

        # the variables that will be saved
        self.__variables = variables

        # the root node of the xml document, if no name is specified then
        # the Class name is used.
        if not root:
            self.__root = self.__class__.__name__
        else:
            self.__root = root

        # the directory where the preferences file will be saved
        if not basename:
            self.__basename = os.getcwd()
        else:
            self.__basename = basename

        # if no filename is found the create one using the root node name
        # and append '.prefs' to the end
        if not filename:
            self.__filename = '%s.prefs' % self.__root
        else:
            self.__filename = filename

        # just to keep our sanity we will always write the files to an
        # absolute path.
        if not os.path.isabs(self.__filename):
            self.__filename = os.path.join(self.__basename, self.__filename)

    def __fixType(self, node, val):
        """Cast the input value 'val' to the type specified in the
        attribute 'type'.  By default all types are strings."""

        # get the 'type' attribute
        attr = node.getAttribute('type')
        # strip then split the value and return a list
        if attr == 'list':
            mylist = val.strip().split()
            ltype  = node.getAttribute('listtype')
            func   = None
            if ltype == 'int':
                func = int
            elif ltype == 'float':
                func = float
            else:
                return mylist
                
            for i in range(len(mylist)):
                try:
                    mylist[i] = func(mylist[i])
                except ValueError:
                    raise PreferencesError("expected %s got %s" % (ltype, str(mylist[i])))

            return mylist

        # cast to an integer
        elif attr == 'int':
            return int(val)
        elif attr == 'float':
            return float(val)

        # just return the same value
        return val.strip()

    def __setValueOLD(self, node):
        """The value of a node can be a string, integer, list, or a
        dictionary.  If the node has at least one Element Node as a
        child then the value will be a dictionary, otherwise the type
        will be determined by the 'type' attribute.  This will recursively
        descend the node and return the appropriate value."""

        # first check if this node has Element Nodes has children, or
        # does it have an attribute 'type' equal to 'dict'

        hasElements = 0
        etype = node.getAttribute('type')
        if etype == 'dict':
            hasElements = 1
        else:        
            for n in node.childNodes:
                if n.nodeType is Node.ELEMENT_NODE:
                    hasElements = 1
                    break

        # if no Element Nodes were found then concatenate all the
        # values of each child node and return the result
        if not hasElements:
            # start with an empty string and build on it
            value = ''
            for n in node.childNodes:
                if n.nodeType is Node.TEXT_NODE:
                    # explicitly cast to a string otherwise it will be
                    # a unicode string
                    value += str(n.data)
                else:
                    raise PreferencesError('expected a TEXT_NODE, got ' + \
                          str(n))
            
            return self.__fixType(node, value)
        # at least one Element node was found so create a dictionary
        # keyed by the Element node children.
        else:
            value = {}
            for n in node.childNodes:
                if n.nodeType is Node.ELEMENT_NODE:
                    # call __getValue() again with this node
                    value[str(n.nodeName)] = self.__setValueOLD(n)

            return value


    def __castString(self, val, vtype):
        """Convert a string to the provided type."""

        if vtype == 'float':
            func = float
        elif vtype == 'int':
            func = int
        elif vtype == 'long':
            func = int
        else:
            # assume this is a string already
            return val

        # convert and return the value
        try:
            return func(val)
        except (ValueError, TypeError):
            raise PreferencesError("expected %s got %s" % (vtype, str(val)))

    def __setValue(self, node, root=0):
        """The value of a node can be a string, integer, list, or a
        dictionary.  If the node has at least one Element Node as a
        child then the value will be a dictionary, otherwise the type
        will be determined by the 'type' attribute.  This will recursively
        descend the node and return the appropriate value."""

        # get the type of node we are dealing with, if this is the root, then
        # assume it is a dictionary
        if root:
            etype = 'dict'
        else:
            etype = node.getAttribute('type')
            if not etype:
                etype = 'str'
            else:
                etype = etype.lower()

        # check for element nodes in the child list, by default if a node
        # has elements and no type, then it is considered a dictionary
        if etype == 'str':
            for child in node.childNodes:
                if child.nodeType is Node.ELEMENT_NODE:
                    etype = 'dict'
                    break

        # process the node as a dictionary
        if etype == 'dict':
            # we will throw all data in here
            result = {}
            key    = None
            for child in node.childNodes:
                # ignore all non element nodes
                if child.nodeType is not Node.ELEMENT_NODE:
                    continue

                # set the key
                if not key and child.nodeName == 'key':
                    # convert the node to a python value
                    key = self.__setValue(child)
                elif key and child.nodeName == 'val':
                    # convert the node to a python value
                    result[key] = self.__setValue(child)
                    key = None
                # if the name is not 'key' or 'val' then treat the name of
                # the node as the key and its data as the value
                elif not key and child.nodeName not in ('key', 'val'):
                    result[str(child.nodeName)] = self.__setValue(child)
                # if we reach here then a 'key' and/or 'val' node is out of
                # place
                else:
                    raise PreferencesError("a 'key' or 'val' node is out " \
                          "of place.")
            # key should of been reset if we are done
            if key:
                raise PreferencesError("a 'key' or 'val' node is out " \
                      "of place.")
            return result
        # process the node as a list or tuple
        elif etype in ('list', 'tuple'):
            # for now we throw everything into a list and if the type is a
            # tuple we convert it at the end            
            result = []
            data   = ''
            for child in node.childNodes:
                # element nodes are used to have any kind of data as a list
                # item
                if child.nodeType is Node.ELEMENT_NODE:
                    # add any text data that may of been building
                    data = data.strip()
                    if data:
                        result.extend(data.split())
                        data = ''

                    # add an individual item
                    if child.nodeName != 'item':
                        raise PreferencesError("expected an 'item' node, " \
                              "got " + child.nodeName)
                    result.append(self.__setValue(child))
                # if a text node is not just white space then it is split
                # to form other items of the list, but only strings
                elif child.nodeType is Node.TEXT_NODE:
                    data += str(child.data)

            # add any text data that may of been building
            data = data.strip()
            if data:
                result.extend(data.split())

            # convert the result to a tuple if that's what it should be
            if etype == 'tuple':
                result = tuple(result)
            return result
        # if the node is the none type, the return None
        elif etype == 'none':
            return None
        # else we expect all the children of the node are text nodes
        else:
            # start with an empty string and build on it
            result = ''
            for child in node.childNodes:
                if child.nodeType is Node.TEXT_NODE:
                    # explicitly cast to a string otherwise it will be
                    # a unicode string
                    result += str(child.data)
                else:
                    raise PreferencesError('expected a TEXT_NODE, got ' + \
                          str(child))
            
            return self.__castString(result, etype)

    # simple re to convert the string representation of a type to something
    # more useful
    typere = re.compile(r"\<type '(\w+?)(Type)?'\>")
    def __getValueType(self, val):
        """Return the type of the value that will be used as an attribute
        in an element node, if the value is not a list, int, or float
        then it is assumed to be a string."""

        vtype = type(val)
        # we support all the major python types
        if vtype in (list, tuple, dict,
                     int, int, float,
                     bytes, type(None)):
            return self.typere.match(str(vtype)).group(1).lower()
        else:
            raise PreferencesError("unsupported type, %s" % str(vtype))

    def __addIndentNode(self, doc, node, level, indent):
        """Add a text node that will indent based on the current level and
        indent spacing."""
        if indent:
            text = doc.createTextNode('\n' + level*indent)
            node.appendChild(text)

    def __addValue(self, doc, node, value, root=0, level=0, indent='  '):
        """Add a value to the tree, and set the attributes of the node to
        identify the type of the value.  If the value is a list, tuple, or
        dictionary then recursively create more nodes."""

        # get the values type
        vtype = self.__getValueType(value)
        # now set the 'type' attribute of the node to reflect this, but
        # only if we aren't the root.  Also don't write out the string type
        # because this is the default and it will only increase file sizes
        if not root and vtype != 'str':
            node.setAttribute('type', vtype)

        # if the value is a list, tuple, or dictionary then we need to
        # recursively set the other values.
        if vtype in ('list', 'tuple'):
            # iterate through each value in the sequence and add a node
            for v in value:
                item = doc.createElement('item')
                # add everything to the item
                self.__addValue(doc, item, v, level=level+1, indent=indent)
                # if indenting is on, then add the text node to do it
                self.__addIndentNode(doc, node, level, indent)
                # add the item to the node
                node.appendChild(item)
            # if indenting is on, then add the text node to do it
            if value: self.__addIndentNode(doc, node, level-1, indent)
        elif vtype == 'dict':
            # sort the keys of the dictionary so it is easier to read
            keys = list(value.keys())
            keys.sort()
            # for each pair add an element for the key and value
            for k in keys:
                v = value[k]
                key = doc.createElement('key')
                # add everything required for the key
                self.__addValue(doc, key, k, level=level+1, indent=indent)
                # if indenting is on, then add the text node to do it
                self.__addIndentNode(doc, node, level, indent)
                # add this to the node
                node.appendChild(key)

                # now add the value
                val = doc.createElement('val')
                # add everything required for the value
                self.__addValue(doc, val, v, level=level+1, indent=indent)
                # if indenting is on, then add the text node to do it
                self.__addIndentNode(doc, node, level, indent)
                # add this to the node now
                node.appendChild(val)
            # if indenting is on, then add the text node to do it
            if keys: self.__addIndentNode(doc, node, level-1, indent)
        # otherwise assume each value can be cast to a string
        elif vtype != 'none':
            # make a text element for the data
            text = doc.createTextNode(str(value))
            # now add the text to the node
            node.appendChild(text)

    def readPrefs(self, filename=None):
        """Read an XML preferences file 'filename', otherwise use the
        default file defined in __init__.  Each Element node whose parent
        is the document will have its value set to 'self'.  Example, if
        the file looks like::

          <?xml version="1.0" encoding="iso-8859-1"?>
          <MyJob>
            <user>john</user>
            <host>doe</host>
          </MyJob>

        The variables 'user' and 'host' will be set to 'john' and 'doe'
        respectively."""

        # choose the filename to use
        if not filename:
            filename = self.__filename

        # read the file into memory
        try:
            dom = parse(filename)
        except (xml.dom.DOMException, xml.sax.SAXException) as err:
            raise PreferencesError(str(err))

        # if the document does not have a prefsver defined, then give it the
        # old parsing methods
        if not dom.documentElement.getAttribute('prefsver'):
            # iterate through each Element Node and set each variable
            for node in dom.documentElement.childNodes:
                if node.nodeType is Node.ELEMENT_NODE:
                    try:
                        func = getattr(self, 'prefs_set_' + str(node.nodeName))
                    except AttributeError:
                        val = self.__setValueOLD(node)
                    else:
                        val = func(node)

                    setattr(self, str(node.nodeName), val)
        else:
            prefs = self.__setValue(dom.documentElement, root=1)
            # set the values in the class now
            for key,val in list(prefs.items()):
                setattr(self, key, val)

    def writePrefs(self, filename=None, createDir=0):
        """Write the list of variables passed in with the __init__ method
        to a file in XML.  The root node of the document will be the name
        of this class be default."""

        # determine the file to use
        if not filename:
            filename = self.__filename

        # create a dictionary of variables to pass to the __addValue method
        varsdict = {}
        for var in self.__variables:
            varsdict[var] = getattr(self, var)

        # create dom object to represent an XML file
        impl = getDOMImplementation()
        doc  = impl.createDocument(None, self.__root, None)
        # add a version to the document
        doc.documentElement.setAttribute('prefsver', self.__PREFS_VERSION)
        # populate the dom with our data
        self.__addValue(doc, doc.documentElement, varsdict, root=1,
                        level=1, indent='  ')

        if createDir:
            dir = os.path.dirname(filename)
            try:
                os.makedirs(dir)
            except OSError as errObj:
                if errObj.errno != 17:
                    raise
            
        # now open the file and write some XML
        file = open(filename, 'w')
        doc.writexml(file)
        # add one more newline so it looks pretty
        file.write('\n')
        file.close()
            
        # clean up the dom
        doc.unlink()
