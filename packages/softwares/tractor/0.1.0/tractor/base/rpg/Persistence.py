"""Objects used to preserve object state."""

import os, types, re, sys, time

import xml.dom, xml.sax
from xml.dom import Node
from xml.dom.minidom import parseString
from xml.dom.minidom import getDOMImplementation

import yaml

import rpg
import rpg.tracebackutil as tracebackutil

__all__ = (
    "XMLObjectError",
    "ModuleNotFound",
    "Preferences",
    "XMLObject",
    "ObjectState",
    "ClassCacheError",
    "ClassCache",
    )

# this is a hack straight out of pickle.py so we can create class instances
# when we read in xml
class _EmptyClass:
    pass

class PersistenceError(rpg.Error):
    pass


class XMLObjectError(PersistenceError):
    pass

class ModuleNotFound(XMLObjectError):
    pass

class Container(object):
    """Simple class that will be used to allow values within nested
    dictionaries to be referenced via dict.key instead of dict['key']."""

    def __getattr__(self, var):
        try:
            return self.__dict__[var]
        except KeyError:
            pass

        return getattr(self.__dict__, var)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, val):
        self.__dict__[key] = val

    def __delitem__(self, key):
        del self.__dict__[key]

    def __repr__(self):
        return repr(self.__dict__)


class XMLObject(object):
    """Designed to be a mix-in class to easily save preferences of a class
    or application.  The preferences are saved to a file in XML format
    and read/written using the Python xml modules."""

    __VERSION = '1.1'

    CONTAINERS = False

    def __getFileName(self, basedir=None, basename=None):
        """Create a filename that will be used to read/write the data
        to and from disk."""

        # the directory where the preferences file will be saved
        if not basedir:
            basedir = os.getcwd()

        # the basename will be the name of the class if one is not provided
        if not basename:
            basename = self.__class__.__name__

        return os.path.join(basedir, basename)

    def __castString(self, val, vtype):
        """Convert a string to the provided type."""

        if vtype == 'float':
            func = float
        elif vtype == 'int':
            func = int
        elif vtype == 'long':
            func = int
        elif vtype == 'bool':
            lower = val.lower()
            if lower == 'true':
                return True
            elif lower == 'false':
                return False
            else:
                raise XMLObjectError("bool type should be 'true' or " \
                      "'false', not '%s'" % val)
        else:
            # assume this is a string already
            return val

        # convert and return the value
        try:
            return func(val)
        except (ValueError, TypeError):
            raise XMLObjectError("expected %s got %s" % (vtype, str(val)))

    def __readXMLNode(self, node, root=0):
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
                    key = self.__readXMLNode(child)
                elif key and child.nodeName == 'val':
                    # convert the node to a python value
                    result[key] = self.__readXMLNode(child)
                    key = None
                # if the name is not 'key' or 'val' then treat the name of
                # the node as the key and its data as the value
                elif not key and child.nodeName not in ('key', 'val'):
                    result[str(child.nodeName)] = self.__readXMLNode(child)
                # if we reach here then a 'key' and/or 'val' node is out of
                # place
                else:
                    raise XMLObjectError("a 'key' or 'val' node is out " \
                          "of place.")
            # key should of been reset if we are done
            if key:
                raise XMLObjectError("a 'key' or 'val' node is out " \
                      "of place.")

            # throw the dictionary into a Container object so things can
            # be referenced with the . notation.
            if self.CONTAINERS and not root:
                obj = Container()
                obj.__dict__ = result
                return obj
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
                        raise XMLObjectError("expected an 'item' node, " \
                              "got " + child.nodeName)
                    result.append(self.__readXMLNode(child))
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
        # if the node is a class instance, then create it
        elif etype == 'instance':
            # get the module and class the instance is from
            modname   = node.getAttribute('module')
            classname = node.getAttribute('class')
            # import the module and create an instance of the class
            try:
                exec('import ' + modname)
            except ImportError:
                raise ModuleNotFound("unable to load module '%s'" % modname)
            # get a pointer to the actual class
            cls = eval("%s.%s" % (modname, classname))
            # check what type of class we have an create a blank instance,
            # we really should be implementing this like pickle and save the
            # init args returned from getinitargs, but it isn't needed right
            # now so why waste our time implementing it?
            if issubclass(cls, object):
                inst = cls.__new__(cls)
            else:
                inst = _EmptyClass()
                inst.__class__ = cls

#            inst = eval("%s.%s()" % (modname, classname))
            # give the current node to the instance
            inst.readXMLNode(node, root=1)
            return inst
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
                    raise XMLObjectError('expected a TEXT_NODE, got ' + \
                          str(child))
            
            return self.__castString(result, etype)

    # simple re to convert the string representation of a type to something
    # more useful
    __typere = re.compile("\<type '(\w+?)(Type)?'\>")
    # new style class instances have to be caught with a special regexp
    __instre = re.compile("\<class '([^']+)'\>")
    def __getValueType(self, val):
        """Return the type of the value that will be used as an attribute
        in an element node, if the value is not a list, int, or float
        then it is assumed to be a string."""

        vtype = type(val)
        # we support all the major python types
        if vtype in (list, tuple, dict,
                     int, int, float,
                     bytes, type(None)):
            return self.__typere.match(str(vtype)).group(1).lower()
        elif isinstance(val, Container):
            return "dict"
        # if we have an instance, then make sure it is a subclass of us
        elif vtype is types.InstanceType or self.__instre.match(str(vtype)):
            return 'instance'
        # special check for the boolean type which is in 2.3 and beyond
        elif hasattr(types, "BooleanType") and vtype is bool:
            return self.__typere.match(str(vtype)).group(1).lower()
        else:
            raise XMLObjectError("unsupported type, %s" % str(vtype))

    def __addIndentNode(self, doc, node, level, indent):
        """Add a text node that will indent based on the current level and
        indent spacing."""
        if indent:
            text = doc.createTextNode('\n' + level*indent)
            node.appendChild(text)

    __specialcharre = re.compile('[^\w_\-]')
    def __writeXMLNode(self, doc, node, value, level=0, indent='  ', root=0,
                       readable=False):
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
                # if human readable is true, and the value is a string with
                # no spaces, then do not add the item tag
                if readable and type(v) is bytes and \
                   not self.__specialcharre.search(v):
                    # create a text node
                    text = doc.createTextNode(v)
                    # if indenting is on, then add the text node to do it
                    self.__addIndentNode(doc, node, level, indent)
                    # add the item to the node
                    node.appendChild(text)
                else:
                    item = doc.createElement('item')
                    # add everything to the item
                    self.__writeXMLNode(doc, item, v, level=level+1,
                                        indent=indent, readable=readable)
                    # if indenting is on, then add the text node to do it
                    self.__addIndentNode(doc, node, level, indent)
                    # add the item to the node
                    node.appendChild(item)
            # if indenting is on, then add the text node to do it
            if value: self.__addIndentNode(doc, node, level-1, indent)
        elif vtype == "dict":
            # sort the keys of the dictionary so it is easier to read
            keys = list(value.keys())
            keys.sort()
            # for each pair add an element for the key and value
            for k in keys:
                v = value[k]
                # if humanReadable is true, then use the key if as the
                # element name if the key is a string
                if readable and type(k) is bytes and \
                   not self.__specialcharre.search(k):
                    key = doc.createElement(k)
                    # add all the value data
                    self.__writeXMLNode(doc, key, v, level=level+1,
                                        indent=indent, readable=readable)
                    # if indenting is on, then add the text node to do it
                    self.__addIndentNode(doc, node, level, indent)
                    # add this to the node
                    node.appendChild(key)
                else:
                    key = doc.createElement('key')
                    # add everything required for the key
                    self.__writeXMLNode(doc, key, k, level=level+1,
                                        indent=indent, readable=readable)
                    # if indenting is on, then add the text node to do it
                    self.__addIndentNode(doc, node, level, indent)
                    # add this to the node
                    node.appendChild(key)

                    # now add the value
                    val = doc.createElement('val')
                    # add everything required for the value
                    self.__writeXMLNode(doc, val, v, level=level+1,
                                        indent=indent, readable=readable)
                    # if indenting is on, then add the text node to do it
                    self.__addIndentNode(doc, node, level, indent)
                    # add this to the node now
                    node.appendChild(val)
            # if indenting is on, then add the text node to do it
            if keys: self.__addIndentNode(doc, node, level-1, indent)
        # handle instances
        elif vtype == 'instance':
            # add the class type as an attribute
            node.setAttribute('class', value.__class__.__name__)
            # add the module name so it can be properly loaded
            # when reading the data back in
            node.setAttribute('module', value.__class__.__module__)
            # now call the writeXMLNode() method of the instance
            value.writeXMLNode(doc, node, level=level, indent=indent, root=1,
                               readable=readable)
        # otherwise assume each value can be cast to a string
        elif vtype != 'none':
            # make a text element for the data
            text = doc.createTextNode(str(value))
            # now add the text to the node
            node.appendChild(text)

    def varsToSave(self):
        return []

    def __getstate__(self):
        """This is used by the pickle modules when saving the state of an
        object.   This makes it so the same variables written in XML can be
        saved when an object is pickled."""
        dict = {}
        for var in self.varsToSave():
            dict[var] = self.__dict__[var]
        return dict

    def __setstate__(self, dict):
        """Set the values that have been parsed from the XML string."""
        for key,val in list(dict.items()):
            setattr(self, key, val)

    def readXMLNode(self, node, root=0):
        prefs = self.__readXMLNode(node, root=root)
        self.__setstate__(prefs)

    def writeXMLNode(self, doc, node, data=None, level=0,
                     indent='  ', root=0, readable=False):
        """This method should be overloaded by subclasses if they have
        special variables to save."""
        
        if not data:
            data = self.__getstate__()

        # populate the dom with our data
        self.__writeXMLNode(doc, node, data, level=level, indent=indent,
                            root=root, readable=readable)

    def readXMLString(self, xmlstr):
        """Unpack a string."""
        
        # read the file into memory
        try:
            dom = parseString(xmlstr)
        except (xml.dom.DOMException, xml.sax.SAXException,
                xml.parsers.expat.ExpatError) as err:
            raise XMLObjectError(str(err))

        # read the xml document and load the values into our dictionary
        self.readXMLNode(dom.documentElement, root=1)

    def writeXMLString(self, data=None, indent='  '):
        """Return an XML representation of the object and the member variables
        defined in the provided list."""

        # create dom object to represent an XML file
        impl = getDOMImplementation()
        doc  = impl.createDocument(None, self.__class__.__name__, None)
        # add a version to the document
        doc.documentElement.setAttribute('version', self.__VERSION)
        # populate the dom with our data
        self.writeXMLNode(doc, doc.documentElement, data=data,
                          level=1, indent=indent, root=1)

        myxml = doc.toxml()
        # clean up the dom
        doc.unlink()
        return myxml

    def readXML(self, filename=None, basedir=None, basename=None):
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
            filename = self.__getFileName(basedir=basedir, basename=basename)

        try:
            file = open(filename)
            xmlstr = file.read()
            file.close()
        except (IOError, OSError) as err:
            raise XMLObjectError("unable to read %s: %s" % \
                  (filename, str(err)))

        self.readXMLString(xmlstr)

    def writeXML(self, data=None, filename=None, createDir=False,
                 basedir=None, basename=None, indent='  '):
        """Write the state of an object to a file in XML.  The root node of the
        document will be the name of this class."""

        # determine the file to use
        if not filename:
            filename = self.__getFileName(basedir=basedir, basename=basename)

        if createDir:
            dir = os.path.dirname(filename)
            try:
                os.makedirs(dir)
            except OSError as errObj:
                if errObj.errno != 17:
                    raise

        # write to a tmp file, then replace the destination
        tmp = filename + ".tmp"
        file = open(tmp, 'w')
        # add one more newline so it looks pretty
        file.write(self.writeXMLString(data=data, indent=indent) + '\n')
        file.close()
        os.rename(tmp, filename)

class YAMLObjectError(PersistenceError):
    pass

class YAMLObject(object):
    """Designed to be a mix-in class to easily load configuration files
    into a class or application.  The configuration file should be
    written using the yaml syntax."""

    CONTAINERS = False

    def __getFileName(self, basedir=None, basename=None):
        """Create a filename that will be used to read/write the data
        to and from disk."""

        # the directory where the preferences file will be saved
        if not basedir:
            basedir = os.getcwd()

        # the basename will be the name of the class if one is not provided
        if not basename:
            basename = self.__class__.__name__

        return os.path.join(basedir, basename)


    def __makeContainer(self, value):
        """Recursively convert dictionaries into Container objects."""

        # check for dictionaries within a list
        if isinstance(value, list):
            for i in range(len(value)):
                if isinstance(value[i], dict):
                    value[i] = self.__makeContainer(value[i])

        # if it's a dictionary, then convert it
        elif isinstance(value, dict):
            cont = Container()
            cont.__dict__ = value

            # check each item for dictionaries
            for key,val in list(value.items()):
                # call ourself if we find a dictionary, also check for
                # dictionaries within a list
                if isinstance(val, dict) or isinstance(val, list):
                    value[key] = self.__makeContainer(val)

            value = cont

        return value
            

    def readYAMLString(self, yamlstr):
        """Unpack a string."""

        # load the file to get a dictionary.
        try:
            values = yaml.load(yamlstr)
        except yaml.YAMLError as err:
            raise YAMLObjectError("unable to parse yamls: %s" % str(err))

        # turn the values into a container object
        if self.CONTAINERS:
            values = self.__makeContainer(values)

        # set the root values as members self
        for key,val in list(values.items()):
            setattr(self, key, val)


    def readYAML(self, filename=None, basedir=None, basename=None):
        """Read a YAML file 'filename'."""

        # choose the filename to use
        if not filename:
            filename = self.__getFileName(basedir=basedir, basename=basename)

        try:
            file = open(filename)
            yamlstr = file.read()
            file.close()
        except (IOError, OSError) as err:
            raise YAMLObjectError("unable to read %s: %s" % \
                  (filename, str(err)))

        self.readYAMLString(yamlstr)


class Preferences(XMLObject):

    CONTAINERS = True

    def __init__(self, filename):
        # save the filename and read it
        self.filename = filename
        self.reload()

    def reload(self):
        """Reload the preferences file using the path that the object
        was initialized with."""
        self.readXML(filename=self.filename)

    def save(self):
        self.writeXML(filename=self.filename, createDir=True)


class ConfigFileError(PersistenceError):
    pass

class ConfigFile(XMLObject, YAMLObject):

    CONTAINERS = True

    def __init__(self, filename):
        # save the filename and read it
        self.filename = filename
        self.reload()

    def reload(self):
        """Reload the preferences file using the path that the object
        was initialized with."""
        # check the header of the file to see if the format is yaml or xml
        try:
            file = open(self.filename)
            prefstr = file.read()
            file.close()
        except (IOError, OSError) as err:
            raise ConfigFileError("unable to read %s: %s" % \
                  (self.filename, str(err)))

        # xml has a header we can look for, otherwise assume it is yaml
        if prefstr[:13] == "<?xml version":
            self.readXMLString(prefstr)
        else:
            self.readYAMLString(prefstr)

    def save(self):
        raise PersistenceError("KQ: should a config file be auto generated?")


class ObjectState(XMLObject):
    """A wrapper around the XMLObject class so common objects can save data
    to a common place."""

    rootStateDir = "/usr/tmp"
    
    def readState(self, filename=None, basedir=None, basename=None,
                  suppress=True):
        """Overloaded so defaults can be set if the caller doesn't provide
        values for where the data should be read from."""
        
        if basedir and basedir[0] != '/':
            basedir = os.path.join(self.rootStateDir, basedir)
        elif not basedir:
            basedir = self.rootStateDir
        if not basename:
            basename = "%s.state" % self.__class__.__name__

        try:
            self.readXML(filename=filename, basedir=basedir, basename=basename)
        except XMLObjectError:
            if not suppress:
                raise

    def saveState(self, filename=None, basedir=None, basename=None, **kwargs):
        """Overloaded so defaults can be set if the caller doesn't provide
        values for where the data should be written to."""
        
        if basedir and basedir[0] != '/':
            basedir = os.path.join(self.rootStateDir, basedir)
        elif not basedir:
            basedir = self.rootStateDir
        if not basename:
            basename = "%s.state" % self.__class__.__name__

        try:
            self.writeXML(filename=filename,
                          basedir=basedir, basename=basename,
                          createDir=True, **kwargs)
        except (IOError, OSError):
            tracebackutil.printTraceback()
            pass

class ClassCacheError(Exception):
    pass

class ClassCache(XMLObject):
    """Gives classes the ability to have data viewable by all its instances.
    Basically this implements a global variable that is scoped within each
    class, thus it is only viewable by instances of the class.  Entries in
    the cache are time stamped and stale values are automatically cleared.

    @cvar cacheLife: the default life of each cache entry is forever
      (setting it to 0 or None).  If set, an entry older than this many
      seconds will be removed automatically.
    """

    cacheLife  = 120
    
    # The cache is a dictionary that is accessed with the addToCache(),
    # delFromCache(), and getFromCache() methods.  Each item added to the
    # cache is tagged with a timestamp.  By default all entries will expire
    # and thus be removed after 'cacheLife' seconds.  This can be overridden
    # if desired, but then it is up to the class to clean up.
    _cache     = {}

    # used to determine if the cache for a class has been read
    _cacheRead = False

    def addToCache(self, key, val, tstamp=True, save=True):
        """Add an entry to the cache that can later be accessed with the
        provided key.  By default the entry will be tagged with a time
        stamp, but setting the tstamp field to False will prevent the
        entry from being deleted from the cache."""

        if tstamp:
            self.__class__._cache[key] = (int(time.time()), val)
        else:
            self.__class__._cache[key] = (None, val)

        # save the latest cache state
        if save:
            self.saveCache()

    def delFromCache(self, key, save=True):
        """Delete a value from the cache.  If the value does not exist
        no exception is raised.  By default the new state of the cache
        is written to disk, but setting the save flag to False will
        prevent this."""
        try:
            del self.__class__._cache[key]
        except KeyError:
            pass

        # save the latest cache state
        if save:
            self.saveCache()

    def getFromCache(self, key, forceRead=False):
        """Retrieve a value from the cache.  If the value is not found then
        a KeyError is raised."""
        # make sure the cache is read
        self.readCache(force=forceRead)
        # the actual value is the second item in the tuple, the first item
        # in the tuple is the entry's time stamp
        return self.__class__._cache[key][1]

    def getCacheFilename(self):
        """Subclasses should overload this method and return a valid
        filename where the cache can be read/written to and from."""
        return None

    def readCache(self, force=False):
        """Read the cache for this class.  By default it will only be read
        once since the data is global for all its instances.  This can be
        overridden by setting the force flag to True."""

        # we only want to read the cached data once, so check if the _cacheRead
        # flag is set for the provided instance's class
        read = self.__class__.__dict__.get('_cacheRead', False)
        if read and not force:
            #print 'bypassing cache read from ', filename
            return

        #print 'reading cache from', filename
        filename = self.getCacheFilename()
        if not filename:
            raise ClassObjectError("no cache filename provided.")
        try:
            XMLObject.readXML(self, filename=self.getCacheFilename())
        except XMLObjectError:
            pass

        # the cache has now been read
        self.__class__._cacheRead = True
        
        # set the value in the class.  We only want to pull the value from
        # this instance, so we don't use getattr because this could potentially
        # grab a cache from a base class.  If it isn't in this instance then
        # set it to the default.
        try:
            val = self.__dict__['_cache']
        except KeyError:
            #print 'instance has no cache'
            setattr(self.__class__, '_cache', {})
        else:
            #print 'instance has cache, removing'
            setattr(self.__class__, '_cache', val)
            # remove the value from the instance, because we only want it in
            # the class
            delattr(self, '_cache')

        #print self.__dict__
        #print self.__class__.__dict__

        # clear any stale values from the cache if the cacheLife is set
        if self.__class__.cacheLife:
            now = time.time()
            for key,(tstamp,val) in list(self.__class__._cache.items()):
                # if the tstamp is None, then this value can't be removed
                if tstamp is None:
                    continue

                if now - tstamp > self.__class__.cacheLife:
                    del self.__class__._cache[key]

    def saveCache(self):
        """Save the current state of the class's cache to the provided file."""
        # we explicity send a pointer to the class variable _cache just in
        # case the instance has a _cache variable defined in it's dictionary.
        filename = self.getCacheFilename()
        if not filename:
            raise ClassObjectError("no cache filename provided.")
        try:
            XMLObject.writeXML(self, data={'_cache': self.__class__._cache},
                           filename=filename, createDir=1)
        except (IOError, OSError):
            tracebackutil.printTraceback()
            pass

        
def testxml():
    class MyClass(XMLObject):
        pass

    data = {'foo': 'bar',
            'abc': 'def',
            #'307': ['1', '2', '3', '4'],
            'qef': ['1', '2', 3, '4', 5],
            'bbb': ['1', '2', 3, '4'],
            'ccc': [1, '2', '3', '4'],
            'ddd': {'a': 1, 'b': 2, 123: '456'}}

    mc = MyClass()
    mc.readXML(filename='/shows/rpg/global/data/human/human.prefs')
    print(mc.__dict__)
    mc.writeXML(data=mc.__dict__, filename='test.xml')

            
def testyaml():
    class MyClass(YAMLObject):
        CONTAINERS = True
        pass

    mc = MyClass()
    mc.readYAML(filename='/shows/rpg/global/data/tina/test_pringles.cfg')
    print(mc.__dict__)
    print(mc.Host2Operators.pringles)

def testconfig():
    class MyConfig(ConfigFile):
        CONTAINERS=False
        pass

    import sys
    mc = MyConfig(sys.argv[1])
    mc.reload()
    print(mc.__dict__)
            
if __name__ == "__main__":
    testxml()
    testyaml()
    testconfig()
