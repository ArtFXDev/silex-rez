import re
import datetime
import json
import tractor.base.EngineClient as EngineClient

from . import AuthorError, RequiredValueError, ParentExistsError, SpoolError

TclIndentLevel = 0
SPACES_PER_INDENT = 2


def tclIndentStr():
    """Return a quantitye of spaces for the given indentation level."""
    global TclIndentLevel
    return " " * TclIndentLevel * SPACES_PER_INDENT


def str2argv(s):
    """Convert a string to a list of strings."""
    return s.split()


# an EngineClient is required for spooling
ModuleEngineClient = EngineClient.TheEngineClient


def _setEngineClient(anEngineClient):
    """Set the global engine client object."""
    global ModuleEngineClient
    ModuleEngineClient = anEngineClient


def setEngineClientParam(**kw):
    """Permit setting of engine connection parameters: hostname, port,
    user, and password.
    """
    ModuleEngineClient.setParam(**kw)


def closeEngineClient():
    """Close connection to engine, ensuring engine no longer needs to
    maintain session.
    """
    ModuleEngineClient.close()


class Attribute(object):
    """The Attribute class presents a way to define the nature of
    attributes of job Elements, such as whether or not they are
    required and how valid values are determined.
    """

    def __init__(self, name, alias=None, required=False, suppressTclKey=False):
        self.name = name
        self.alias = alias
        self.required = required
        self.value = None
        self.suppressTclKey = suppressTclKey

    def hasValue(self):
        """Return True if the attribute has been set; otherwise, False."""
        return self.value is not None

    def setValue(self, value):
        """Set the value of the attribute."""
        if not self.isValid(value):
            raise TypeError("%s is not a valid value for %s" % (str(value), self.name))
        self.value = value

    def isValid(self, value):
        """Return True if value is a valid value for Attribute."""
        raise NotImplementedError("Attribute.isValid() not implemented")

    def raiseIfRequired(self):
        """Raise an exception if value is required and no value is present."""
        if self.required and not self.hasValue():
            raise RequiredValueError("A value is required for %s" % self.name)

    def tclKey(self):
        """Return the name as -name if it is not to be suppressed."""
        if not self.suppressTclKey:
            return " -%s" % self.name
        else:
            return ""

    def asTcl(self):
        """Return the Tcl representation of the attribute name and value."""
        self.raiseIfRequired()
        if not self.hasValue():
            return ""
        return "%s {%s}" % (self.tclKey(), self.value)


class Constant(Attribute):
    """A Constant is a constant value associated with an attribute name."""

    def __init__(self, value):
        super(Constant, self).__init__("constant", suppressTclKey=True)
        self.value = value

    def asTcl(self):
        """Return the Tcl representation of the constant value."""
        self.raiseIfRequired()
        if not self.hasValue():
            return ""
        return self.value


class FloatAttribute(Attribute):
    """A FloatAttribute is a float value associated with an attribute name."""

    def __init__(self, name, precision=1, **kw):
        super(FloatAttribute, self).__init__(name, **kw)
        self.precision = precision

    def isValid(self, value):
        """Return True if value is a float or int."""
        return isinstance(value, (float, int))

    def asTcl(self):
        """Return the Tcl representation of the attribute name and value."""
        self.raiseIfRequired()
        if not self.hasValue():
            return ""
        format = "%%s {%%0.%df}" % self.precision
        return format % (self.tclKey(), self.value)


class IntAttribute(Attribute):
    """An IntAttribute is an integer value associated with an
    attribute name.
    """

    def isValid(self, value):
        """Return True if value is a valid value for an FloatAttribute."""
        return isinstance(value, int)

    def asTcl(self):
        """Return the Tcl representation of the attribute name and value."""
        self.raiseIfRequired()
        if not self.hasValue():
            return ""
        return "%s %d" % (self.tclKey(), self.value)


class DateAttribute(Attribute):
    """A DateAttribute is a datetime value associated with an
    attribute name.
    """

    def setValue(self, value):
        """Set the value only if one of datetime type is specified."""
        if not isinstance(value, datetime.datetime):
            raise TypeError(
                "%s is a %s, not a datetime type for %s"
                % (str(value), type(value), self.name)
            )
        self.value = value

    def isValid(self, value):
        """Return True if the value is a datetime value."""
        return isinstance(value, datetime.datetime)

    def asTcl(self):
        """Return the Tcl representation of the attribute name and value."""
        self.raiseIfRequired()
        if not self.hasValue():
            return ""
        return "%s {%s}" % (self.tclKey(), self.value.strftime("%m %d %H:%M"))


class StringAttribute(Attribute):
    """A StringAttribute is a string value associated with an
    attribute name.
    """

    def isValid(self, value):
        """Return True if the value is a string."""
        return isinstance(value, str)


class WhenStringAttribute(StringAttribute):
    """A WhenStringAttribute is a string value associated with an
    postscript command attribute name.  It can be one of
    "done", "error", or "always".
    """

    def isValid(self, value):
        """Return True if the value is doen, error, or always."""
        return value in ("done", "error", "always")


class StringListAttribute(Attribute):
    """A StringListAttribute is a list of string values associated with an
    attribute name.
    """

    def isValid(self, value):
        """Return True if the value is a list of strings."""
        if not isinstance(value, list):
            return False
        for item in value:
            if not isinstance(item, str):
                return False
        return True

    def hasValue(self):
        """Return True if there is at least one element in the list."""
        return self.value and len(self.value) > 0

    def asTcl(self):
        """Return the Tcl representation of the attribute name and value."""
        self.raiseIfRequired()
        if not self.hasValue():
            return ""
        args = []
        for value in self.value:
            val = value.replace("\\", "\\\\")
            args.append("{%s}" % val)
        return "%s {%s}" % (self.tclKey(), " ".join(args))


class IntListAttribute(Attribute):
    """An IntListAttribute is a list of integer values associated with an
    attribute name.
    """

    def isValid(self, value):
        """Return True if the value is a list of integers."""
        if not isinstance(value, list):
            return False
        for item in value:
            if not isinstance(item, int):
                return False
        return True

    def hasValue(self):
        """Return True if there is at least one element in the list."""
        return self.value and len(self.value) > 0

    def asTcl(self):
        """Return the Tcl representation of the attribute name and value."""
        self.raiseIfRequired()
        if not self.hasValue():
            return ""
        args = []
        for value in self.value:
            args.append(str(value))
        return "%s {%s}" % (self.tclKey(), " ".join(args))


class ArgvAttribute(StringListAttribute):
    """An ArgvAttribute is a list of string values associated with an
    attribute name.
    """

    def setValue(self, value):
        """Set the value, converting a string value to a list of strings."""
        if isinstance(value, str):
            self.value = str2argv(value)
        else:
            self.value = value[:]


class BooleanAttribute(Attribute):
    """A BooleanAttribute is a boolean value associated with an
    attribute name.
    """

    def isValid(self, value):
        """Return True if the value is 0 or 1."""
        # values of True and False will pass as well
        return value in [0, 1]

    def asTcl(self):
        """Return the Tcl representation of the attribute name and value."""
        self.raiseIfRequired()
        if not self.hasValue():
            return ""
        return "%s %d" % (self.tclKey(), int(self.value))


class GroupAttribute(Attribute):
    """A GroupAttribute is an attribute that contains multiple elements as a value
    (e.g. -init, -subtasks, -cmds), associated with an attribute name.
    """

    def __init__(self, *args, **kw):
        super(GroupAttribute, self).__init__(*args, **kw)
        self.value = []

    def addElement(self, element):
        """Add the given element to the list of elements in this group."""
        self.value.append(element)

    def hasValue(self):
        """Return True if there is at least one element in the group."""
        return len(self.value) > 0

    def __getitem__(self, index):
        """Return the index'th element of the group."""
        return self.value[index]

    def asTcl(self):
        """Return the Tcl representation of the attribute name and value."""
        self.raiseIfRequired()
        if not self.hasValue():
            return ""
        global TclIndentLevel
        TclIndentLevel += 1
        lines = [tclIndentStr() + element.asTcl() for element in self.value]
        TclIndentLevel -= 1
        return " -%s {\n%s\n%s}" % (self.name, "\n".join(lines), tclIndentStr())


class Element(object):
    """An Element is a base class to represent components of a job that define
    the structure and content of a job.
    """

    # maintain a list of members permitted to be set through __setattr__
    MEMBERS = ["parent"]

    def __init__(self):
        # keep track of parent to support instancing and to detect errors
        self.parent = None

    def __getattr__(self, attr):
        """Enable python attributes of the object to be accessed by name."""
        return self.__dict__.get(attr)

    def __setattr__(self, attr, value):
        """Restrict setting of attributes of Element to only those specified
        in the class MEMBERS list.
        """
        if attr in self.MEMBERS:
            super(Element, self).__setattr__(attr, value)
        else:
            raise AttributeError(
                "%s is not a valid attribute of a %s" % (attr, self.__class__.__name__)
            )


class KeyValueElement(Element):
    """A KeyValueElement is an element that can have multiple attributes
    with associated values.  For example, a Job can have priority and
    title attributes.
    """

    MEMBERS = Element.MEMBERS + ["attributes", "attributeByName"]

    def __init__(self, attributes, **kw):
        # lookup of attribute by name required for __getattr__ and __setattr__
        self.attributes = attributes
        self.attributeByName = {}
        for attr in attributes:
            self.attributeByName[attr.name] = attr
            if attr.alias:
                self.attributeByName[attr.alias] = attr
        # initialize attributes passes as keyword parameters
        for key, value in kw.items():
            setattr(self, key, value)

    def __getattr__(self, attr):
        """Enable Attributes, which are specified in the self.attributes member,
        to be accessed as though they were members of the Element.
        """
        if "attributeByName" in self.__dict__ and attr in self.attributeByName:
            attribute = self.attributeByName[attr]
            return attribute.value
        else:
            return super(KeyValueElement, self).__getattr__(attr)

    def __setattr__(self, attr, value):
        """Enable Attributes, which are specified in the self.attributes member,
        to be set as though they were members of the Element.  Attributes
        are restricted to those listed in self.attributes to avoid spelling
        mistakes from silently failing.  e.g. job.titlee = "A Title" will fail.
        """
        if "attributeByName" in self.__dict__ and attr in self.attributeByName:
            attribute = self.attributeByName[attr]
            attribute.setValue(value)
        else:
            super(KeyValueElement, self).__setattr__(attr, value)

    def asTcl(self):
        """Return the Tcl representation of the Element's attribute
        names and values.
        """
        parts = []
        for attribute in self.attributes:
            parts.append(attribute.asTcl())
        return "".join(parts)


class Assign(Element):
    """An Assign element defines a variable in the global context of a
    job. (e.g. Assign mypath {/some/path})
    """

    MEMBERS = Element.MEMBERS + ["varname", "value"]

    def __init__(self, varname, value):
        if not isinstance(varname, str):
            raise TypeError(
                "Assign.__init__(): varname %s is not a " "string" % str(varname)
            )
        if not isinstance(value, str):
            raise TypeError(
                "Assign.__init__(): value %s is not a " "string" % str(value)
            )
        self.varname = varname
        self.value = value

    def asTcl(self):
        """Return the Tcl representation of the Assign variable name
        and value.
        """
        return "Assign %s {%s}" % (self.varname, self.value)


class DirMap(Element):
    """A DirMap element defines a mapping between paths of two
    different OSes.
    """

    MEMBERS = Element.MEMBERS + ["src", "dst", "zone"]

    def __init__(self, src, dst, zone):
        if not isinstance(src, str):
            raise TypeError(
                "DirMap.__init__(): src %s is not a " "string" % str(varname)
            )
        if not isinstance(dst, str):
            raise TypeError("DirMap.__init__(): dst %s is not a " "string" % str(value))
        if not isinstance(zone, str):
            raise TypeError(
                "DirMap.__init__(): zone %s is not a " "string" % str(value)
            )
        self.src = src
        self.dst = dst
        self.zone = zone

    def asTcl(self):
        """Return the Tcl representation of the dirmap expression."""
        return "{{%s} {%s} %s}" % (self.src, self.dst, self.zone)


class SubtaskMixin(object):
    """SubtaskMixin is a mix-in class for elements that can have child
    tasks, namely the Job, Task, and Iterate elements.
    """

    def addChild(self, element):
        """Add the given element as a child of this element."""
        if not isinstance(element, (Task, Instance, Iterate)):
            raise TypeError(
                "%s is not an instance of Task, Instance, or Iterate"
                % element.__class__.__name__
            )
        if isinstance(element, Task) and element.parent:
            # this task already has a parent, so replace with an Instance
            instance = Instance(title=element.title)
            self.attributeByName["subtasks"].addElement(instance)
        elif element.parent:
            raise ParentExistsError(
                "%s is already a child of %s" % (str(element), str(element.parent))
            )
        else:
            self.attributeByName["subtasks"].addElement(element)
        element.parent = self

    def newTask(self, **kw):
        """Instantiate a new Task element, add to subtask list, and return
        element.
        """
        task = Task(**kw)
        self.addChild(task)
        return task

    def spool(
        self,
        block=False,
        owner=None,
        spoolfile=None,
        spoolhost=None,
        hostname=None,
        port=None,
    ):
        """Send script representing the job or task subtree, returning
        the job id of the new job.  Setting block to True will wait
        for the engine to submit the job before returning; in such a
        case, it's possible for an exception to be raised if the
        engine detects a syntax or logic error in the job.  A
        SpoolError exception is raised in the event of a communication
        error with the engine, or in the event the engine has a
        problem processing the job file (when blocked=True).
        The job's spoolfile and spoolhost attributes can be set
        with the coresponding keyword parameters; typically these
        are to show from which host a job has been spooled and
        the full path to the spooled job file.
        The engine can be targeted with the hostname port
        keyword parameters.
        """
        # force the module engine client to set up a new connection.
        # EngineClient.close() doesn't work here because the spooler
        # is using EngineClient.spool(skipLogin=True), which causes
        # the EngineClient to reuse a cached TrHttpRPC connection.
        if hostname or port:
            ModuleEngineClient.conn = None
            # prep engine client
            if hostname:
                ModuleEngineClient.setParam(hostname=hostname)
            if port:
                ModuleEngineClient.setParam(port=port)
        # send spool message
        try:
            result = ModuleEngineClient.spool(
                self.asTcl(),
                skipLogin=True,
                block=block,
                owner=owner,
                filename=spoolfile,
                hostname=spoolhost,
            )
        except EngineClient.EngineClientError as err:
            raise SpoolError("Spool error: %s" % str(err))
        resultDict = json.loads(result)
        return resultDict.get("jid")


class CleanupMixin(object):
    """CleanupMixin is a mix-in class for elements that can have a cleanup
    attribute, namely the Job and Task elements.
    """

    def newCleanup(self, **kw):
        """Instantiate a new Command element, adds to cleanup command
        list, and returns element.
        """
        command = Command(**kw)
        self.addCleanup(command)
        return command

    def addCleanup(self, command):
        """Add an existing cleanup command to element."""
        if not isinstance(command, Command):
            raise TypeError("%s is not an instance of Command" % str(command))
        self.attributeByName["cleanup"].addElement(command)


class PostscriptMixin(object):
    """CleanupMixin is a mix-in class for elements that can have a cleanup
    attribute.  Currently this is only the Job element.
    """

    def newPostscript(self, **kw):
        """Instantiate a new Command element, add to postscript command list,
        and return element.
        """
        command = Command(**kw)
        self.addPostscript(command)
        return command

    def addPostscript(self, command):
        """Add an existing postscript command to element."""
        if not isinstance(command, Command):
            raise TypeError("%s is not an instance of Command" % str(command))
        self.attributeByName["postscript"].addElement(command)


class Job(KeyValueElement, SubtaskMixin, CleanupMixin, PostscriptMixin):
    """A Job element defines the attributes of a job and contains other
    elements definining the job, such as Assign statements and Tasks."""

    def __init__(self, **kw):
        attributes = [
            Constant("Job"),
            StringAttribute("title", required=True),
            StringAttribute("tier"),
            StringAttribute("spoolcwd"),
            StringListAttribute("projects"),
            StringListAttribute("crews"),
            IntAttribute("maxactive"),
            BooleanAttribute("paused"),
            DateAttribute("after"),
            IntListAttribute("afterjids"),
            GroupAttribute("init"),
            IntAttribute("atleast"),
            IntAttribute("atmost"),
            IntAttribute("etalevel"),
            StringListAttribute("tags"),
            FloatAttribute("priority"),
            StringAttribute("service"),
            StringListAttribute("envkey"),
            StringAttribute("comment"),
            StringAttribute("metadata"),
            StringAttribute("editpolicy"),
            GroupAttribute("cleanup"),
            GroupAttribute("postscript"),
            GroupAttribute("dirmaps"),
            BooleanAttribute("serialsubtasks"),
            GroupAttribute("subtasks", required=True),
        ]
        super(Job, self).__init__(attributes, **kw)

    def newAssignment(self, attr, value):
        """Instantiate a new Assign element, add to job's assign list, and
        returns element.
        """
        assign = Assign(attr, value)
        self.attributeByName["init"].addElement(assign)
        return assign

    def newDirMap(self, src, dst, zone):
        """Instantiates a new DirMap element, add to job's dirmap list, and
        returns element.
        """
        dirmap = DirMap(src, dst, zone)
        self.attributeByName["dirmaps"].addElement(dirmap)
        return dirmap

    def __str__(self):
        return "Job %s" % (self.title or "<no title>")


class Task(KeyValueElement, SubtaskMixin, CleanupMixin):
    """A Task element defines the attributes of a task and contains
    other elements definining the task such as commands and subtasks."""

    def __init__(self, argv=None, **kw):
        cmdkw = {}
        if argv and kw.get("service"):
            # move service from Task to Command if also creating a Command
            kw = kw.copy()
            cmdkw["service"] = kw.pop("service")

        attributes = [
            Constant("Task"),
            StringAttribute("title", required=True, suppressTclKey=True),
            StringAttribute("id"),
            StringAttribute("service"),
            IntAttribute("atleast"),
            IntAttribute("atmost"),
            GroupAttribute("cmds"),
            ArgvAttribute("chaser"),
            ArgvAttribute("preview"),
            BooleanAttribute("serialsubtasks"),
            BooleanAttribute("resumeblock"),
            GroupAttribute("cleanup"),
            StringAttribute("metadata"),
            GroupAttribute("subtasks"),
        ]
        super(Task, self).__init__(attributes, **kw)

        # provide shortcut for defining a command inline
        if argv:
            command = Command(argv=argv, **cmdkw)
            self.addCommand(command)

    def addCommand(self, command):
        """Add the specified Command to command list of the Task."""
        if not isinstance(command, Command):
            raise TypeError("%s is not an instance of Command" % str(command))
        self.attributeByName["cmds"].addElement(command)

    def newCommand(self, **kw):
        """Instantiate a new Command element, add to command list, and return
        element.
        """
        command = Command(**kw)
        self.addCommand(command)
        return command

    def __str__(self):
        return "Task %s" % (self.title or "<no title>")


class Instance(KeyValueElement):
    """An Instance is an element whose state is tied to that of another
    task.
    """

    def __init__(self, **kw):
        attributes = [
            Constant("Instance"),
            StringAttribute("title", required=True, suppressTclKey=True),
        ]
        super(Instance, self).__init__(attributes, **kw)

    def __str__(self):
        return "Instance %s" % (self.title or "<no title>")


class Iterate(KeyValueElement, SubtaskMixin):
    """An Iterate element defines a corresponding iteration loop."""

    def __init__(self, **kw):
        attributes = [
            Constant("Iterate"),
            StringAttribute("varname", required=True, suppressTclKey=True),
            IntAttribute("from", alias="frm", required=True),
            IntAttribute("to", required=True),
            IntAttribute("by"),
            GroupAttribute("template", required=True),
            GroupAttribute("subtasks"),
        ]
        super(Iterate, self).__init__(attributes, **kw)

    def addToTemplate(self, task):
        """Add the specified task to the Iterate template."""
        if not isinstance(task, (Task, Instance, Iterate)):
            raise TypeError(
                "%s is not an instance of Task, Instance, or Iterate" % type(task)
            )
        self.attributeByName["template"].addElement(task)

    def __str__(self):
        return "Iterate %s" % (self.varname or "<no iterator>")


class Command(KeyValueElement):
    """A Command element defines the attributes of a command."""

    def __init__(self, local=False, **kw):
        if local:
            cmdtype = "Command"
        else:
            cmdtype = "RemoteCmd"
        attributes = [
            Constant(cmdtype),
            ArgvAttribute("argv", required=True, suppressTclKey=True),
            StringAttribute("msg"),
            StringListAttribute("tags"),
            StringAttribute("service"),
            StringAttribute("metrics"),
            StringAttribute("id"),
            StringAttribute("refersto"),
            BooleanAttribute("expand"),
            IntAttribute("atleast"),
            IntAttribute("atmost"),
            IntAttribute("minrunsecs"),
            IntAttribute("maxrunsecs"),
            BooleanAttribute("samehost"),
            StringListAttribute("envkey"),
            IntListAttribute("retryrc"),
            WhenStringAttribute("when"),
            StringListAttribute("resumewhile"),
            BooleanAttribute("resumepin"),
            StringAttribute("metadata"),
        ]
        super(Command, self).__init__(attributes, **kw)
