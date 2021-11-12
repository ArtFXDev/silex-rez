"""Definition of all the tq commands that provide extra help."""

import sys, textwrap, re

import rpg.Formats as Formats
import rpg.stringutil as stringutil
import rpg.listutil as listutil
import rpg.CmdLineTool as CmdLineTool
import rpg.sql.DBCmdLineTool as DBCmdLineTool

import tractor.base.EngineDB as EngineDB
from . import ListCmds
from .. import tq

class TqHelp(CmdLineTool.CmdLineTool):
    """The base class for all help topic commands."""

    # by default all help commands have not dashed options
    options = []

    # subclasses should set this to the message that should be displayed
    # when the command is invoked.
    message = None

    def execute(self):
        """Display the help message for this topic."""
        print(self.message)


class Alias(object):

    def __init__(self, type, usedby, aname, avalue):
        """Initialize an alias with a type, category, alias name, and
        alias value."""
        self.type   = type
        self.usedby = usedby
        self.alias  = aname
        self.avalue = avalue

def aliassort(a, b):
    av = (a.type, a.usedby, a.alias, a.avalue)
    bv = (b.type, b.usedby, b.alias, b.avalue)

    if av < bv:
        return -1
    elif av > bv:
        return 1
    return 0


typeOrder = ["attr", "search", "argument", "command"]
contextOrder = ["Job", "jobs", "Task", "tasks", "Command", "commands", "Blade", "blades", "global"]
def aliasSort(a, b):
    if a.type != b.type:
        return cmp(typeOrder.index(a.type), typeOrder.index(b.type))
    if a.usedby != b.usedby:
        return cmp(contextOrder.index(a.usedby), contextOrder.index(b.usedby))
    return cmp(a.alias, b.alias)

class AliasHelp(TqHelp):
    """Help command for listing aliases that can be used in a query."""

    usage = "alias"

    description = """
    List attribute, clause, argument, and command aliases.
    """

    examples = """
  Syntax for listing aliases is:
    > tq aliases
  """

    options = [
          ] + CmdLineTool.CmdLineTool.options


        
    def getExamples(self):
        return self.examples.lstrip('\n').rstrip()

    def execute(self):
        """Display the help message for this topic."""

        # if no arguments are provided then just list all the currently
        # set aliases
        # create a list of Aliases to be printed
        aliases = []

        #mrdprefs = MisterD.UserPrefs()
        mrdprefs = {}
        prefs = self.parent.userPrefs
        # add all the name and search aliases
        for table in FieldsHelp.tables:
            bname = table.baseClass.__name__
            # attribute aliases  for each object
            for objClass in table.objects:
                for alias,mem in list(objClass.__dict__.get("Aliases", {}).items()):
                    aliases.append(("attr", bname, alias, mem))

            # aliases for the table, grab the user aliases too
            try:
                ualiases = mrdprefs.aliases.where.table[bname]
            except (AttributeError, KeyError):
                ualiases = {}
            for alias,where in list(table.whereAliases.items()) + list(ualiases.items()):
                aliases.append(("search", FieldsHelp.table2cmd[table],
                                alias, where))

        # add the global aliases for each db
        for db in (EngineDB.EngineDB,):
            for alias,where in list(db.WhereAliases.items()):
                aliases.append(("search", "global", alias, where))

        # add the global user aliases
        try:
            for alias,where in list(mrdprefs.aliases.where["global"].items()):
                aliases.append(("search", "global", alias, where))
        except (AttributeError, KeyError):
            pass


        # add command argument aliases
        for cmd,cmdobj in list(self.parent.commands.items()):
            # check for user aliases
            try:
                ualiases = prefs.aliases.args.cmds[cmd]
            except (AttributeError, KeyError):
                ualiases = {}
            for alias,args in list(cmdobj.__dict__.get("aliases", {}).items()) + \
                              list(ualiases.items()):
                aliases.append(("argument", cmd, alias, args))

        # add the global command argument aliases
        try:
            for alias,args in list(prefs.aliases.args["global"].items()):
                aliases.append(("argument", "global", alias, args))
        except (AttributeError, KeyError):
            pass

        # add command aliases
        try:
            ualiases = prefs.aliases.command
        except (AttributeError, KeyError):
            ualiases = {}
        for alias,args in list(self.parent.aliases.items()) + list(ualiases.items()):
            aliases.append(("command", "global", alias, args))

        # convert alias tuples into Alias objects
        aliases = [Alias(*a) for a in aliases]
        aliases.sort(aliasSort)

        form = Formats.Formatter(
            ["type",
             Formats.StringFormat("usedby", header="context"),
             "alias",
             Formats.StringFormat("avalue", header="equivalent",
                                  maxWidth=60, truncate="wrap"),
             ])

        print(form.formatList(aliases))


class AliasHelpOld(TqHelp):
    """Help command for listing aliases that can be used in a query."""

    usage = "alias"

    description = """
    Set/unset and view search clause aliases.  This feature is useful
    if you have a particular search clause or desired format.  Several
    aliases are set by default in tq and can be accessed by running
    the alias command with no arguments.
    """

    examples = """
  Syntax for setting and unsetting aliases is:
    > tq alias -t TYPE -u USEDBY --add alias_name value_of_alias ...
    > tq alias -t TYPE -u USEDBY --delete alias_name

  The 'type' argument is used to identify whether the alias is for a command,
  search clause, or a name (db field) alias.  The 'usedby' argument is used
  to identify the scope of the alias (i.e. it is only used by the jobs command,
  or only in the slots, or it is used globally by all commands).

  Examples:
    add a search alias for the jobs command that will always print your
    errored jobs
      > tq alias -t where -u jobs --add myerrors "mine and error"
    delete the where alias we just made
      > tq alias -t where -u jobs --delete myerrors
    add a command alias to list the nemo jobs in reverse priority order
      > tq alias -t cmd --add nemoqueue "jobs -s pri,-spooled not errwait and title like nemo"
    delete the command alias we just made
      > tq alias -t cmd --delete nemoqueue
  """

    options = [
          CmdLineTool.BooleanOption ("-a", "--add",
                                     help="add/overwrite an alias"),

          CmdLineTool.StringOption  ("-d", "--delete",
                                     help="delete/remove an alias"),

          CmdLineTool.StringOption  ("-t", "--type", default="where",
                                     help="the type of alias (cmd, args, "
                                          "or where)"),

          CmdLineTool.StringOption  ("-u", "--usedby", default="global",
                                     help="the command or class this alias "
                                          "will be used by, i.e. jobs, "
                                          "machines, slots, etc. if the "
                                          "alias type is a where.  If not "
                                          "provided then the alias will be "
                                          "global to everything under a "
                                          "given type."),

          ] + CmdLineTool.CmdLineTool.options


    def getExamples(self):
        return self.examples.lstrip('\n').rstrip()


    def parseArgs(self, *args, **kwargs):
        # do the actual parsing
        result = super(AliasHelp, self).parseArgs(*args, **kwargs)

        # make sure everything is okay
        if self.opts.add:
            if not self.args:
                raise tq.TqError("no alias provided")
            elif len(self.args) == 1:
                raise tq.TqError("no value for alias provided")
            # save the alias as a key, value pair
            self.opts.add = (self.args[0], ' '.join(self.args[1:]))

        if self.opts.add or self.opts.delete:
            if self.opts.type not in ("cmd", "command", "args", "where"):
                raise tq.TqError("unknown alias type")

            #if self.opts.type == "name" and \
            #   (self.opts.usedby not in ["global"] + \
            #    [t.baseClass.__name__ for t in FieldsHelp.tables]):
            #    raise tq.TqError, "unknown class type"

            if self.opts.type == "where":
                if self.opts.usedby != "global":
                    cmd2table = {}
                    for tbl,cmd in list(FieldsHelp.table2cmd.items()):
                        cmd2table[cmd] = tbl.baseClass.__name__
                    cmd2table["tasks"] = "Task"
                    try:
                        self.opts.usedby = cmd2table[self.opts.usedby]
                    except KeyError:
                        raise tq.TqError("cannot add/delete " \
                              "aliases for the '%s' command" % self.opts.usedby)

            elif self.opts.type in ("args", "where") and \
               self.opts.usedby != "global" and \
               self.opts.usedby not in self.parent.commands:
                raise tq.TqError("unknown command name")

        # this is a hack so we don't have to change the code in
        # do_alias, but we can call command types with 'cmd'
        if self.opts.type == "cmd":
            self.opts.type = "command"
        

    def execute(self):
        """Display the help message for this topic."""

        # should we add an alias?
        if self.opts.add:
            # make sure the alias name is valid
            are  = re.compile(r'^[a-zA-Z_0-9]+$')
            name = self.opts.add[0]
            if not are.match(name):
                raise tq.TqError("invalid alias name '%s' provided." % name)

            def makedicts(root, dictnames):
                top = dictnames.pop(0)
                if top not in root:
                    root[top] = {}
                if dictnames:
                    makedicts(root[top], dictnames)

            #mprefs = MisterD.UserPrefs()
            mprefs = {}
            oprefs = self.parent.userPrefs
            prefs  = None
            atype  = self.opts.type
            used   = self.opts.usedby
            val    = self.opts.add[1]
            # command aliases don't have a used by because they are
            # global by definition
            if atype == 'command':
                if val.split()[0] not in self.parent.commands:
                    raise tq.TqError("'%s' cannot begin a " \
                          "command alias, it is not a tq command." % \
                          val.split()[0])
                makedicts(oprefs.aliases, ["command", name])
                oprefs.aliases.command[name] = val
                prefs = oprefs
            elif atype == "args":
                if used == "global":
                    makedicts(oprefs.aliases, ["args", "global", name])
                    oprefs.aliases.args["global"][name] = val
                else:
                    makedicts(oprefs.aliases, ["args", "cmds", used, name])
                    oprefs.aliases.args.cmds[used][name] = val
                prefs = oprefs
            elif used == "global":
                makedicts(mprefs.aliases, ["where", "global", name])
                mprefs.aliases.where["global"][name] = val
                prefs = mprefs
            else:
                makedicts(mprefs.aliases, ["where", "table", used, name])
                mprefs.aliases.where.table[used][name] = val
                prefs = mprefs

            try:
                prefs.save()
            except IOError as errObj:
                sys.stderr.write('unable to save new alias.\n')
                sys.stderr.write('%s\n' % str(errObj))

            return

        # should we delete an alias
        if self.opts.delete:
            #mprefs = MisterD.UserPrefs()
            mprefs = {}
            oprefs = self.parent.userPrefs
            atype  = self.opts.type
            used   = self.opts.usedby
            name   = self.opts.delete
            prefs  = None
            try:
                # command aliases do not have a usedby category
                if atype == 'command':
                    del oprefs.aliases.command[name]
                    prefs = oprefs
                elif atype == "args":
                    if used == "global":
                        del oprefs.aliases.args[used][name]
                    else:
                        del oprefs.aliases.args.cmds[used][name]
                    prefs = oprefs
                elif used == "global":
                    del mprefs.aliases.where["global"][name]
                    prefs = mprefs
                else:
                    del mprefs.aliases.where.table[used][name]
                    prefs = mprefs

            except KeyError:
                if EngineDB.EngineClientDB.getWhereAlias(name):
                    raise tq.TqError("default aliases cannot be " \
                          "deleted, only overridden.")

                err = "alias '%s' not found under type='%s'" % \
                      (name, atype)
                if atype == 'command':
                    err += " and used by='%s'" % used
                raise tq.TqError(err)

            try:
                prefs.save()
            except IOError as errObj:
                sys.stderr.write('unable to save new alias.\n')
                sys.stderr.write('%s\n' % str(errObj))

            return


        # if no arguments are provided then just list all the currently
        # set aliases
        # create a list of Aliases to be printed
        aliases = []

        #mrdprefs = MisterD.UserPrefs()
        mrdprefs = {}
        prefs = self.parent.userPrefs
        # add all the name and search aliases
        for table in FieldsHelp.tables:
            bname = table.baseClass.__name__
            # name aliases for each object
            for objClass in table.objects:
                for alias,mem in list(objClass.__dict__.get("Aliases", {}).items()):
                    aliases.append(("name", bname, alias, mem))

            # aliases for the table, grab the user aliases too
            try:
                ualiases = mrdprefs.aliases.where.table[bname]
            except (AttributeError, KeyError):
                ualiases = {}
            for alias,where in list(table.whereAliases.items()) + list(ualiases.items()):
                aliases.append(("where", FieldsHelp.table2cmd[table],
                                alias, where))

        # add the global aliases for each db
        for db in (EngineDB.EngineDB,):
            for alias,where in list(db.WhereAliases.items()):
                aliases.append(("where", "global", alias, where))

        # add the global user aliases
        try:
            for alias,where in list(mrdprefs.aliases.where["global"].items()):
                aliases.append(("where", "global", alias, where))
        except (AttributeError, KeyError):
            pass


        # add command argument aliases
        for cmd,cmdobj in list(self.parent.commands.items()):
            # check for user aliases
            try:
                ualiases = prefs.aliases.args.cmds[cmd]
            except (AttributeError, KeyError):
                ualiases = {}
            for alias,args in list(cmdobj.__dict__.get("aliases", {}).items()) + \
                              list(ualiases.items()):
                aliases.append(("args", cmd, alias, args))

        # add the global command argument aliases
        try:
            for alias,args in list(prefs.aliases.args["global"].items()):
                aliases.append(("args", "global", alias, args))
        except (AttributeError, KeyError):
            pass

        # add command aliases
        try:
            ualiases = prefs.aliases.command
        except (AttributeError, KeyError):
            ualiases = {}
        for alias,args in list(self.parent.aliases.items()) + list(ualiases.items()):
            aliases.append(("command", "global", alias, args))

        # sort the aliases
        aliases = [Alias(*a) for a in listutil.getUnion(aliases)]

        form = Formats.Formatter(
            ["type",
             Formats.StringFormat("usedby", header="used by"),
             "alias",
             Formats.StringFormat("avalue", header="resolves to",
                                  maxWidth=50, truncate="wrap"),
             ])

        print(form.formatList(aliases))


class ColumnsHelp(TqHelp):
    """Help for customizing the displayed columns."""

    message = """
For help on listing the available attributes for a given entity, use:
tq attributes <entity>

Customizing the displayed columns.  By default, tq tries to give an
informative summary of each object that is printed, but the output can
be modified very easily.  All of the list and operation commands
support a -c,--cols option to specify a comma delimited list of
attributes to display.

Examples:
  list jobs, but only the owner, priority, spooltime of each job
    > tq jobs -c owner,priority,spooltime
  the same query, but take advantage of some name aliases
    > tq jobs -c user,pri,spooled
  list errored tasks, but only the owner, task title, and last invocation's blade
    > tq tasks error -c owner,title,blade

Using the - and + operators, it is possible to add or remove attribute from the
default list without typing the full comma delimited attributes.

Examples:
  view the current ready tasks for an owner, but print the keys in addition
  to the default list of attributes
    > tq tasks ready and owner=fonz -c +keys
  add the crews of each tasks' job too
    > tq tasks ready and owner=fonz -c +keys,+crews
  remove the title and owner of the job
    > tq jobs -c -owner,-title
  remove a column and add a column
    > tq jobs -c -status,+crews

By default, tq will pick a good width for the columns.  If you would
like to override this and specify a maximum width, follow the attribute with the
size, i.e. "title=100".  Setting the width to zero forces the full attribute to
be displayed.  In addition, prepending = to a attribute will modify the width
of a default attribute.

Examples:
  force the job title to use at most 100 characters
    > tq jobs -c title=100
  print active tasks with the state, blade, and title (don't restrict widths)
    > tq tasks active -c state=0,blade=0,title=0
  force the job title to use at most 50 characters, but still print all
  the other attributes too
    > tq jobs -c =title=50

The : operator can be used to color a attribute.  Available colors are red,
green, blue, yellow, cyan, black, and white.

Examples:
  print the title of each job in yellow
    > tq jobs -c =title:yellow
"""

class DistinctHelp(TqHelp):
    """Help topic for listing distinct rows."""

    message = """
Printing distinct values of columns.  Each list command has a -d,--distinct
option to specify a comma delimited list of columns, so that only the distinct
values of those columns are printed.  This is useful when scripting and you
need a unique list of some column.  By default, the columns provided with the
-d,--distinct list are the only ones printed and sorted by.  This can be
overriden with by adding a new column list via the -c,--cols argument (see
'tq help columns' for more help).  The default sort can be changed via
the -s,--sortby argument (see 'tq help sorting' for more help).

Examples:
  list all the owners with active tasks
    > tq jobs active -d owners
  list all the owners with active tasks with "ring" in the title
    > tq jobs active and title like ring -d owner
  get a distinct list of blades an owner has active tasks on, reverse
  sorted by blade names
    > tq tasks active and owner=thing -d blade -s -blade
  distinct list of owner/spoolhost combinations for all ready jobs
    > tq jobs ready -d owner/spoolhost
"""


class ExamplesHelp(TqHelp):
    """Help topic to list examples of using tq."""

    message = """
A list of simple examples which will hopefully get you started.

list all the jobs for yourself that are not finished:
  > tq jobs mine and not done

list all your errored jobs:
  > tq jobs mine and error

list all the jobs you have not deleted yet:
  > tq jobs mine

list all of julia's jobs that were spooled on May 16:
  > tq jobs "user=julia and spooltime >= 5/16 and spooltime < 5/17"

list all the jobs with cars in the title with priority 400 or higher, and sort by priority:
  > tq jobs title like cars and "priority >= 400" -s priority

list the owner, title and priority of the jobs spooled in the last hour
  > tq jobs "spooltime > -1h" -c owner,title,priority

list all errored tasks for an owner:
  > tq tasks owner=louise and error

view the output log from a task:
  > tq log 1134800 334

retry a specific task:
  > tq retry jid=1134800 and tid=334

retry a specific task using alternative notation:
  > tq retry 1134800 334

list all the blades with a mac profile with less than 1 MB of disk space:
  > tq blades profile like mac and availdisk "<" 1M

list all the commands with PixarRender in its service key expression:
  > tq commands service like PixarRender

list all the of weezie's currently running invocations
  > tq invocations not stoptime and owner=weezie
"""


class FieldsHelp(DBCmdLineTool.FieldsHelp):
    """Help topic for listing available attributes."""

    usage = "attributes <entity>"

    # explicitly list all the tables we want users to know about
    tables = [
        EngineDB.EngineClientDB.JobTable,
        EngineDB.EngineClientDB.TaskTable,
        EngineDB.EngineClientDB.CommandTable,
        EngineDB.EngineClientDB.InvocationTable,
        EngineDB.EngineClientDB.BladeTable
        ]

    # the command that a given table is used in
    table2cmd = {
        EngineDB.EngineClientDB.JobTable                : "jobs",
        EngineDB.EngineClientDB.TaskTable               : "tasks",
        EngineDB.EngineClientDB.CommandTable            : "commands",
        EngineDB.EngineClientDB.InvocationTable         : "invocations",
        EngineDB.EngineClientDB.BladeTable              : "blades",
        }

    description = """
    
    List all the attributes available through tq.  By default, tq
    displays a general overview of each entity (e.g. job, task, blade, etc.)
    matching a given query, but many more attributes are available.  This command
    displays all the attributes available for each entity.  Most attributes can
    be referenced from any command, but some attributes will require the entity name
    to be prepended to avoid name conflicts. For example, the Task and Job
    entities both have title, and to view job titles while using the tasks
    command the attribute should be referenced with Job.title.
    """

    def __init__(self, **kwargs):
        # hardcode the table
        super(FieldsHelp, self).__init__(tables=self.tables, **kwargs)

    def getMapping(self):
        """Overloaded so we can give some alternate names for some of
        our tables."""
        mymap = super(FieldsHelp, self).getMapping()
        return mymap


class QueriesHelp(TqHelp):
    """Help topic for forming queries."""

    message = """
Building a query to list specific data.  The query language is based on the
Python scripting language syntax and is very flexible.  It is smart enough to
figure out what needs to be quoted before sending the query to the database,
allowing for queries like:
  owner=thing

which will be converted to:
  owner='thing'

The operators in the language are:
  =, != >, <, >=, <=, like, in, not, and, or

The 'in' operator is used to check for list comparisons:
  owner in [tom, dick, harry]

which is equivalent to:
  owner=tom or owner=dick or owner=harry

The 'like' operator is used for regular expressions.

Examples:
  list all the jobs for an owner
    > tq jobs owner=dharma
  list all the nemo jobs that are active
    > tq jobs title like nemo and active
  list all jobs with priority higher than 300, notice this needs to be quoted
  since the query contains a special shell character.
    > tq jobs "priority > 300"
  select jobs for a list of owners
    > tq jobs owner in [jack, janet, chrissy]
  select jobs from a mail alias using the shell back ticks
    > tq jobs owner in [`als cars-rendering`]
  get all the jobs with Nuke in the title
    > tq jobs title like Nuke
  use grouping to build a complex query to list west10* blades low on memory or disk
    > tq blades name like west10 and "(disk < 1 or mem < 1)"

Many of the attributes are time values, and they can be compared using an
absolute time or a time relative to now.  Below is a breakdown of the
acceptable time formats:
  10am       - 10am on the current day
  5pm        - 5pm on the current day
  11:37      - 11:37am on the current day
  16:21      - 4:21pm on the current day
  4:21pm     - same thing
  3/15       - midnight on March 15 of the year closest to the current date
  3/15|4pm   - 4pm on March 15 of the year closest to the current date
  3/15|17:37 - 5:37pm on March 15 of the year closest to the current date
  3/15/05    - midnight on March 15, 2005
  -1s        - 1 seconds ago
  -1m        - 1 minutes ago
  -1h        - 1 hours ago
  -1d        - 1 day ago
  -1w        - 1 week ago

Examples:
  show all the jobs spooled after 12pm today
    > tq jobs "spooled > 12pm"
  show all the tasks that errored in the last 5 minutes
    > tq tasks error and "statetime > -5m"

Attributes that represent total seconds and total bytes can be compared by
appending the appropriate unit to a number for easy typing.  This way an
attribute can be compared with 60 minutes using 60m.  The following units are
available:
  s - seconds
  m - minutes
  h - hours
  d - days
  w - weeks
  B - bytes
  K - kilobytes
  M - megabytes
  G - gigabytes
  T - terabytes
  P - petabytes

Examples:
  show all the invocations on nemo that used more than 4 GB of memory
    > tq invocations Job.title like nemo and mem \> 4G
  show all the invocations on cars that used less than 1 minute of system time
    > tq invocations Job.title like cars and stime \< 1m
"""


class SortingHelp(TqHelp):
    """Help topic for sorting results."""

    message = """
Sorting the results.  Each list and operation command has a -s,--sortby
option to specify a comma delimited list of attributes the results should be
sorted one.  By default, each attribute is sorted in ascending order, but
prepending a - to a attribute will reverse sort the attribute.

Examples:
  sort all the jobs by number of active tasks
    > tq jobs -s active
  sort all the jobs by priority, but include spooltime since this is the
  tie breaker when priorities are equal
    > tq jobs -s priority,-spooled
"""


class UsageHelp(TqHelp):
    """List the global options."""

    def getMessage(self):
        mystr = """
All commands in tq have the same usage:

  tq [GLOBAL_OPTS] COMMAND [CMD_OPTS]

The help for each command should be read for more information on specific
command options, but tq has several global options that can change
the behavior of each command.  These must always be placed before the
command that is being called.

Examples:
  to retry a task that is not your own you need to add the --force flag
    > tq --force retry 15000348 37
  to retry several tasks that are not yours without being prompted add the
  -y or --yes flag as well
    > tq --force -y retry owner=cunningham and host=chevy and error

GLOBAL OPTIONS:
"""

        # add the options
        optstr = self.parent.parent.getOptionsHelp()
        # strip the leading "options:" string off
        mystr += optstr[optstr.find('\n') + 1:]

        return mystr
    message = property(fget=getMessage)


class CommandsHelp(TqHelp):
    """Help topic to list all the commands with a summary of each."""

    def getMessage(self):
        myStr = """
Commands available in tq.  Additional help for each command can be
viewed with:
  tq help COMMAND

"""
        # get all the defined commands from tq and add them
        # to the usage string.
        cmds = {}
        for cmdname,cmdobj in list(self.parent.parent.commands.items()):
            if cmdname is None: continue
            # get the description of the command
            docstr = cmdobj.description
            if docstr is None: docstr = ""
            # grab the first sentence
            cmds[cmdname] = docstr.strip().split('.')[0] + '.'

        formatStr = "  %%-%ds %%s\n" % (stringutil.maxStrLen(list(cmds.keys())))

        myStr += "Renderfarm Informational Commands:\n"
        for cmd in ('jobs', 'tasks', 'commands', 'invocations', 'blades'):
            myStr += formatStr % (cmd, cmds.get(cmd))

        myStr += "\nJob Operational Commands:\n"
        for cmd in ('chcrews', 'chpri', 'delete', 'delay', 'jattr', 'lock', 'jobdump',
                    'pause', 'interrupt', 'restart', 'retryallerrs',
                    'skipallerrs', 'undelay', 'undelete', 'unlock', 'unpause'):
            myStr += formatStr % (cmd, cmds.get(cmd))

        myStr += "\nTask Operational Commands:\n"
        for cmd in ('retry', 'resume', 'kill', 'skip', 'log'):
            myStr += formatStr % (cmd, cmds.get(cmd))

        myStr += "\nCommand Operational Commands:\n"
        for cmd in ('cattr',):
            myStr += formatStr % (cmd, cmds.get(cmd))

        myStr += "\nBlade Operational Commands:\n"
        for cmd in ('delist', 'eject', 'nimby', 'unnimby', 'trace'):
            myStr += formatStr % (cmd, cmds.get(cmd))

        myStr += "\nOther Commands:\n"
        for cmd in ('notes', 'attributes', 'ping', 'dbreconnect', 'queuestats',
                    'reloadconfig'):
            myStr += formatStr % (cmd, cmds.get(cmd))

        return myStr
    message = property(fget=getMessage)


class UnknownHelp(CmdLineTool.UnknownCommand):
    def __str__(self):
        return "unknown help topic '%s', refer to 'tq help' for " \
               "all the options." % self.command

class MainHelp(CmdLineTool.MultiCmdLineTool):
    """The main help command in tq has several subtopics that are
    treated as separate commands."""

    options = [CmdLineTool.HelpOption()]

    # set all the sub-topics we can have
    commands = {
        "subcommands" : CommandsHelp,
        "aliases"  : AliasHelp,
        "columns"  : ColumnsHelp,
        "distinct" : DistinctHelp,
        "examples" : ExamplesHelp,
        "attributes" : FieldsHelp,
        "queries"  : QueriesHelp,
        "sorting"  : SortingHelp,
        "usage"    : UsageHelp,
        }

    def getHelpStr(self):
        """Get the main help message."""

        return """
  tq is a command line tool to query information about Tractor entities
  such as jobs, tasks, commands, and blades.  Simple tq subcommands exist 
  for displaying information about and for operating on these entities.

  For more help try the following:

    tq help subcommands - list of all the subcommands
    tq help COMMAND     - get help on a specific subcommand

  Additional help topics:

    tq help columns     - customizing the output of subcommands
    tq help distinct    - printing distinct values of attributes
    tq help examples    - simple examples to get started
    tq help attributes  - the available attributes for list commands
    tq help queries     - building queries to list specific information
    tq help sorting     - sorting results
    tq help usage       - global options
"""

    def getCommandToRun(self):
        """Get an L{TqHelp} instance that should be used for the
        provided command.

        @return: (str of command name, L{TqHelp}, list of arguments) 
        that will be used
        """
        try:
            command,cmdobj,args = super(MainHelp, self).getCommandToRun()
        except CmdLineTool.UnknownCommand as e:
            # assume it is part of the main program
            if not self.args:
                command = 'help'
            else:
                command = self.args[0]

            try:
                cmdobj = self.parent.getCommand(command)
            except CmdLineTool.UnknownCommand:
                raise UnknownHelp(command)
            return (command, cmdobj, ["-h"])

        if command == "aliases":
            args = ["-h"]

        return (command,cmdobj,args)
