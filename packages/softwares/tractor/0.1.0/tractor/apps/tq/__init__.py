"""The tq module contains multiple subclasses of the table querying class
in L{rpg.sql.DBCmdLineTool}.  They are all intended to be accessed from a
a L{rpg.CmdLineTool.MultiCmdLineTool} object, but they can be subclassed
to alter the functionality and used in a different application.
"""

import getpass, time, sys, select, os, json

import tractor.base.rpg
import rpg.osutil as osutil
import rpg.terminal as terminal
import rpg.stringutil as stringutil
import rpg.socketlib.Sockets as Sockets
import rpg.OptionParser as OptionParser
import rpg.CmdLineTool as CmdLineTool
import rpg.sql.Database as Database
import rpg.sql.DBCmdLineTool as DBCmdLineTool

import tractor.base.EngineDB as EngineDB
import tractor.base.EngineClient as EngineClient
import tractor.api.query as query

__all__ = (
    "TqError",
    "QueryCmd",
    "OperateCmd",
    )

def copyModifyList(items, remove=None, append=None, dups=False):
    """Given a list of items, remove and append the specified items from a copy of the list.
    If dups is True, duplicate items are permitted"""
    copy = items[:]
    if remove:
        for item in remove:
            if item in copy:
                copy.remove(item)
    if append:
        for item in append:
            if item not in copy or dups:
                copy.append(item)
    return copy

class TqError(CmdLineTool.CmdLineToolError):
    """Base error type for all tq related errors."""
    pass

def usernamePasswordForFilename(filename):
    """Return (username, password) values as store in file."""
    try:
        f = open(filename)
        jsonDict = json.load(f)
        f.close()
    except (IOError, ValueError) as err:
        raise TqError("problem reading password file %s: %s" % (filename, str(err)))

    username = jsonDict.get("username")
    if not username:
        raise TqError("username is not defined in password file %s" % filename)
    password = jsonDict.get("password")
    if password is None:
        raise TqError("password is not defined in password file %s" % filename)
    return username, password


class EngineClientMixin(object):
    """A mixin class for establishing a connection with the engine based
     on command line options and defaults.
     """
    def engineClient(self):
        """Return an instance of EngineClient pointing to desired engine."""
        # use the module's singleton EngineClient object
        engineClient = EngineClient.TheEngineClient
        # set non-default EngineClient parameters based on options
        params = {}
        if self.parent.opts.engine:
            params["hostname"], params["port"] = \
                                EngineClient.hostnamePortForEngine(self.parent.opts.engine)
        # set user/password parameters based on options or password file contents
        if self.parent.opts.user:
            params["user"] = self.parent.opts.user
        if self.parent.opts.password:
            params["password"] = self.parent.opts.password
        if self.parent.opts.password_file:
            params["user"], params["password"] = usernamePasswordForFilename(self.parent.opts.password_file)
        params["debug"] = self.parent.opts.debug

        params["newSession"] = self.parent.opts.login
        # set engine  parameters
        engineClient.setParam(**params)
        # waited until now to set session filename as it can depend on evaluation of other params
        if self.parent.opts.noSaveSession:
            sessionFilename = None
        elif self.parent.opts.sessionFilename:
            sessionFilename = self.parent.opts.sessionFilename
        else:
            sessionFilename = EngineClient.sessionFilename(
                app="tq", engineHostname=engineClient.hostname, port=engineClient.port,
                clientHostname=osutil.getlocalhost(), user=params.get("user", engineClient.user))
        if sessionFilename:
            engineClient.setParam(sessionFilename=sessionFilename)
        # prompt for password if necessary
        if engineClient.needsPassword():
            password = getpass.getpass("Enter password for %s@%s:%d: " % (engineClient.user, engineClient.hostname, engineClient.port))
            engineClient.setParam(password=password)
            
        return engineClient
        
    
class QueryCmd(DBCmdLineTool.TableQueryCmd, EngineClientMixin):
    """Base command type for all tq sub-commands that will be querying
    a table in a database.  The C{getDescription} method is overloaded so
    all commands have a standard help statement.

    @cvar examples: a string containing examples for this command.  The
      formatting in this string will not be altered and will be added to
      the final description as-is.

    @cvar defaultSort: a list of the default member names the results should
      be sorted with.
    """

    examples = None

    # the default list of member names to sort the results with
    defaultSort = None

    # the default list of member names to use as a key when checking for
    # distinct rows.
    defaultDistinct = None

    optionStyle = 'interspersed'

    # argument aliases that can be used for this command
    aliases = {}

    def __init__(self, table, *args, **kwargs):
        # set the default sort list if one wasn't already set.
        kwargs.setdefault("defaultSort", self.defaultSort)
        kwargs.setdefault("defaultDistinct", self.defaultDistinct)
        # set it as 'parent' for the super
        super(QueryCmd, self).__init__(
            EngineDB.EngineClientDB, table, EngineDB.EngineClientDBFormatter, *args, **kwargs)


    def parseArgs(self, *args, **kwargs):
        # before calling the super, we need to check for argument aliases
        newargs = []
        # check for any argument aliases
        uprefs = self.parent.userPrefs
        for arg in kwargs["args"]:
            try:
                alias = uprefs.aliases.args.cmds[self.usage][arg]
            except (AttributeError, KeyError):
                try:
                    alias = uprefs.aliases.args["global"][arg]
                except (AttributeError, KeyError):
                    alias = self.aliases.get(arg)

            if alias:
                # split the string on whitespace, keeping quoted strings together
                addargs = stringutil.quotedSplit(alias, removeQuotes=True)
            else:
                addargs = [arg]

            for arg in addargs:
                # could do additional processing of args here before committing to newargs
                newargs.append(arg)

        # update the kwargs with our new argument list
        kwargs["args"] = newargs
        
        # now call the super
        return super(QueryCmd, self).parseArgs(*args, **kwargs)
    

    def getExamples(self):
        """Return the string that should be use as the examples section
        in the help usage statement."""
        mystr = super(QueryCmd, self).getExamples()
        # add a default list of fields
        defFieldList = self.getDefaultFields()
        if defFieldList:
            mystr += "\n\n  The default list of columns that are displayed:\n" \
                     "    %s\n" \
                     "  For a full list of available columns see " \
                     "'tq help attributes'" % defFieldList
        return mystr

    def getDefaultFields(self):
        """Return the default list of columns that will be displayed."""
        return self.formatterClass.defaultFormatLists[self.table]

    def opendb(self):
        """Establish a connection with the engine."""
        # superclass will make actual open() call
        super(QueryCmd, self).opendb(dbargs={"engineClient": self.engineClient()})

    def closedb(self):
        """Close the database connection if command-line options indicate so."""
        if self.parent.opts.logout or self.parent.opts.noSaveSession:
            super(QueryCmd, self).closedb()


class OperateCmd(QueryCmd):
    """An operation command will query the database and run an arbitrary
    operation on each object returned."""

    # the default set of options for all operation commands
    options = [
        CmdLineTool.StrListOption("-c", "--cols",
                                  help="list of columns that will be "
                                       "printed for each each row."),

        CmdLineTool.StrListOption("-s", "--sortby",
                                  help="sort the results in a specified "
                                       "order before performing any "
                                       "operations."),

        CmdLineTool.BooleanOption("--ns", "--nosort", dest="nosort",
                                  help="do not sort the results"),

        CmdLineTool.IntOption    ("--limit",
                                  help="only operate on the first l rows"),

        ] + CmdLineTool.CmdLineTool.options

    def __init__(self, table, *args, **kwargs):
        # call the super
        super(OperateCmd, self).__init__(table, *args, **kwargs)

        # if a list of objects is provided at the command line, then
        # subclasses can fill in this value.  Setting this will prevent
        # the database from being contacted
        self.objects = []

    def parseArgs(self, *args, **kwargs):
        result = super(OperateCmd, self).parseArgs(*args, **kwargs)
        
        # make sure a where exists
        if not self.where:
            raise OptionParser.HelpRequest(self.getHelpStr())

        # make sure a value is set for options we are not supporting, but
        # that the super expects to exist.

        for name,default in (("distinct", None),
                             ("delimiter", ' '),
                             ("raw", False),
                             ("timefmt", None),
                             ("noformat", False),
                             ("noheader", False)):
            self.opts.__dict__.setdefault(name, default)

        return result

    def opendb(self):
        """We don't need to open the database if the objects member is set."""
        if not self.objects:
            super(OperateCmd, self).opendb()

    def getNewObjs(self, num, curr, objs, distinct):
        """Return at most 'num' new objects for the operationLoop.  The
        objects returned are tq Operation Job or Task objects.  By
        default the mrd cursor is checked for waiting job objects, but
        a list can be provided which will be modified."""

        # a list of all the operation objects we are going to return
        newobjs = []
        # loop until we have enough objects
        while len(newobjs) < num and curr < len(objs):
            obj = objs[curr]
            curr += 1
            # check if this object is distinct
            if not self.isDistinct(obj, distinct):
                if self.parent.opts.debug: print('already worked on')
                continue

            # skip over this object if we don't have permission
            if not (self.parent.force or obj.hasPermission()):
                msg = "not job owner; use tq --force"
                if self.parent.color:
                    msg = terminal.TerminalColor('red').colorStr(msg)
                print(self.formatter.format(obj), msg)
                continue

            # we passed all the tests, so add it to the list
            newobjs.append(obj)

        return newobjs,curr

    def processResult(self, qresult):
        """Process and print all the objects returned from the query."""

        # if we had no results, then do nothing
        if not qresult or \
           (self.opts.limit is not None and self.opts.limit < 0):
            return

        # make sure the query result doesn't cache objects after they are
        # created, as we will only need them once.
        if isinstance(qresult, Database.QueryResult):
            qresult.cacheObjects = False

        file = sys.stdout

        # set the widths of our formatter so everything is neatly spaced
        if not self.opts.noformat:
            self.formatter.setWidths(qresult)

        # print a header
        if not self.opts.noheader:
            print(self.formatter.header(), file=file)

        # keep track of which objects are distinct
        distinct = {}

        # our current list of objects that have had the operation started,
        # but the operation has not finished.  Typically because the dispatcher
        # is busy reading from it and would block the process.
        queue = []

        # keep track of how many objects we've worked on
        workedon = 0
        # time we last created an object (only useful when pausing between
        # operations).
        lastcreate = 0

        # make sure threads is set to at least one
        if self.parent.threads < 1:
            self.parent.threads = 1

        # setup a blocking flag
        if not self.parent.yes or self.parent.threads <= 1:
            self.blocking = True
        else:
            self.blocking = False

        # keep track of our current index into the query result.
        curr = 0

        try:
            # start operating on the objects
            while True:
                # if we are running in parallel mode (i.e. non-blocking) we
                # want to have as many concurrent connections as possible.
                # Thus make sure the queue size is always the size of
                # self.threads, or at least as big as it can be.

                newobjs = []

                # check if we need to pause inbetween operations
                if self.parent.pause > 0:
                    # how much time has passed since our last object create
                    elapsed = self.parent.pause - (time.time() - lastcreate)
                    # if we don't have any objects on the queue, and 'pause'
                    # seconds hasn't elapsed
                    if not queue and elapsed > 0:
                        time.sleep(elapsed)
                        elapsed = 0

                    # add another item to the queue
                    if elapsed <= 0 and len(queue) < self.parent.threads:
                        newobjs,curr = self.getNewObjs(1, curr,
                                                       qresult, distinct)
                        lastcreate = time.time()

                else:
                    # get however many more we can take on now
                    newobjs,curr = self.getNewObjs(
                        self.parent.threads - len(queue),
                        curr, qresult, distinct)

                # if we have nothing left, then we're all done!
                if not (queue or newobjs) or \
                   (not queue and \
                    self.opts.limit not in [None, 0] and \
                    self.opts.limit <= workedon):
                    break

                # start the new operations now.  If we are in blocking
                # mode, (self.yesToAll == False or self.threads <= 1),
                # then the entire operation will be done here.  If we are
                # in parallel mode (i.e. non-blocking), then the connect
                # will initialize here, raise a WouldBlock exception, and
                # be put onto the queue.
                for obj in newobjs:
                    try:
                        # work on this object
                        self.processObject(obj)
                    except Sockets.WouldBlock:
                        # if this would block, then add it to the queue
                        queue.append(obj)
                    except Sockets.SocketError as err:
                        # catch any other socket related error
                        obj.post(self, str(err), error=True)
                        obj.close()
                    except EngineClient.EngineClientError as err:
                        obj.post(self, str(err), error=True)
                        obj.close()
                    except:
                        # make sure this socket gets closed
                        obj.close()
                        raise

                    workedon += 1

                    # check if we should stop
                    if self.opts.limit not in [None, 0] and \
                       self.opts.limit <= workedon:
                        # set the 'curr' index to the number of objects in
                        # the list to prevent new ones from being processed.
                        # we can't break completely out of the while loop
                        # until our queue is emptied
                        curr = len(qresult)
                        break

                # if we don't have anything on the queue, then go back to the
                # top of the loop
                if not queue: continue

                # now for each object on our queue, we want to send it to
                # select so we can handle those that are ready.  We have to
                # pay careful attention to which objects have finished the
                # initial connect and those that have not.  Also, we want to
                # get the minimum amount of time to wait before timing out
                # of select.
                input,output,timeouts = [],[],[]
                now = time.time()
                for obj in queue:
                    # if this object has not finished the connection, then
                    # add it to the output list for select
                    if not obj.opentime:
                        output.append(obj)
                    # otherwise, assume the connection is established
                    else:
                        input.append(obj)
                    # keep track of how long this socket has left before we
                    # consider it timed out.
                    timeout = self.parent.timeout - (now - obj.lastaction)
                    if timeout < 0: timeout = 0
                    timeouts.append(timeout)

                # call select and take the smallest timeout in our list
                if self.parent.timeout > 0:
                    input,output,err = select.select(input, output, [],
                                                     min(timeouts))
                # if no timeout is set, then select will sit until a
                # socket is ready
                else:
                    input,output,err = select.select(input, output, [])

                # recheck those that are ready
                for obj in input + output:
                    try:
                        # try to process the object again
                        self.processObject(obj)
                    except Sockets.WouldBlock:
                        # if this would block, then just ignore it for now.
                        pass
                    except Sockets.SocketError as err:
                        # catch any other socket related error
                        obj.post(self, str(err), error=True)
                        obj.close()
                        queue.remove(obj)
                    else:
                        # it finished, so remove it from the queue
                        queue.remove(obj)

                # if a timeout is in place, then check if any operations on
                # the queue have timed out.
                if self.parent.timeout > 0:
                    # make a copy of the list to give to 'for' so we can
                    # remove items
                    for obj in list(queue):
                        # if we haven't read anything from the socket in
                        # timeout seconds then abort.
                        if (time.time() - obj.lastaction) >= \
                            self.parent.timeout:
                            # print an error for the user
                            obj.post(self, "operation timed out", error=True)
                            obj.close()
                            # remove it from the queue
                            queue.remove(obj)

        finally:
            # if an error occured then close any open files
            for obj in queue:
                obj.close()
        

class OperationRow(EngineDB.Row):
    """This subclass of a Row provides functionality for managing permissions
    and concurrency for operating on a set of rows."""

    def __init__(self, *args, **kw):
        """Overload the init() method since this is intended to be a mix-in
        with a DBObject."""
        super(OperationRow, self).__init__(*args, **kw)
        self.operation = None
        self.precalled = False

    def fileno(self):
        if not self.operation: return -1
        return self.operation.fileno()

    def close(self):
        if self.operation:
            self.operation.close()

    def getOpentime(self):
        """Get the time we finished the open."""
        if not self.operation:
            return -1
        return self.operation.opentime
    opentime = property(fget=getOpentime)

    def getLastAction(self):
        """Get thet last time the socket had anything for us."""
        if not self.operation:
            return -1
        return self.operation.lastaction
    lastaction = property(fget=getLastAction)

    def hasPermission(self):
        """Return True or False if the owner of this process has permission
        to perform an operation on this object."""
        try:
            owner = self.owner
        except AttributeError:
            try:
                owner = self.Job.owner
            except AttributeError:
                # was not a job-related object (basically, a blade)
                return True
        return owner == getpass.getuser()

    def pre(self, command, msg):
        """Run once before each call to an operation."""
        if self.precalled:
            return True

        # print out a string describing this job before we work on it
        if command.blocking:
            sys.stdout.write(command.formatter.format(self) + ' ')
            sys.stdout.flush()

        # do we need to ask the use before operating?
        if not command.parent.yes:
            sys.stdout.write('\n' + msg + ' yes/no [y]: ')
            answer = sys.stdin.readline().strip().lower()
            if answer not in ('y', 'yes', ''):
                return False

        self.precalled = True
        return True

    def post(self, command, msg, error=False):
        """Run after the operation has successfully completed."""
        # print a status message if are in non-blocking mode
        if not command.blocking:
            sys.stdout.write(command.formatter.format(self) + ' ')
            sys.stdout.flush()

        if error:
            if command.parent.color:
                msg = terminal.TerminalColor('red').colorStr(msg)
            sys.stderr.write("%s\n" % msg)
            sys.stderr.flush()
        else:
            if command.parent.color:
                msg = terminal.TerminalColor('green').colorStr(msg)
            sys.stdout.write("%s\n" % msg)
            sys.stdout.flush()

