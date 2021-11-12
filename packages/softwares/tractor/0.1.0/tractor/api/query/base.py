import os
import types
import datetime
import operator

import tractor.base.EngineClient as EngineClient
import tractor.base.EngineDB as EngineDB
import rpg.timeutil as timeutil

from . import TractorQueryError, PasswordRequired, InvalidValue, \
     MissingSearchClause, MissingParameter, MissingTargetKey, \
     SortNotAllowed
import numbers

# an EngineClient is required for operations
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

def needsPassword():
    """Returns True if the EngineClient needs a password."""
    return ModuleEngineClient.needsPassword()
    
# list functions

def _tractorSelect(table=None, where="", columns=[], sortby=[], limit=None, archive=False):
    """Query the specified table using the postgres server-side
    function TractorSelect(), using the engine as a proxy.
    """
    if where == "":
        raise MissingSearchClause("A search clause must be specified.")
    try:
        rows = ModuleEngineClient.select(table, where, columns=columns, sortby=sortby, limit=limit, archive=archive)
    except EngineClient.PasswordRequired as err:
        raise PasswordRequired(str(err))
    except EngineClient.EngineClientError as err:
        raise TractorQueryError(str(err))
    return rows

def jobs(search="", columns=[], sortby=[], limit=None, archive=False):
    """Retrieve a list of jobs."""
    return _tractorSelect("job", where=search, columns=columns, sortby=sortby, limit=limit, archive=archive)

def tasks(search="", columns=[], sortby=[], limit=None, archive=False):
    """Retrieve a list of tasks."""
    return _tractorSelect("task", where=search, columns=columns, sortby=sortby, limit=limit, archive=archive)

def commands(search="", columns=[], sortby=[], limit=None, archive=False):
    """Retrieve a list of commands."""
    return _tractorSelect("command", where=search, columns=columns, sortby=sortby, limit=limit, archive=archive)

def invocations(search="", columns=[], sortby=[], limit=None, archive=False):
    """Retrieve a list of invocations."""
    return _tractorSelect("invocation", where=search, columns=columns, sortby=sortby, limit=limit, archive=archive)

def blades(search="", columns=[], sortby=[], limit=None, archive=False):
    """Retrieve a list of blades."""
    if not archive:
        if search:
            search = "registered and (%s)" % search
    return _tractorSelect("blade", where=search, columns=columns, sortby=sortby, limit=limit, archive=archive)

def params(search="", columns=[], sortby=[], limit=None, archive=False):
    """Retrieve a list of engine parameters."""
    return _tractorSelect("param", where=search, columns=columns, sortby=sortby, limit=limit, archive=archive)

def _checkRequiredAttributes(objs, attrs):
    """Raises an exception if any object is missing the specified
    attributes.  Separate tests are done whether the object is an
    EngineDB.Row or a dict.
    """
    for obj in objs:
        for attr in attrs:
            if type(obj) is dict:
                if attr not in obj:
                    raise MissingTargetKey("Target dictionary does not have required key %s: %s" % (attr, str(obj)))
            elif isinstance(obj, EngineDB.Row):
                if not hasattr(obj, attr):
                    raise MissingTargetKey("Target row does not have required attribute %s: %s" % (attr, str(obj)))

def _jidsForArgs(firstarg, sortby, limit, archive=False):
    """Determine the jids for the specified jobs, which could be
    expressed as a dictionary, a list of dictionaries, or a search
    clause.
    """
    if firstarg == "":
        raise MissingSearchClause("A search clause must be specified.")
    if type(firstarg) is str:
        # user has specified a search string; fetch the jobs
        jobz = jobs(firstarg, columns=["jid"], sortby=sortby, limit=limit, archive=archive)
    elif type(firstarg) is list:
        if sortby:
            raise SortNotAllowed("'sortby' is not allowed when passing a list of objects to an operation.")
        if limit:
            jobz = firstarg[:limit]
        else:
            jobz = firstarg
        _checkRequiredAttributes(jobz, ["jid"])
    else:
        jobz = [firstarg]
        _checkRequiredAttributes(jobz, ["jid"])
    jids = [job["jid"] for job in jobz]
    return jids


# job operations

def chcrews(search="", sortby=[], limit=None, crews=None):
    """Change crews of matching jobs.  crews= specifies the new list of crews."""
    if crews is None:
        raise MissingParameter("chcrews(): crews must be specified")
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.setJobCrews(jids, crews)
        
def chpri(search="", sortby=[], limit=None, priority=None):
    """Change priority of matching jobs.  priority= specifies the new
    priority.
    """
    if priority is None:
        raise MissingParameter("chpri(): priority must be specified")
    if not isinstance(priority, numbers.Number):
        raise InvalidValue("chpri(): priority is not numeric")
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.setJobPriority(jids, priority)
        
def jattr(search="", sortby=[], limit=None, key=None, value=None):
    """Set an attribute of matching jobs.  key= specifies the
    attribute, and value= specifies the attribute value.
    """
    if key is None:
        raise MissingParameter("jattr(): key must be specified")
    if value is None:
        raise MissingParameter("jattr(): value is not specified")
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.setJobAttribute(jids, key, value)

def pause(search="", sortby=[], limit=None):
    """Pause matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.pauseJob(jids)
        
def unpause(search="", sortby=[], limit=None):
    """Unpause matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.unpauseJob(jids)
        
def lock(search="", note=None, sortby=[], limit=None):
    """Lock matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.lockJob(jids, note=note)
        
def unlock(search="", sortby=[], limit=None):
    """Unlock matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.unlockJob(jids)
        
def interrupt(search="", sortby=[], limit=None):
    """Interrupt matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.interruptJob(jids)
        
def restart(search="", sortby=[], limit=None):
    """Restart matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.restartJob(jids)
        
def retryactive(search="", sortby=[], limit=None):
    """Retry all active tasks of matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.retryAllActiveInJob(jids)
        
def retryerrors(search="", sortby=[], limit=None):
    """Retry all errored tasks of matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.retryAllErrorsInJob(jids)
        
def skiperrors(search="", sortby=[], limit=None):
    """Skip all errored tasks of matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.skipAllErrorsInJob(jids)
        
def delay(search="", sortby=[], limit=None, aftertime=None):
    """Delay matching jobs.  aftertime= specifies the time at which
    the job should be undelayed.
    """
    if aftertime is None:
        raise MissingParameter("delay(): aftertime must be specified")
    elif isinstance(aftertime, datetime.datetime):
        aftertime = int(timeutil.date2secs(aftertime))
    elif isinstance(aftertime, str):
        try:
            aftertime = int(timeutil.date2secs(datetime.datetime.strptime(aftertime, "%Y-%m-%d %H:%M:%S")))
        except ValueError as err:
            raise InvalidValue("aftertime, %s, is not of the format Y-m-d H:M:S : %s" % \
                (aftertime, str(err)))
    elif not isinstance(aftertime, numbers.Number):
        raise InvalidValue("aftertime %s is not seconds after the epoch or a datetime object or string." % \
             str(aftertime))
    
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.delayJob(jids, aftertime)
        
def undelay(search="", sortby=[], limit=None):
    """Undelay matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.undelayJob(jids)
    
def delete(search="", sortby=[], limit=None):
    """Delete matching jobs."""
    jids = _jidsForArgs(search, sortby, limit)
    if jids:
        ModuleEngineClient.deleteJob(jids)
        
def undelete(search="", sortby=[], limit=None):
    """Restore matching jobs from the archive."""
    jids = _jidsForArgs(search, sortby, limit, archive=True)
    if jids:
        ModuleEngineClient.undeleteJob(jids)
        
def jobdump(search="", sortby=[], limit=None):
    """Fetch sql dump of job.  Dump output is returned in a
    dictionary keyed by jid.
    """
    jids = _jidsForArgs(search, sortby, limit, archive=True)
    dumpByJid = {}
    for jid in jids:
        dumpByJid[jid] = ModuleEngineClient.getJobDump(jid)
    return dumpByJid

# task operations

def _jidsTidsOthersForArgs(firstarg, sortby, limit, otherMembers=[]):
    """Determine the jids and tids for the specified tasks, which
    could be expressed as a dictionary, a list of dictionaries, or a
    search clause.
    """
    if firstarg == "":
        raise MissingSearchClause("A search clause must be specified.")
    if type(firstarg) is str:
        # user has specified a search string; fetch the tasks
        taskz = tasks(firstarg, columns=["jid", "tid"] + otherMembers, sortby=sortby, limit=limit)
    elif type(firstarg) is list:
        if sortby:
            raise SortNotAllowed("'sortby' is not allowed when passing a list of objects to an operation.")
        if limit:
            taskz = firstarg[:limit]
        else:
            taskz = firstarg
        _checkRequiredAttributes(taskz, ["jid", "tid"] + otherMembers)
    else:
        taskz = [firstarg]
        _checkRequiredAttributes(taskz, ["jid", "tid"] + otherMembers)
    jidsTidsOthers = [tuple([task[member] for member in ["jid", "tid"] + otherMembers]) for task in taskz]
    return jidsTidsOthers

def retry(search="", sortby=[], limit=None):
    """Retry matching tasks."""
    jidsTids = _jidsTidsOthersForArgs(search, sortby, limit)
    for jidTid in jidsTids:
        ModuleEngineClient.retryTask(jidTid[0], jidTid[1])

def resume(search="", sortby=[], limit=None):
    """Resume matching tasks."""
    jidsTids = _jidsTidsOthersForArgs(search, sortby, limit)
    for jidTid in jidsTids:
        ModuleEngineClient.resumeTask(jidTid[0], jidTid[1])

def kill(search="", sortby=[], limit=None):
    """Kill matching tasks."""
    jidsTids = _jidsTidsOthersForArgs(search, sortby, limit)
    for jidTid in jidsTids:
        ModuleEngineClient.killTask(jidTid[0], jidTid[1])

def skip(search="", sortby=[], limit=None):
    """Skip matching tasks."""
    jidsTids = _jidsTidsOthersForArgs(search, sortby, limit)
    for jidTid in jidsTids:
        ModuleEngineClient.skipTask(jidTid[0], jidTid[1])

def log(search="", sortby=[], limit=None):
    """Fetch logs of matching tasks.  Logs are returned in a
    dictionary keyed by (jid, tid).  Job.owner must be a key in the
    object.
    """
    # the requirement of Job.owner to be in the object may need to be relaxed for
    # sites that wish to use the API but don't organize their log files by owner.
    
    jidsTidsOwners = _jidsTidsOthersForArgs(search, sortby, limit, otherMembers=["Job.owner"])
    logByJidTid = {}
    for jidTidOwner in jidsTidsOwners:
        jid, tid, owner = jidTidOwner
        logByJidTid[(jid, tid)] = ModuleEngineClient.getTaskLog(jid, tid, owner)
    return logByJidTid

# command operations

def _jidsCidsForArgs(firstarg, sortby, limit):
    """Determine the jids and cids for the specified commmands, which
    could be expressed as a dictionary, a list of dictionaries, or a
    search clause.
    """
    if firstarg == "":
        raise MissingSearchClause("A search clause must be specified.")
    if type(firstarg) is str:
        # user has specified a search string; fetch the commands
        cmdz = commands(firstarg, columns=["jid", "cid"], sortby=sortby, limit=limit)
    elif type(firstarg) is list:
        if sortby:
            raise SortNotAllowed("'sortby' is not allowed when passing a list of objects to an operation.")
        if limit:
            cmdz = firstarg[:limit]
        else:
            cmdz = firstarg
        _checkRequiredAttributes(cmdz, ["jid", "cid"])
    else: # assume type(firstarg) is dict or issubclass(firstarg.__class__, EngineDB.Row)
        cmdz = [firstarg]
        _checkRequiredAttributes(cmdz, ["jid", "cid"])
    jidsCids = [(cmd["jid"], cmd["cid"]) for cmd in cmdz]
    return jidsCids

def cattr(search="", sortby=[], limit=None, key=None, value=None):
    """Set an attribute of matching commands.  key= specifies the
    attribute and value= specifies the new attribute value.
    """
    if key is None:
        raise MissingParameter("cattr(): key must be specified")
    if value is None:
        raise MissingParameter("cattr(): value is not specified")
    jidsCids = _jidsCidsForArgs(search, sortby, limit)
    for jidCid in jidsCids:
        ModuleEngineClient.setCommandAttribute(jidCid[0], jidCid[1], key, value)

def chkeys(search="", sortby=[], limit=None, keystr=None):
    """Set the service key expression of matching commands.  keystr=
    specifies the new service key expression.
    """
    if keystr is None:
        raise MissingParameter("chkeys(): keystr must be specified")
    jidsCids = _jidsCidsForArgs(search, sortby, limit)
    for jidCid in jidsCids:
        ModuleEngineClient.setCommandAttribute(jidCid[0], jidCid[1], "service", keystr)

# blade operations

def _namesIpaddrsForArgs(firstarg, sortby, limit):
    """Determine the names for the specified blades, which could be
    expressed as a dictionary, a list of dictionaries, or a search
    clause.
    """
    if firstarg == "":
        raise MissingSearchClause("A search clause must be specified.")
    if type(firstarg) is str:
        # user has specified a search string; fetch the blades
        bladez = blades(firstarg, columns=["name", "ipaddr"], sortby=sortby, limit=limit)
    elif type(firstarg) is list:
        if sortby:
            raise SortNotAllowed("'sortby' is not allowed when passing a list of objects to an operation.")
        if limit:
            bladez = firstarg[:limit]
        else:
            bladez = firstarg
        _checkRequiredAttributes(bladez, ["name", "ipaddr"])
    else: # assume type(firstarg) is dict or issubclass(firstarg.__class__, EngineDB.Row)
        bladez = [firstarg]
        _checkRequiredAttributes(bladez, ["name", "ipaddr"])
    namesIpaddrs = [(blade["name"], blade["ipaddr"]) for blade in bladez]
    return namesIpaddrs

def nimby(search="", allow=None, sortby=[], limit=None):
    """Nimby matching blades."""
    namesIpaddrs = _namesIpaddrsForArgs(search, sortby, limit)
    for nameIpaddr in namesIpaddrs:
        ModuleEngineClient.nimbyBlade(nameIpaddr[0], nameIpaddr[1], allow=allow)

def unnimby(search="", sortby=[], limit=None):
    """Unnimby matching blades."""
    namesIpaddrs = _namesIpaddrsForArgs(search, sortby, limit)
    for nameIpaddr in namesIpaddrs:
        ModuleEngineClient.unnimbyBlade(nameIpaddr[0], nameIpaddr[1])

def eject(search="", sortby=[], limit=None):
    """Retry active tasks of matching blades."""
    namesIpaddrs = _namesIpaddrsForArgs(search, sortby, limit)
    for nameIpaddr in namesIpaddrs:
        ModuleEngineClient.ejectBlade(nameIpaddr[0], nameIpaddr[1])

def delist(search="", sortby=[], limit=None):
    """Remove database entry for matching blades."""
    namesIpaddrs = _namesIpaddrsForArgs(search, sortby, limit)
    for nameIpaddr in namesIpaddrs:
        ModuleEngineClient.delistBlade(nameIpaddr[0], nameIpaddr[1])

def trace(search="", sortby=[], limit=None):
    """Fetch trace output of matching blades.  Output is returned as a
    dict keyed by (name, ipaddr).
    """
    namesIpaddrs = _namesIpaddrsForArgs(search, sortby, limit)
    traceByNameIpaddr = {}
    for nameIpaddr in namesIpaddrs:
        traceByNameIpaddr[nameIpaddr] = ModuleEngineClient.traceBlade(nameIpaddr[0], nameIpaddr[1])
    return traceByNameIpaddr
