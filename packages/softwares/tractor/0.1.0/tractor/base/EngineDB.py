"""
This module defines the schema of the Tractor engine's database using the rpg.sql ORM.
"""
import os

import tractor.base.rpg
from . import rpg.terminal as terminal
from . import rpg.progutil as progutil
from . import rpg.Formats as Formats

from . import rpg.sql.PGDatabase as PGDatabase
from . import rpg.sql.Fields as DBFields
from . import rpg.sql.Where as DBWhere
from . import rpg.sql.DBObject as DBObject
from . import rpg.sql.DBFormatter as DBFormatter
from . import rpg.sql.Table as Table
from . import rpg.sql.Function as Function
from . import rpg.sql.View as View
from . import rpg.sql.Index as Index

import tractor.base.EngineClient as EngineClient

ROLES_BY_BASE_ROLE = {
    "writeroles": [
        "bootstrap",     # locks database for engine; prevents multiple engines on same database
        "spooler",       # spool jobs
        "loader",        # reads for loading jobs; yes, it has write access but may not use it
        "archiver",      # archives or deletes jobs
        "dispatcher",    # updates for starting and stopping of commands, plus attribute edits
        "blademetrics",  # updates blade metrics
        "progress",      # updates task progress
        "annotator",     # performs miscellaneous low-load updates, like notes
        "dev_write"      # a role for devs to perform non-standard reads/writes
        ],
    "readroles":  [
        "tqreader",      # reads for tq command line tool, python query API, and dashboard query pane
        "dashboard",     # reads for dashboard, or other custom tools using URL API
        "dev_read"       # a role for devs to perform non-standard reads
        ]
    }

TABLE_OWNER = "archiver"

STATE_BLOCKED = "blocked"
STATE_READY = "ready"
STATE_ACTIVE = "active"
STATE_ERROR = "error"
STATE_DONE = "done"
ALL_STATES = (STATE_BLOCKED, STATE_READY, STATE_ACTIVE, STATE_ERROR, STATE_DONE)

class RunTypeEnumField(DBFields.Field):
    VALUES = ("regular", "cleanup", "post_always", "post_error", "post_done")
    FTYPE = "runtype_enum"
    def __init__(self, fieldname, **kwargs):
        super(RunTypeEnumField, self).__init__(fieldname, ftype=self.FTYPE, **kwargs)
        
class Job(DBObject.DBObject):
    """A Job consists of one or more dependent tasks.

    @ivar jid:         unique identifier of the job
    @ivar owner:       user that submitted the job
    @ivar spoolhost:   host where the job was submitted from
    @ivar spoolfile:   path to job spooled
    @ivar spoolcwd:    working directory of command that spooled job
    @ivar spooladdr:   ip address of host that spooled job
    @ivar title:       title of the job
    @ivar assignments: global job variable assignments

    @ivar tier:        the tier which is an ordered partitioning of the queue
    @ivar priority:    priority of this job to determine placement in the queue
    @ivar crews:       crews job is spooled under
    @ivar projects:    a list of designations which affects how the active tasks are counted in sharing limits
    @ivar tags:        limit tags applied to all commands of job
    @ivar service:     service key expression of job
    @ivar envkey:      environment key
    @ivar editpolicy:  name of policy affecting which users can manipulate job
    @ivar minslots:    minimum number of slots required to run a command
    @ivar maxslots:    maximum number of slots required to run a command
    @ivar etalevel:    level of job graph used to estimate remaining time to completion (unused)
    @ivar afterjids:   list of ids of jobs that must finish before this job is started

    @ivar spooltime:   time job was spooled
    @ivar pausetime:   time job was paused
    @ivar aftertime:   time until which job was / will be delayed
    @ivar starttime:   time the first task of job became active
    @ivar stoptime:    time job last processed a task (to error or done state)
    @ivar deletetime:  time this job was deleted from the scheduling system.
    @ivar elapsedsecs: total elapsed task seconds
    @ivar esttotalsecs: estimated total elapsed task seconds

    @ivar numtasks:    number of tasks in this job
    @ivar numblocked:  number of blocked tasks in this job
    @ivar numready:    number of ready tasks in this job
    @ivar numactive:   number of active tasks in this job
    @ivar numerror:    number of errored tasks in this job
    @ivar numdone:     number of done tasks in this job

    @ivar maxtid:      highest task id of all tasks of job, including detached ones
    @ivar maxcid:      highest command id of all commands of job, including ones of detached tasks

    @ivar comment:     job comment
    @ivar metadata:    user defined metadata
    @ivar maxactive:   the maximum number of concurrently active commands the job can have
    @ivar serialsubtasks: boolean indicating whether subtasks are to be executed serially
    @ivar dirmap:      a map for translating paths according to architecture
    @ivar pil:         place in line: ordering of job relative to other jobs of same priority; initially set to jid
    @ivar lastnoteid:  id of most recent note of job
    """

    Fields = [
        DBFields.BigIntField("jid", key=True),
        DBFields.TextField("owner", index=True, default=""),
        DBFields.TextField("spoolhost", index=True, default=""),
        DBFields.TextField("spoolfile", default=""),
        DBFields.TextField("spoolcwd",  default=""),
        DBFields.TextField("spooladdr", default=""),
        DBFields.TextField("title", default=""),
        DBFields.TextField("assignments", default=""),

        DBFields.TextField("tier", default=""),
        DBFields.FloatField("priority", ftype="real", index=True),
        DBFields.StrArrayField("crews"),
        DBFields.StrArrayField("projects"),
        DBFields.StrArrayField("tags"),
        DBFields.TextField("service", default=""),
        DBFields.StrArrayField("envkey"),
        DBFields.TextField("editpolicy"),
        DBFields.IntField("minslots", default=1),
        DBFields.IntField("maxslots", default=1),
        DBFields.IntField("etalevel", default=1),
        DBFields.IntArrayField("afterjids", default=[]),
            
        DBFields.TimestampField("spooltime", index=True),
        DBFields.TimestampField("pausetime", index=True),
        DBFields.TimestampField("aftertime", index=True),
        DBFields.TimestampField("starttime", index=True),
        DBFields.TimestampField("stoptime", index=True),
        DBFields.TimestampField("deletetime", index=True),
        DBFields.SecsFloatField("elapsedsecs", ftype="real"),
        DBFields.SecsFloatField("esttotalsecs", ftype="real"),

        DBFields.IntField("numtasks"),
        DBFields.IntField("numblocked"),
        DBFields.IntField("numready"),
        DBFields.IntField("numactive"),
        DBFields.IntField("numerror"),
        DBFields.IntField("numdone"),
        
        DBFields.IntField("maxtid"),
        DBFields.IntField("maxcid"),
        DBFields.TextField("comment", default=""),
        DBFields.TextField("metadata", default=""),
        DBFields.IntField("maxactive", default=0),
        DBFields.BooleanField("serialsubtasks", default=False),
        DBFields.JSONField("dirmap"),
        DBFields.BigIntField("pil"),
        DBFields.IntField("lastnoteid")
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)

    Aliases = {
        "user": "owner",
        "username": "owner",
        "cwd": "spoolcwd",
        "deleted": "deletetime",
        "jobid": "jid",
        "ntasks": "numtasks",
        "pri": "priority",
        "spooled": "spooltime",
        "elapsed": "elapsedsecs",
        "est": "esttotalsecs",
        }
    def getAliases(cls):
        return cls.Aliases
    getAliases = classmethod(getAliases)


class Task(DBObject.DBObject):
    """A Task object records the typically static information about a task.

    @ivar jid:         unique identifier for the job the tasks belong to
    @ivar tid:         unique identifier for the task within the job
    @ivar title:       task title
    @ivar id:          unique string id for the task within the job
    @ivar service:     service key expression of task
    @ivar minslots:    minimum number of slots required to run a command
    @ivar maxslots:    maximum number of slots required to run a command
    @ivar cids:        list of command ids of all commands
    @ivar serialsubtasks: boolean indicating whether subtasks are to be executed serially
    @ivar ptids:       task id of parent tasks (a task with associated Instances has multiple parents)
    @ivar attached:    if false, task was result of an expand task that was retried
    @ivar state:       task state
    @ivar statetime:   time that the task became in its current state
    @ivar readytime:   time that the task became ready
    @ivar activetime:  time that the task became active
    @ivar currcid:     cid of current command
    @ivar haslog:      boolean indicating whether task has output in log
    @ivar preview:     argv of preview command
    @ivar chaser:      argv of chaser command
    @ivar progress:    task progress
    @ivar metadata:    user defined metadata
    @ivar resumeblock: boolean indicating whether task denotes end of a resume block
    @ivar retrycount:  id which starts at 0; increments after each task retry or job restart
    """

    Fields = [
        DBFields.BigIntField("jid", key=True),
        DBFields.IntField("tid", key=True),
        DBFields.TextField("title", default=""),
        DBFields.TextField("id", default=""),
        DBFields.TextField("service", default=""),
        DBFields.IntField("minslots", default=1),
        DBFields.IntField("maxslots", default=1),
        DBFields.IntArrayField("cids"),
        DBFields.BooleanField("serialsubtasks", default=False),
        DBFields.IntArrayField("ptids"),
        DBFields.BooleanField("attached", default=True),
        DBFields.TextField("state", default=STATE_BLOCKED, index=True),
        DBFields.TimestampField("statetime", index=True),
        DBFields.TimestampField("readytime", index=True),
        DBFields.TimestampField("activetime", index=True),
        DBFields.IntField("currcid"),
        DBFields.BooleanField("haslog", default=False),
        DBFields.StrArrayField("preview"),
        DBFields.StrArrayField("chaser"),
        DBFields.FloatField("progress", ftype="real"),
        DBFields.TextField("metadata", default=""),
        DBFields.BooleanField("resumeblock", default=False),
        DBFields.IntField("retrycount", default=0),
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)

    Aliases = {
        "jobid": "jid",
        "taskid": "tid"
        }
    def getAliases(cls):
        return cls.Aliases
    getAliases = classmethod(getAliases)


class Command(DBObject.DBObject):
    """A Command object records  information about a command.

    @ivar jid:         unique identifier for the job the tasks belong to
    @ivar tid:         unique identifier for the task within the job
    @ivar cid:         id of command
    @ivar argv:        list of strings representing command
    @ivar local:       true if command is to be run on spooling host
    @ivar expand:      true if output of command emits script defining more tasks
    @ivar runtype:     indicates type of command ("regular", "cleanup", and others)
    @ivar msg:         a string to be piped into the command
    @ivar service:     service key expression of command
    @ivar tags:        limit tags applied to command
    @ivar id:          a string id for the command
    @ivar refersto:    id the command refers to
    @ivar minslots:    minimum number of slots required to run command
    @ivar maxslots:    maximum number of slots required to run command
    @ivar envkey:      env key
    @ivar retryrcodes: return codes that should induce auto-retry of command
    @ivar metadata:    user defined metadata
    @ivar resumewhile: list of return codes or command arguments for testing whether task can be resumed
    @ivar resumepin:   boolean indicating whether command should run on same host when resuming
    @ivar minrunsecs:  minimum number of second for command to run to not be considered an error
    @ivar maxrunsecs:  maximum number of second for command to run before being killed
    """

    Fields = [
        DBFields.BigIntField("jid", key=True),
        DBFields.IntField("tid"),
        DBFields.IntField("cid", key=True),
        DBFields.StrArrayField("argv", default=[]),
        DBFields.BooleanField("local", default=False),
        DBFields.BooleanField("expand", default=False),
        RunTypeEnumField("runtype", default="regular"),
        DBFields.TextField("msg", default=""),
        DBFields.TextField("service", default=""),
        DBFields.StrArrayField("tags"),
        DBFields.TextField("id", default=""),
        DBFields.TextField("refersto", default=""),
        DBFields.IntField("minslots", default=1),
        DBFields.IntField("maxslots", default=1),
        DBFields.StrArrayField("envkey"),
        DBFields.IntArrayField("retryrcodes"),
        DBFields.TextField("metadata", default=""),
        DBFields.StrArrayField("resumewhile"),
        DBFields.BooleanField("resumepin", default=False),
        DBFields.FloatField("minrunsecs", ftype="real"),
        DBFields.FloatField("maxrunsecs", ftype="real"),
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)

    Aliases = {
        "jobid": "jid",
        "taskid": "tid",
        "cmdid": "cid",
        "retryrc": "retryrcodes",
        "retrycodes": "retryrcodes"
        }
    def getAliases(cls):
        return cls.Aliases
    getAliases = classmethod(getAliases)


class Invocation(DBObject.DBObject):
    """An Invocation object records timing info on a command that executed on a blade.

    @ivar jid:         id of job
    @ivar tid:         id of task
    @ivar cid:         id of command
    @ivar iid:         id of invocation of this command; starts at 1
    @ivar current:     boolean indicating true if this is the most recent invocation; resumable commands can have multiple current invocations
    @ivar numslots:    number of slots used by the invocation
    @ivar limits:      a list of limits in use by the invocation
    @ivar starttime:   start time
    @ivar stoptime:    stop time
    @ivar pid:         process id of command invocation
    @ivar rss:         resident set size of process, in GB
    @ivar vsz:         memory usage of process, in GB
    @ivar cpu:         current cpu utilization of process
    @ivar elapsedapp:  elapsed user time of process, in seconds
    @ivar elapsedsys:  elapsed system time of process, in seconds
    @ivar elapsedreal: elapsed wall-clock time of process, in seconds
    @ivar rcode:       return code
    @ivar retrycount:  retry pass number; increments after each task retry or job restart
    @ivar resumecount: resume pass number; increments after each resume pass
    @ivar resumable:   boolean indicating whether command can be resumed
    @ivar bladeid:     bladeid of the blade the invocation is running or ran on
    """

    Fields = [
        DBFields.BigIntField("jid", key=True),
        DBFields.IntField("tid", key=True),
        DBFields.IntField("cid", key=True),
        DBFields.IntField("iid", key=True),
        DBFields.BooleanField("current", default=False),
        DBFields.IntField("numslots"),
        DBFields.StrArrayField("limits"),
        DBFields.TimestampField("starttime", index=True),
        DBFields.TimestampField("stoptime", index=True),
        DBFields.IntField("pid"),
        DBFields.GigaByteFloatField("rss"),
        DBFields.GigaByteFloatField("vsz"),
        DBFields.FloatField("cpu", ftype="real"),
        DBFields.SecsFloatField("elapsedapp", ftype="real"),
        DBFields.SecsFloatField("elapsedsys", ftype="real"),
        DBFields.SecsFloatField("elapsedreal", ftype="real"),
        DBFields.SmallIntField("rcode"),
        DBFields.IntField("retrycount"),
        DBFields.IntField("resumecount"),
        DBFields.BooleanField("resumable"),
        DBFields.UUIDField("bladeid", index=True)
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)

    Aliases = {
        "jobid": "jid",
        "taskid": "tid",
        "cmdid": "cid",
        "utime": "elapsedapp",
        "stime": "elapsedsys",
        "wtime": "elapsedreal",
        "mem": "vsz"
        }
    def getAliases(cls):
        return cls.Aliases
    getAliases = classmethod(getAliases)
 

class Blade(DBObject.DBObject):
    """Blades are the remote execution servers and can run one or more tasks.

    @ivar name:        blade name
    @ivar ipaddr:      ip address of the host
    @ivar port:        port
    @ivar osname:      operating system name
    @ivar osversion:   operating system version
    @ivar boottime:    boot time of the host
    @ivar numcpu:      number of cpus/cores of the host
    @ivar loadavg:     cpu load average of the host
    @ivar availmemory: available memory in Gb of the host
    @ivar availdisk:   availble disk space in Gb of the host
    @ivar version:     tractor blade version
    @ivar profile:     tractor profile
    @ivar nimby:       nimby status of blade
    @ivar starttime:   starttime of the blade process
    @ivar numslots:    total number of slots
    @ivar udi:         universal desirability index
    @ivar status:      status note
    @ivar heartbeattime:  time the blade last contacted the engine
    @ivar instanceid:  id of server instance running blade
    @ivar bladeid:     universally unique id of blade
    @ivar cleartime:   time a user requested the blade data be cleared
    @ivar gpulabel:    information regarding the gpu
    """
    
    Fields = [
        DBFields.TextField("name", index=True),
        DBFields.InetField("ipaddr"),
        DBFields.IntField("port"),
        DBFields.TextField("osname"),
        DBFields.TextField("osversion"),
        DBFields.TimestampField("boottime"),
        DBFields.SmallIntField("numcpu"),
        DBFields.FloatField("loadavg", ftype="real"),
        DBFields.GigaByteFloatField("availmemory"),
        DBFields.GigaByteFloatField("availdisk"),
        DBFields.TextField("version"),
        DBFields.TextField("profile"),
        DBFields.TextField("nimby"),
        DBFields.TimestampField("starttime"),
        DBFields.SmallIntField("numslots"),
        DBFields.FloatField("udi", ftype="real"),
        DBFields.TextField("status"),
        DBFields.TimestampField("heartbeattime"),
        DBFields.IntField("instanceid"),
        DBFields.UUIDField("bladeid", key=True),
        DBFields.TimestampField("cleartime"),
        DBFields.TextField("gpulabel")
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)

    Aliases = {
        "disk": "availdisk",
        "mem": "availmemory",
        "availmem": "availmemory",
        "cores": "numcpu",
        "load": "loadavg",
        "os": "osname",
        "instid": "instanceid",
        "bid": "bladeid"
        }
    def getAliases(cls):
        return cls.Aliases
    getAliases = classmethod(getAliases)


class BladeUse(DBObject.DBObject):
    """BladeUse tracks summary info for running command invocations.

    @ivar bladeid:     universally unique id of blade
    @ivar taskcount:   number of active tasks
    @ivar slotsinuse:  number of slots in use
    @ivar owners:      owners of active tasks
    """
    Fields = [
        DBFields.UUIDField("bladeid", key=True),
        DBFields.SmallIntField("taskcount"),
        DBFields.SmallIntField("slotsinuse"),
        DBFields.StrArrayField("owners")
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)


class Instance(DBObject.DBObject):
    """Instances are the host servers on which blades are run.

    @ivar instanceid:  tractor's id representing the (eventual) instance
    @ivar provider:    the computing service provider (AWS, Pixar, GC)
    @ivar requestid:   provider's id representing the request for an instance
    @ivar pinstanceid: the provider's instance id
    @ivar type:        the instance type
    @ivar reqstarttime: time at which request to start instance was made
    @ivar reqstoptime: time at which request to stop instance was made
    @ivar starttime:   time at which instance started (for billing purposes)
    @ivar stoptime:    time at which instance stopped (for billing purposes)
    @ivar status:      instance status: requested, started, stopped
    """
    
    Fields = [
        DBFields.SerialField("instanceid", key=True),
        DBFields.TextField("provider"),
        DBFields.TextField("requestid"),
        DBFields.TextField("pinstanceid"),
        DBFields.TextField("type"),
        DBFields.TimestampField("reqstartttime"),
        DBFields.TimestampField("starttime"),
        DBFields.TimestampField("stoptime"),
        DBFields.TimestampField("reqstoptime"),
        DBFields.TextField("status")
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)

    Aliases = {
        "instid": "instanceid",
        }
    def getAliases(cls):
        return cls.Aliases
    getAliases = classmethod(getAliases)


class Param(DBObject.DBObject):
    """A Param is a configuration or statistical parameter of the engine,
    essentially, a key/value store.
    @ivar name:        parameter name
    @ivar value:       parameter value
    """
    
    Fields = [
        DBFields.TextField("name", key=True),
        DBFields.TextField("value")
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)

    Aliases = {
        }
    def getAliases(cls):
        return cls.Aliases
    getAliases = classmethod(getAliases)


class Note(DBObject.DBObject):
    """The Note table contains both automated and manually generated notes for a jobs, tasks, and blades.
    @ivar noteid:      unique identifier for this note
    @ivar notetype:    the type of note (e.g. wrangler, user, listener, etc.)
    @ivar noteuser:    user that added this note
    @ivar notetime:    time the note was added
    @ivar notetext:    the actual note text
    @ivar itemtype:    the type of item referred to (e.g. job, task, blade)
    @ivar itemid:      the id of the item (the jid for a job note, jid,tid for a job note, name for a blade)
    """
    
    Fields = [
        DBFields.SerialField("noteid", key=True),
        DBFields.TextField("notetype", index=True),
        DBFields.TextField("noteuser", index=True),
        DBFields.TimestampField("notetime", index=True),
        DBFields.TextField("notetext"),
        DBFields.TextField("itemtype", index=True),
        DBFields.StrArrayField("itemid", index=True),
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)

    Aliases = {
        }
    def getAliases(cls):
        return cls.Aliases
    getAliases = classmethod(getAliases)

NoteIndexes = [
    Index.Index(tablename="note", name="note_cast_jid_idx",
                columns="itemtype, CAST(itemid[1] AS INT)",
                where="itemtype='job'"),
    Index.Index(tablename="note", name="note_cast_jid_tid_idx",
                columns="itemtype, CAST(itemid[1] AS INT), CAST(itemid[2] AS INT)",
                where="itemtype='task'"),
    Index.Index(tablename="note", name="note_cast_blade_idx",
                columns="itemtype, CAST(itemid[1] AS TEXT)",
                where="itemtype='blade'")
    ]


class JobLock(DBObject.DBObject):
    """The JobLock table contains an entry for every job that is locked.
    @ivar jid:         the job id of the locked jobs
    @ivar lockowner:   user that locked job
    @ivar locktime:    time the job was locked
    @ivar locknote:    a comment regarding the lock
    """
    
    Fields = [
        DBFields.BigIntField("jid", key=True),
        DBFields.TextField("lockowner", index=True),
        DBFields.TimestampField("locktime", index=True),
        DBFields.TextField("locknote")
        ]
    def getFields(cls):
        return cls.Fields
    getFields = classmethod(getFields)

    Aliases = {
        "jobid": "jid",
        "owner": "lockowner",
        "time": "locktime",
        "locked": "locktime",
        "note": "locknote"
        }
    def getAliases(cls):
        return cls.Aliases
    getAliases = classmethod(getAliases)


JobAliases = {
    "blocked": "numblocked > 0",
    "ready": "numready > 0",
    "active": "numactive > 0",
    "error": "numerror > 0",
    "done": "numdone=numtasks"
    }

StateAliases = {
    "blocked": "state=blocked",
    "ready": "state=ready",
    "active": "state=active",
    "error": "state=error",
    "done": "state=done"
    }

TaskAliases = StateAliases.copy()
CommandAliases = StateAliases.copy()
InvocationAliases = StateAliases.copy()
BladeAliases = {
    "up": "heartbeattime > -6m",
    "registered": "heartbeattime > cleartime or (heartbeattime and not cleartime)"
    }
InstanceAliases = {}
ParamAliases = {}

# TODO: add more table aliases here

# view definitions
# order matters here if some views use other views in their definition

VIEWS = [
    # --------------------------------------------------------------------------------
    View.View("jobinfo", """\
    SELECT DISTINCT ON (jid) spoolcwd AS cwd, envkey, jid, pil,
    ARRAY[EXTRACT(EPOCH FROM NOW())::integer, EXTRACT(EPOCH FROM spooltime)::integer, EXTRACT(EPOCH FROM starttime)::integer, EXTRACT(EPOCH FROM stoptime)::integer] AS jtimes,
    elapsedsecs AS "elapsedTaskSecs", esttotalsecs AS "estTotalTaskSecs",
    maxslots AS "maxSlots", minslots AS "minSlots", metadata, maxactive AS "maxActive",
    priority, (pausetime IS NOT NULL)::int AS paused,
    spoolfile AS sourcefile, spooladdr, SUBSTRING(spooltime::text from 1 for 19) AS spooldate, spoolhost,
    0 AS tid, title, owner AS user, tags, service, projects, tier, editpolicy, comment, crews, serialsubtasks, dirmap as dirmaps,
    EXTRACT(EPOCH FROM aftertime)::int as "afterTime",
    afterjids as "afterJids", '-B'::text AS state, '{}'::integer[] AS tdh,
    ARRAY[numtasks, numactive, numdone, numerror] AS "nTasks",
    lockowner, EXTRACT(EPOCH FROM locktime)::integer as locktime, locknote, lastnoteid
    FROM ONLY job LEFT JOIN joblock USING(jid)
    """),

    # --------------------------------------------------------------------------------
    View.View("jobinfowitharchives", """\
    SELECT DISTINCT ON (jid) spoolcwd AS cwd, envkey, jid, pil,
    ARRAY[EXTRACT(EPOCH FROM NOW())::integer, EXTRACT(EPOCH FROM spooltime)::integer, EXTRACT(EPOCH FROM starttime)::integer, EXTRACT(EPOCH FROM stoptime)::integer] AS jtimes,
    EXTRACT(EPOCH FROM deletetime)::integer AS deletetime,
    elapsedsecs AS "elapsedTaskSecs", esttotalsecs AS "estTotalTaskSecs",
    maxslots AS "maxSlots", minslots AS "minSlots", metadata, maxactive AS "maxActive",
    priority, (pausetime IS NOT NULL)::int AS paused,
    spoolfile AS sourcefile, spooladdr, SUBSTRING(spooltime::text from 1 for 19) AS spooldate, spoolhost,
    0 AS tid, title, owner AS user, tags, service, projects, tier, editpolicy, comment, crews, serialsubtasks, dirmap as dirmaps,
    EXTRACT(EPOCH FROM aftertime)::int as "afterTime",
    afterjids as "afterJids", '-B'::text AS state, '{}'::integer[] AS tdh,
    ARRAY[numtasks, numactive, numdone, numerror] AS "nTasks",
    lockowner, EXTRACT(EPOCH FROM locktime)::integer as locktime, locknote, lastnoteid
    FROM job LEFT JOIN joblock USING(jid)
    """),

    # --------------------------------------------------------------------------------
    View.View("SysLocks", """\
    SELECT locktype, relation::regclass,mode, transactionid AS tid,
    virtualtransaction AS vtid,pid, granted
    FROM pg_catalog.pg_locks l LEFT JOIN pg_catalog.pg_database db
    ON db.oid=l.database WHERE (db.datname='tractor' OR db.datname IS NULL)
    AND NOT pid = pg_backend_pid()
    """),

    # --------------------------------------------------------------------------------
    View.View("SysBlockingTrans", """\
    SELECT blockeda.pid AS blocked_pid, blockeda.usename as blocked_usename,
    blockeda.query as blocked_query,
    blockinga.pid AS blocking_pid, blockinga.usename as blocking_usename,
    blockinga.query as blocking_query
    FROM pg_catalog.pg_locks blockedl
    JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
    JOIN pg_catalog.pg_locks blockingl ON(blockingl.transactionid=blockedl.transactionid
    AND blockedl.pid != blockingl.pid)
    JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
    WHERE NOT blockedl.granted AND blockinga.datname='tractor'
    """),

    # --------------------------------------------------------------------------------
    View.View("SysBlockingRel", """\
    SELECT blockingl.relation::regclass,
    blockeda.pid AS blocked_pid, blockeda.query as blocked_query,
    blockedl.mode as blocked_mode,
    blockinga.pid AS blocking_pid, blockinga.query as blocking_query,
    blockingl.mode as blocking_mode
    FROM pg_catalog.pg_locks blockedl
    JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
    JOIN pg_catalog.pg_locks blockingl ON(blockingl.relation=blockedl.relation
    AND blockingl.locktype=blockedl.locktype AND blockedl.pid != blockingl.pid)
    JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
    WHERE NOT blockedl.granted AND blockinga.datname='tractor'
    """),

    # --------------------------------------------------------------------------------
    View.View("SysWaiting", """\
    SELECT pid, usename, query, now() - query_start  AS waiting_duration
    FROM pg_catalog.pg_stat_activity WHERE datname='tractor' AND waiting
    """),

    # --------------------------------------------------------------------------------
    View.View("SysLocksFull", """\
    SELECT
    COALESCE(blockingl.relation::regclass::text,blockingl.locktype) as locked_item,
    now() - blockeda.query_start AS waiting_duration, blockeda.pid AS blocked_pid,
    blockeda.usename as blocked_usename,
    blockeda.query as blocked_query, blockedl.mode as blocked_mode,
    blockinga.pid AS blocking_pid, blockinga.usename as blocking_usename,
    blockinga.query as blocking_query,
    blockingl.mode as blocking_mode
    FROM pg_catalog.pg_locks blockedl
    JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
    JOIN pg_catalog.pg_locks blockingl ON (
    (
    (blockingl.transactionid=blockedl.transactionid) OR
    (blockingl.relation=blockedl.relation AND blockingl.locktype=blockedl.locktype)
    )
    AND blockedl.pid != blockingl.pid
    )
    JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid AND blockinga.datid = blockeda.datid
    WHERE NOT blockedl.granted AND blockinga.datname = 'tractor'
    """)
    ]

# pyplython function definitions

FUNCTIONS = [
    # --------------------------------------------------------------------------------
    Function.Function("TractorNewJid", "", "integer", "SQL", """\

UPDATE param SET value=value::int+1 WHERE name='jidcounter' RETURNING value::int;
"""),
    
    # --------------------------------------------------------------------------------
    Function.Function("TractorSQLForSearchClause", "tablename text, clause text, aliasstr text", "text", "plpython2u", r"""

import ast
import tractor.base.EngineDB as EngineDB
import tractor.base.rpg
import rpg.sql.Where

db = EngineDB.EngineDB()

# lookup table
table = db.tableByName(tablename)
if not table:
    plpy.error("TractorSQLForSearchClause(): %s is not a valid table name" % str(tablename))

# convert JSON encoded alias dictionary to python
aliases = None
if aliasstr:
    try:
        aliases = ast.literal_eval(aliasstr)
    except (SyntaxError, ValueError), err:
        plpy.error("TractorSQLForSearchClause(): problem evaluating aliase string %s: %s" % (str(aliasstr), str(err)))

# generate SQL
try:
    return db._getWhereStr(table, clause, {}, [], aliases=aliases).replace(" WHERE ", "")
except rpg.sql.Where.WhereError, err:
    plpy.error("TractorSQLForSearchClause(): problem with search clause '%s': %s" % (str(clause), str(err)))
"""),
    
    # --------------------------------------------------------------------------------
    Function.Function("TractorPartitionCreate", "spooltime timestamp with time zone", "text", "plpython2u", r"""

# This function creates the necessary YYYY_MM partitions for the given timestamp if they do not exist.
# The _YYYY_MM suffix string is returned so that the caller can easily construct the
# target partition name.

import tractor.base.EngineDB as EngineDB

tables = ("job", "task", "command", "invocation", "note")
parts = spooltime.split("-")
year = int(parts[0])
month = int(parts[1])
suffix = "_%4d_%02d" % (year, month)
for table in tables:
    archiveTable = table + suffix
    # first check if archive partition already exists
    existsQuery = "SELECT 1 FROM information_schema.tables "\
                  "WHERE table_catalog=current_database() AND table_schema='public' "\
                  "AND table_name='%s'" % archiveTable
    result = plpy.execute(existsQuery)
    if len(result) == 0:
        # archive partition needs creation
        plpy.execute("CREATE TABLE %s (LIKE %s INCLUDING ALL)" % (archiveTable, table))
        # ensure new partition inherits all attributes from parent table
        plpy.execute("ALTER TABLE %s INHERIT %s" % (archiveTable, table))
        # ensure table owner has full permissions to modify new partition
        plpy.execute("GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE %s TO %s" % (archiveTable, EngineDB.TABLE_OWNER))
return suffix
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorPartitionDrop", "spooltime timestamp with time zone", "text", "plpython2u", r"""

# This function creates the necessary YYYY_MM partitions for the given timestamp if they do not exist.
# The _YYYY_MM suffix string is returned so that the caller can easily construct the
# target partition name.

tables = ("job", "task", "command", "invocation", "note")
parts = spooltime.split("-")
year = int(parts[0])
month = int(parts[1])
suffix = "_%4d_%02d" % (year, month)
for table in tables:
    archiveTable = table + suffix
    existsQuery = "SELECT 1 FROM information_schema.tables "\
                  "WHERE table_catalog=current_database() AND table_schema='public' "\
                  "AND table_name='%s'" % archiveTable
    result = plpy.execute(existsQuery)
    if len(result) == 1:
        plpy.execute("DROP TABLE %s" % archiveTable)

return suffix
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorPurgeLive", "", "text", "plpython2u", r"""

# This function removes all records associated with non-deleted jobs.

tables = ("job", "task", "command", "invocation", "note")
query = "TRUNCATE TABLE %s" % ", ".join(["ONLY %s" % table for table in tables])
try:
    plpy.execute(query)
except Exception, err:
    msg = "TractorPurgeLive(): Unable to truncate table %s: %s" % (table, str(err))
    plpy.error(msg)
return query
"""),

    #--------------------------------------------------------------------------------
    Function.Function("TractorPurgeArchive", "", "text", "plpython2u", r"""

# This function removes all records associated with deleted ("archived") jobs.

query = "SELECT table_name FROM information_schema.tables WHERE table_catalog='tractor' AND table_schema='public' AND table_name ~ '_\d\d\d\d_\d\d$';"
result = plpy.execute(query)

output = []
for row in result:
    table = row["table_name"]
    query = "DROP TABLE %s" % table
    try:
        plpy.execute(query)
    except Exception, err:
        msg = "TractorPurgeArchive(): Unable to drop table %s: %s" % (table, str(err))
        plpy.notice(msg)
        output.append(msg)
return "\n".join(output)
"""),

    #--------------------------------------------------------------------------------
    Function.Function("TractorPurgeArchiveToYearMonth", "year integer, month integer", "text", "plpython2u", r"""

# This function removes all records associated with deleted ("archived") jobs
# that were spooled in or before the specified year and month.

query = "SELECT table_name FROM information_schema.tables WHERE table_catalog='tractor' AND table_schema='public' AND table_name ~ '_\d\d\d\d_\d\d$';"
result = plpy.execute(query)

output = []
for row in result:
    table = row["table_name"]
    parts = table.split("_")
    tableMonth = int(parts[-1])
    tableYear = int(parts[-2])
    if year > tableYear or (year == tableYear and month >= tableMonth):
        query = "DROP TABLE %s" % table
        try:
            plpy.execute(query)
        except Exception, err:
            msg = "TractorPurgeArchiveToYearMonth(): Unable to drop table %s: %s" % (table, str(err))
            plpy.notice(msg)
            output.append(msg)

return "\n".join(output)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorResetJobCounter", "", "text", "plpython2u", r"""

# This function resets the jid counter so that job numbering starts at 1.

query = "UPDATE param SET value=0 WHERE name='jidcounter'"
plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorToggleArchiving", "doarchive integer", "text", "plpython2u", r"""

# This function resets the jid counter so that job numbering starts at 1.

query = "UPDATE param SET value=%d WHERE name='archiving'" % doarchive
plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorIsArchiving", "", "boolean", "plpython2u", r"""

# This function returns True if the system is configured for archiving jobs.

result = plpy.execute("SELECT value FROM param WHERE name='archiving'")
if len(result) > 0 and result[0]["value"] == "1":
    return True
else:
    return False
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobRestart", "jid integer", "text", "plpython2u", r"""

# This function does the necessary bookkeeping for restarting a job,
# which includes detaching all tasks that are descendents of an 
# expand task and resetting the job state counters.
# PRECONDITION: Active invocations must have been stopped and accounted for.

# the smallest tid whose parent is an expand task indicates the beginning of
# enumerating expand tasks; so all tids less than that are the tasks at job submission

query = "WITH expand_tasks AS "\
        "(SELECT task.jid AS jid,task.tid AS tid FROM ONLY task "\
        "JOIN ONLY command ON(task.jid=command.jid AND task.tid=command.tid) WHERE task.jid={jid} AND expand),"\
        "child_tasks AS "\
        "(SELECT jid,tid,ptids[1] as ptid FROM ONLY task WHERE jid={jid} and ptids[1] != 0) "\
        "SELECT MIN(child_tasks.tid) AS maxtid FROM child_tasks JOIN expand_tasks "\
        "ON(child_tasks.jid=expand_tasks.jid AND child_tasks.ptid=expand_tasks.tid)".format(jid=jid)

# a prior version of the above query that turned out to be much more expensive on a large job in a large data set
# query = "SELECT MIN(task.tid) AS maxtid FROM ONLY task JOIN ONLY command ON(command.jid=task.jid AND command.tid=ANY(task.ptids)) WHERE task.jid=%d AND expand" % jid

result = plpy.execute(query)

if len(result) and result[0]["maxtid"]:
    maxtid = result[0]["maxtid"]
    # detach tasks above maxtid
    query = "UPDATE ONLY task SET attached='f' WHERE jid=%d AND attached AND (tid>%d OR id='__TR_EXPAND_CHUNK__')" % (jid, maxtid-1)
    plpy.execute(query)
    hasExpands = True
else:
    #dml test for 2.x workaround for expand chunk results, set in dbstaging
    query = "UPDATE ONLY task SET attached='f' WHERE jid=%d AND attached AND id='__TR_EXPAND_CHUNK__'" % (jid)
    result = plpy.execute(query)
    hasExpands = result.nrows() > 0

# set all attached tasks to blocked
query = "UPDATE ONLY task SET state='blocked',statetime=NOW(),readytime=NULL,"\
        "activetime=NULL,haslog='f',progress=0,currcid=COALESCE(cids[1], 0),"\
        "retrycount=retrycount+CASE WHEN activetime IS NOT NULL THEN 1 ELSE 0 END "\
        "WHERE jid=%d AND attached" % jid
plpy.execute(query)

# update job counters by explicitly counting tasks (no fancy increment/decrement here)
query = "SELECT count(*) AS c FROM ONLY task WHERE jid=%d AND attached" % jid
result = plpy.execute(query);

numtasks = result[0]["c"]
updates = {
    "numblocked": numtasks, "numready": 0, "numactive": 0, "numdone": 0, "numerror": 0,
    "stoptime": "NULL", "starttime": "NULL", "elapsedsecs": 0, "esttotalsecs": 0
    }
if hasExpands:
    updates["numtasks"] = numtasks
query = "UPDATE ONLY job SET %s WHERE jid=%d" % (",".join(["%s=%s" % (k, str(v)) for k, v in updates.items()]), jid)
plpy.execute(query)

# invocations are no longer current

query = "UPDATE ONLY invocation SET current='f' WHERE jid=%d AND current" % jid
plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorTasksChangeState", "jid integer, tids text, state text, taskupdates text", "text", "plpython2u", r"""

# This function sets the state of the specified tasks and updates the job counters.

# first get a count of the existing task states so that job counters can be decremented correctly
query = "SELECT state,COUNT(*) AS c FROM ONLY task WHERE jid=%d AND tid IN (%s) GROUP BY state" % (jid, tids)
result = plpy.execute(query)
if not len(result):
    return

oldCounts = {}
for row in result:
    oldCounts[row["state"]] = row["c"]

updates = []
if taskupdates:
    updates.extend(taskupdates.split(","))
updates.append("state='%s'" % state)
# migrate tasks to requested state
plpy.execute("UPDATE ONLY task SET %s WHERE jid=%d AND tid IN (%s)" % (",".join(updates), jid, tids))

# iterate through results to tally new counter offsets
# convert counts into SQL SET arguments for UPDATE statement
updates = []
newCount = 0
for oldState, count in oldCounts.iteritems():
    if oldState != state:
        updates.append("num%s=num%s-%d" % (oldState, oldState, count))
        newCount += count
if newCount:
    updates.append("num%s=num%s+%d" % (state, state, newCount))

# update job counters
if updates:
    plpy.execute("UPDATE ONLY job SET %s WHERE jid=%d" % (",".join(updates), jid))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorTasksRetry", "jid integer, readytids text, blockedtids text, resuming boolean", "text", "plpython2u", r"""
    
# This function sets the specified tasks to a ready or blocked state.
updateStr = "activetime=NULL,statetime=NOW(),progress=0"
if not resuming:
    updateStr += ",retrycount=retrycount+CASE WHEN activetime IS NOT NULL THEN 1 ELSE 0 END"

if readytids:
    plpy.execute("SELECT TractorTasksChangeState(%d, '%s', 'ready', 'readytime=NOW(),%s')"
                 % (jid, readytids, updateStr))

if blockedtids:
    plpy.execute("SELECT TractorTasksChangeState(%d, '%s', 'blocked', 'readytime=NULL,%s')"
                 % (jid, blockedtids, updateStr))

# invocations are no longer current
if readytids and blockedtids:
    alltids = "%s,%s" % (readytids, blockedtids)
else:
    # only one list of tids must be set
    alltids = readytids + blockedtids
plpy.execute("UPDATE ONLY invocation SET current='f' WHERE jid=%d AND tid IN (%s) AND current" % (jid, alltids))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorTasksReady", "jid integer, tids text, readytime timestamp with time zone", "text", "plpython2u", r"""

# This function is called ONLY when a task becomes ready for its first command.  
plpy.execute("SELECT TractorTasksChangeState(%d, '%s', 'ready', 'readytime=''%s'',statetime=''%s''')"
             % (jid, tids, readytime, readytime))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorTaskNoCommandsDone", "jid integer, tid integer", "text", "plpython2u", r"""

# This function is called when a task that has no commands becomes done.
plpy.execute("SELECT TractorTasksChangeState(%d, '%d', 'done', 'statetime=NOW()')" % (jid, tid))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorTaskSkip", "jid integer, tid integer", "text", "plpython2u", r"""

# This function is called when a task has been skipped
plpy.execute("SELECT TractorTasksChangeState(%d, '%d', 'done', 'statetime=NOW()')" % (jid, tid))
# in the future, a note regarding the skipped task could be added
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorTaskHasLog", "jid integer, tid integer", "text", "plpython2u", r"""

# This function is called by the engine to indicate that a task has logs.
plpy.execute("UPDATE ONLY task SET haslog='t' WHERE jid=%d AND tid=%d AND haslog='f'" % (jid, tid))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorTasksHasLog", "tidstr text", "text", "plpython2u", r"""

# This function is called by the engine to indicate when one or more tasks have logs.
# The encoding is a space-separated list of jid:tid tuples, where a jid is followed by
# one or more comma-separted tids.
# e.g. '<jid1>:<tid1.1>[,<tid1.2>...] <jid2>:<tid2.1>[,<tid2.2>...]'  (or 123:1 125:2,10)

jobs = tidstr.split()
for job in jobs:
    jid,tids = job.split(":")
    plpy.execute("UPDATE ONLY task SET haslog='t' WHERE jid=%s AND tid IN (%s) AND haslog='f'" % (jid, tids))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobsUpdateTotalTime", "newtimes text", "text", "plpython2u", r"""

# Called by the engine to provide job total run-time estimates.
# The 'newtimes' arg is a space-separated list of tuples: jid,tmCur,tmEst
# where tmCur is the approximate current total job elapsed time,
# and tmEst is a projected estimate of elapsed time by job end.
# like:  '1001,20.2,50.7 1280,1.7,10587.3 ...'

query = "UPDATE ONLY job SET "\
        "elapsedsecs = tmp.elapsedsecs, "\
        "esttotalsecs = tmp.esttotalsecs "\
        "FROM (VALUES {values}) AS tmp(jid, elapsedsecs, esttotalsecs) "\
        "WHERE job.jid = tmp.jid".format(values=",".join(["(%s)" % item for item in newtimes.split()]))
plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorTasksUpdateProgress", "updatestr text", "text", "plpython2u", r"""

# This function is called by the engine to update the progress of one or more tasks.
# The encoding is a JSON-encoded list of jid, [tid, progress, ...] values, where a jid is followed by
# one or more comma-separted tids.
# e.g. '[<jid1>, [<tid1.1>, <progress1.1>, <tid1.2>, <progress1.2>, ...], <jid2>, [<tid2.1>, <progress2.1>, ...]'
# e.g. '[10101, [1, 50, 3, 75], 10202, [2, 99]]'

import json
ids = json.loads(updatestr)

# re-express list as dictionaries that can be accessed in (jid, tid) order
values = []
numJobs = len(ids) / 2
for j in range(numJobs):
    jid = ids[2*j]
    tidProgressPairs = ids[2*j + 1]
    numTasks = len(tidProgressPairs) / 2
    for t in range(numTasks):
        tid, progress = tidProgressPairs[2*t:2*(t+1)]
        values.append("(%d,%d,%d)" % (jid, tid, progress))

query = "UPDATE ONLY task SET progress=tmp.progress FROM (VALUES {values}) AS tmp(jid, tid, progress) "\
        "WHERE task.jid = tmp.jid AND task.tid = tmp.tid".format(values=",".join(values))
plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobsUpdatePils", "updatestr text", "text", "plpython2u", r"""

# This function is called by the engine to update the pils of one or more jobs.
# The encoding is a JSON-encoded list of [jid, pil] values.
# e.g. SELECT TractorJobsUpdatePils('[[10101, 10103], [10103, 10190], [10190, 10101]]');

import json
jidsPils = json.loads(updatestr)
# avoid creating invalid query if there are no updates
if not jidsPils:
    return
query = "UPDATE job SET pil=incoming.pil FROM (VALUES " + \
        ",".join(["(%d, %d)" % (jid, pil) for jid, pil in jidsPils]) + \
        ") AS incoming(jid, pil) WHERE job.jid=incoming.jid"
plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobsUpdatePilsPris", "updatestr text", "text", "plpython2u", r"""

# This function is called by the engine to update the pils and priorities of one or more jobs.
# The encoding is a JSON-encoded list of [jid, pil, pri] values.
# e.g. SELECT TractorJobsUpdatePilsPris('[[10101, 10103, 3], [10103, 10190, 10], [10190, 10101, 2]]');

import json
jidsPilsPris = json.loads(updatestr)
# avoid creating invalid query if there are no updates
if not jidsPilsPris:
    return
query = "UPDATE job SET pil=incoming.pil,priority=incoming.priority FROM (VALUES " + \
        ",".join(["(%d, %d, %f)" % (jid, pil, pri) for jid, pil, pri in jidsPilsPris]) + \
        ") AS incoming(jid, pil, priority) WHERE job.jid=incoming.jid"
plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorCommandStart", "jid integer, tid integer, cid integer, numslots integer, bladesstr text, limitsused text, resumecount integer, starttime timestamp with time zone, jobstarted boolean", "text", "plpython2u", r"""

# This function is called when a command has started.  The invocation record is created, and the
# task and job records are updated.

# determine the task retrycount
query = "SELECT retrycount FROM ONLY task WHERE jid=%d AND tid=%d" % (jid, tid)
result = plpy.execute(query)
if len(result):
    retrycount = result[0]["retrycount"] or 0
else:
    retrycount = 0

# determine next invocation id and add invocation record
query = "SELECT MAX(iid) AS maxiid FROM ONLY invocation WHERE jid=%d AND cid=%d" % (jid, cid)
result = plpy.execute(query)
if len(result) and result[0]["maxiid"]:
    iid = result[0]["maxiid"] + 1
else:
    iid = 1

limitsStr = "{%s}" % limitsused

# convert blade names to blade ids
# TODO: when engine passes a bladeid rather than a blade name, this call won't be necessary
bladeListStr = ",".join(["'%s'" % blade for blade in bladesstr.split(",")])
result = plpy.execute("SELECT bladeid FROM blade WHERE name IN (%s)" % bladeListStr)

# if no blades are found, then still write an invocation record with a blank blade id
bladeids = ["'{bladeid}'".format(bladeid=row["bladeid"]) for row in result] or ["NULL"]

for bladeid in bladeids:
    query = "INSERT INTO invocation (jid, tid, cid, iid, current, bladeid, limits, numslots, starttime, stoptime, pid, rcode, retrycount, resumecount, resumable) "\
            "VALUES (%d, %d, %d, %d, True, %s, '%s', %d, '%s', NULL, NULL, NULL, %d, %d, NULL)" \
            % (jid, tid, cid, iid, bladeid, limitsStr, numslots, starttime, retrycount, resumecount)
    iid += 1
    plpy.execute(query)
    # update the blade use table
    if bladeid != "NULL":
        updateQuery = "UPDATE BladeUse SET taskcount=taskcount+1,slotsinuse=slotsinuse+{numslots},"\
            "owners=owners || TractorOwnerForJid({jid}) "\
            "WHERE bladeid={bladeid} RETURNING bladeid".format(numslots=numslots,jid=jid,bladeid=bladeid)
        updateResult = plpy.execute(updateQuery)
        # insert record if one wasn't updated.
        if not len(updateResult):
            insertQuery = "INSERT INTO BladeUse VALUES ({bladeid}, 0, 0, '{{}}')".format(bladeid=bladeid)
            plpy.execute(insertQuery)
            plpy.execute(updateQuery)

# update task state and job counters
query = "SELECT TractorTasksChangeState(%d, '%d', 'active', 'activetime=COALESCE(activetime,NOW()),statetime=NOW(),currcid=%d,progress=0')"\
        % (jid, tid, cid)
plpy.execute(query)


# update the job starttime if necessary
if jobstarted:
    plpy.execute("UPDATE job SET starttime='%s' WHERE jid=%d AND starttime IS NULL" % (starttime, jid))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorCommandStop", "jid integer, tid integer, cid integer, rcode integer, nextstate text, stoptime timestamp with time zone, haslog boolean, resumable boolean, maxrss real, maxvsz real, elapsedapp real, elapsedsys real, elapsedreal real, cpu real", "text", "plpython2u", r"""

# This function is called when a command as stopped.  The invocation, task, and job records are updated.
# PRECONDITION: There must have been a complementary call to TractorCommandStart for the invocation record
# to exist, and for the job counters to be properly in sync.

# update invocation
query = "UPDATE ONLY invocation SET stoptime='%s',resumable='%s',"\
        "rss=%s,vsz=%s,elapsedapp=%s,elapsedsys=%s,elapsedreal=%s,cpu=%s,rcode=%d"\
        "WHERE jid=%d AND tid=%d AND cid=%d AND current RETURNING bladeid,numslots"\
        % (stoptime, "t" if resumable else "f",
        maxrss, maxvsz, elapsedapp, elapsedsys, elapsedreal, cpu, rcode,
        jid, tid, cid)
result = plpy.execute(query)


if not len(result):
    plpy.error("TractorCommandStop(): No invocation record found for jid={jid} AND tid={tid} AND cid={cid}".format(jid=jid, tid=tid, cid=cid))
    
# update the blade use table
for row in result:
    if row["bladeid"]: # bladeid could be empty/null if blade record happened to not be present when invocation started
        query = "UPDATE BladeUse SET taskcount=GREATEST(0, taskcount-1),slotsinuse=GREATEST(0, slotsinuse-{numslots}),"\
            "owners=TractorListRemove(TractorOwnerForJid({jid}), owners) "\
            "WHERE bladeid='{bladeid}'".format(numslots=row["numslots"],jid=jid,bladeid=row["bladeid"])
        plpy.execute(query)

# there is no task record to update if this is a job cleanup command
if tid == 0:
    return

# fetch the cids from task
result = plpy.execute("SELECT cids FROM ONLY task WHERE jid=%d AND tid=%d" % (jid, tid))
if not len(result):
    plpy.notice("TractorCommandStop(): no task record for task (jid=%d and tid=%d) cid=%d" % (jid, tid, cid))
    return

cids = result[0]["cids"]

# determine all task updates
updates = []
updates.append("statetime='%s'" % stoptime)    
if nextstate == "ready":
    # advance currcid to point to next command
    try:
        prevIndex = cids.index(cid)
        if len(cids) > prevIndex + 1:
            nextCid = cids[prevIndex + 1]
            updates.append("currcid=%d" % nextCid)
    except ValueError:
        pass
    updates.append("progress=0")
elif nextstate == "done":
    updates.append("progress=100")
if haslog:
    updates.append("haslog='t'")
# update task state and job counters
plpy.execute("SELECT TractorTasksChangeState(%d, '%d', '%s', '%s')" % (jid, tid, nextstate, ",".join(updates).replace("'", "''")))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobLock", "jid integer, lockowner text, locknote text", "text", "plpython2u", r"""

# This function is called to lock a job
plpy.execute("INSERT INTO JobLock VALUES(%d, '%s', NOW(), '%s')" % (jid, lockowner, locknote.replace("'", "''")))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobUnlock", "jid integer, unlockuser text", "text", "plpython2u", r"""

# This function is called to unlock a job; unlock user specified for future note support
plpy.execute("DELETE FROM JobLock WHERE jid=%d" % jid)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobDelete", "jid integer, deleteuser text", "text", "plpython2u", r"""

# This function deletes the specified job and its associated tasks, commands, 
# and invocations from the live dataset tables.  If archiving is turned on,
# they will first be copied to an archive partition.  If the archive partition
# does not yet exist, it will be created.
# PRECONDITION: Active commands must have been stopped and accounted for in their invocations.

tables = ("job", "task", "command", "invocation", "note")

# update deletetime of job
plpy.execute("UPDATE ONLY job SET deletetime=NOW() WHERE jid=%d" % jid)

# make a note of the user deleting the job
plpy.execute("SELECT TractorNoteAdd('comment', '{user}', 'job', '{jid}', 'delete job')".format(user=deleteuser, jid=jid))

# test to see if archiving is turned on
result = plpy.execute("SELECT TractorIsArchiving() AS archiving")
if len(result) > 0 and result[0]["archiving"]:
    archiving = True
else:
    archiving = False

suffix = None
if archiving:
    # create new partitions if they do not exist
    result = plpy.execute("SELECT TractorPartitionCreate(spooltime) AS suffix FROM ONLY job WHERE jid=%d" % jid);
    if len(result):
        suffix = result[0]["suffix"]

if suffix: # suffix will only be set if the job exists in the non-archive table
    # move rows to archive partitions
    for table in tables:
        archiveTable = table + suffix
        if table == "note":
            where = "itemtype='job' AND itemid[1]::int=%d" % jid
        else:
            where = "jid=%d" % jid
        query = "WITH moved_rows AS (DELETE FROM ONLY {table} WHERE {where} RETURNING *) INSERT INTO {archive} SELECT * FROM moved_rows".\
                format(table=table, where=where, archive=archiveTable)
        plpy.execute(query)
else:
    for table in tables:
        if table == "note":
            where = "itemtype='job' AND itemid[1]::int=%d" % jid
        else:
            where = "jid=%d" % jid
        query = "DELETE FROM ONLY %s WHERE %s" % (table, where)
        plpy.execute(query)

plpy.execute("DELETE FROM JobLock WHERE jid=%d" % jid)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobUndelete", "jid integer", "text", "plpython2u", r"""

# This function moves the specified job and its associated tasks, task commands, 
# and invocations from their corresponding archive partitions to the "live" parent  partition.

# update  deletetime of job
plpy.execute("UPDATE ONLY job SET deletetime=NULL WHERE jid=%d" % jid)

# move rows to archive partitions
tables = ("job", "task", "command", "invocation", "note")
for table in tables:
    if table == "note":
        where = "itemtype='job' AND itemid[1]::int=%d" % jid
    else:
        where = "jid=%d" % jid
    moveQuery = "WITH moved_rows AS (DELETE FROM %s WHERE %s RETURNING *) INSERT INTO %s SELECT * FROM moved_rows"\
        % (table, where, table)
    plpy.execute(moveQuery)
plpy.execute("UPDATE ONLY job SET deletetime=NULL WHERE jid=%d" % jid)
return ""
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobStopped", "jid integer, stoptime timestamp with time zone", "text", "plpython2u", r"""

# This function is called when the engine has determined the job has stopped and will be unloaded.

# update stoptime of job
plpy.execute("WITH total AS (SELECT EXTRACT(EPOCH FROM SUM(stoptime - starttime)) AS total "\
             "FROM invocation WHERE jid={jid} AND current) "\
             "UPDATE ONLY job SET elapsedsecs=total.total,esttotalsecs=total.total,stoptime='{stoptime}' "\
             "FROM total WHERE jid={jid}".format(jid=jid, stoptime=stoptime))
return ""
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorBladesUpdate", "updatestr text", "text", "plpython2u", r"""

# This function takes a list of blade updates, expressed as a python string
# of a list of dictionaries, where each dictionary specifies the blade name and
# the items to update.

import ast
try:
    updateEntries = ast.literal_eval(updatestr)
except ValueError, err:
    plpy.notice("TractorBladesUpdate: problem evaluating %s: %s" % (str(updatestr), str(err)))
    return

for update in updateEntries:
    blade = update.pop("name", None)
    if not blade:
        continue # for update
    # build up the query to update the blade
    updates = []
    for key, value in update.iteritems():
        # string values must have quotes placed around them -- except for time-related fields,
        # which are currently identified through their suffix "time"
        if type(value) is str and key[-4:] != "time":
            value = value.replace("'", "''") # ' fix formatting
            value = "'%s'" % value
        updates.append("%s=%s" % (key, str(value)))
    if updates:
        updateQuery = "UPDATE ONLY blade SET %s WHERE name='%s' RETURNING name" % (",".join(updates), blade)
        result = plpy.execute(updateQuery)
        if not len(result):
            # blade record must not exist, so insert one
            plpy.execute("SELECT TractorBladeInsert('%s')" % blade)
            # retry update query
            plpy.execute(updateQuery)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorBladeInsert", "name text", "text", "plpython2u", r"""

# This function inserts a new blade record with the given name.

query = "INSERT INTO blade (name, bladeid) "\
        "VALUES('%s', uuid_in(md5(random()::text || now()::text)::cstring))" % name
plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorBladeClear", "bladeid text", "text", "plpython2u", r"""

# This function sets the blade's cleartime in response to a user's clearing of blade data.

# TODO: must update this to bladeid when engine uses it as a key
# e.g. query = "UPDATE blade SET cleartime=NOW() WHERE bladeid='%s'" % bladeid

# set the cleartime of the blade by name (note: engine is sending blade name, even though bladeid is arg name)
query = "UPDATE blade SET cleartime=NOW() WHERE name='%s' RETURNING bladeid" % bladeid
result = plpy.execute(query)
if len(result):
    bid = result[0]["bladeid"]
    # clear record in case there are no matches in following query
    query = \
        "UPDATE BladeUse SET taskcount=0,slotsinuse=0,owners='{{}}' WHERE BladeUse.bladeid='{bid}'".format(bid=bid)
    plpy.execute(query)
    query = \
        "WITH b AS (SELECT "\
        "invocation.bladeid AS bladeid, COUNT(*) AS taskcount, SUM(numslots) AS slotsinuse, ARRAY_AGG(job.owner) AS owners "\
        "FROM ONLY invocation LEFT JOIN ONLY job USING(jid) "\
        "WHERE invocation.stoptime IS NULL AND invocation.current AND invocation.bladeid='{bid}' "\
        "GROUP BY invocation.bladeid) "\
        "UPDATE BladeUse SET taskcount=b.taskcount,slotsinuse=b.slotsinuse,owners=b.owners "\
        "FROM b WHERE BladeUse.bladeid=b.bladeid".format(bid=bid)
    plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorSelect", "tablename text, whereclause text, columnsstr text, orderbystr text, limitval integer, archive boolean, aliasstr text", "text", "plpython2u", r"""

# This function builds a SELECT clause for the specified table, 
# using the rpg.sql framework to convert the natural language
# where clause to a SQL where clause, and determine the necessary
# JOINs to display the requested columns.  Columns ordering may also 
# be specified.  The columnsstr and orderbystr are comma-separated
# strings of column names.  Descending order is specified with "-"
# for orderbystr
# e.g. 'user,spoolhost' or 'user,-spoolhost'
# NOTE: tablename must be capitalized according to python class names. 
# e.g. 'Task' (not 'task') 

import sys, json, itertools, re, ast
import tractor.base.rpg
import rpg.tracebackutil as tracebackutil
import tractor.base.EngineDB as EngineDB

db = EngineDB.EngineDB()
columns = None if not columnsstr else columnsstr.split(",")
orderby = None if not orderbystr else orderbystr.split(",")
table = db.tableByName(tablename)
aliases = None
if aliasstr:
    try:
        aliases = ast.literal_eval(aliasstr)
    except (SyntaxError, ValueError), err:
        plpy.error("TractorSelect: problem evaluating aliase string %s: %s" % (str(aliasstr), str(err)))

try:
    # establish value for limit
    limit = limitval
    if limit is None:
        # limit was not specified; use site default
        result = plpy.execute("SELECT value FROM param WHERE name='maxrecords'")
        if len(result) and result[0]["value"].isdigit():
            limit = int(result[0]["value"])
    # generate SQL and perform query
    query = db._select(table, where=whereclause, members=columns, orderby=orderby,
                       limit=limit, only=not archive, aliases=aliases)
    queryResult = plpy.execute(query)
    # return result as json list of dictionaries
    return json.dumps(list(queryResult))

except Exception, err:
    tb = tracebackutil.getTraceback()
    return json.dumps(tb)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobinfoFile", "jid integer", "text", "plpython2u", r"""
    
# Return a json string matching a 1.x on-disk representationd of the job.
# Some additional values have been computed for use in q=jobs queries.

import json

result = plpy.execute("SELECT * FROM jobinfo WHERE jid=%d" % jid)
if len(result) == 0:
    return "{}"
jobinfo = dict(result[0])

# add cids of job postscript commands
result = plpy.execute("SELECT cid FROM ONLY command WHERE jid=%d AND tid=0" % jid)
cids = [row["cid"] for row in result]
if cids:
    jobinfo["cids"] = cids
# unpack json-encoded values
if jobinfo["dirmaps"]:
    jobinfo["dirmaps"] = json.loads(jobinfo["dirmaps"])
# job is done if all tasks are done, otherwise blocked (default in jobinfo view)
if jobinfo["nTasks"][0]==jobinfo["nTasks"][2]:
    jobinfo["state"] = "-D"

return json.dumps(jobinfo)

"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorTasktreeFile", "jid integer", "text", "plpython2u", r"""
    
# Return a json string matching a 1.x on-disk representationd of the tasktree file.
# Some additional values have been computed for use in q=jtree queries.

import json
import tractor.base.plpyutil as plpyutil

result = plpy.execute("SELECT tid,id,ptids,title,service,cids,state,haslog,preview,"
                      "chaser,minslots,maxslots,serialsubtasks,resumeblock "
                      "FROM ONLY task WHERE jid=%d AND attached ORDER BY tid" % jid)
tasktree = plpyutil.tasktreeForRows(result)
return json.dumps(tasktree)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorCmdlistFile", "jid integer", "text", "plpython2u", r"""
    
# return a json string matching a 1.x on-disk representationd of the cmdlist file.

import json
import tractor.base.plpyutil as plpyutil

import time

query = "SELECT cid,argv,msg,command.service as service,"\
        "tags,local,expand,runtype,envkey,retryrcodes,command.id as id,refersto,"\
        "command.minslots as minslots,command.maxslots as maxslots,"\
        "resumewhile,resumepin,state,minrunsecs,maxrunsecs "\
        "FROM ONLY command LEFT JOIN ONLY task USING(jid, tid) "\
        "WHERE jid=%d AND (attached OR command.tid=0) " % jid
cmds = plpy.execute(query)

query = "SELECT DISTINCT ON (cid) "\
        "jid,cid,iid,rcode,current,resumable,"\
        "EXTRACT(EPOCH FROM invocation.starttime) AS starttime,"\
        "EXTRACT(EPOCH FROM stoptime) AS stoptime,"\
        "bladetable.name AS blade "\
        "FROM ONLY invocation LEFT JOIN blade AS bladetable USING (bladeid) "\
        "WHERE jid={jid} ORDER BY cid,iid DESC".format(jid=jid)
invos = plpy.execute(query)

cmdlist = plpyutil.cmdlistForRows(cmds, invos)
return json.dumps(cmdlist)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJSONJobsForSQL", "sqlclause text, limitval integer, archives bool, withmetadata bool", "text", "plpython2u", r"""
    
# This function returns the tractor job list in a dashboard-friendly format.

import json

sqlstr = sqlclause.strip()
# restrict search to deleted jobs if archives has been set
if archives:
    view = "jobinfowitharchives"
    viewColumns = ["deletetime"] # only archive view has deletetime
    onlyStr = ""
    if sqlstr:
        sqlstr = "(%s) AND deletetime IS NOT NULL" % sqlstr
    else:
        sqlstr = "deletetime IS NOT NULL"
else:
    view = "jobinfo"
    viewColumns = []
    onlyStr = "ONLY"

if sqlstr:
    sqlstr = "WHERE " + sqlstr

limit = limitval
if limit is None:
    # limit was not specified; use site default
    result = plpy.execute("SELECT value FROM param WHERE name='maxrecords'")
    if len(result) and result[0]["value"].isdigit():
        limit = int(result[0]["value"])

# only limit if a positive value has been specified as an arg or by maxrecords
limitstr = "LIMIT %d" % limit if limit else ""
# only sort values if there is a limit
orderby = "ORDER BY jid DESC" if limitstr else ""

# first determine matching jobs -- avoid JOINing with the jobinfo* views to avoid long query times!
query = "SELECT jid FROM {only} job {sql} {order} {limit}" \
        .format(only=onlyStr, sql=sqlstr, order=orderby, limit=limitstr)
result = plpy.execute(query)
jids = [str(row["jid"]) for row in result]

if jids:
    # only get required fields; in particular, metadata can be huge, but not required
    columns = ["cwd","envkey","jid","pil","jtimes","elapsedTaskSecs","estTotalTaskSecs",
                 "maxSlots","minSlots","maxActive","priority","paused","sourcefile",
                 "spooladdr","spooldate","spoolhost","tid","title","user","tags","service","projects",
                 "tier","editpolicy","comment","crews","serialsubtasks","dirmaps","afterTime",
                 "afterJids","state","tdh","nTasks","lockowner","locktime","locknote","lastnoteid"
               ] + viewColumns
    
    if withmetadata:
        columns.append("metadata")
    columnStr = ",".join(['"%s"' % column for column in columns])
    query = "select {columns} from {view} WHERE jid in ({jids})".\
            format(columns=columnStr, view=view, jids=",".join(jids))
    result = plpy.execute(query)
else:
    result = []

jobsByUser = {}
for row in result:
    jobinfo = dict(row)
    jobsByUser.setdefault(jobinfo["user"], {})
    jobsByUser[jobinfo["user"]][str(jobinfo["jid"])] = {"data": jobinfo}

# NOTE: in 1.x, numVisible is the number of ALL jobs, not just the matching ones; but that
# may not be cheap to calculate; can revisit this if deemed important
numVisible = len(result) 
isTruncated = 0 if limit and numVisible < limit else 1
return json.dumps({"users": jobsByUser,
                   "recordlimit": {"limit": limit, "visible": numVisible, "truncated": isTruncated}})

"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJSONBladesForSQL", "sqlclause text, limitval integer, archives bool", "text", "plpython2u", r"""
    
# This function returns the tractor blade list in a dashboard-friendly format.

# The archives argument is currently not used, but will likely be employed to target cleared/unresponsive blades

import json, time

sqlstr = sqlclause.strip()
# do not return "cleared" records, based on cleartime
if sqlstr:
    sqlstr = "WHERE (%s) AND (cleartime < heartbeattime OR cleartime IS NULL) " % sqlstr
else:
    sqlstr = "WHERE (cleartime < heartbeattime OR cleartime IS NULL)"

limit = limitval
if limit is None:
    # limit was not specfied; use site default
    result = plpy.execute("SELECT value FROM param WHERE name='maxrecords'")
    if len(result) and result[0]["value"].isdigit():
        limit = int(result[0]["value"])

# only limit if a positive value has been specified as an arg or by maxrecords
limitstr = "LIMIT %d" % limit if limit else ""

# go ahead and read blade records, with limit in place
result = plpy.execute("SELECT COALESCE(name, CONCAT('instance-', instanceid::text)) AS hnm, "
                      "bladeid, ipaddr AS addr, port AS lp, version AS vers, profile, "
                      "osname, osversion, gpulabel, "
                      "EXTRACT (EPOCH FROM heartbeattime) AS t, "
                      "EXTRACT (EPOCH FROM blade.starttime) AS t0, "
                      "numcpu AS ncpu, COALESCE(numslots, 0) AS ns, COALESCE(slotsinuse, 0) AS siu, "
                      "owners, "
                      "COALESCE(taskcount, 0) AS numcmd, "
                      "numslots - COALESCE(slotsinuse, 0) AS as, "
                      "udi, loadavg AS cpu, availmemory AS mem, availdisk AS disk, nimby, blade.status AS note "
                      "FROM ONLY blade LEFT JOIN BladeUse USING(bladeid) "
                      "FULL JOIN instance USING(instanceid) "
                      "{sqlstr} ORDER BY name {limitstr}".format(sqlstr=sqlstr, limitstr=limitstr))

# TODO: may not want to show old instances -- may have status or age

blades = []
for row in result:
    blades.append(dict(row))

# NOTE: in 1.x, numVisible is the number of ALL jobs, not just the matching ones; but that
# may not be cheap to calculate; can revisit this if deemed important
numVisible = len(blades) # NOTE: in 1.x, this value was the number of ALL blades
isTruncated = 0 if limit and numVisible < limit else 1
return json.dumps({"timestamp": int(time.time()), "blades": blades,
                   "recordlimit": {"limit": limit, "visible": numVisible, "truncated": isTruncated}})
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJSONBladeInfo", "bladename text", "text", "plpython2u", r"""
    
# This function returns a JSON dictionary of the blade info for the specified blade.

# TODO: bladeid, not bladename, should be specified

import json, time

# fetch blade record
# go ahead and read blade records, with limit in place

# if instance has been requested and no blade is running, just use
# the request id to find instance record
if bladename[:9] == "instance-":
    whereStr = "instanceid='%s'" % bladename[9:]
else:
    whereStr = "name='%s'" % bladename

result = plpy.execute("SELECT COALESCE(name, CONCAT('instance-', instanceid::text)) AS hnm, "
                      "bladeid, ipaddr AS addr, port AS lp, version AS vers, profile, "
                      "osname, osversion, gpulabel, "
                      "EXTRACT (EPOCH FROM heartbeattime) AS t, "
                      "EXTRACT (EPOCH FROM blade.starttime) AS t0, "
                      "EXTRACT (EPOCH FROM boottime) AS boottime, "
                      "numcpu AS ncpu, numslots AS ns, COALESCE(slotsinuse, 0) AS siu, "
                      "owners, "
                      "COALESCE(taskcount, 0) AS numcmd, "
                      "numslots - COALESCE(slotsinuse, 0) AS as, "
                      "udi, loadavg AS cpu, availmemory AS mem, availdisk AS disk, nimby, blade.status AS note, "
                      "provider, pinstanceid, type "
                      "FROM ONLY blade LEFT JOIN BladeUse USING(bladeid) "
                      "FULL JOIN instance USING(instanceid) "
                      "WHERE %s" % whereStr)

if len(result) == 0:
    # early out if no matching records
    return "{}"
bladeinfo = dict(result[0])

# get all active invocations so that number of active slots can be calculated and commands reported
result = plpy.execute("SELECT jid,command.tid as tid,cid,owner as juser,argv,"
                      "EXTRACT(EPOCH FROM invocation.starttime) AS t,invocation.numslots AS numslots "
                      "FROM ONLY invocation JOIN command USING(jid,cid) JOIN job USING(jid) "
                      "LEFT JOIN blade USING (bladeid) "
                      "WHERE invocation.stoptime IS NULL AND current AND blade.name='%s'" % bladename)
bladeinfo["cmds"] = list(result)

# get all notes for blade
result = plpy.execute("SELECT notetype,noteuser,notetext,EXTRACT(EPOCH FROM notetime) AS notetime "\
                      "FROM note WHERE itemtype='blade' AND itemid[1]='{bladeid}' ORDER BY noteid".\
                      format(bladeid=bladeinfo["bladeid"]))
bladeinfo["notes"] = list(result)

return json.dumps(bladeinfo)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobDetails", "jid integer, getgraph bool, getnotes bool", "text", "plpython2u", r"""

# This function returns the job details in a dashboard-friendly format.  Job graph
# and job note inforamtion are optionally retrieved.

import json
import tractor.base.plpyutil as plpyutil

jobdetails = {}

# get job info
isArchived = False
result = plpy.execute("SELECT * FROM jobinfo WHERE jid=%d" % jid)
if len(result) == 0:
    # this may be an archived job
    result = plpy.execute("SELECT * FROM jobinfowitharchives WHERE jid=%d" % jid)
    if len(result) == 0:
        return "{}"
    isArchived = True
row = result[0]
jobinfo = dict(row)

# target non-archive partitions if not is not archived with ONLY preamble to table names
onlyStr = "" if isArchived else "ONLY "

# add cids of job postscript commands
result = plpy.execute("SELECT cid FROM {ONLY} command WHERE jid={jid} AND tid=0".format(ONLY=onlyStr, jid=jid))
jobinfo["cids"] = [row["cid"] for row in result]
    
jobdetails["data"] = jobinfo

if getgraph:
    # the dashboard needs to know whether to decorate a task as "resumable"
    # this is done by checking the resumable attribute of the most recently
    # executed invocation of each command of the task; the cids of all of the
    # commands that are resumable are aggregated into a value "rcids"

    # the calculation of these rcids can be done by first locating the
    # maximum invocation id of each command and intersecting that with the
    # maximum invocation id of each command that is both current and resumable.
    # the intersection is a handy and efficient workaroud the limitations
    # of SQL in that you can't query the columns of the row that has the
    # maximum value.

    query = "WITH resumable_iids AS ("\
            "SELECT jid,tid,cid,MAX(iid) AS iid FROM {ONLY} invocation "\
            "WHERE jid={jid} AND stoptime IS NOT NULL GROUP BY jid,tid,cid "\
            "INTERSECT "\
            "SELECT jid,tid,cid,MAX(iid) AS iid FROM {ONLY} invocation "\
            "WHERE jid={jid} AND stoptime IS NOT NULL AND current AND resumable GROUP BY jid,tid,cid), "\
            "resumable_cids AS (SELECT tid, ARRAY_AGG(cid) AS rcids FROM resumable_iids GROUP BY jid,tid) "\
            "SELECT task.tid as tid,id,ptids,title,service,cids,state,haslog,progress,"\
            "preview,chaser,minslots,maxslots,serialsubtasks,"\
            "EXTRACT(EPOCH FROM statetime) AS statetime,"\
            "EXTRACT(EPOCH FROM activetime) AS activetime,"\
            "bladetable.name AS blade,rcode,"\
            "resumable_cids.rcids as rcids "\
            "FROM {ONLY} task LEFT JOIN {ONLY} invocation "\
            "ON(task.jid=invocation.jid AND task.tid=invocation.tid AND "\
            "task.currcid=invocation.cid AND invocation.current) "\
            "LEFT JOIN resumable_cids ON(task.tid=resumable_cids.tid) "\
            "LEFT JOIN blade AS bladetable USING (bladeid) "\
            "WHERE task.jid={jid} AND attached ORDER BY tid".format(ONLY=onlyStr, jid=jid)
    result = plpy.execute(query)
    jobdetails["children"] = plpyutil.tasktreeForRows(result)["children"]
 
if getnotes:
    result = plpy.execute("SELECT notetype,noteuser,notetext,EXTRACT(EPOCH FROM notetime) as notetime "\
                          "FROM {ONLY} note WHERE itemtype='job' AND itemid[1]::int={jid} "\
                          "ORDER BY noteid".format(ONLY=onlyStr, jid=jid))
    jobdetails["notes"] = list(result)    

# build result
result = {"users": {jobinfo["user"]: {"J%d" % jobinfo["jid"]: jobdetails}}}
retval = json.dumps(result, indent=1, separators=(",", ":"))
return retval
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobDetailsFlat", "jid integer, getgraph bool, getnotes bool", "text", "plpython2u", r"""

# This function returns the job details in a dashboard-friendly format.  Job graph
# and job note inforamtion are optionally retrieved.
# This is an optimized version of TractorJobDetails.

import json
import tractor.base.plpyutil as plpyutil

# get job info
isArchived = False
result = plpy.execute("SELECT * FROM jobinfo WHERE jid=%d" % jid)
if len(result) == 0:
    # this may be an archived job
    result = plpy.execute("SELECT * FROM jobinfowitharchives WHERE jid=%d" % jid)
    if len(result) == 0:
        return "{}"
    isArchived = True
row = result[0]
jobinfo = dict(row)

# target non-archive partitions if not is not archived with ONLY preamble to table names
onlyStr = "" if isArchived else "ONLY "

# add cids of job postscript commands
result = plpy.execute("SELECT cid FROM {ONLY} command WHERE jid={jid} AND tid=0".format(ONLY=onlyStr, jid=jid))
jobinfo["cids"] = [row["cid"] for row in result]
jobinfoStr = json.dumps(jobinfo, indent=1, separators=(",", ":"))

tasksResult = [{"tasks": '[]'}]
if getgraph:
    # the dashboard needs to know whether to decorate a task as "resumable"
    # this is done by checking the resumable attribute of the most recently
    # executed invocation of each command of the task; the cids of all of the
    # commands that are resumable are aggregated into a value "rcids"

    # the calculation of these rcids can be done by first locating the
    # maximum invocation id of each command and intersecting that with the
    # maximum invocation id of each command that is both current and resumable.
    # the intersection is a handy and efficient workaroud the limitations
    # of SQL in that you can't query the columns of the row that has the
    # maximum value.

    query = "SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(t))) AS tasks "\
            "FROM ( "\
            "WITH resumable_iids AS ("\
            "SELECT jid,tid,cid,MAX(iid) AS iid FROM {ONLY} invocation "\
            "WHERE jid={jid} AND stoptime IS NOT NULL GROUP BY jid,tid,cid "\
            "INTERSECT "\
            "SELECT jid,tid,cid,MAX(iid) AS iid FROM {ONLY} invocation "\
            "WHERE jid={jid} AND stoptime IS NOT NULL AND current AND resumable GROUP BY jid,tid,cid), "\
            "resumable_cids AS (SELECT tid, ARRAY_AGG(cid) AS rcids FROM resumable_iids GROUP BY jid,tid) "\
            "SELECT task.tid as tid,id,ptids,title,service,cids,state,haslog,progress/100.0 AS progress,"\
            "preview,chaser,minslots,maxslots,serialsubtasks,"\
            "EXTRACT(EPOCH FROM statetime) AS statetime,"\
            "EXTRACT(EPOCH FROM activetime) AS activetime,"\
            "bladetable.name AS blade,rcode,"\
            "resumable_cids.rcids as rcids,EXTRACT(EPOCH FROM invocation.stoptime) AS stoptime "\
            "FROM {ONLY} task LEFT JOIN {ONLY} invocation "\
            "ON(task.jid=invocation.jid AND task.tid=invocation.tid AND "\
            "task.currcid=invocation.cid AND invocation.current) "\
            "LEFT JOIN resumable_cids ON(task.tid=resumable_cids.tid) "\
            "LEFT JOIN blade AS bladetable USING (bladeid) "\
            "WHERE task.jid={jid} AND attached ORDER BY tid "\
            ") t".format(ONLY=onlyStr, jid=jid)
    tasksResult = plpy.execute(query)

notesResult = [{"notes": '[]'}]
if getnotes:
    notesResult = plpy.execute(
        "SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(t))) AS notes "\
        "FROM ( "\
        "SELECT notetype,noteuser,notetext,EXTRACT(EPOCH FROM notetime) as notetime "\
        "FROM {ONLY} note WHERE itemtype='job' AND itemid[1]::int={jid} "\
        "ORDER BY noteid"\
        ") t".format(ONLY=onlyStr, jid=jid))

# build result
retval = '{"data": %s,\n "notes": %s,\n "tasks": %s}' % (jobinfoStr, notesResult[0]["notes"] or "[]", tasksResult[0]["tasks"] or "[]")
return retval
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJSONTaskDetails", "jid integer, tid integer", "text", "plpython2u", r"""
    
# This function returns the task details in a dashboard-friendly format.

import json
import tractor.base.plpyutil as plpyutil

# see if job has been archived
isArchived = len(plpy.execute("SELECT 1 FROM job WHERE jid=%d AND deletetime IS NOT NULL" % jid)) > 0
# target non-archive partitions if not is not archived with ONLY preamble to table names
onlyStr = "" if isArchived else "ONLY"

# get commands for task
cmdrows = plpy.execute("SELECT cid,argv,command.service as service,tags,runtype,"
                       "local,expand,envkey,refersto,command.id as cmdid,"
                       "state,EXTRACT(EPOCH FROM statetime) AS statetimesecs,"
                       "command.minslots as minslots,command.maxslots as maxslots,"
                       "minrunsecs,maxrunsecs,"
                       "command.metadata as metadata,task.metadata as taskmetadata "
                       "FROM {ONLY} command LEFT JOIN {ONLY} task USING(jid, tid) "
                       "WHERE jid={jid} AND command.tid={tid} ORDER BY cid ASC".format(
                           ONLY=onlyStr, jid=jid, tid=tid))
invorows = plpy.execute("SELECT cid,bladetable.name as blade,rcode,current,rss,vsz,cpu,"\
                        "invocation.numslots as numslots,elapsedapp,elapsedsys,"\
                        "EXTRACT(EPOCH FROM invocation.starttime) AS t0, "\
                        "EXTRACT(EPOCH FROM stoptime) AS t1 "\
                        "FROM {ONLY} invocation LEFT JOIN blade AS bladetable USING (bladeid) "\
                        "WHERE jid={jid} AND tid={tid} and current ORDER BY invocation.starttime".format(
                            ONLY=onlyStr, jid=jid, tid=tid))
details = plpyutil.taskDetailsForRows(cmdrows, invorows)
return json.dumps(details, indent=2, separators=(",", ":"))
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorSpoolFile", "filename text", "text", "plpython2u", r"""
    
# This function reads the specified job/expand file and populates the postgresql database.

import shlex
import tractor.apps.spooler._dbstaging as dbstaging

result = None
with open(filename, "r") as f:
    # the first line of the file contains command line flags that are used to
    # activate options in the _dbstaging module.
    firstLine = f.readline()
    args = shlex.split(firstLine)
    # process the file based on the args and get some URL result to return to caller
    rc, result = dbstaging.main(args[3:], plpy)

if rc:
    # function exits here on error
    plpy.error("TractorSpoolFile(): problem processing %s: %s" % (filename, str(result)))
    # helpful debugging technique if stack trace is longer than 1024 chars
    #plpy.error("TractorSpoolFile(): problem processing %s: %s" % (filename, str(result)[-1024:]))

return result
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorNoteAdd", "notetype text, noteuser text, itemtype text, itemid text, notetext text", "text", "plpython2u", r"""
query = "INSERT INTO note (notetype, noteuser, itemtype, itemid, notetext, notetime) "\
        "VALUES ('%s', '%s', '%s', '{%s}', '%s', NOW()) RETURNING noteid" % \
        (notetype, noteuser, itemtype, itemid, notetext.replace("'", "''"))
result = plpy.execute(query)
if len(result) and itemtype == "job":
    noteid = result[0]["noteid"]
    query = "UPDATE ONLY job SET lastnoteid={noteid} WHERE jid={jid}".format(noteid=noteid, jid=itemid)
    plpy.execute(query)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorJobDump", "jid int, fmt text", "text", "plpython2u", r"""
# return a dump of the specified job in specified format
# 'JSON' and 'SQL' are formats are currently supported

import tractor.base.plpyutil as plpyutil
return plpyutil.jobDump(plpy, jid, fmt)
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorOwnerForJid", "int", "text", "sql", r"""
SELECT owner FROM ONLY Job WHERE jid=$1;
"""),

    # --------------------------------------------------------------------------------
    Function.Function("TractorListRemove", "text, text[]", "text[]", "sql", r"""
WITH o AS (
 SELECT ','||ARRAY_TO_STRING($2,',')||',' AS arrstr
  )
SELECT STRING_TO_ARRAY(
  TRIM(BOTH ',' FROM
    OVERLAY(arrstr PLACING ',' FROM
      POSITION (','||$1||',' IN arrstr) FOR CHAR_LENGTH($1) + 2
    )
  )
, ',')
FROM o WHERE POSITION(','||$1||',' IN arrstr) > 0;
"""),
    
    # --------------------------------------------------------------------------------
    Function.Function("TractorError", "", "text", "plpython2u", r"""
    
plpy.error("I'm sorry Dave.  I can't let you do that.")
return "If you see this, then you did not get my error."
"""),

    # --------------------------------------------------------------------------------
    Function.Function("SysSrc", "func text", "text", "plpython2u", r"""
    
# Returns the plpython source for the specified function.

query = "SELECT prosrc FROM pg_catalog.pg_proc WHERE proname='%s'" % func
result = plpy.execute(query)
if len(result) > 0:
    return result[0]["prosrc"]
return query
""")
]             

class EngineDB(PGDatabase.PGDatabase):

    MaxRows = 1000 # limit the number of rows returned
    EnumTypes = [RunTypeEnumField]
    
    JobTable = Table.Table(Job, tablename="Job", whereAliases=JobAliases)
    TaskTable = Table.Table(Task, tablename="Task", whereAliases=TaskAliases)
    CommandTable = Table.Table(Command, tablename="Command", whereAliases=CommandAliases)
    InvocationTable = Table.Table(Invocation, tablename="Invocation", whereAliases=InvocationAliases)
    BladeTable = Table.Table(Blade, tablename="Blade", whereAliases=BladeAliases)
    BladeUseTable = Table.Table(BladeUse, tablename="BladeUse")
    InstanceTable = Table.Table(Instance, tablename="Instance", whereAliases=InstanceAliases)
    ParamTable = Table.Table(Param, tablename="Param", whereAliases=ParamAliases)
    NoteTable = Table.Table(Note, tablename="Note", indexes=NoteIndexes)
    JobLockTable = Table.Table(JobLock, tablename="JobLock")
    
    Tables = [
    JobTable, TaskTable, CommandTable, InvocationTable, BladeTable, BladeUseTable,
    InstanceTable, ParamTable, NoteTable, JobLockTable
    ]

    Joins = [
        PGDatabase.PGJoin(TaskTable, JobTable, oneway=True, fields=["jid"]),
        PGDatabase.PGJoin(CommandTable, JobTable, oneway=True, fields=["jid"]),
        PGDatabase.PGJoin(CommandTable, TaskTable, oneway=True, fields=["jid", "tid"]),
        PGDatabase.PGJoin(InvocationTable, JobTable, oneway=True, fields=["jid"]),
        PGDatabase.PGJoin(InvocationTable, TaskTable, oneway=True, fields=["jid", "tid"]),
        PGDatabase.PGJoin(InvocationTable, CommandTable, oneway=True, fields=["jid", "cid"]),
        PGDatabase.PGJoin(TaskTable, CommandTable, oneway=True, 
                          onclause="Task.jid=Command.jid and Task.tid=Command.tid " +
                          "and Task.currcid=Command.cid"),
        PGDatabase.PGJoin(TaskTable, InvocationTable, oneway=True, 
                          onclause="Task.jid=Invocation.jid and Task.tid=Invocation.tid " +
                          "and Task.currcid=Invocation.cid and Invocation.current"),
        PGDatabase.PGJoin(CommandTable, InvocationTable, oneway=True,
                          onclause="Command.jid=Invocation.jid and Command.cid=Invocation.cid " +
                          "and Invocation.current"),
        PGDatabase.PGJoin(TaskTable, BladeTable, preTables=[InvocationTable], oneway=True),
        PGDatabase.PGJoin(CommandTable, BladeTable, preTables=[InvocationTable], oneway=True),
        PGDatabase.PGJoin(InvocationTable, BladeTable, onclause="blade.bladeid=invocation.bladeid", oneway=True),

        PGDatabase.PGJoin(BladeTable, JobTable, preTables=[InvocationTable], oneway=True),
        PGDatabase.PGJoin(BladeTable, TaskTable, preTables=[InvocationTable]),
        PGDatabase.PGJoin(BladeTable, CommandTable, preTables=[InvocationTable], oneway=True),
        PGDatabase.PGJoin(BladeTable, InvocationTable, onclause="invocation.stoptime is NULL AND blade.bladeid=invocation.bladeid", oneway=True),
        PGDatabase.PGJoin(BladeTable, BladeUseTable, fields=["bladeid"]),

        PGDatabase.PGJoin(NoteTable, JobTable, onclause="itemtype='job' AND job.jid=CAST(note.itemid[1] AS INT)"),
        PGDatabase.PGJoin(NoteTable, TaskTable, onclause="itemtype='task' AND task.jid=CAST(note.itemid[1] AS INT) AND task.tid=CAST(note.itemid[2] AS INT)"),
        PGDatabase.PGJoin(NoteTable, BladeTable, onclause="itemtype='blade' AND blade.bladeid=CAST(note.itemid[1] AS UUID)"),
        
        PGDatabase.PGJoin(JobLockTable, JobTable, fields=["jid"])
        ]

    Functions = FUNCTIONS
    Views = VIEWS

    def __init__(self, engineClient=None, *args, **kw):
        super(EngineDB, self).__init__(*args, **kw)
        self.engineClient = engineClient
        
    def getObjects(self, table, objtype=None, members=[], notMembers=[],
                   virtual=True, where=None, orderby=[], limit=0, only=True, **whereargs):
        # inject a MaxRows
        return super(EngineDB, self).getObjects(
            table, objtype=objtype, members=members, notMembers=notMembers,
            virtual=virtual, where=where, orderby=orderby,
            limit=limit or self.MaxRows, only=only, **whereargs)


class EngineWhere(DBWhere.Where):
    """Subclassed so we can specify our default search order more easily,
    and so we could overload _handle_UnquotedString."""

    # list of objects that contain a state field and should be checked
    # by the _handle_UnquotedString() method below.  The list will be
    # passed to isinstance() and can be extended by subclasses.
    StateObjects = (Task,)

    def __init__(self, where, table=None, **kwargs):
        super(EngineWhere, self).__init__(where, database=EngineDB, table=table,
                                          **kwargs)

    def _handle_UnquotedString(self, token, context, stack):
        # if the token is one of the task states, and the current left
        # operand token is Task.state, then make this a String
        if token.text in ALL_STATES and \
           isinstance(context.left, DBWhere.Member) and \
           context.left.member == "state" and \
           issubclass(context.left.cls, self.StateObjects):
            return DBWhere.String("'%s'" % token.text)

        # otherwise, call the default method
        return super(EngineWhere, self)._handle_UnquotedString(token, context, stack)


class EngineDBFormatter(DBFormatter.DBFormatter):
    """Formatter for objects returned from the Engine database."""

    # the default attributes (keyword args) that will be used when
    # instantiating a MemberFormat object for a member.  The key should be
    # the full name of the member (i.e. not an alias), and the value should
    # be a dictionary of keyword arguments.
    #
    # copy from the base, then add to it
    defaultFormatAttrs = {
        "Job.numactive": {"color": terminal.TerminalColor("green"),
                          "zeros": False, "header": "actv"},
        "Job.numblocked": {"color": terminal.TerminalColor("yellow"),
                           "zeros": False, "header": "blkd"},
        "Job.numready": {"color": terminal.TerminalColor("cyan"),
                           "zeros": False, "header": "redy"},
        "Job.numerror": {"color": terminal.TerminalColor("red"),
                         "zeros": False, "header": "err"},
        "Job.numdone": {"zeros": False, "header": "done"},
        # avoid verbose "Task.activetime" or "Task.donetime" in headings
        "Task.activetime"    : {"header"   : "activetime"}, 
        "Task.statetime"     : {"header"   : "statetime"}, 
        }

    # the default list of members that will be used in a formatter for a
    # given table if the formatter is initialized with a QueryResult and
    # no list is provided.
    defaultFormatLists = {
        EngineDB.JobTable        : "jid=10,user,title,pri=4.0,numblocked,numready,numactive,numerror,numdone,spooled",
        EngineDB.TaskTable       : "jid=10,tid=4,user,title,state,statetime,Blade.name",
        EngineDB.CommandTable    : "jid=10,tid=4,cid=4,argv=40",
        EngineDB.InvocationTable : "jid=10,tid=4,cid=4,iid=4,Blade.name,starttime,stoptime,rcode,argv=40",
        EngineDB.BladeTable      : "name,port,profile,load,availdisk,availmem,starttime,nimby",
        EngineDB.ParamTable      : "name,value",
        EngineDB.NoteTable       : "notetime,noteuser,notetype,itemtype,itemid,notetext"
        }
    # setup some custom formats for some of the members
    memberToFormat = {
        #"Task.memlimit"          : Formats.KiloBytesFormat,
        }

    def __init__(self, *mformats, **attrs):
        attrs.setdefault("database", EngineDB)
        attrs.setdefault("memberToFormat", self.memberToFormat)
        super(EngineDBFormatter, self).__init__(*mformats, **attrs)


class Row(object):
    """
    This class is a wrapper around a dictionary so that a dictionary can be accessed as <table>.<column>.
    For example, a row from the Task table may contain the task tid and the owner from the Job table. 
    The dictionary would look like {"tid": 1234, "Job.owner": "adamwg"}.  For accessing the tid with row.tid, 
    __getattr__ would be passed "tid" and could simply call getattr(self, "tid").  However, for accessing
    the owner with row.Job.owner, __getattr__ would be passed "Job", and there would be no "Job" in the
    dictionary.  To overcome this, the class makes the assumption that capitalized members refer to
    table names, so a new Row object is returned for row.Job with prefix="Job.".  When the owner of
    that object is subsequently references, __getattr__ prepends the prefix "Job." to "owner" to
    successfully locate "Job.owner" in the dictionary.  This work serves two purposes: one is 
    to support an easy to use and understand object-oriented notation of rows; the other is to provide
    backwards compatibility of the command line tool infrastructure.
    """

    def __init__(self, dictionary={}, prefix=""):
        # rowdata holds the simply key/value dictionary returned by postgres
        self.rowdata = dictionary
        # the prefix is prepended to an attributes name in __getattr__ for columns in joined tables
        self.prefix = prefix
        # rowByTable ensures that only one new Row object is created for each joined table
        self.rowByTable = {}

    def __getattr__(self, attr):
        # we make the assumption here that a capitalized attribute means a table name
        # alternatively, we could compare the attr value against all known table names
        if attr[0].isupper():
            # check for joined row data
            if attr in self.rowByTable:
                return self.rowByTable[attr]
            else:
                self.rowByTable[attr] = Row(self.rowdata, prefix=self.prefix + attr + ".")
                return self.rowByTable[attr]

        if self.prefix:
            attr = self.prefix + attr
        if attr in self.rowdata:
            return self.rowdata[attr]
        else:
            raise AttributeError("Attribute '%s' not found in row %s" % (attr, str(self.rowdata)))

    def __getitem__(self, attr):
        # support dictionary access
        return self.rowdata.get(attr)


class EngineClientDBFormatter(EngineDBFormatter):
    pass
EngineClientDBFormatter.fieldToFormat[DBFields.TimestampField] = Formats.StringTimeFormat


class EngineClientDBQueryResult(object):
    """A container class for returning results of a query.  Follows the interface for other query results
     to facilitate access in DBCmdLineTool."""
    
    def __init__(self, dictRows, objtype=Row):
        objtype = objtype or Row # override None with the default class, Row
        self.rows = [objtype(d) for d in dictRows]

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, index):
        """Overloaded to give the object the appearance of a list."""
        return self.rows[index]


def getUserAliases():
    """Returns a list of aliases specific to user, such as how "mine" is defined."""
    aliases = {}
    if os.environ.get("USER"):
        aliases["mine"] = "owner=%s" % os.environ.get("USER")
    return aliases


class EngineClientDB(EngineDB):
    """EngineClientDB is a class that follows the same interface as the rpg.sql.Database interface,
    but sends its query to the EngineClient and unpacks the received results.
    NOTE: subclassing from EngineDB is to have access to the object definitions,
    formatting, and aliases, NOT for directly accessing the database.  Database
    access is overriden in the getObjects() method so that such database communication
    happens by proxy through the engine.
    """

    def __init__(self, engineClient, *args, **kw):
        super(EngineClientDB, self).__init__(*args, **kw)
        # the worker client will be set according to how the manager routes the query
        self.engineClient = engineClient

    def open(self, **kwargs):
        self.engineClient.open(**kwargs)

    def close(self):
        self.engineClient.close()

    def getObjects(self, table, members=[], where=None, orderby=[], limit=0, only=True, aliases=None, **kw):
        """Assemble and execute a query on a worker, converting the result
        to a standard QueryResult."""
        rows = self.engineClient.select(
            table.tablename, where=where, columns=members, sortby=orderby, limit=limit,
            archive=not only,
            aliases=getUserAliases())
        queryResult = EngineClientDBQueryResult(rows, objtype=kw.get("objtype"))
        return queryResult
