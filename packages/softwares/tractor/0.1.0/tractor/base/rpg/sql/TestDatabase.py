# TestDatabase.py

"""This module defines a light version of MisterD for testing purposes.  The
intent is to ensure all areas of the sql module are tested."""

import re
import rpg.sql.DBObject as DBObject
import rpg.sql.Fields as DBFields
import rpg.sql.Table as DBTable
import rpg.sql.Where as DBWhere
import rpg.sql.Join as DBJoin
import rpg.sql.Database as Database

from rpg.progutil import logError

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# test MRD db definition

class DispatcherField(DBFields.VirtualField):
    """A virtual field for querying the user and host fields at once."""

    # regular expression to pull user, host, and port from a dispatcher string
    dispre = re.compile("([^@]+)(?:@([^:]+)(?:\:(\d+))?)?")

    def getValue(self, obj):
        """Called when this member is referenced.

        @param obj: the object instance to return the virtual value for
        @type obj: L{DBObject.DBObject} subclass instance

        @return: virtual value
        @rtype: varies
        """
        # make sure we have everything
        if not (obj.user and obj.host and obj.port):
            raise "dispatcher can't be returned with out user, host, and port."
        return "%s@%s:%d" % (obj.user, obj.host, obj.port)

    def getWhere(self, phrase):
        """Return the mysql equivalent by stripping the user and host out
        of the string to compare with."""

        # if the field is in a stand-alone phrase, then the query will be
        # different
        if phrase.__class__ is DBWhere.StandAlone:
            pieces = []
            # create the query
            for member in ('user', 'host', 'port'):
                pieces.append(self.table.fieldByMember(member).getWhere())
            mystr = "(%s)" % ' AND '.join(pieces)
            if phrase.notOp:
                mystr = "NOT " + mystr
            return mystr

        # figure out which operand of the phrase has the data
        if phrase.left.__class__ is DBWhere.VirtualMember:
            data = phrase.right
        else:
            data = phrase.left

        # get the data with no quotes
        data = data.text.strip("'\"")

        # try to split the data up
        match = self.dispre.match(data)
        if not match:
            raise "unable to parse dispatcher argument"

        user,host,port = match.groups()
        pieces = []
        # get all the pieces of the query
        for member,data in (('user', user), ('host', host), ('port', port)):
            if data:
                # grab the field object
                field = self.table.fieldByMember(member)
                pieces.append("%s.%s='%s'" % (self.table.tablename,
                                              field.fieldname, data))
        return "(%s)" % ' AND '.join(pieces)
                

class Job(DBObject.DBObject):
    """The base type for all jobs submitted to the scheduling system.

    @ivar jobid:      unique identifier of the job
    @ivar user:       user that submitted the job
    @ivar host:       host where the job was submitted from
    @ivar title:      title of the job
    @ivar priority:   priority of this job to determine placement in the queue
    @ivar huntgroups: list of slot groups this job can run on
    @ivar spooltime:  time this job was spooled
    @ivar stoptime:   time this job finished or was canceled.
    @ivar deletetime: time this job was deleted from the scheduling system.
    @ivar numtasks:   total number of tasks in this job
    @ivar active:     total number of active tasks in this job.
    @ivar blocked:    total number of blocked tasks in this job.
    @ivar done:       total number of done tasks in this job.
    @ivar error:      total number of errored tasks in this job.
    @ivar ready:      total number of ready tasks in this job.
    @ivar other:      total number of tasks in a state other than active,
                       blocked, done, error, or ready.

    @ivar graph:      a compressed representation of the job graph containing
                      all the dependencies. (gziped as well)

    """

    Fields = [
        DBFields.AutoIncField ('jobid'),
        DBFields.VarCharField ('user', length=16, index=True),
        DBFields.VarCharField ('host', length=64, index=True),
        DBFields.IntField     ('port', unsigned=True),
        DBFields.VarCharField ('title', length=255, index=True),
        DBFields.FloatField   ('priority', index=True),
        DBFields.StrListField ('huntgroups', ftype='varchar(255)', index=True),
        DBFields.TimeIntField ('spooltime', index=True),
        DBFields.TimeIntField ('stoptime', index=True),
        DBFields.TimeIntField ('deletetime', index=True),
        DBFields.IntField     ('numtasks', unsigned=True, index=True),
        DBFields.IntField     ('active', unsigned=True, index=True),
        DBFields.IntField     ('blocked', unsigned=True, index=True),
        DBFields.IntField     ('done', unsigned=True, index=True),
        DBFields.IntField     ('error', unsigned=True, index=True),
        DBFields.IntField     ('ready', unsigned=True, index=True),
        DBFields.IntField     ('other', unsigned=True, index=True),
        DBFields.BlobField    ('graph')
        ]

    VirtualFields = [
        DispatcherField('dispatcher', ['user', 'host', 'port'])
        ]

    Aliases = {
        'actv'     : 'active',
        'crews'    : 'huntgroups',
        'deleted'  : 'deletetime',
        'jid'      : 'jobid',
        'pri'      : 'priority',
        'spooled'  : 'spooltime',
        'stopped'  : 'stoptime',
        'disp'     : 'dispatcher'
        }

class StudioJob(Job):
    """Job submitted to the scheduling system from within the studio.

    @ivar tool:   name of the tool that created this job (e.g. lumos,
                  supe, etc.)
    @ivar menv:   version of menv that the job needs.
    @ivar dept:   department this job was submitted from.
    """

    Fields = [
        DBFields.VarCharField ('tool', length=32),
        DBFields.VarCharField ('menv', length=32),
        DBFields.VarCharField ('dept', length=64, index=True)
        ]


class ShotJob(StudioJob):
    """Any job submitted for the purpose of a single shot.  Most jobs are
    of this type and often render one or more frames in the shot.

    @ivar shot: the name of the shot
    @ivar firstframe: first frame this job is responsible for
    @ivar lastframe: last frame this job is responsible for
    @ivar numframes: total number of frames this job is responsible for
    @ivar framestep: job submitted on 1s, 2s, 4s, etc.
    """

    Fields = [
        DBFields.VarCharField ('shot', length=255, index=True),
        DBFields.IntField     ('firstframe', index=True),
        DBFields.IntField     ('lastframe',  index=True),
        DBFields.IntField     ('numframes', unsigned=True, index=True),
        DBFields.IntField     ('framestep')
        ]

    Aliases = {
        'fframe'   : 'firstframe',
        'first'    : 'firstframe',
        'last'     : 'lastframe',
        'lframe'   : 'lastframe',
        'nframes'  : 'numframes',
        'fstep'    : 'framestep',
        'on'       : 'framestep'
        }

JobObjects      = [Job, StudioJob, ShotJob]
JobWhereAliases = {'done'    : "stoptime > 0",
                   'errwait' : "error > 0 and ready=0 and active=0"}
              

class Task(DBObject.DBObject):
    """Base class for all tasks.

    @ivar jobid:  unique identifier for the job the tasks belong to
    @ivar taskid: unique identifier for the task within the job
    @ivar previds: list of the task ids that must finish before we can start
    @ivar nextids: list of the task ids that are waiting for us to finish
    @ivar title:   title of this task
    @ivar priority: the priority of this task
    @ivar keystr: the original key expression used to determine if this
                  task can run on a given slot.
    @ivar keys: a list of all the keys found in the key string
    @ivar tags: a list of the tags needed by the task
    @ivar wdir: working directory of the task.
    """

    States = ('active', 'blocked', 'canceled', 'cleaning',
              'done', 'error', 'groupstall', 'larval',
              'linger', 'paused', 'ready', 'thwarted', 'unknown')

    Fields = [
        DBFields.IntField      ('jobid', unsigned=True, key=True),
        DBFields.IntField      ('taskid', unsigned=True, key=True),
        DBFields.IntListField  ('previds'),
        DBFields.IntListField  ('nextids'),
        DBFields.VarCharField  ('state', length=12, index=True, indexlen=8),
        DBFields.VarCharField  ('title', length=255, index=True),
        DBFields.FloatField    ('priority', index=True),
        DBFields.VarCharField  ('keystr', length=128, index=True),
        DBFields.StrListField  ('keylist', ftype='varchar(128)', index=True,
                                member='keys'),
        DBFields.VarCharField  ('tags', length=64),
        DBFields.BlobField     ('wdir')
        ]

    Aliases = {
        'jid'    : 'jobid',
        'tid'    : 'taskid',
        'pri'    : 'priority'
        }

class TaskAttempt(DBObject.DBObject):
    """Every time a task is attempted (either from initial startup or from
    a retry) a TaskInvocation object is created.  This holds all the data
    relevent to the attempt.

    @ivar jobid:  unique identifier for the job the tasks belong to
    @ivar taskid: unique identifier for the task within the job
    @ivar state: the current state of the task
    @ivar slots: list of the slots checked out for this invocation.
    @ivar readytime: the time this attempt became ready.
    @ivar pickuptime: the time the first command started (derived)
    @ivar statetime: the time the task entered its current state (derived)
    @ivar activesecs: the total time the task was active (derived)
    """

    Fields = [
        DBFields.IntField      ('jobid', unsigned=True, key=True),
        DBFields.IntField      ('taskid', unsigned=True, key=True),
        DBFields.VarCharField  ('state', length=12, index=True, indexlen=8),
        DBFields.StrListField  ('slots', index=True, indexlen=8),
        DBFields.TimeIntField  ('readytime', index=True),
        DBFields.TimeIntField  ('pickuptime', index=True),
        DBFields.TimeIntField  ('statetime', index=True),
        DBFields.IntField      ('activesecs', unsigned=True, index=True)
        ]

    Aliases = {
        'jid'   : 'jobid',
        'tid'   : 'taskid'
        }


class MRDWhere(DBWhere.Where):
    """Subclassed so we can specify our default search order more easily,
    and so we could overload handle_UnquotedString."""

    def __init__(self, where, database=None, **kwargs):
        """Overloaded as a convenience to users so the database doesn't
        have to be passed in."""
        if database is None:
            database = MRD        
        DBWhere.Where.__init__(self, where, database, **kwargs)

    def _handle_UnquotedString(self, token, context, stack):
        # if the token is one of the task states, and the current left
        # operand token is Task.state or TaskAttempt.state, then make this
        # a String
        if token.text in Task.States and \
           context.left.__class__ is DBWhere.Member and \
           context.left.member == 'state' and \
           context.left.cls in (Task, TaskAttempt):
            return DBWhere.String("'%s'" % token.text)
        
        # otherwise, call the default method
        return DBWhere.Where._handle_UnquotedString(self, token, context, stack)

class MRD(Database.Database):

    JobTable         = DBTable.Table(JobObjects, whereAliases=JobWhereAliases)
    TaskTable        = DBTable.Table(Task)
    TaskAttemptTable = DBTable.Table(TaskAttempt)

    Tables = [JobTable, TaskTable, TaskAttemptTable]

    SearchOrder = {JobTable:  [JobTable, TaskTable, TaskAttemptTable],
                   TaskTable: [TaskTable, TaskAttemptTable, JobTable]}

    Where = MRDWhere

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# test Produce database definition

class Fruit(DBObject.DBObject):
    """
    @ivar fruit: name of the fruit
    @ivar flavour: what does the fruit taste like
    @ivar seasons: when is this fruit in season
    """
    
    Fields = [
        DBFields.VarCharField('fruit', length=16, key=True),
        DBFields.VarCharField('taste', length=16, index=True,
                              member='flavour'),
        DBFields.StrListField('seasons')
        ]

    Aliases = {'seas': 'seasons'}
        
    def __init__(self, *args, **kw):
        DBObject.DBObject.__init__(self, *args, **kw)
        self.price = 1.99
        
    
class Taste(DBObject.DBObject):
    Fields = [
        DBFields.VarCharField('taste', length=16, key=True),
        DBFields.VarCharField('goodbad', length=16, index=True),
        DBFields.IntListField('states', index=True, indexlen=8)
        ]

    def __init__(self, *args, **kw):
        DBObject.DBObject.__init__(self, *args, **kw)


class ProduceWhere(DBWhere.Where):
    def __init__(self, where, database=None, **kwargs):
        if database is None:
            database = ProduceDB
        DBWhere.Where.__init__(self, where, database, **kwargs)


class ProduceDB(Database.Database):
    FruitTable = DBTable.Table(Fruit)
    TasteTable = DBTable.Table(Taste)
    
    Tables = [FruitTable, TasteTable]

    Joins = [
    DBJoin.Join(FruitTable, TasteTable, 'Fruit.taste=Taste.taste')
    ]

    Where = ProduceWhere
    
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("debug", True)
        kwargs.setdefault("user", "rpg")
        Database.Database.__init__(self,
                                   dbhost='tinadb', db='DelMonte',
                                   password='password',
                                   readonly=0, *args, **kwargs)

import rpg.sql.DBFormatter as DBFormatter
class ProduceFormatter(DBFormatter.DBFormatter):
    defaultFormatLists = {
        ProduceDB.FruitTable: "fruit,flavour",
        ProduceDB.TasteTable: "taste,goodbad,states",
        }

    def __init__(self, *mformats, **attrs):
        super(ProduceFormatter, self).__init__(
            database=ProduceDB, *mformats, **attrs)



class Artist(DBObject.DBObject):
    Fields = (
        DBFields.AutoIncField ('artistid'),
        DBFields.VarCharField ('name', length=128, equivKey=True),
        DBFields.SmallIntField('albums')
        )

    Aliases = {
        'artist': 'name'
        }

    def __init__(self, name):
        """Initialize the Artist object with the their name."""
        # we must call the init of the base class before we can do anything.
        DBObject.__init__(self)
        self.name = name

class Album(DBObject.DBObject):
    Fields = (
        DBFields.AutoIncField ('albumid'),
        DBFields.IntField     ('artistid', unsigned=True, equivKey=True),
        DBFields.VarCharField ('name',     length=128, indexlen=8, index=True),
        DBFields.VarCharField ('genre',    length=128, indexlen=8, index=True),
        DBFields.TimeIntField ('released', index=True),
        DBFields.SmallIntField('year',     unsigned=True, index=True),
        DBFields.SmallIntField('tracks',   unsigned=True),
        DBFields.SmallIntField('discs',    unsigned=True)
        )

    Aliases = {
        'album': 'name'
        }

class Song(DBObject.DBObject):
    Fields = (
        DBFields.IntField     ('artistid', unsigned=True, key=True),
        DBFields.IntField     ('albumid',  unsigned=True, key=True),
        DBFields.VarCharField ('title',    length=128, indexlen=8, index=True),
        DBFields.SmallIntField('tracknum', unsigned=True),
        DBFields.SmallIntField('discnum',  unsigned=True),
        DBFields.SecsIntField ('length',   index=True),
        DBFields.ByteIntField ('filesize')
        )

    Aliases = {
        'song': 'title',
        'name': 'title'
        }

class Music(Database.Database):
    ArtistTable = DBTable.Table(Artist)
    AlbumTable  = DBTable.Table(Album)
    SongTable   = DBTable.Table(Song)

    Tables = [ArtistTable, AlbumTable, SongTable]


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

def testPrintProduceDB():
    db = ProduceDB()
    print(db._create())

def testJoin():
    db = ProduceDB()
    print(db.Joins)

def testFruit():
    f = Fruit()
    print(f)
    
if __name__=='__main__':
    #t = Task()
    #t.taskid = 30
    #print t.taskid
    #print t.tid
    testPrintProduceDB()
    testFruit()
    
