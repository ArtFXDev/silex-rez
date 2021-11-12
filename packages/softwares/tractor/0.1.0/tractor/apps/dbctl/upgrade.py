"""This module provides classes to represent modifications to the DDL of the database
so that upgrades to the schema can be accurately represented."""

import tractor.base.EngineDB as EngineDB

class UpgradeError(Exception):
    """Base class for raising an upgrade exception."""

class MissingArg(UpgradeError):
    """Missing argument to method."""

class UnknownVersion(UpgradeError):
    """Version is not a known version."""

class ExistingUpgrade(UpgradeError):
    """An upgrade already exists between specified versions."""
    
class NoUpgradePath(UpgradeError):
    """No upgrade path exists bewteen specified versions."""

class Upgrade(object):
    """An abstract class that ensures protocol is followed."""
    def getSQL(self):
        raise UpgradeException("Upgrade.getSQL(): subclass must override this method.")

    def __str__(self):
        return self.getSQL()
    

class UpgradeWithSQL(Upgrade):
    def __init__(self, sql):
        self.sql = sql
        
    def getSQL(self):
        return self.sql


class CreateAllFunctions(Upgrade):
    def getSQL(self):
        return "\n".join([f.getCreate() for f in EngineDB.EngineDB.Functions])
    
    def __str__(self):
        return "Create all functions."

class DropAllFunctions(Upgrade):
    def getSQL(self):
        # a tractordummy() function is created to ensure that the EXECUTE
        # gets at least one statement to run; this is more relevant
        # during development when there are no tractor functions defined
        return """
DO $$
BEGIN
CREATE OR REPLACE FUNCTION public.tractordummy() RETURNS int LANGUAGE sql AS $dummy$ select 0; $dummy$;
EXECUTE (
    SELECT string_agg('DROP FUNCTION IF EXISTS ' || ns.nspname || '.' || proname || '(' || oidvectortypes(proargtypes) || ');', ' ')
       FROM  pg_proc INNER JOIN pg_namespace ns ON (pg_proc.pronamespace = ns.oid)
       WHERE proname LIKE 'tractor%'
       );
END
$$;
"""
    
    def __str__(self):
        return "Drop all functions."


class CreateAllViews(Upgrade):
    def getSQL(self):
        parts = ["DROP VIEW IF EXISTS %s;\n%s;\nGRANT SELECT ON %s TO readroles, writeroles;\n"
                 % (view.name, view.getCreate(), view.name) for view in EngineDB.EngineDB.Views]
        return "".join(parts)

    def __str__(self):
        return "Create all views."

class DropAllViews(Upgrade):
    def getSQL(self):
        # a dummyview is created to ensure that the EXECUTE
        # gets at least one statement to run; this is more relevant
        # during development when there are no tractor views defined
        return """
DO $$
BEGIN
CREATE OR REPLACE VIEW public.dummyview AS SELECT 0;
EXECUTE (
    SELECT string_agg('DROP VIEW IF EXISTS ' || t.oid::regclass || ';', ' ')
       FROM   pg_class t
       JOIN   pg_namespace n ON n.oid = t.relnamespace
       WHERE  t.relkind = 'v'
       AND    n.nspname = 'public'
       );
END
$$;
"""

    def __str__(self):
        return "Drop all views."
    
class UpgradeAddTable(Upgrade):
    def __init__(self, table):
        self.tablename = table

    def getSQL(self):
        table = EngineDB.EngineDB.tableByName(self.tablename)
        parts = [
            table.getCreate(),
            "GRANT SELECT,DELETE,INSERT,UPDATE ON %s TO writeroles;" % self.tablename,
            "GRANT SELECT ON %s TO readroles;" % self.tablename
            ]
        return "\n".join(parts)
        
    def __str__(self):
        return "Add table %s." % self.tablename

class UpgradeRemoveTable(Upgrade):
    def __init__(self, table):
        self.tablename = table

    def getSQL(self):
        return "DROP TABLE %s" % self.tablename
        
    def __str__(self):
        return "Remove table %s." % self.tablename

class UpgradeAddColumn(Upgrade):
    def __init__(self, table=None, column=None, coltype=None, default=None):
        self.table = table
        self.column = column
        self.coltype = coltype
        self.default = "DEFAULT %s" % default if default is not None else ""

    def getSQL(self):
        return "ALTER TABLE %s ADD COLUMN %s %s %s;" % (self.table, self.column, self.coltype, self.default)

    def __str__(self):
        return "Add column %s to table %s." % (self.column, self.table)

class UpgradeDropColumn(Upgrade):
    """Drop a column from a table.  Includes logic for dropping column from inherited tables,
    which must have a suffix separated by an underscore.  e.g. task_2015_03."""
    def __init__(self, table=None, column=None, inherited=False):
        self.table = table
        self.column = column
        self.inherited = inherited

    def getSQL(self):
        if self.inherited:
            # drop all matching tables
            return """
DO $$DECLARE r record;
BEGIN
    FOR r IN SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND (table_name='%s' OR table_name LIKE '%s_%%') ORDER BY table_name
     LOOP
         EXECUTE 'ALTER TABLE ' || quote_ident(r.table_name) || ' DROP COLUMN IF EXISTS %s';
     END LOOP;
END$$;""" %  (self.table, self.table, self.column)
        else:
            # a simple column drop
            return "ALTER TABLE %s DROP COLUMN %s;" % (self.table, self.column)

    def __str__(self):
        return "Remove column %s from table %s." % (self.column, self.table)

class UpgradeAddIndex(Upgrade):
    """Upgrade class for creating a new index."""
    def __init__(self, table=None, name=None, columns=None, where=None):
        """table is a string representing the table.  columns is a string expression of the
        columns used in the index; multiple columns are separated by a column and functions can
        be used.  where is optional for creating partial indexes.
        """
        self.table = table
        self.name = name
        self.columns = columns
        self.where = ""
        if where:
            self.where = " WHERE %s" % where

    def getSQL(self):
        parts = [
            "DROP INDEX IF EXISTS %s;" % self.name,
            "CREATE INDEX %s ON %s (%s)%s;" % (self.name, self.table, self.columns, self.where)
            ]
        return "\n".join(parts)
        
    def __str__(self):
        return "Added index %s on column(s) %s of table %s." % (self.name, self.columns, self.table)

class UpgradeChangeTableOwner(Upgrade):
    """Change owner of all tables and sequences."""
    def __init__(self, owner=None):
        self.owner = owner

    def getSQL(self):
        return """
DO $$DECLARE r record;
BEGIN
    FOR r IN SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type='BASE TABLE' ORDER BY table_name
     LOOP
         EXECUTE 'ALTER TABLE ' || quote_ident(r.table_name) || ' OWNER TO {owner}';
     END LOOP;
    FOR r IN SELECT sequence_name  FROM information_schema.sequences WHERE sequence_schema = 'public' ORDER BY sequence_name
     LOOP
         EXECUTE 'ALTER SEQUENCE ' || quote_ident(r.sequence_name) || ' OWNER TO {owner}';
     END LOOP;
     
END$$;""".format(owner=self.owner)

    def __str__(self):
        return "Changed owner of tables and sequences to %s." % self.owner

class UpgradeAddRole(Upgrade):
    """Add the given username to the given role."""
    def __init__(self, username=None, role=None):
        self.username = username
        self.role = role

    def getSQL(self):
        return "CREATE ROLE %s WITH LOGIN IN ROLE %s;" % (self.username, self.role)
        
    def __str__(self):
        return "Added user %s to role %s." % (self.username, self.role)
        
class UpgradeRemoveRole(Upgrade):
    """Add the given username to the given role."""
    def __init__(self, username=None):
        self.username = username

    def getSQL(self):
        return "DROP ROLE IF EXISTS %s;" % self.username
        
    def __str__(self):
        return "Removed user %s." % self.username


class UpgradeManager(object):
    """The UpgradeManager is a class for managing the branches and
    for determining the optimal upgrade path from one branch and schema
    version to another branch and schema version.
    """
    def __init__(self, initialVersion):
        # a known list of versions
        self.knownVersions = set([initialVersion])
        # the default branch when no branch has been specified
        self.defaultBranch = initialVersion.split("-")[0]
        # the current version ID
        self.currentVersion = initialVersion
        # a mapping of updates from (srcVersion, dstVersion) to upgrades
        # e.g. self.updatesByVersions[("2.0-60", "2.1-7")] = [Upgrade(), ...]
        self.upgradesByVersions = {}

    def setDefaultBranch(self, branch):
        """Set the branch used when no branch is specified in a version."""
        self.defaultBranch = branch

    def addVersion(self, dstVersion=None, upgrades=None, srcVersion=None, makeCurrent=True):
        """Associate the specified upgrades to upgrade from the src version
        to the dst version.  The dst version and upgrades must be specified.
        If no src version is specified, the current version is assumed.
        If makeCurrent is True, the curent version is upgraded to the
        dst version.  Setting makeCurrent to False permits the addition
        of alternative upgrade paths without affecting what is considered
        the current version.  If version IDs are not specified with their
        branch, the current branch is assumed.
        """
        # ensure that required arguments have been specified
        if not dstVersion:
            raise MissingArg("dstVersion has not be specified")
        if upgrades is None:
            raise MissingArg("upgrades has not be specified")
        # convert versions to fully qualified version IDs
        if "-" not in dstVersion:
            dstVersion = "%s-%s" % (self.defaultBranch, dstVersion)
        srcVersion = srcVersion or self.currentVersion
        if "-" not in srcVersion:
            srcVersion = "%s-%s" % (self.defaultBranch, srcVersion)
        # ensure that source version is valid
        if srcVersion not in self.knownVersions:
            raise UnknownVersion("source version %s is not in known version %s" % (srcVersion, self.knownVersions))
        # ensure that version upgrade path has not already been recorded
        key = (srcVersion, dstVersion)
        if key in self.upgradesByVersions:
            raise ExistingUpgrade("upgrades already exist between %s and %s" % \
                                  (srcVersion, dstVersion))
        # record upgrade
        self.upgradesByVersions[key] = upgrades
        # destination version is now a known version
        self.knownVersions.add(dstVersion)
        # make this the current version
        if makeCurrent:
            self.currentVersion = dstVersion
            # if transitioning to a new branch, make it a new branch
            self.setDefaultBranch(dstVersion.split("-")[0])
                  
    def versionGraph(self):
        """Represent all versions as an adjacency list (or dict, as it were)."""
        # key is a version ID
        # value is a list of versions for which there is an upgrade or transition.
        graph = {}
        # add version changes within branches to graph
        for srcVersion, dstVersion in list(self.upgradesByVersions.keys()):
            graph.setdefault(srcVersion, []).append(dstVersion)
        return graph

    def shortestPath(self, graph, start, end, path=[]):
        path = path + [start]
        if start == end:
            return path
        if start not in graph:
            return None
        shortest = None
        for node in graph[start]:
            if node not in path:
                newpath = self.shortestPath(graph, node, end, path)
                if newpath:
                    # len() could be replaced with some other cost function,
                    # such as a cost() method on each upgrade
                    if not shortest or len(newpath) < len(shortest):
                        shortest = newpath
        return shortest

    def upgradesInRange(self, srcVersion, dstVersion):
        """Returns the upgrades necessary to upgrade from the source
        branch/version to the destination branch/version."""
        # express the versions that are connected through upgrades as a graph
        # and find the shortest upgrade path among them
        versionGraph = self.versionGraph()
        versions = self.shortestPath(versionGraph, srcVersion, dstVersion)
        if not versions:
            raise NoUpgradePath("no upgrade path from version %s to %s" % (srcVersion, dstVersion))
        # aggregate upgrades according to returned versions
        upgrades = []
        for i in range(len(versions) -1):
            currVersion = versions[i]
            nextVersion = versions[i+1]
            key = (currVersion, nextVersion)
            upgrades.extend(self.upgradesByVersions[key])
        return upgrades

    def getUpgrades(self, srcVersion, dstVersion=None):
        """Returns the upgrade objects necessary to from the given schema version to the newest version.
        If a path is not possible to upgrade, an exception is raised.
        """
        # convert version ids to branch and version
        upgrades = [DropAllViews(), DropAllFunctions()] + \
                   self.upgradesInRange(srcVersion, dstVersion or self.newestVersion()) + \
                   [CreateAllViews(), CreateAllFunctions()]
        return upgrades
 
    def newestVersion(self):
        return self.currentVersion

# create a new upgrade manager; sets initial version 2.0-1 and default branch to 2.0
manager = UpgradeManager(initialVersion="2.0-1")

# branch 2.0
# ----------

manager.addVersion("3", [
    UpgradeAddColumn(table="invocation", column="limits", coltype="text[]")
    ])
manager.addVersion("4", [
    UpgradeWithSQL("ALTER ROLE dispatch RENAME TO dispatcher"),
    UpgradeWithSQL("ALTER ROLE query RENAME TO reader")
    ])
manager.addVersion("5", upgrades=[
    UpgradeWithSQL("ALTER ROLE dispatch RENAME TO dispatcher"),
    UpgradeWithSQL("ALTER ROLE query RENAME TO reader")
    ])
manager.addVersion("8", [
    UpgradeWithSQL(
        "ALTER TABLE job ALTER COLUMN project SET DATA TYPE text[] USING "
        "CASE project WHEN '' THEN '{}' WHEN NULL THEN '{}' ELSE ARRAY[project] END"),
    UpgradeWithSQL("ALTER TABLE job RENAME project TO projects")
    ])
# add new columns to invocation table for tracking process stats
manager.addVersion("10", [
    UpgradeWithSQL("ALTER TABLE invocation RENAME COLUMN mem TO vsz"),
    UpgradeWithSQL("ALTER TABLE invocation RENAME COLUMN utime TO elapsedapp"),
    UpgradeWithSQL("ALTER TABLE invocation RENAME COLUMN stime TO elapsedsys"),
    UpgradeAddColumn(
        table="invocation", column="elapsedreal", coltype="real", default="0.0")
    ])
# retrofit tags to be compatible with new decoder
manager.addVersion("11", [
    UpgradeWithSQL(
        "UPDATE job SET tags=STRING_TO_ARRAY(ARRAY_TO_STRING(tags, ''), ' ')")
    ])
# renamed command's refid column to refersto to be consistent with job syntax
manager.addVersion("17", [
    UpgradeWithSQL("ALTER TABLE command RENAME COLUMN refid TO refersto")
    ])
# rename delaytime to aftertime
manager.addVersion("22", [
    UpgradeWithSQL("ALTER TABLE job RENAME COLUMN delaytime TO aftertime")
    ])
# remove the host field from the blade table
manager.addVersion("23", [UpgradeWithSQL("ALTER TABLE blade DROP COLUMN host")])
# added maxactive attribute to job and the jobinfo view
manager.addVersion("33", [
    UpgradeAddColumn(table="job", column="maxactive", coltype="integer", default="0")
    ])
# add osname and osversion columns to blade table
manager.addVersion("42", [
    UpgradeAddColumn(table="blade", column="osname", coltype="text"),
    UpgradeAddColumn(table="blade", column="osversion", coltype="text")
    ])
# add serialsubtasks to job table and jobinfo view
manager.addVersion("44", [
    UpgradeAddColumn(
        table="job", column="serialsubtasks", coltype="BOOLEAN", default="false"),
    ])
# add dirmap to job table and jobinfo view
manager.addVersion("48", [
    UpgradeAddColumn(table="job", column="dirmap", coltype="JSON")
    ])
# added support for task resuming
manager.addVersion("58", [
    UpgradeAddColumn(table="task", column="resumeblock", coltype="BOOLEAN", default="false"),
    UpgradeAddColumn(table="task", column="retrycount", coltype="INT", default="1"),
    UpgradeAddColumn(table="command", column="resumewhile", coltype="text[]"),
    UpgradeAddColumn(table="command", column="resumepin", coltype="BOOLEAN", default="false"),
    UpgradeAddColumn(table="invocation", column="retrycount", coltype="INT"),
    UpgradeAddColumn(table="invocation", column="resumecount", coltype="INT"),
    UpgradeAddColumn(table="invocation", column="resumable", coltype="BOOLEAN")
    ])
# start retrycount at 0 instead of 1
manager.addVersion("65", [
    UpgradeWithSQL("ALTER TABLE task ALTER COLUMN retrycount SET DEFAULT 0")
    ])
# create versions to guide to transition to 2.1; this is only necessary
# to account for pre-2.1 schema version increments that did not have upgrades
manager.addVersion("66", [])
manager.addVersion("67", [])
# some other version 68 creates a dev version of the JobLock table;
# we are not required to create it here
manager.addVersion("68", [])
# but make sure it does get removed so that it can be recreated in a 2.1 upgrade
manager.addVersion("69", [
    # the UpgradeRemoveTable does not use IF EXISTS to be strict, so spell it out here
    UpgradeWithSQL("DROP TABLE IF EXISTS JobLock;"),
    # this addresses a bug in which the job starttime could be set to an invalid value due to engine buffer overrun
    UpgradeWithSQL("WITH bad_jobs AS (SELECT jid FROM job WHERE job.starttime > '2099-01-01'::timestamp), "\
                   "new_starttimes AS (SELECT jid,min(invocation.starttime) AS starttime FROM invocation JOIN bad_jobs USING(jid) GROUP BY jid) "\
                   "UPDATE job SET starttime=new_starttimes.starttime FROM new_starttimes WHERE job.jid=new_starttimes.jid;")
    ])


# branch 2.1
# ----------

# this will automatically make the default branch 2.1 and set the upgrade
# path between the last 2.0 version that was made current and this 2.1 branch
manager.addVersion("2.1-0", [])

# added JobLock table
manager.addVersion("1", [UpgradeAddTable("JobLock")])
# add indices to note table to make it faster to locate job notes
manager.addVersion("2", [
    UpgradeAddIndex(table="note", name="note_cast_jid_idx",
                    columns="itemtype, CAST(itemid[1] AS INT)",
                    where="itemtype='job'"),
    UpgradeAddIndex(table="note", name="note_cast_jid_tid_idx",
                    columns="itemtype, CAST(itemid[1] AS INT), CAST(itemid[2] AS INT)",
                    where="itemtype='task'"),
    UpgradeAddIndex(table="note", name="note_cast_blade_idx",
                    columns="itemtype, CAST(itemid[1] AS TEXT)",
                    where="itemtype='blade'"),
    UpgradeWithSQL("ALTER TABLE note RENAME COLUMN note TO notetext;")
    ])
# added Instance table, add instanceid field to blade table
manager.addVersion("3", [
    UpgradeAddTable("Instance"),
    UpgradeAddColumn(table="blade", column="instanceid", coltype="INT")
    ])
# added bladeid field to Blade table, make it the primary key;
# keep an index around for the blade name; we must prepopulate
# the bladeid field before making it the primary key
manager.addVersion("4", [
    UpgradeAddColumn(table="blade", column="bladeid", coltype="UUID"),
    UpgradeWithSQL(
        "UPDATE blade SET "
        "bladeid=uuid_in(md5(random()::text || now()::text)::cstring) "
        "WHERE bladeid IS NULL;"),
    UpgradeAddIndex(table="blade", name="blade_name_idx", columns="name"),
    UpgradeWithSQL("ALTER TABLE blade DROP CONSTRAINT blade_pkey;"),
    UpgradeWithSQL("ALTER TABLE blade ADD PRIMARY KEY (bladeid);")
    ])
# retrofit the invocation table with blade ids; 
# first create new blade records for blade names referenced in invocations that
# do not have a blade record; then create a bladeid column on the invocation
# table and populate it; then drop the invocation blade column
manager.addVersion("5", [
    UpgradeWithSQL(
        "INSERT INTO blade (name, bladeid) SELECT "
        "invocation.blade AS name,uuid_in(md5(random()::text || now()::text)::cstring) AS bladeid "
        "FROM invocation LEFT JOIN blade ON(invocation.blade=blade.name) "
        "WHERE bladeid IS NULL GROUP BY invocation.blade;"),
    UpgradeAddColumn(table="invocation", column="bladeid", coltype="UUID"),
    UpgradeWithSQL(
        "UPDATE invocation SET bladeid=blade.bladeid FROM blade "
        "WHERE invocation.blade=blade.name;"),
    UpgradeDropColumn(table="invocation", column="blade", inherited=True)
    ])
# added cleartime field to blade table
manager.addVersion("6", [
    UpgradeAddColumn(table="blade", column="cleartime", coltype="timestamp with time zone")
    ])
# added pil field to job table and initialize value to job id
manager.addVersion("7", [
    UpgradeAddColumn(table="job", column="pil", coltype="bigint"),
    UpgradeWithSQL("UPDATE job SET pil=jid;")
    ])
# added index to bladeid field of invocation table
manager.addVersion("8", [
    UpgradeAddIndex(table="invocation", name="invocation_bladeid_idx", columns="bladeid")
    ])
# added minrunsecs and maxrunsecs to command table
manager.addVersion("9", [
    UpgradeAddColumn(table="command", column="minrunsecs", coltype="real"),
    UpgradeAddColumn(table="command", column="maxrunsecs", coltype="real")
    ])
# removed OldJob table
manager.addVersion("10", [
    UpgradeWithSQL("DROP TABLE OldJob;")
    ])
# translate blade name in note table to bladeid
manager.addVersion("11", [
    UpgradeWithSQL("UPDATE note SET itemid=ARRAY[blade.bladeid::text] FROM blade WHERE itemtype='blade' AND itemid[1]::text=blade.name;")
    ])
# add new roles; change ownership of tables
manager.addVersion("12", [
    UpgradeAddRole(username="archiver", role="writeroles"),
    UpgradeAddRole(username="loader", role="writeroles"),
    UpgradeAddRole(username="blademetrics", role="writeroles"),
    UpgradeAddRole(username="progress", role="writeroles"),
    UpgradeAddRole(username="annotator", role="writeroles"),
    UpgradeAddRole(username="tqreader", role="readroles"),
    UpgradeRemoveRole(username="reader"),
    UpgradeChangeTableOwner(owner="archiver")
    ])
# ensure that sequences like noteid are usable by write roles, such as note additions by annotator role
manager.addVersion("13", [
    UpgradeWithSQL("GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO writeroles;")
    ])
# add gpulabel column to blade table
manager.addVersion("14", [
    UpgradeAddColumn(table="blade", column="gpulabel", coltype="text")
    ])
# clean up empty blade record that may have appeared from migration from 2.0
manager.addVersion("15", [
    UpgradeWithSQL("DELETE FROM blade WHERE name='' AND heartbeattime IS NULL;")
    ])

# branch 2.2
# ----------

# this will automatically make the default branch 2.2 and set the upgrade
# path between the last 2.1 version that was made current and this 2.2 branch
manager.addVersion("2.2-0", [])

# add a new bladeuse table for caching active task/slot counts on a blade
manager.addVersion("1", [
    UpgradeAddTable("BladeUse"),
    UpgradeWithSQL("DELETE FROM BladeUse;"),
    UpgradeWithSQL(
        "INSERT INTO BladeUse (bladeid, taskcount, slotsinuse, owners) "
        "SELECT invocation.bladeid AS bladeid,"
        "COUNT(*) AS taskcount,"
        "SUM(numslots) AS slotsinuse,"
        "ARRAY_AGG(job.owner) AS owners "
        "FROM ONLY invocation LEFT JOIN ONLY job USING(jid) "
        "WHERE invocation.stoptime IS NULL AND invocation.current "
        "GROUP BY invocation.bladeid;"
        )
    ])

# add a last nodeid column to the job table and initialize it to point to most recent note
manager.addVersion("2", [
    UpgradeAddColumn(table="job", column="lastnoteid", coltype="int"),
    UpgradeWithSQL(
        "WITH noteinfo AS "
        "(SELECT itemid[1]::int AS jid, MAX(noteid) AS lastnoteid FROM note WHERE itemtype='job' GROUP BY itemid) "
        "UPDATE job SET lastnoteid=noteinfo.lastnoteid FROM noteinfo WHERE job.jid=noteinfo.jid;"
        )
    ])

# fix invocation records that were not properly updated to false when active tasks were retried
manager.addVersion("3", [
    UpgradeWithSQL(
        "WITH maxinvocation AS (SELECT jid,tid,cid,COUNT(iid) AS currentcount,MAX(iid) AS maxiid "
        "FROM ONLY invocation WHERE current GROUP BY jid,tid,cid HAVING COUNT(iid) > 1) "
        "UPDATE ONLY invocation SET current='f',rcode=-2015 FROM maxinvocation "
        "WHERE invocation.jid=maxinvocation.jid AND invocation.tid=maxinvocation.tid AND "
        "invocation.cid=maxinvocation.cid AND invocation.current AND invocation.iid < maxinvocation.maxiid;"
        )
    ])

# set state of all done expand tasks of incomplete jobs to ready,  leaving engine
# to properly re-migrate those whose subtrees are entirely done to done.  this avoids
# a problem in which incomplete subtrees are not completed after an upgrade, due to the
# engine changing which state is used to represent executed expand nodes whose subtrees
# are incomplete; formerly they were stored as done, whereas now they are stored as ready.
manager.addVersion("4", [
    UpgradeWithSQL(
        "CREATE OR REPLACE FUNCTION readifytask(j BIGINT, t INT) RETURNS INT AS $$ "
        "UPDATE ONLY task SET state='ready' WHERE jid=$1 AND tid=$2; "
        "UPDATE ONLY job SET numready=numready+1,numdone=numdone-1 WHERE jid=$1; "
        "SELECT 1; "
        "$$ LANGUAGE SQL;"
        ),
    UpgradeWithSQL(
        "WITH ready_tasks AS (SELECT DISTINCT jid,tid,state FROM ONLY task LEFT JOIN ONLY command "
        "USING (jid, tid) LEFT JOIN ONLY job USING (jid) "
        "WHERE numtasks <> numdone AND expand AND attached and state='done') "
        "SELECT jid,tid,readifytask(r.jid, r.tid) FROM ready_tasks AS r;"
        ),
    UpgradeWithSQL("DROP FUNCTION IF EXISTS readifytask(BIGINT, INT);")
    ])
