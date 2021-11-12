"""This module contains functions useful to plpython scripts."""

import time
import dateutil.parser
from . import rpg.timeutil as timeutil

RUNTYPE_CHAR_BY_NAME = {
    "cleanup": "D", "regular": "C",
    "post_always": "P", "post_error": "PE", "post_done": "PD"
}

CHAR_BY_STATE = {"active": "A", "blocked": "B", "ready": "B", "done": "D", "error": "E"}

def tasktreeForRows(rows):
    """Return a json string matching a 1.x on-disk representation of the tasktree file.
    Some additional values have been computed for use in q=jtree queries."""

    # allow rows to be indexed by tid
    rowByTid = {}
    for row in rows:
        rowByTid[row.get("tid", 0)] = row

    # set up children and antecedent references in each row
    for row in rows:
        ptids = row.get("ptids")
        if ptids:
            if ptids[0] != 0:
                # add this row to parent row's children
                parent = rowByTid.get(ptids[0])
                if parent:
                    parent.setdefault("children", []).append(row)
            # add tid as antecedent of remaining ptids
            for ptid in ptids[1:]:
                parent = rowByTid.get(ptid)
                if parent:
                    parent.setdefault("ants", []).append(row["tid"])

    def taskForRow(aRow):
        task = {
            "#": "T%d" % aRow["tid"],
            "children": [taskForRow(child) for child in aRow.get("children", [])],
            "data": {
                "tid": aRow["tid"],
                "title": aRow["title"],
                "state": ("+" if aRow.get("haslog") else "-") + CHAR_BY_STATE.get(aRow.get("state"), "B") +
                (",%d" % int(aRow.get("progress")) if aRow.get("progress") else ""),
                "cids": aRow.get("cids", [])
                }
            }

        # only add the following attributes if they have values
        if aRow["minslots"]:
            task["data"]["minSlots"] = aRow["minslots"]
        if aRow["maxslots"]:
            task["data"]["maxSlots"] = aRow["maxslots"]
        task["data"].update(
            dict([(k, aRow[k])
                  for k in ("id", "ants", "service", "preview", "chaser", "serialsubtasks",
                            "statetime", "activetime", "blade", "resumeblock")
                  if aRow.get(k)
                  ]))
        if aRow.get("rcode") is not None:
            task["data"]["rcode"] = aRow["rcode"]
        if aRow.get("rcids"):
            task["data"]["rcids"] = aRow.get("rcids")

        return task

    # compute list of children tasks
    children = []
    # march through tasks, starting with those that are root tasks, and recursively descending
    for row in rows:
        if row.get("ptids") and row["ptids"][0] == 0:
            children.append(taskForRow(row))

    tasktree = {"children": children}
    return tasktree

def cmdlistForRows(rows, invos):
    """Return a json string matching a 1.x on-disk representation of the command list."""

    cmdlist = {}
    
    invoByCid = {}
    for invo in invos:
        invoByCid[invo["cid"]] = invo


    for row in rows:
        invo = invoByCid.get(row.get("cid"), {})
        state = "B"
        if row["state"] == "done":
            # both done and skipped tasks have all command states marked as "D"
            state = "D"
        elif invo.get("current"):
            if invo.get("rcode"):
                state = "E"
            elif invo.get("stoptime"):
                state = "D"
            elif invo.get("starttime"):
                state = "A"
        cmd = {
            "argv": row.get("argv", []),
            "cid": row.get("cid", 0),
            "state": state,
            "service": row.get("service", ""),
            "msg": row.get("msg", ""),
            "type": ("L" if row.get("local") else "R") + \
            RUNTYPE_CHAR_BY_NAME.get(row.get("runtype", "regular"), "C") + ("X" if row.get("expand") else ""),
            "tags": row.get("tags", []),
            "minrunsecs": row.get("minrunsecs") or 0,
            "maxrunsecs": row.get("maxrunsecs") or 0,
            }
        for k in ("envkey", "id", "refersto", "resumewhile", "resumepin"):
            if row.get(k):
                cmd[k] = row[k]

        if invo.get("iid"):
            cmd["iid"] = invo["iid"]
        if invo.get("current"):
            if invo.get("starttime"):
                cmd["starttime"] = invo["starttime"]
            if invo.get("stoptime"):
                cmd["stoptime"] = invo["stoptime"]
        
        if row["retryrcodes"]:
            cmd["retryrc"] = row["retryrcodes"]
        if row["minslots"]:
            cmd["minSlots"] = row["minslots"] # note capitalization
        if row["maxslots"]:
            cmd["maxSlots"] = row["maxslots"] # note capitalization
        # the blade is required for a command for resumable commands that must run on the same blade,
        if row.get("resumepin") and invo.get("resumable") and invo.get("blade"):
            cmd["blade"] = invo.get("blade")
        elif invo.get("blade") and invo.get("current"):
            # as well as commands that must run on a certain blade as specified through refersto
            cmd["blade"] = invo["blade"]
        cmdlist["C%d" % row.get("cid", 0)] = cmd
    return cmdlist

def taskDetailsForRows(cmdRows, invoRows):
    """Return a dictionary representing the details for the given task and commands."""

    # the list of commands to be returned
    cmds = []

    # task metadata
    taskmetadata = None

    # build a LUT to locate invocations by cid
    invoRowsByCid = {}
    for invoRow in invoRows:
        cid = invoRow.get("cid")
        invoRowsByCid.setdefault(cid, []).append(invoRow)

    # iterate over each command to generate a command entry
    for cmdRow in cmdRows:
        cid = cmdRow.get("cid")
        cmd = {}

        # iterate over invocations of the command to deteremine T1-compatible command-level attributes
        # (remember, multi-blade tasks can have multiple current invocations)

        # flags get set to True if any current invocations are in that state
        hasActive = False
        hasDone = False
        hasError = False
        invos = invoRowsByCid.get(cid, [])
        for invo in invos:
            if invo.get("t1"):
                if invo.get("rcode"):
                    hasError = True
                else:
                    hasDone = True
            elif invo.get("t0"):
                hasActive = True

        # deduce overall command state based on invocations
        if hasActive:
            state = "Active"
        elif hasError:
            state = "Error"
        elif hasDone:
            state = "Done"
        else:
            state = "Blocked"

        blades = [invo.get("blade") for invo in invos if invo.get("blade")]
        if invos:
            # take maximum rss, vsz, and cpu if multiple current invocations
            rss = max([invo.get("rss", 0) for invo in invos])
            vsz = max([invo.get("vsz", 0) for invo in invos])
            cpu = max([invo.get("cpu", 0) for invo in invos])
            numslots = max([invo.get("numslots", 0) for invo in invos])
            # take sum of elapsedapp/sys if multiple current invocations
            elapsedapp = sum([invo.get("elapsedapp", 0) for invo in invos])
            elapsedsys = sum([invo.get("elapsedsys", 0) for invo in invos])
        else:
            rss = None
            vsz = None
            cpu = None
            numslots = None
            elapsedapp = None
            elapsedsys = None
        
        t0 = None # will be the minimum starttime of a command's invocations
        t1 = None # will be the maximum stoptime of a command's invocations, unless there are active ones
        for invo in invos:
            if invo["t0"] and (t0 is None or invo["t0"] < t0):
                t0 = invo["t0"]
            if invo["t1"] and (t1 is None or invo["t1"] > t1):
                t1 = invo["t1"]

        exitCode = None
        for invo in invos:
            if invo["rcode"] is not None:
                exitCode = exitCode or invo["rcode"] # a non-zero exit code trumps a zero exit code

        # deal with special case where task was skipped
        if not t0 and not t1 and cmdRow.get("state") == "done":
            state = "Skipped"
            t0 = cmdRow.get("statetimesecs")
            t1 = t0
            
        cmd = {
            "argv": cmdRow.get("argv", []),
            "cid": cid,
            "service": cmdRow.get("service", ""),
            "type": ("L" if cmdRow.get("local") else "R") + \
            RUNTYPE_CHAR_BY_NAME.get(cmdRow.get("runtype", "regular"), "C") + ("X" if cmdRow.get("expand") else ""),
            "tags": cmdRow.get("tags", []),
            "envkey": cmdRow.get("envkey", []),
            "blades": blades,
            "state": state,
            "exitcode": str(exitCode) if exitCode is not None else None,
            "t0": t0,
            "t1": t1,
            "elapsedapp": elapsedapp,
            "elapsedsys": elapsedsys,
            "rss": rss,
            "vsz": vsz,
            "cpu": cpu,
            "numslots": numslots,
            "minrunsecs": cmdRow.get("minrunsecs") or 0,
            "maxrunsecs": cmdRow.get("maxrunsecs") or 0
            }
        
        # update certain fields only if they exist to reduce size of dictionary
        if cmdRow.get("cmdid"):
            cmd["id"] = cmdRow["cmdid"]
        if cmdRow.get("refersto"):
            cmd["refersto"] = cmdRow["refersto"]
        if cmdRow.get("minslots"):
            cmd["minSlots"] = cmdRow["minslots"] # note: capitalization
        if cmdRow.get("maxslots"):
            cmd["maxSlots"] = cmdRow["maxslots"] # note: capitalization
        if cmdRow.get("metadata"):
            cmd["metadata"] = cmdRow["metadata"]
        if not taskmetadata and cmdRow.get("taskmetadata"):
            taskmetadata = cmdRow["taskmetadata"]

        # add cmd to list of commands returned
        cmds.append(cmd)

    return {"metadata": taskmetadata, "cmds": cmds}


def jobDump(plpy, jid, fmt):
    """Return a dump of the specified job in the specified format."""
    if fmt == "SQL":
        return _jobDumpSQL(plpy, jid)
    elif fmt == "JSON":
        return _jobDumpJSON(plpy, jid)
    else:
        plpy.error("%s is an invalid format for TractorJobDump." % str(fmt))


def _jobDumpSQL(plpy, jid):
    """Return a dump of the specified job in SQL format that can be ingested with psql."""

    # return a pg_dump like dump of all records pertaining to the specified job
    # The file can be used by the psql client to reconstruct the records in a
    # test database, assuming the database is empty or that there are no collisions
    # with existing records.

    import json
    import tractor.base.EngineDB as EngineDB
    import tractor.base.rpg.sql.Fields as Fields

    db = EngineDB.EngineDB()
    
    # set up encoding for importing data; copied from pg_dump
    lines = [
        "SET statement_timeout = 0;",
        "SET lock_timeout = 0;",
        "SET client_encoding = 'UTF8';",
        "SET standard_conforming_strings = on;",
        "SET check_function_bodies = false;",
        "SET client_min_messages = warning;",
        "SET search_path = public, pg_catalog;"
        ]

    for tableName in ("job", "task", "command", "invocation"):
        # add spacer
        lines.append("")
        # get positional column names for COPY
        fetchColumnsFormat = "SELECT STRING_AGG(column_name, ', ') AS columns FROM information_schema.columns WHERE table_catalog='tractor' AND table_name='%s' GROUP BY table_name;"
        result = plpy.execute(fetchColumnsFormat % tableName)
        columns = result[0]["columns"].split(", ")

        # set up COPY statement to output to file
        copyFrom = "COPY %s (%s) FROM stdin CSV;" % (tableName, ",".join(columns))
        lines.append(copyFrom)

        # fetch rows for this table
        result = plpy.execute("SELECT * FROM %s WHERE jid=%d" % (tableName, jid))
        table = db.tableByName(tableName)
        # build a line for each row
        for row in result:
            # reformat each column as a CSV-compatible string
            # use as reference COPY (SELECT * FROM <table>) TO STDOUT CSV;
            values = []
            for column in columns:
                value = row[column]
                field = table.fieldByName(column)
                # only do packing for certain values since values are strings
                if isinstance(field, (Fields.StrArrayField, Fields.IntArrayField, Fields.BooleanField)):
                    value = field.asCSV(value)
                if value is None:
                    value = ""
                values.append(str(value))

            line = ",".join(values)
            lines.append(line)

        # add terminus for table
        lines.append("\\.")

    # return result as a single "row" -- EngineClient is expecting a list
    return json.dumps([{"dump": "\n".join(lines)}])

def _jobDumpJSON(plpy, jid):
    """Return a dump of the specified job in JSON format."""
    import json

    # first see if job is in archives
    archives = False
    result = plpy.execute("SELECT jid FROM ONLY job WHERE jid=%d" % jid)
    if len(result) == 0:
        archives = True

    only = "" if archives else "ONLY"
        
    result = {}
    jobs = list(plpy.execute("SELECT * FROM {only} job WHERE jid={jid}".format(only=only, jid=jid)))
    if len(jobs) == 0:
        plpy.error("job %d not found" % jid)
    result["job"] = jobs[0]

    result["tasks"] = list(plpy.execute("SELECT * FROM {only} task WHERE jid={jid}".format(only=only, jid=jid)))
    result["commands"] = list(plpy.execute("SELECT * FROM {only} command WHERE jid={jid}".format(only=only, jid=jid)))
    result["invocations"] = list(plpy.execute("SELECT * FROM {only} invocation WHERE jid={jid}".format(only=only, jid=jid)))

    # EngineClient expects all results to be lists, so place result in []
    return json.dumps([result])

