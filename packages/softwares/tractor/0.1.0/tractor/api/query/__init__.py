"""
This module is an API to query Tractor entities such as
jobs, tasks, commands, invocations, and blades and
perform operations on them.

These examples assume the module is imported as follows:
import tractor.api.query as tq

Gather the 10 largest jobs spooled in the last hour:
>>> jobs = tq.jobs("spooltime > -1m")

List all jobs for a user:
>>> jobs = tq.jobs("user=lizzy")

List all the errored jobs for a user:
>>> jobs = tq.jobs("user=margaret and numerror")

List all the ready jobs and sort by number of ready tasks:
>>> jobs = tq.jobs("numready", sort=["numready"])

List all jobs with priority over 400 for a group of users:
>>> jobs = tq.jobs("priority > 400 and user in [john paul george ringo]")


Example usage to perform an operation:

Pause all jobs spooled in the last 1 minute:
>>> tq.pause("spooltime > -1m")

Pause the same jobs using the search keyword parameter:
>>> tq.pause(search="spooltime > -1m")

Pause the same jobs using a dictionary that identifies the job:
>>> job = {"jid": 1234}
>>> tq.pause(job)

Pausing jobs using a list of dictionaries:
>>> jobs = [{"jid": 1234}, {"jid", 1337}]
>>>  tq.pause(jobs)

Pausing jobs using results from other queries:
>>> tq.pause(tq.jobs("spooltime > -1m"))


List Functions

The list functions (jobs, tasks, commands, invocations, blades, params)
are used for retrieving entities from the database by way of the engine.

They each have the same list of arguments:

search=

An optionally specified search string.  It uses the same
natural-language-like syntax as used on the command line, not SQL.

*BE AWARE* that not specifying a search clause will get *ALL* of the
objects from the database and could impact database and/or engine
performance for large result sets.

Because it is the first argument, the argument name does not need to
be specified.  For example, the following two queries are the same:

active_jobs = jobs(search="active")
active_jobs = jobs("active")

columns=

An optionally specified list of columns.  Not specifying
any columns will retrieve all columns for the give table.
Using this argument to specify only the required columns
may be used to achieve Space and time efficiencies since
less data will need to be retrieved from the database.

Columns of other tables can be specified with a dot
notation.  For example, to get the jobs' owner when
retrieving errored tasks:

errored_tasks = tasks(search="error", columns=["Job.owner"]

sortby=

The result set can be sorted on the database server
on the specified columns before being sent to the client.  Multiple columns
can be specified for secondary sorting.  Prefixing the column
name with '-' causes reverse sorting order.  For example, the
following call retrieves jobs spooled in the last hour, with
the jobs sorted alphabetically by owner, and secondarily with 
each owner's most recently spooled jobs appearing first.

recent_jobs = jobs("spooltime > -1h", sortby=["owner", "-spooltime"])

While item sorting could be done client side, it is not possible
to do so if the result set is truncated with the limit= argument,
since truncation will happen server side, before the client
has a chance to sort the results.

*BE AWARE* that sorting can have an impact on the database server.

limit=

Space and time efficiencies can be obtained by using the limit=
argument to place an upper bound on the number of rows returned
by the database server.

*BE AWARE* that the default setting of 0 places *NO LIMIT* on the
number of records returned, as opposed to returning no results
whatsoever.  Keep this in mind if the limit will be programmatically
determined.

archives=

This is a boolean flag that will search the archive tables, which
represent the records of jobs that have been deleted (plus associated
tasks, commands, etc.)

*BE AWARE* that archives can be *much* bigger than the live data
set, so queries can be *MUCH* more expensive to execute,
transmit, and store in memory.

Such expense can be avoided by:
* using well-specified search clauses to ensure small result sets
* not sorting on the server for larger sets
* using the limit argument and no sorting for potentially large result sets


Operations

The operations functions (chcrews, chpri, jattr, pause, unpause, lock,
unlock, interrupt, restart, retryactive, retryerrors, skiperrors,
delay, undelay, delete, undelete, retry, resume, skip, kill, log, cattr,
chkeys, nimby, unnimby, trace, delist, eject) are used to perform
operations on jobs, tasks, commands, and blades.  They each have a
similar list of arguments:

*** BE CAREFUL *** using these operation functions because they are
*VERY* powerful and can be *VERY* *VERY* *VERY* *DESTRUCTIVE*.

firstarg=

The name of this generically-named argument is not intended to be
explicitly specified by the caller, but is merely used as a
place holder for a first argument of various types.  This permits
an operation to be specified by:
* a search clause
* a single object from a list returned by a query function
* a list of objects returned by a query function
* a dictionary specifying the required attributes for the operation
* a list of dictionaries specifying the required attributes for the operation

For example, all of the following are equivalent:

retry("jid=123 and tid=1")
retry(tasks("jid=123 and tid=1")[0])
retry(tasks("jid=123 and tid=1"))
retry({"jid": 123, "tid": 1})
retry([{"jid": 123, "tid": 1}])

search=

An optionally specified search string.  It uses the same
natural-language-like syntax as used on the command line, not SQL.

*BE AWARE* that not specifying a search clause will get *ALL* of the
objects from the database and could impact database and/or engine
performance for large result sets.

sort=

The result set can be sorted on the database server
on the specified columns before being sent to the client.  Multiple columns
can be specified for secondary sorting.  Prefixing the column
name with '-' causes reverse sorting order.  For example, the
following call pauses jobs spooled in the last hour, with
the operations performed in order of job owner, and secondarily by
pausing each owner's most recently spooled jobs first.

pause("spooltime > -1h", sortby=["owner", "-spooltime"])

While item sorting could be done client side, it is not possible
to do so if the result set is truncated with the limit= argument,
since truncation will happen server side, before the client
has a chance to sort the results.

*BE AWARE* that sorting can have an impact on the database server
for certain queries.

limit=

Space and time efficiencies can be obtained by using the limit=
argument to place an upper bound on the number of rows returned
by the database server, and hence the number of items operated on.

*BE AWARE* that the default setting of 0 places *NO LIMIT* on the
number of records returned, as opposed to returning no results
whatsoever.  Keep this in mind if the limit will be programmatically
determined.

ANOTHER WARNING LABEL

*** BE CAREFUL *** using these operation functions because they are
*VERY* powerful and can be *VERY* *VERY* *VERY* *DESTRUCTIVE*.

For exampe, either of these very short commands will delete
*every job*.

delete(jobs()) 
delete("jid")  # "jid" means "jid != 0", which is every job

Remember, you can use limit= to reduce the number of operations
performed at any one time.  So, for example, if you just want to
delete a single job, you could use limit=1 to ensure that a typo in
the search clause doesn't cause widespread damage.

In the following example, an accidentally specified "or jid"
would cause all jobs to be matched; although an incorrect job may be
deleted here, widespread damage has been avoided with limit=1.

delete("jid=123 or jid", limit=1)

Have fun!
"""

from .exceptions import TractorQueryError, PasswordRequired, \
     InvalidValue, MissingSearchClause, MissingParameter, MissingTargetKey, \
     SortNotAllowed

from .base import jobs, tasks, commands, invocations, blades, params, \
     chcrews, chpri, jattr, pause, unpause, lock, unlock, interrupt, \
     restart, retryactive, retryerrors, skiperrors, delay, undelay, \
     delete, undelete, retry, resume, skip, kill, log, cattr, chkeys, \
     jobdump, \
     nimby, unnimby, trace, eject, delist, setEngineClientParam, \
     closeEngineClient, needsPassword
 
__all__ = (
    "TractorQueryError", "PasswordRequired", "InvalidValue",
    "MissingSearchClause", "MissingParameter", "MissingTargetKey",
    "SortNotAllowed",
    "jobs", "tasks", "commands", "invocations", "blades", "params",
    "chcrews", "chpri", "jattr", "pause", "unpause", "interrupt", "restart",
    "retryactive", "retryerrors", "skiperrors", "delay", "undelay", "delete",
    "undelete", "jobdump",
    "retry", "resume", "skip", "kill", "log", "cattr", "chkeys", "nimby", "unnimby",
    "trace", "eject", "delist",
    "setEngineClientParam", "closeEngineClient", "needsPassword"
    )
