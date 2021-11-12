import time
import tractor.api.author as author
import tractor.api.query as tq
import tractor.base.EngineClient as EngineClient

def test_job_ops(job):
    print("chcrews")
    tq.chcrews(job, crews=["newcrew1", "newcrew2"])
    print("chpri")
    tq.chpri(job, priority=123)
    print("jattr")
    tq.jattr(job, key="comment", value="new comment")
    print("pause")
    tq.pause(job)
    print("unpause")
    tq.unpause(job)
    print("lock")
    tq.lock(job)
    print("unlock")
    tq.unlock(job)
    print("interrupt")
    try:
        tq.interrupt(job)
    except EngineClient.TransactionError as err:
        print("received exception for interrupting job - we should fix that")
    print("restart")
    tq.restart(job)
    print("retryactive")
    tq.retryactive(job)
    print("retryerrors")
    tq.retryerrors(job)
    print("skiperrors")
    tq.skiperrors(job)
    print("delay")
    try:
        tq.delay(job, aftertime="2015-01-01 1:35:00")
    except TypeError as err:
        print("we should fix modules so we don't get this type error: %s" % str(err))
    print("undelay")
    tq.undelay(job)
    print("delete")
    tq.delete(job)
    print("undelete")
    tq.undelete(job)

def test_task_ops(task):
    print("retry")
    tq.retry(task)
    print("resume")
    tq.resume(task)
    print("kill")
    tq.kill(task)
    print("skip")
    tq.skip(task)
    print("log")
    log = tq.log(task)
    print(log)

def test_command_ops(command):
    print("cattr")
    tq.cattr(command, key="tags", value=["new", "tags"])
    print("chkeys")
    tq.chkeys(command, keystr="newService")

def test_blade_ops(blade):
    print("nimby")
    tq.nimby(blade)
    print("nimby but allow self")
    tq.nimby(blade, allow="adamwg")
    print("unnimby")
    tq.unnimby(blade)
    print("trace")
    trace = tq.trace(blade)
    print(trace)
    print("eject")
    tq.eject(blade)
    print("delist")
    tq.delist(blade)

def spoolJob():
    """Spool a test job and return its jid."""
    job = author.Job(title="test job")
    task = job.newTask(title="test task", argv=["sleep 60"], service="pixarRender")
    jid = job.spool()
    print("spooled job %d" % jid)
    time.sleep(2)
    return jid

def test_jobdump(jid):
    dump = tq.jobdump("jid=%d" % jid)
    print(dump)

def test():
    """Spool a test job and run test operations on it."""
    jid = spoolJob()
    job = tq.jobs("jid=%d" % jid)[0]
    test_job_ops(job)
    task = tq.tasks("jid=%d and tid=1" % jid, columns=["Job.owner"])
    test_task_ops(task)
    command = tq.commands("jid=%d and cid=1" % jid)
    test_command_ops(command)
    #test_jobdump(jid)

def test_params():
    # test setting of all possible parameters
    for param in EngineClient.EngineClient.VALID_PARAMETERS:
        print("set %s" % param)
        # value of 0 is okay since type is not checked by API (we could add that though)
        tq.setEngineClientParam(**{param: 0})
    # check that invalid parameters are caught
    try:
        tq.setEngineClientParam(**{"invalidxyz": 0})
    except EngineClient.InvalidParamError as err:
        print("Successfuly caught invalid parameter exception.")
    
if __name__=="__main__":
    test_params()
