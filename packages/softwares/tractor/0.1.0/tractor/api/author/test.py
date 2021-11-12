import datetime
import tractor.api.author as author


def test_short():
    """This test shows how a two task job can be created with as few
    statements as possible.
    """
    job = author.Job(title="two layer job", priority=10,
                     after=datetime.datetime(2012, 12, 14, 16, 24, 5))
    compTask = job.newTask(title="comp", argv="comp fg.tif bg.tif final.tif")
    fgTask = compTask.newTask(title="render fg", argv="prman foreground.rib")
    bgTask = compTask.newTask(title="render bg", argv="prman foreground.rib")
    print(job)


def test_long():
    """This test shows how a two task job can be built with many more
    statements.
    """
    job = author.Job()
    job.title = "two layer job"
    job.priority = 10
    job.after = datetime.datetime(2012, 12, 14, 16, 24, 5)

    fgTask = author.Task()
    fgTask.title = "render fg"
    fgCommand = author.Command()
    fgCommand.argv = "prman foreground.rib"
    fgTask.addCommand(fgCommand)

    bgTask = author.Task()
    bgTask.title = "render bg"
    bgCommand = author.Command()
    bgCommand.argv = "prman background.rib"
    bgTask.addCommand(bgCommand)

    compTask = author.Task()
    compTask.title = "render comp"
    compCommand = author.Command()
    compCommand.argv = "comp fg.tif bg.tif final.tif"
    compCommand.argv = ["comp"]
    compTask.addCommand(compCommand)

    compTask.addChild(fgTask)
    compTask.addChild(bgTask)
    job.addChild(compTask)

    print(job.asTcl())


def test_all():
    """This test covers setting all possible attributes of the Job, Task,
    Command, and Iterate objects.
    """
    job = author.Job()
    job.title = "all attributes job"
    job.after = datetime.datetime(2012, 12, 14, 16, 24, 5)
    job.afterjids = [1234, 5678]
    job.paused = True
    job.tier = "express"
    job.projects = ["animation"]
    job.atleast = 2
    job.atmost = 4
    job.newAssignment("tempdir", "/tmp")
    job.serialsubtasks = True
    job.spoolcwd = "/some/path/cwd"
    job.newDirMap(src="X:/", dst="//fileserver/projects", zone="UNC")
    job.newDirMap(src="X:/", dst="/fileserver/projects", zone="NFS")
    job.etalevel = 5
    job.tags = ["tag1", "tag2", "tag3"]
    job.priority = 10
    job.service = "linux||mac"
    job.envkey = ["ej1", "ej2"]
    job.comment = "this is a great job"
    job.metadata = "show=rat shot=food"
    job.editpolicy = "canadians"
    job.addCleanup(author.Command(argv="/bin/cleanup this"))
    job.newCleanup(argv=["/bin/cleanup", "that"])
    job.addPostscript(author.Command(argv=["/bin/post", "this"]))
    job.newPostscript(argv="/bin/post that")

    compTask = author.Task()
    compTask.title = "render comp"
    compTask.resumeblock = True
    compCommand = author.Command()
    compCommand.argv = "comp /tmp/*"
    compTask.addCommand(compCommand)
    
    job.addChild(compTask)

    for i in range(2):
        task = author.Task()
        task.title = "render layer %d" % i
        task.id = "id%d" % i
        task.chaser = "chase file%i" % i
        task.preview = "preview file%i" % i
        task.service = "services&&more"
        task.atleast = 7
        task.atmost = 8
        task.serialsubtasks = 0
        task.metadata = "frame=%d" % i
        task.addCleanup(author.Command(argv="/bin/cleanup file%i" % i))

        command = author.Command(local=bool(i % 2)) # alternates local and remote commands
        command.argv = "prman layer%d.rib" % i
        command.msg = "command message"
        command.service = "cmdservice&&more"
        command.tags = ["tagA", "tagB"]
        command.metrics = "metrics string"
        command.id = "cmdid%i" % i
        command.refersto = "refersto%i" % i
        command.expand = 0
        command.atleast = 1
        command.atmost = 5
        command.minrunsecs = 8
        command.maxrunsecs = 88

        command.samehost = 1
        command.envkey = ["e1", "e2"]
        command.retryrc = [1, 3, 5, 7, 9]
        command.resumewhile = ["/usr/bin/grep", "-q", "Checkpoint", "file.%d.exr" % i]
        command.resumepin = bool(i)
        command.metadata = "command metadata %i" % i

        task.addCommand(command)
        compTask.addChild(task)

    iterate = author.Iterate()
    iterate.varname = "i"
    iterate.frm = 1
    iterate.to = 10
    iterate.addToTemplate(
        author.Task(title="process task", argv="process command"))
    iterate.addChild(author.Task(title="process task", argv="ls -l"))
    job.addChild(iterate)

    instance = author.Instance(title="id1")
    job.addChild(instance)

    print(job.asTcl())


def test_instance():
    """This test checks that an instance will be created when a task is
    added as a child to more than one task.
    """
    job = author.Job(title="two layer job")
    compTask = job.newTask(title="comp", argv="comp fg.tif bg.tif final.tif")
    fgTask = compTask.newTask(title="render fg", argv="prman foreground.rib")
    bgTask = compTask.newTask(title="render bg", argv="prman foreground.rib")
    ribgen = author.Task(title="ribgen", argv="ribgen 1-10")
    fgTask.addChild(ribgen)
    bgTask.addChild(ribgen)
    print(job)


def test_double_add():
    """This test verifies that an interate object cannot be a child to
    more than one task.
    """
    iterate = author.Iterate()
    iterate.varname = "i"
    iterate.frm = 1
    iterate.to = 10
    iterate.addToTemplate(
        author.Task(title="process task", argv="process command"))
    iterate.addChild(author.Task(title="process task", argv="ls -l"))

    t1 = author.Task(title="1")
    t2 = author.Task(title="2")

    t1.addChild(iterate)
    try:
        t2.addChild(iterate)
    except author.ParentExistsError as err:
        print("Good, we expected to get an exception for adding a iterate "\
            "to two parents: %s" % str(err))


def test_bad_attr():
    """This test verifies that an exception is raised when trying to set
    an invalid attribute.
    """
    job = author.Job()
    try:
        job.title = "okay to set title"
        job.foo = "not okay to set foo"
    except AttributeError as err:
        print("Good, we expected to get an exception for setting an invalid "\
            "attribute: %s" % str(err))


def test_spool():
    """This tests the spool method on a job."""
    job = author.Job(
        title="two layer job", priority=10,
        after=datetime.datetime(2012, 12, 14, 16, 24, 5))
    compTask = job.newTask(
        title="comp", argv="comp fg.tif bg.tif out.tif", service="pixarRender")
    fgTask = compTask.newTask(
        title="render fg", argv="prman foreground.rib", service="pixarRender")
    bgTask = compTask.newTask(
        title="render bg", argv="prman foreground.rib", service="pixarRender")
    print(job.spool(spoolfile="/spool/file", spoolhost="spoolhost", hostname="torchwood", port=8081))
    print(job.spool(spoolfile="/spool/file", spoolhost="spoolhost", hostname="torchwood", port=8080))


def test_postscript():
    """This builds a job with varios postscript commands.  Submit the
    job to ensure that only the "none", "always", and "done"
    postscript commands run.
    """
    job = author.Job(title="Test Postscript Done")
    job.newTask(title="sleep", argv="sleep 1", service="pixarRender")
    job.newPostscript(argv="touch /tmp/postscript.none.%j", service="pixarRender")
    job.newPostscript(argv="touch /tmp/postscript.done.%j", when="done", service="pixarRender")
    job.newPostscript(argv="touch /tmp/postscript.error.%j", when="error", service="pixarRender")
    job.newPostscript(argv="touch /tmp/postscript.always.%j", when="always", service="pixarRender")
    try:
        job.newPostscript(argv="touch /tmp/postscript.always.%j", when="nope")
    except TypeError as err:
        print("Good, we caught an invalid value for when: %s" % str(err))
    print(job.asTcl())
    
def test_postscript_error():
    """This builds a job with varios postscript commands.  Submit the
    job to ensure that only the "none", "always", and "error"
    postscript commands run.
    """
    job = author.Job(title="Test Postscript Error")
    job.newTask(title="fail", argv="/bin/false", service="pixarRender")
    job.newPostscript(argv="touch /tmp/postscript.none.%j", service="pixarRender")
    job.newPostscript(argv="touch /tmp/postscript.done.%j", when="done", service="pixarRender")
    job.newPostscript(argv="touch /tmp/postscript.error.%j", when="error", service="pixarRender")
    job.newPostscript(argv="touch /tmp/postscript.always.%j", when="always", service="pixarRender")
    try:
        job.newPostscript(argv="touch /tmp/postscript.always.%j", when="nope")
    except TypeError as err:
        print("Good, we caught an invalid value for when: %s" % str(err))
    print(job.asTcl())
    
if __name__ == "__main__":
    """Run tests."""
    test_short()
    test_long()
    test_all()
    test_instance()
    test_double_add()
    test_bad_attr()
    test_postscript()
    test_postscript_error()
#    test_spool()
