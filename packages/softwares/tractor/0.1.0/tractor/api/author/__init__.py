"""
This module is an API for authoring Tractor jobs in a job file format
agnostic manner.  Currently, only an heirarchical Tcl format is
supported; however, other formats could be added later.

--
Jobs are built using the Job, Task, Instance, Command, and Iterate
classes, and the resultant Tcl can be output using the Job.asTcl()
method, or spooled using the Job.spool() method.  Here is a simple
example:

job = Job(title="a one-task render job", priority=100, service="PixarRender")
job.newTask(title="A one-command task", argv=["/usr/bin/prman", "file.rib"])
print job.asTcl()
job.spool()

--
Attributes can be set using keyword args as above, or using the = operator.
The same job could have been created as follows:

job = Job()
job.title = "a one-task render job"
job.priority = 100
job.service = "PixarRender"
job.newTask(title="A one-command task", argv=["/usr/bin/prman", "file.rib"])

--
The steps of creating a new task and adding it as a child of a job
or other task can be broken down into separate steps.

job = Job(title="a one-task render job", priority=100, service="PixarRender")
task = Task(title="A one-command task", argv=["/usr/bin/prman", "file.rib"])
job.addChild(task)

--
For tasks to run serially, so that one task must finish before another
one starts, the task to run first is declared as a child of the
second.

parent = Task(title="parent runs second", argv=["/usr/bin/command"])
child = Task(title="child runs first", argv=["/usr/bin/command"])
parent.addChild(child)

--
For tasks to run in parallel, they need only be instantiated with the
same parent.

parent = Task(title="comp", argv=["/usr/bin/comp", "fg.tif", "bg.tif"])
parent.newTask(title="render fg", argv=["/usr/bin/prman", "fg.rib"])
parent.newTask(title="render bg", argv=["/usr/bin/prman", "bg.rib"])

--
One can optionally run *all* child tasks of a parent serially by
setting the parent's serialsubtasks=1.

parent.serialsubtasks = 1

--
Instances are implicitly determined when a task has been added to more
than one parent task.

render1 = Task(title="render rib 1", argv=["/usr/bin/prman", "1.rib"])
render2 = Task(title="render rib 2", argv=["/usr/bin/prman", "2.rib"])
ribgen = Task(title="rib generator",
              argv=["/usr/bin/ribgen", "1,2", "scene.file"])
render1.addChild(ribgen)
render2.addChild(ribgen)

--
Instances can also be explicitly defined, passing the title of the
referred task.

render1 = Task(title="render rib 1", argv=["/usr/bin/prman", "1.rib"])
render2 = Task(title="render rib 2", argv=["/usr/bin/prman", "2.rib"])
ribgen = Task(title="make rib", argv=["/usr/bin/ribgen", "1,2", "scene.file"])
render1.addChild(ribgen)
instance = Instance(title="rib generator")
render2.addChild(instance)

--
In the above examples, each Task had a single Command.  Of course,
Tasks can have multiple commands.

task = Task(title="multi-command task", service="PixarRender")
task.newCommand(argv=["scp", "remote:/path/file.rib", "/local/file.rib"])
task.newCommand(argv=["/usr/bin/prman", "/local/file.rib"])
task.newCommand(argv=["scp", "/local/file.tif", "remote:/path/file.tif"])

--
And commands can be added manually if so desired.

command = Command(argv["/usr/bin/prman", "/tmp/a.rib"], service="PixarRender")
task.addCommand(command)

--
Iterate is supported.  To add Tasks that are to be within the iterate
loop, use addToTemplate().

iterate = Iterate(varname="i", frm=1, to=10)
fetchRib = Task(title="fetch rib $i",
                argv=["scp", "remote:/path/file.$i.rib", "/local/file.$i.rib"])
render = Task(title="render rib $i",
              argv=["/usr/bin/prman", "/local/file.$i.rib"])
iterate.addToTemplate(fetchRib)
iterate.addToTemplate(render)

--
Like the Job and Task elements, if the Iterate instance needs to wait
for other tasks to finish before starting, use the addChild() or
newTask() methods.

iterate.newTask(title="rib generation", argv=["/usr/bin/ribgen", "1-10"])

--
When assigning a value to an element's attribute, an exception is
raised if it is not a valid value.

try:
    job.atmost = "three"
except TypeError, err:
    print "you can expect to see this message"

--
Attributes and other elements have asTcl() to express the job in Tcl format.
However, other formats such as JSON or SQL could be supported by adding
similar methods.
"""

from .exceptions import AuthorError, RequiredValueError, ParentExistsError,\
    SpoolError
from .base import Job, Task, Instance, Command, Iterate, setEngineClientParam, closeEngineClient

__all__ = (
    "AuthorError", "RequiredValueError", "ParentExistsError", "SpoolError",
    "Job", "Task", "Instance", "Command", "Iterate",
    "setEngineClientParam", "closeEngineClient"
    )
