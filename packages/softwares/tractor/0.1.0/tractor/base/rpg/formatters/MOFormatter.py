import types

from rpg.formatters.Formatter import Formatter, FormatterError

__all__ = (
        'UnknownVariable',
        'MOFormatter',
        )

# ----------------------------------------------------------------------------

class UnknownVariable(FormatterError):
    pass

# ----------------------------------------------------------------------------

class MOFormatter(Formatter):
    """Multiple Object Formatter for merging existing subclasses of
    Formatter into one formatter.  This allows for one formatter to be
    used when printing variables from more than one object type.  Suppose
    you want to print variables in a Job object along with variables from
    a Slot object.  Instead of creating some special formatter, or worse
    bypassing the Formatter class entirely and having to write your own
    format strings, just merge a JobFormatter and a SlotFormatter.

    When printing common variables (i.e. the variable 'huntgroups' is
    defined in both Job and Slot) specify them in the variable list as
    'Job.huntgroups' and 'Slot.huntgroups'.  By default if no class name
    is prepended MOFormatter will search for a valid variable name using
    the formatters list defined when a MOFormatter is initialized.  Below
    is an example to print all the Tasks that are waiting for a specific
    machine to have a free slot::

      # import the needed modules
      import misterd.MRD as MRD
      from misterd.Formatters import *
  
      # create a multiple object formatter for printing Job
      # and Task variables together
      jtf = MOFormatter([JobFormatter, TaskFormatter],
                        ['dispatcher=20', 'shot=15',
                         'Task.title=25', 'frame=6',
                         'slots=10', 'statesecs=8'])
      # create the MRD object
      mrd = MRD.MRD()
      # open connection to the database
      mrd.open()
      # get all the ready Tasks and order by statetime so the
      # Tasks that have been waiting the longest are first.
      tasks = mrd.getTasks(state='ready', orderby=['statetime'])
      # print a header
      jtf.printHeader(headers={'statesecs': 'waiting'})
      # print a nice divider line
      jtf.printDivider()
      # a dictionary for saving Jobs that we've already fetched
      # from the database.
      jobs = {}
      # go through each Task and find out if it is waiting for
      # a specific machine.


      for t in tasks:
          # if the pickuptime is not zero then this Task
          # has been active, most likely to do a migrate
          if t.pickuptime > 0:
              # if we catch a KeyError then we know we haven't
              # fetched that Job yet.
              try:
                  job = jobs[t.jid]
              except KeyError:
                  job = mrd.getJob(t.jid)
                  jobs[t.jid] = job

              # if the Job isn't paused then it is waiting for
              # slots
              if not job.pausetime:
                  # notice we pass the Job and Task objects in
                  # a list, it doesn't matter what order they
                  # are passed in as.
                  jtf.printObject([job, t])

      # close the connection with the database
      mrd.close()
    
    the above code will produce output similar to::

      dispatcher           shot            Task.title                frame  slots      waiting  
      ==================== =============== ========================= ====== ========== ======== 
      alpert@odie:9001     cdev_rsprings_3 Render.fg_occlusion       50     u1157_001  06:20:22
      desiree@hathor:9001  n41a_1cweb      Render.film_crush         111    sf90_002   04:24:51
      alpert@odie:9001     cdev_rsprings_3 Render.fg_occlusion       36     u1609_002  03:16:27
      lisaf@etch:9001      bdn_1           Render.pertest_noenv      443    u1411_001  02:32:59
      lisaf@etch:9001      bdn_1           Render.pertest_noenv      449    u1411_001  02:32:50
      lisaf@etch:9001      bdn_1           Render.pertest_noenv      465    u1148_001  02:28:56
      alpert@odie:9001     cdev_rsprings_2 FinalRender.ltt           12     u1153      01:56:56
      abroms@scorpion:9001 bdn_6           Render.light_hair         26     u1421_001  01:56:33
      abroms@scorpion:9001 bdn_6           Render.light_hair         11     u1407_001  01:56:19
      abroms@scorpion:9001 bdn_6           Render.light_hair         18     u1117_002  01:53:38
      abroms@scorpion:9001 bdn_6           Render.light_hair         25     u1419_001  01:52:09
      abroms@scorpion:9001 bdn_4a          Render.light_prelim       109    u1116_001  01:23:30
      abroms@scorpion:9001 bdn_4a          Render.light_prelim       110    u1116_002  01:23:07
      bmrosen@bach:9001    bdn_fur_10      Render.light              96     u1409_001  01:22:13
      ssb@karloff:9001     h21b_2          Render.auto...,autoapic_z 43     u1630_002  00:50:29
      ssb@karloff:9001     h21b_2a         Render.auto...,autoapic_z 298    u1209_002  00:48:36
      ssb@karloff:9001     h21b_2          Render.auto...,autoapic_z 124    u1223_002  00:47:45
      ssb@karloff:9001     h21b_2          Render.auto...,autoapic_z 126    u1314_001  00:47:44
      ssb@karloff:9001     h21b_2          Render.auto...,autoapic_z 127    u1225_001  00:46:58
      ssb@karloff:9001     h21b_2          Render.auto...,autoapic_z 129    u1312_002  00:46:50
      ...."""
    
    def __init__(self, formatters, variables, separator=' ', formatStr=None):
        """Initialize a MOFormatter object with a list of formatters
        to be merged and the variables list.

        @raise FormatterError: no Formatters specified.
        """

        # sanity check
        if not formatters:
            raise FormatterError("no Formatters specified.")

        # initialize the variables
        # keeps the order that the formatters were defined as
        self.order      = []
        # keyed by base format class name, value is a Formatter object
        # for that class type
        self.formatters = {}
        # keyed by base format class name, value is an object of that
        # type
        self.objects    = {}

        for f in formatters:
            # create a Formatter object with no variables to print
            fobj = f([])
            # get the classname of the object being formatted
            classname = fobj.className.__class__.__name__
            # add everything to the list
            self.order.append(classname)
            self.formatters[classname] = fobj
            self.objects[classname] = fobj.className

        # cached used to identify a variable name with a class name
        # if it has already been referenced.
        self.varcache = {}

        # since we have overloaded the appropriate functions, it doesn't
        # matter that we aren't passing a valid class name to Formatter.
        Formatter.__init__(self, None, variables, separator=separator,
                           formatStr=formatStr)

        # now, as if what we've done isn't crazy enough, point the widths
        # data back to all the formatters so they know it too
        for val in list(self.formatters.values()):
            val.widths = self.widths
            val.lines  = self.lines
            val.colors = self.colors

    def __isNameInBases(self, cls, name):
        """Check if any base class of cls has the name 'name'"""

        # if the current class is what we are looking for than say so
        if cls.__name__ == name:
            return True

        # start searching all the base classes
        for base in cls.__bases__:
            # recursively check each base class's base classes
            if self.__isNameInBases(base, name):
                return True

        # no match was found
        return False
                

    def _getValForClass(self, classname, var):
        """Returns a variable for specified class, and references the
        object in self.formatters for an overloaded variable name and
        self.objects for a normal one.

        @raise FormatterError: no classname provided.
        @raise UnkownVariable: no variable found in the given class.
        """

        try:
            object = self.objects[classname]
        except KeyError:
            # check if the instance we are formatting is a subclass of one
            # of our objects
            for object in list(self.objects.values()):
                if self.__isNameInBases(object.__class__, classname):
                    self.objects[classname] = object
                    break
            else:
                raise FormatterError("no %s class provided" % classname)

        formatter = self.formatters[classname]
        
        try:
            # try to find a get_var function
            func = getattr(formatter, 'get_' + var)
        except AttributeError:
            # if no get_var function found, then just assume
            # a valid value exists in the object.
            try:
                val = getattr(object, var)
            except AttributeError:
                raise UnknownVariable("no variable=%s found in the " \
                      "class %s" % (var, classname))
            else:
                return val
        else:
            # call the get_var function and pass the called variable name
            val = func(object, var)
            return val

    def _getValue(self, name, var):
        """Return the appropriate variable based on the passed in
        variable name.  This requires splitting the var string
        to see if a class is specified, if not then iterate through
        the predefined order.

        @raise UnkownVariable: no variable found in the given class.
        """

        # first look in the cache for this variable's class name
        try:
            cname = self.varcache[var]
        except KeyError:
            pass
        else:
            return self._getValForClass(cname, var)

        # is a class specified?
        svar = var.split('.')
        if len(svar) == 2:
            return self._getValForClass(svar[0], svar[1])

        # otherwise return the first solution we find
        for classname in self.order:
            try:
                val = self._getValForClass(classname, var)
            except UnknownVariable:
                pass
            else:
                # save this in the cache for next time
                self.varcache[var] = classname
                return val
        # if no value is found then raise an exception
        else:
            raise UnknownVariable("no variable=%s found in Job, Task, " \
                  "or Slot." % var)

    def getString(self, objects, fitString='squeeze'):
        """
        Returns a string based on the format string set in __init__
        and uses the objects defined in the list 'objects' as the input.
        The order of the objects does not matter, and you also need not
        pass in an object that won't be used.  For example, if your
        format string doesn't require any variables from the Slot object,
        then one need not be passed.

        """

        # get rid of the old values
        self.objects.clear()
        
        for obj in objects:
            self.objects[obj.__class__.__name__] = obj

        # since we have overloaded _getValue we need not pass a valid
        # object.
        return Formatter.getString(self, None, fitString=fitString)
    

