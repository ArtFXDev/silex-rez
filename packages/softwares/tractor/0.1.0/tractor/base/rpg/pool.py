import _thread
import threading
import queue

# ------------------------------------------------------------------------------

class ThreadPool(object):
    def __init__(self, threadcount):
        # We need at least 1 thread to run on
        self.thread_count = max(1, threadcount)

        # The queue we put tasks into.
        self.ready_queue = queue.Queue()

        # spawn off some worker threads
        self.threads = []
        for i in range(self.thread_count):
            thread = WorkerThread(self.ready_queue)
            self.threads.append(thread)
            _thread.start()

    def __del__(self):
        self.join()

    def map(self, function, args):
        """A function that applies the function to the arguments in parallel,
        and returns the results a list ordered by the initial arguments."""

        results = [None for i in range(len(args))]

        for task in self._evaluate(function, args):
            results[task.index] = task.result

        return results

    def imap(self, function, args):
        """A function that applies the function to the arguments in parallel,
        and returns the results in a generator in arbitrary order."""

        for task in self._evaluate(function, args):
            yield task.result

    def _evaluate(self, function, args):
        """Evaluate the functions."""

        # Construct all our tasks.
        tasks = [Task(function, arg, index) for index, arg in enumerate(args)]

        # Where the results will come back.
        done_queue = queue.Queue()

        # Load up the work queue.
        for task in tasks:
            self.ready_queue.put((done_queue, task))

        # How many tasks we are waiting on.
        count = len(tasks)

        while count != 0:
            task = done_queue.get()
            count -= 1

            # If we got an exception, error out.
            if task.exc is not None:
                # Make sure to clear out our task queue
                for t in tasks:
                    t.done = True

                raise task.exc

            yield task

    def join(self):
        """Stop processing work, and shut down the threads."""

        # Add the sentinels
        for thread in self.threads:
            self.ready_queue.put(None)

        for thread in self.threads:
            _thread.join()

# ------------------------------------------------------------------------------

class WorkerThread(threading.Thread):
    def __init__(self, ready_queue):
        super(WorkerThread, self).__init__()

        self.setDaemon(True)
        self.__ready_queue = ready_queue
        self.__finished = False

    def shutdown(self):
        self.__finished = True

    def run(self):
        try:
            while not self.__finished:
                if self.run_one():
                    break
        except KeyboardInterrupt:
            # Let the main thread know we got a SIGINT.
            _thread.interrupt_main()
            raise

    def run_one(self, *args, **kwargs):
        queue_task = self.__ready_queue.get(*args, **kwargs)

        try:
            # Exit cleanly if we were passed a None.
            if queue_task is None:
                return True

            out_queue, task = queue_task

            try:
                task.run()
            finally:
                out_queue.put(task)

            return False
        finally:
            # Work around python 2.3 not having task_done.
            if hasattr(self.__ready_queue, 'task_done'):
                self.__ready_queue.task_done()

# ------------------------------------------------------------------------------

class Task(object):
    def __init__(self, function, arg, index):
        self.function = function
        self.arg = arg
        self.index = index
        self.exc = None
        self.done = False

    def run(self):
        # Exit early if we've already run.
        if self.done:
            return

        try:
            self.result = self.function(self.arg)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.exc = e
