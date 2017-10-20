from Queue import Queue
from threading import Thread

# Worker for multithreading all though adding subprocess prevents
class Worker(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            if func:
                try:
                    func(*args, **kargs)
                except Exception, e:
                    print "Unable to run worker: {} Function: {} Args: {}".format(e, func, args)
            self.tasks.task_done()

# Thread class for multithreading 
class ThreadPool(object):
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        self.tasks.join()
