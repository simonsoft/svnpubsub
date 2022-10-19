import logging
from threading import Thread
from queue import PriorityQueue

BACKLOG_TOO_HIGH = 500


class BackgroundWorker(Thread):

    def __init__(self, recursive=False, **kwargs):
        Thread.__init__(self)
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.q = PriorityQueue()
        # Set the kwargs as class attributes
        self.daemon = True  # The main thread/process should not wait for this thread to exit.
        self.recursive = recursive
        self.started = False

    def run(self):
        while True:
            # This will block until something arrives
            job: BackgroundJob = self.q.get()['job']
            # Warn if the queue is too long.
            # Note: The other thread might have added entries to self.q after the .get() and before the .qsize()
            qsize = self.q.qsize() + 1
            if qsize > BACKLOG_TOO_HIGH:
                logging.warning('The background worker backlog is at: %d', qsize)
            try:
                prev_exists = self.__validate(job)
                if prev_exists:
                    job.run()
                elif self.recursive:
                    logging.info('Rev - 1 has not been processed, adding it to the queue')
                    self.queue(job)
                    self.queue(type(job)(job.repo, job.rev - 1, job.head))
                self.q.task_done()
            except Exception:
                logging.exception('Exception in background worker.')

    def queue(self, job):
        # Start the thread when work first arrives. Thread-start needs to
        # be delayed in case the process forks itself to become a daemon.
        if not self.started:
            self.start()
            self.started = True
        # Add the new job to the queue
        self.q.put({'rev': job.rev, 'job': job})

    def __validate(self, job):
        logging.info("Validating r%s in: %s" % (job.rev, job.repo))
        return job.validate()


class BackgroundJob(object):

    def __init__(self, repo, rev, head, **kwargs):
        self.repo = repo
        self.rev = rev
        self.head = head
        # Set the kwargs as class attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

    def validate(self) -> bool:
        raise NotImplementedError("The child class must supply its own implementation!")

    def run(self):
        raise NotImplementedError("The child class must supply its own implementation!")

