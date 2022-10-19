import os
import logging
import daemonize
from typing import List
from svnpubsub.client import MultiClient, Commit
from svnpubsub.bgworker import BackgroundWorker


class DaemonTask(object):

    def __init__(self, urls: List[str], worker: BackgroundWorker = BackgroundWorker()):
        self.urls = urls
        self.worker = worker

    def start(self):
        raise NotImplementedError("The child class must supply its own implementation!")

    def commit(self, url: str, commit: Commit):
        raise NotImplementedError("The child class must supply its own implementation!")


class Daemon(daemonize.Daemon):

    def __init__(self, name, logfile, pidfile, umask, task: DaemonTask):
        daemonize.Daemon.__init__(self, logfile=logfile, pidfile=pidfile)
        self.name = name
        self.umask = umask
        self.task = task

    def setup(self):
        # There is no setup which the parent needs to wait for.
        pass

    def run(self):
        logging.info('Starting: %s (PID=%d)', self.name, os.getpid())
        # Set the umask in the daemon process. Defaults to 000 for daemonized processes.
        # Foreground processes simply inherit the value from the parent process.
        if self.umask is not None:
            umask = int(self.umask, 8)
            os.umask(umask)
            logging.info('Set daemon umask to: %03o', umask)

        # Start the BDEC (on the main thread), then start the client
        self.task.start()

        mc = MultiClient(self.task.urls, self.task.commit, self.__event_callback)
        mc.run_forever()

    def __event_callback(self, url, event_name, event_arg):
        if event_name == 'error':
            logging.exception('Exception from: %s', url)
        elif event_name == 'ping':
            logging.debug('Pinged from: %s', url)
        else:
            logging.info('Received event: "%s" from: %s', event_name, url)
