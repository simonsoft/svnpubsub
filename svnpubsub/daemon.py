import os
import logging
import daemonize
from typing import List
from svnpubsub.client import MultiClient, Commit
from svnpubsub.bgworker import BackgroundWorker


class DaemonTask(object):

    def __init__(self, urls: List[str], excluded_repos: List[str] = [], worker: BackgroundWorker = BackgroundWorker()):
        self.urls = urls
        self.worker = worker
        self.excluded_repos = excluded_repos

    def start(self):
        raise NotImplementedError("The child class must supply its own implementation!")

    def commit(self, url: str, commit: Commit):
        raise NotImplementedError("The child class must supply its own implementation!")

    def event_callback(self, url: str, commit: Commit):
        if commit.type != 'svn' or commit.format != 1:
            logging.info("Skipped unknown commit format: (%s.%d)", commit.type, commit.format)
            return
        logging.info("Commit r%d (%d paths) from: %s" % (commit.id, len(commit.changed), url))
        for repo in self.excluded_repos:
            if commit.repositoryname.startswith(repo):
                logging.info('Commit in excluded repository, ignoring: %s' % commit.repositoryname)
                return
        self.commit(url=url, commit=commit)


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

        mc = MultiClient(self.task.urls, self.task.event_callback, self.metadata_callback)
        mc.run_forever()

    def metadata_callback(self, url, event_name, event_arg):
        if event_name == 'error':
            logging.exception('Exception from: %s', url)
        elif event_name == 'ping':
            logging.debug('Pinged from: %s', url)
        else:
            logging.info('Received event: "%s" from: %s', event_name, url)
