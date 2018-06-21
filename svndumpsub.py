#!/usr/bin/env python
# encoding: UTF-8
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# SvnDumpSub - Subscribe to a SvnPubSub stream, dumps commits, zips and uploads them to AWS S3.
#
# Example:
#  svndumpsub.py
#
# On startup SvnDumpSub starts listening to commits in all repositories.
#

import errno
import subprocess
import threading
import sys
import stat
import os
import re
import posixpath
import json
try:
  import ConfigParser
except ImportError:
  import configparser as ConfigParser
import time
import logging.handlers
try:
  import Queue
except ImportError:
  import queue as Queue
import optparse
import functools
try:
  import urlparse
except ImportError:
  import urllib.parse as urlparse

import daemonize
import svnpubsub.client
import svnpubsub.util

HOST = "127.0.0.1"
PORT = 2069
AWS = "/home/vagrant/.local/bin/aws"
SVNADMIN = "/usr/bin/svnadmin"
SVNROOT = '/srv/cms/svn'

assert hasattr(subprocess, 'check_call')
def check_call(*args, **kwds):
    """Wrapper around subprocess.check_call() that logs stderr upon failure,
    with an optional list of exit codes to consider non-failure."""
    assert 'stderr' not in kwds
    if '__okayexits' in kwds:
        __okayexits = kwds['__okayexits']
        del kwds['__okayexits']
    else:
        __okayexits = set([0]) # EXIT_SUCCESS
    kwds.update(stderr=subprocess.PIPE)
    pipe = subprocess.Popen(*args, **kwds)
    output, errput = pipe.communicate()
    if pipe.returncode not in __okayexits:
        cmd = args[0] if len(args) else kwds.get('args', '(no command)')
        logging.error('Command failed: returncode=%d command=%r stderr=%r',
                      pipe.returncode, cmd, errput)
        raise subprocess.CalledProcessError(pipe.returncode, args)
    return pipe.returncode # is EXIT_OK   

class Job(object):
    
    def __init__(self, repo, rev, head):
        self.repo = repo
        self.rev = rev
        self.head = head
        self.env = {'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8'}
    
    def get_key(self, rev):
        #/v1/Cloudid/reponame/shardX/0000001000/reponame-0000001000.svndump.gz
        return '%s/%s' % (self._get_s3_base(), self.get_name(rev)) 
        
    def get_name(self, rev):
        revStr = str(rev)
        revStr = revStr.zfill(10)
        name = self.repo + '-' + revStr + '.svndump.gz'
        #reponame-0000001000.svndump.gz
        return name
        
    def _get_bucket_name(self):
        #TODO: Should not be hardcoded
        return 'cms-review-jandersson'  
    
    def _get_s3_base(self):
        d = self.rev / 1000;
        d = str(int(d)) + '000'
        shard_number = d.zfill(10)
        
        #TODO: Should not be hardcoded
        bucket = self._get_bucket_name()
        version = 'v1'
        cloudid = 'jandersson'
        shard_type = 'shard0'
        # v1/jandersson/demo1/shard0/0000000000
        return '%s/%s/%s/%s/%s' % (version, cloudid, self.repo, shard_type, shard_number) 
        
    def _get_svn_dump_args(self):
        path = '%s/%s' % (SVNROOT, self.repo)
        dump_rev = '-r%s:%s' % (self.rev, self.rev)
        #svnadmin dump --incremental --deltas /srv/cms/svn/demo1 -r 237:237
        return [SVNADMIN, 'dump', '--incremental', '--deltas', path, dump_rev]
    
    def _get_aws_cp_args(self):
        # aws s3 cp - s3://cms-review-jandersson/v1/jandersson/demo1/shard0/0000000000/demo1-0000000363.svndump.gz
        return [AWS, 's3', 'cp', '-',  's3://%s/%s' % (self._get_bucket_name() ,self.get_key(self.rev))]
        
    #Will recursively check a bucket if (rev - 1) exists until it finds a rev dump. 
    def validate_rev(self, repo, rev):
        
        validate_to_rev = self._get_validate_to_rev()
        rev_to_validate = rev - 1
        
        if rev_to_validate < validate_to_rev:
            logging.info('At first possible rev in shard, will not validate further rev: %s', rev_to_validate)
            return True
        
        key = self.get_key(rev_to_validate)
        args = [AWS, 's3api', 'head-object', '--bucket', self._get_bucket_name(), '--key', key] # Maybe use s3 cli or s3 api to do this.
        
        pipe = subprocess.Popen((args), stdout=subprocess.PIPE) 
        output, errput = pipe.communicate()
        
        if pipe.returncode != 0:
            logging.info('S3 Key do not exist %s' % key)
            return False
        else:
            try:
                #FUTURE: Parsing response to json. Will allow us to check size. e.g response_body['ContentLength']
                response_body = json.loads(output)
                logging.info('Previous key do exists %s will dump from %s' % (key, rev))
                return True
            except ValueError:
                logging.error('Could not parse response from s3api head-object with key: %s' % key)
                raise 'Could not parse response from s3api head-object with key: %s' % key
            
    def _get_validate_to_rev(self):
        rev_round_down = int((self.head - 1) / 1000)
        return rev_round_down * 1000
            
    def dump_cm_to_s3(self):
        logging.info('Dumping and uploading rev: %s from repo: %s' % (self.rev, self.repo))
        
        gz = '/bin/gzip'
        gz_args = [gz]

        # Svn admin dump
        p1 = subprocess.Popen((self._get_svn_dump_args()), stdout=subprocess.PIPE, env=self.env)
        # Zip stout
        p2 = subprocess.Popen((gz_args), stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        # Upload zip.stdout to s3
        output = subprocess.check_output((self._get_aws_cp_args()), stdin=p2.stdout)
        #TODO: Do we need to close stuff?
        p2.communicate()[0]    
                
        
class BigDoEverythingClasss(object):
    #removed the config object from __init__.
    def __init__(self):
        #TODO: Should home be vagrant? or SVN HOME? Not sure if this is needed.
        self.env = {'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8', 'LC_CTYPE': 'en_US.UTF-8'}
        self.streams = ["http://%s:%d/commits" %(HOST, PORT)]

        self.hook = None
        self.svnbin = '/usr/bin/svnadmin';
        self.worker = BackgroundWorker(self.svnbin, self.env, self.hook)
        self.watch = [ ]

    def start(self):
        logging.info('start')

    def commit(self, url, commit):
        if commit.type != 'svn' or commit.format != 1:
            logging.info("SKIP unknown commit format (%s.%d)",
                         commit.type, commit.format)
            return
        logging.info("COMMIT r%d (%d paths) from %s"
                     % (commit.id, len(commit.changed), url))
                     
        job = Job(commit.repositoryname, commit.id, commit.id)
        self.worker.add_job(job)
        
                
# Start logging warnings if the work backlog reaches this many items
BACKLOG_TOO_HIGH = 500

class BackgroundWorker(threading.Thread):
    def __init__(self, svnbin, env, hook):
        threading.Thread.__init__(self)

        # The main thread/process should not wait for this thread to exit.
        ### compat with Python 2.5
        self.setDaemon(True)

        self.svnbin = svnbin
        self.env = env
        self.hook = hook
        self.q = Queue.PriorityQueue()

        self.has_started = False

    def run(self):
        while True:
            # This will block until something arrives
            tuple = self.q.get()
            job = tuple[1]

            # Warn if the queue is too long.
            # (Note: the other thread might have added entries to self.q
            # after the .get() and before the .qsize().)
            qsize = self.q.qsize()+1
            if qsize > BACKLOG_TOO_HIGH:
                logging.warn('worker backlog is at %d', qsize)
            
            try:
                prev_exists = self._validate(job)
                if prev_exists:
                    job.dump_cm_to_s3()
                else:
                    logging.info('Rev - 1 has not been dumped, adding it to the queue')
                    self.add_job(job)
                    self.add_job(Job(job.repo, job.rev - 1, job.head))
                    
                self.q.task_done()
            except:
                logging.exception('Exception in worker')

    def add_job(self, job):
        # Start the thread when work first arrives. Thread-start needs to
        # be delayed in case the process forks itself to become a daemon.
        if not self.has_started:
            self.start()
            self.has_started = True

        self.q.put((job.rev, job))

    def _validate(self, job, boot=False):
        "Validate the specific job."
        logging.info("Starting validation of rev: %s in repo: %s" % (job.rev, job.repo))
        return job.validate_rev(job.repo, job.rev)

class Daemon(daemonize.Daemon):
    def __init__(self, logfile, pidfile, umask, bdec):
        daemonize.Daemon.__init__(self, logfile, pidfile)

        self.umask = umask
        self.bdec = bdec

    def setup(self):
        # There is no setup which the parent needs to wait for.
        pass

    def run(self):
        logging.info('svndumpsub started, pid=%d', os.getpid())

        # Set the umask in the daemon process. Defaults to 000 for
        # daemonized processes. Foreground processes simply inherit
        # the value from the parent process.
        if self.umask is not None:
            umask = int(self.umask, 8)
            os.umask(umask)
            logging.info('umask set to %03o', umask)

        # Start the BDEC (on the main thread), then start the client
        self.bdec.start()

        mc = svnpubsub.client.MultiClient(self.bdec.streams,
                                          self.bdec.commit,
                                          self._event)
        mc.run_forever()

    def _event(self, url, event_name, event_arg):
        if event_name == 'error':
            logging.exception('from %s', url)
        elif event_name == 'ping':
            logging.debug('ping from %s', url)
        else:
            logging.info('"%s" from %s', event_name, url)


def prepare_logging(logfile):
    "Log to the specified file, or to stdout if None."

    if logfile:
        # Rotate logs daily, keeping 7 days worth.
        handler = logging.handlers.TimedRotatingFileHandler(
          logfile, when='midnight', backupCount=7,
          )
    else:
        handler = logging.StreamHandler(sys.stdout)

    # Add a timestamp to the log records
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s',
                                  '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    # Apply the handler to the root logger
    root = logging.getLogger()
    root.addHandler(handler)

    ### use logging.INFO for now. switch to cmdline option or a config?
    root.setLevel(logging.INFO)


def handle_options(options):
    # Set up the logging, then process the rest of the options.
    prepare_logging(options.logfile)

    # In daemon mode, we let the daemonize module handle the pidfile.
    # Otherwise, we should write this (foreground) PID into the file.
    if options.pidfile and not options.daemon:
        pid = os.getpid()
        # Be wary of symlink attacks
        try:
            os.remove(options.pidfile)
        except OSError:
            pass
        fd = os.open(options.pidfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                     stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        os.write(fd, '%d\n' % pid)
        os.close(fd)
        logging.info('pid %d written to %s', pid, options.pidfile)

    if options.gid:
        try:
            gid = int(options.gid)
        except ValueError:
            import grp
            gid = grp.getgrnam(options.gid)[2]
        logging.info('setting gid %d', gid)
        os.setgid(gid)

    if options.uid:
        try:
            uid = int(options.uid)
        except ValueError:
            import pwd
            uid = pwd.getpwnam(options.uid)[2]
        logging.info('setting uid %d', uid)
        os.setuid(uid)


def main(args):
    parser = optparse.OptionParser(
        description='An SvnPubSub client to keep working copies synchronized '
                    'with a repository.',
        usage='Usage: %prog [options] CONFIG_FILE',
        )
    parser.add_option('--logfile',
                      help='filename for logging')
    parser.add_option('--pidfile',
                      help="the process' PID will be written to this file")
    parser.add_option('--uid',
                      help='switch to this UID before running')
    parser.add_option('--gid',
                      help='switch to this GID before running')
    parser.add_option('--umask',
                      help='set this (octal) umask before running')
    parser.add_option('--daemon', action='store_true',
                      help='run as a background daemon')

    options, extra = parser.parse_args(args)

    if options.daemon and not options.logfile:
        parser.error('LOGFILE is required when running as a daemon')
    if options.daemon and not options.pidfile:
        parser.error('PIDFILE is required when running as a daemon')

    # Process any provided options.
    handle_options(options)
    bdec = BigDoEverythingClasss()
    
    # We manage the logfile ourselves (along with possible rotation). The
    # daemon process can just drop stdout/stderr into /dev/null.
    d = Daemon('/dev/null', os.path.abspath(options.pidfile),
               options.umask, bdec)
    if options.daemon:
        # Daemonize the process and call sys.exit() with appropriate code
        d.daemonize_exit()
    else:
        # Just run in the foreground (the default)
        d.foreground()


if __name__ == "__main__":
    main(sys.argv[1:])
