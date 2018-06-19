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
# SvnWcSub - Subscribe to a SvnPubSub stream, and keep a set of working copy
# paths in sync
#
# Example:
#  svnwcsub.py svnwcsub.conf
#
# On startup svnwcsub checks the working copy's path, runs a single svn update
# and then watches for changes to that path.
#
# See svnwcsub.conf for more information on its contents.
#

# TODO:
# - bulk update at startup time to avoid backlog warnings
# - fold BDEC into Daemon
# - fold WorkingCopy._get_match() into __init__
# - remove wc_ready(). assume all WorkingCopy instances are usable.
#   place the instances into .watch at creation. the .update_applies()
#   just returns if the wc is disabled (eg. could not find wc dir)
# - figure out way to avoid the ASF-specific PRODUCTION_RE_FILTER
#   (a base path exclusion list should work for the ASF)
# - add support for SIGHUP to reread the config and reinitialize working copies
# - joes will write documentation for svnpubsub as these items become fulfilled
# - make LOGLEVEL configurable

import errno
import subprocess
import threading
import sys
import stat
import os
import re
import posixpath
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
    
OP_DUMPSINGLE = 'dump_single'
OP_VALIDATE = 'dump_validate'
#TODO: Remove this is refactored and moved to Job, still here for inspiration.
def decide_OP(job):
    args = [AWS, 's3', 'ls', job.s3_base_path]
    print('decide_OP %s' % args)
    
    pipe = subprocess.Popen((args), stdout=subprocess.PIPE) # Maybe use s3 api to do this.
    output, errput = pipe.communicate()
    
    if errput is not None:
        raise subprocess.CalledProcessError(pipe.returncode, args)
    if errput is None:
        print('S3 path do not exist will dump from 0 in shard %s' % job.s3_base_path)
        #The folder does not exist, dump all commits that belong to that shard
        #TODO: Own Method
        rev_round_down = int((job.rev - 1) / 1000)
        if rev_round_down is 0: # int(345 /1000) = 0, int(1345 / 1000) = 1
            from_rev = rev_round_down * 1000
        else:
            from_rev = rev_round_down * 1000

        return from_rev
        
    if output:
        print ('output %s' % output)
        # Extract all revision numbers from output and convert to int. Needs to be changed when preceeded by zeros
        # One request to s3, check if previous exist.
        find = [int(s) for s in re.findall(r'\-(\d{10})\b', output)]
        return max(find)

try:
    import glob
    glob.iglob
    def is_emptydir(path):
        # ### If the directory contains only dotfile children, this will readdir()
        # ### the entire directory.  But os.readdir() is not exposed to us...
        for x in glob.iglob('%s/*' % path):
            return False
        for x in glob.iglob('%s/.*' % path):
            return False
        return True
except (ImportError, AttributeError):
    # Python ≤2.4
    def is_emptydir(path):
        # This will read the entire directory list to memory.
        return not os.listdir(path)

class Job(object):
    
    def __init__(self, repo, rev):
        self.repo = repo
        self.rev = rev
    
    def get_key(self, repo, rev):
        #/v1/Cloudid/reponame/shardX/0000001000/reponame-0000001000.svndump.gz
        return '%s/%s' % (self._get_s3_base(repo, rev), self.get_name(repo, rev)) 
        
    def get_name(self, repo, rev):
        revStr = str(rev)
        revStr = revStr.zfill(10)
        name = repo + '-' + revStr + '.svndump.gz'
        #reponame-0000001000.svndump.gz
        return name
    
    def _get_s3_base(self, repo, rev):
        d = rev / 1000;
        d = str(int(d)) + '000'
        shard_number = d.zfill(10)
        
        #TODO: Should not be hardcoded
        bucket = 'cms-review-jandersson'
        version = 'v1'
        cloudid = 'jandersson'
        shard_type = 'shard0'
        return 's3://%s/%s/%s/%s/%s/%s' % (bucket, version, cloudid, repo, shard_type, shard_number) 
        
    def _get_svn_dump_args(self, repo, rev):
        path = '%s/%s' % (SVNROOT, repo)
        dump_rev = '-r%s:%s' % (rev, rev)
        #svnadmin dump --incremental --deltas /srv/cms/svn/demo1 -r 237:237
        return [SVNADMIN, 'dump', '--incremental', '--deltas', path, dump_rev]
    
    def _get_aws_cp_args(self, repo, rev):
        # aws s3 cp - s3://cms-review-jandersson/v1/jandersson/demo1/shard0/0000000000/demo1-0000000363.svndump.gz
        return [AWS, 's3', 'cp', '-', self.get_key(repo, rev)]
        
    #Will recursively check a bucket if (rev - 1) exists until it finds a rev dump. 
    def validate_rev(self, repo, rev):
        
        rev_to_validate = rev - 1
        print('Rev to validate: %s' % rev_to_validate)
        key = self.get_key(repo, rev_to_validate)
        print('s3_key %s' % key)
        args = [AWS, 's3', 'ls', key]
        print('Aws S3 validate args: %s' % args)
        
        pipe = subprocess.Popen((args), stdout=subprocess.PIPE) # Maybe use s3 api to do this.
        output, errput = pipe.communicate()
        
        if self.get_name(repo, rev_to_validate) in output:
            print ('key do exist %s' % output)
            return rev
        if errput is not None:
            print('Error output is not None, something went wrong')
            raise subprocess.CalledProcessError(pipe.returncode, args)
        if errput is None:
            print('errput is none, key do not exist: %s' % key)
            return self.validate_rev(repo, rev_to_validate)
            
    def dump_cm_to_s3(self, svn_dump_args, aws_cp_args):
        
        gz = '/bin/gzip'
        gz_args = [gz]

        # Svn admin dump
        p1 = subprocess.Popen((svn_dump_args), stdout=subprocess.PIPE)
        # Zip stout
        p2 = subprocess.Popen((gz_args), stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        # Upload zip.stdout to s3
        output = subprocess.check_output((aws_cp_args), stdin=p2.stdout)
        #TODO: Do we need to close stuff?
        p2.communicate()[0]    
                
        
class BigDoEverythingClasss(object):
    #removed the config object from __init__.
    def __init__(self):
        #TODO: Should home be vagrant? or SVN HOME? Not sure if this is needed.
        self.env = {'HOME': '/home/vagrant', 'LANG': 'en_US.UTF-8'}
        self.streams = ["http://%s:%d/commits" %(HOST, PORT)]
        print('streams %s' % self.streams)

        #TODO: svnadmin path is set hardcoded, might want to handle it an other way.
        self.hook = None
        self.svnbin = '/usr/bin/svnadmin';
        self.worker = BackgroundWorker(self.svnbin, self.env, self.hook)
        self.watch = [ ]

    def start(self):
        print('start')

    def wc_ready(self, wc):
        # called when a working copy object has its basic info/url,
        # Add it to our watchers, and trigger an svn update.
        logging.info("Watching WC at %s <-> %s" % (wc.path, wc.url))
        self.watch.append(wc)
        self.worker.add_job(OP_BOOT, wc)

    def _normalize_path(self, path):
        if path[0] != '/':
            return "/" + path
        return posixpath.abspath(path)

    def commit(self, url, commit):
        if commit.type != 'svn' or commit.format != 1:
            logging.info("SKIP unknown commit format (%s.%d)",
                         commit.type, commit.format)
            return
        logging.info("COMMIT r%d (%d paths) from %s"
                     % (commit.id, len(commit.changed), url))
                     
        job = Job(commit.repositoryname, commit.id)
        self.worker.add_job(OP_VALIDATE, job)
        
                
# Start logging warnings if the work backlog reaches this many items
BACKLOG_TOO_HIGH = 20
OP_BOOT = 'boot'
OP_UPDATE = 'update'
OP_CLEANUP = 'cleanup'

class BackgroundWorker(threading.Thread):
    def __init__(self, svnbin, env, hook):
        threading.Thread.__init__(self)

        # The main thread/process should not wait for this thread to exit.
        ### compat with Python 2.5
        self.setDaemon(True)

        self.svnbin = svnbin
        self.env = env
        self.hook = hook
        self.q = Queue.Queue()

        self.has_started = False

    def run(self):
        while True:
            # This will block until something arrives
            operation, job = self.q.get()

            # Warn if the queue is too long.
            # (Note: the other thread might have added entries to self.q
            # after the .get() and before the .qsize().)
            qsize = self.q.qsize()+1
            if operation != OP_BOOT and qsize > BACKLOG_TOO_HIGH:
                logging.warn('worker backlog is at %d', qsize)

            try:
                if operation == OP_VALIDATE:
                    self._update(job)
                elif operation == OP_DUMPSINGLE:
                    print('dumping and uploading %s' % job.rev)
                    #TODO: all created jobs should have a accurate self.repo, self.job. dump_cm_to_s3 might not need to take any args
                    job.dump_cm_to_s3(job._get_svn_dump_args(job.repo, job.rev), job._get_aws_cp_args(job.repo, job.rev))
                else:
                    logging.critical('unknown operation: %s', operation)
            except:
                logging.exception('exception in worker')

            # In case we ever want to .join() against the work queue
            self.q.task_done()

    def add_job(self, operation, job):
        # Start the thread when work first arrives. Thread-start needs to
        # be delayed in case the process forks itself to become a daemon.
        if not self.has_started:
            self.start()
            self.has_started = True

        self.q.put((operation, job))

    def _update(self, job, boot=False):
        "Validate the specific commit."

        # For giggles, let's clean up the working copy in case something
        # happened earlier.
        #TODO: Nothing to clean up, remove?
        # self._cleanup(wc)


class ReloadableConfig(ConfigParser.SafeConfigParser):
    def __init__(self, fname):
        ConfigParser.SafeConfigParser.__init__(self)

        self.fname = fname
        self.read(fname)

        ### install a signal handler to set SHOULD_RELOAD. BDEC should
        ### poll this flag, and then adjust its internal structures after
        ### the reload.
        self.should_reload = False

    def reload(self):
        # Delete everything. Just re-reading would overlay, and would not
        # remove sections/options. Note that [DEFAULT] will not be removed.
        for section in self.sections():
            self.remove_section(section)

        # Now re-read the configuration file.
        self.read(fname)

    def get_value(self, which):
        return self.get(ConfigParser.DEFAULTSECT, which)

    def get_optional_value(self, which, default=None):
        if self.has_option(ConfigParser.DEFAULTSECT, which):
            return self.get(ConfigParser.DEFAULTSECT, which)
        else:
            return default

    def get_env(self):
        env = os.environ.copy()
        default_options = self.defaults().keys()
        for name, value in self.items('env'):
            if name not in default_options:
                env[name] = value
        return env

    def get_track(self):
        "Return the {PATH: URL} dictionary of working copies to track."
        track = dict(self.items('track'))
        for name in self.defaults().keys():
            del track[name]
        return track

    def optionxform(self, option):
        # Do not lowercase the option name.
        return str(option)


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
