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

import subprocess
import threading
import sys
import stat
import os
import re
import json
import socket
import logging.handlers
try:
  import Queue
except ImportError:
  import queue as Queue
  
import optparse
import daemonize
import svnpubsub.client
import svnpubsub.util

HOST = "127.0.0.1"
PORT = 2069
#Will not handle commits if repo starts with any name icluded in REPO_EXCLUDES
REPO_EXCLUDES = ['demo', 'repo']

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
        self.shard_type = 'shard0'
    def get_key(self, rev):
        #/v1/Cloudid/reponame/shardX/0000001000/reponame-0000001000.svndump.gz
        return '%s/%s' % (self._get_s3_base(), self.get_name(rev)) 
        
    def get_name(self, rev):
        revStr = str(rev)
        revStr = revStr.zfill(10)
        name = self.repo + '-' + revStr + '.svndump.gz'
        #reponame-0000001000.svndump.gz
        return name  
    
    def _get_s3_base(self, **optional_rev):
        
        if 'rev' in optional_rev:
            shard_number = str(optional_rev['rev']).zfill(10)
        else:
            d = self.rev / 1000;
            d = str(int(d)) + '000'
            shard_number = d.zfill(10)
            
        version = 'v1'
        cloudid = 'jandersson'
        # v1/jandersson/demo1/shard0/0000000000
        return '%s/%s/%s/%s/%s' % (version, cloudid, self.repo, self.shard_type, shard_number)
        
    def _get_svn_dump_args(self, from_rev, to_rev):
        path = '%s/%s' % (SVNROOT, self.repo)
        dump_rev = '-r%s:%s' % (from_rev, to_rev)
        #svnadmin dump --incremental --deltas /srv/cms/svn/demo1 -r 237:237
        return [SVNADMIN, 'dump', '--incremental', '--deltas', path, dump_rev]
    
    def _get_aws_cp_args(self, rev):
        # aws s3 cp - s3://cms-review-jandersson/v1/jandersson/demo1/shard0/0000000000/demo1-0000000363.svndump.gz
        return [AWS, 's3', 'cp', '-',  's3://%s/%s' % (BUCKET, self.get_key(rev))]
        
    #Will recursively check a bucket if (rev - 1) exists until it finds a rev dump. 
    def validate_rev(self, rev):
        
        validate_to_rev = self._get_validate_to_rev()
        rev_to_validate = rev - 1
        
        if rev_to_validate < validate_to_rev:
            logging.info('At first possible rev in shard, will not validate further rev: %s', rev_to_validate)
            return True
        
        key = self.get_key(rev_to_validate)
        args = [AWS, 's3api', 'head-object', '--bucket', BUCKET, '--key', key] # Maybe use s3 cli or s3 api to do this.
        
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
            
    def _backup_commit(self):
        logging.info('Dumping and uploading rev: %s from repo: %s' % (self.rev, self.repo))
        self.dump_zip_upload(self._get_svn_dump_args(self.rev, self.rev), self._get_aws_cp_args(self.rev))
        
        
    def dump_zip_upload(self, dump_args, aws_args):
        gz = '/bin/gzip'
        gz_args = [gz]

        # Svn admin dump
        p1 = subprocess.Popen((dump_args), stdout=subprocess.PIPE, env=self.env)
        # Zip stout
        p2 = subprocess.Popen((gz_args), stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        # Upload zip.stdout to s3
        output = subprocess.check_output((aws_args), stdin=p2.stdout)
        #TODO: Do we need to close stuff?
        p2.communicate()[0]


# 1. Always go for all repos and exclude repos in history option
# 2. One repo at the time, repos spcified in history option
class JobMulti(Job):
    
    def __init__(self, repo):
        self.shard_type = 'shard3'
        self.repo = repo
        self.env = {'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8'}
        self.head = self._get_head(self.repo)
        shards = self._get_shards(self.head)
        for shard in shards:
            missing_dump = self._validate_shard(shard)
            if missing_dump:
                logging.info('Shard is missing will dump and upload shard %s' % shard)
                self._backup_shard(shard)
            
    def _get_head(self, repo):
        fqdn = socket.getfqdn()
        url = 'http://%s/svn/%s' % (fqdn, repo)
        
        args = [SVN, 'info', url]
        grep_args = ['/bin/grep', 'Revision:']
        
        p1 = subprocess.Popen((args), stdout=subprocess.PIPE)
        output = subprocess.check_output((grep_args), stdin=p1.stdout)

        rev = int(filter(str.isdigit, output))
        return rev
    
    def _get_shards(self, head):
        shards = []
        number_of_shards = int(head / 1000)
        for shard in range(number_of_shards):
            shards.append(shard)
            
        return shards
    
    def _validate_shard(self, shard):
        key = self.get_key(shard)
        args = [AWS, 's3api', 'head-object', '--bucket', BUCKET, '--key', key]
        
        pipe = subprocess.Popen((args), stdout=subprocess.PIPE) 
        output, errput = pipe.communicate()
        
        if pipe.returncode != 0:
            logging.info('Shard Key do not exist %s' % key)
            return True
        else:
            try:
                #FUTURE: Parsing response to json. Will allow us to check size. e.g response_body['ContentLength']
                response_body = json.loads(output)
                logging.info('Shard key do exist %s' % key)
                return False
            except ValueError:
                logging.error('Could not parse response from s3api head-object with key: %s' % key)
                raise 'Could not parse response from s3api head-object with key: %s' % key        
        
    def _backup_shard(self, shard):
        logging.info('Dumping and uploading shard: %s from repo: %s' % (shard, self.repo))
        start_rev = str(shard) + '000'
        to_rev = str(shard) + '999'
        
        svn_args = self._get_svn_dump_args(start_rev, to_rev)
        self.dump_zip_upload(svn_args, self._get_aws_cp_args(shard))
   
    def get_key(self, rev):
        #/v1/Cloudid/reponame/shardX/0000001000/reponame-0000001000.svndump.gz
        return '%s/%s' % (self._get_s3_base(rev = rev), self.get_name(rev)) 
        
class BigDoEverythingClasss(object):
    #removed the config object from __init__.
    def __init__(self):
        #TODO: Should home be vagrant? or SVN HOME? Not sure if this is needed.
        self.env = {'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8', 'LC_CTYPE': 'en_US.UTF-8'}
        self.streams = ["http://%s:%d/commits" %(HOST, PORT)]

        self.hook = None
        self.svnbin = SVNADMIN;
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
        
        excluded = False
        for repo in REPO_EXCLUDES:
            if commit.repositoryname.startswith(repo):
                logging.info('Commit happend in exlcuded repository, will not procced with backup, repo: %s' % commit.repositoryname)      
                excluded = True
                
        if not excluded:
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
                    job._backup_commit()
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
        return job.validate_rev(job.rev)

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

def handle_options(options):

    if not options.aws:
        raise ValueError('A valid --aws has to be provided (path to aws executable)')
    else:
        global AWS
        AWS = options.aws        

    if not options.svnadmin:
        raise ValueError('A valid --svnadmin has to be provided (path to svnadmin executable)')
    else:
        global SVNADMIN
        SVNADMIN = options.svnadmin        
        
    if not options.svnroot:
        raise ValueError('A valid --svnroot has to be provided (path to location of svn repositories)')
    else:
        global SVNROOT
        SVNROOT = options.svnroot
  
    if not options.bucket:
        raise ValueError('A valid --bucket has to be provided (bucket where dump files will be stored)')
    else:
        global BUCKET
        BUCKET = options.bucket        

    if not options.cloudid:
        raise ValueError('A valid --cloudid has to be provided (aws cloudid)')
    else:
        global CLOUDID
        CLOUDID = options.cloudid        

    if options.history and not options.svn:
        raise ValueError('A valid --svn has to be provided if combined with --history (path to svn executable)')    
    else:    
        global SVN
        SVN = options.svn        
        
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
    parser.add_option('--daemon', action='store_true',
                    help='run as a background daemon')
    parser.add_option('--umask',
                    help='set this (octal) umask before running')                  
    parser.add_option('--history',
                    help='Will dump and backup all repositories within shard3 ranges (even thousands) e.g --history reponame')
    parser.add_option('--aws',
                    help='path to aws executable e.g /usr/bin/aws')
    parser.add_option('--svnadmin',
                    help='path to svnadmin executable e.g /usr/bin/svnadmin')
    parser.add_option('--svnroot',
                    help='path to repository locations /srv/cms/svn')
    parser.add_option('--svn',
                    help='path to svn executable only required when combined with --history e.g /usr/bin/svn')                    
    parser.add_option('--bucket',
                    help='name of S3 bucket where dumps will be stored')
    parser.add_option('--cloudid',
                    help='AWS cloud-id')
                                                    
    options, extra = parser.parse_args(args)
    
    # Process any provided options.
    handle_options(options)
    
    if options.history and not options.daemon:
        JobMulti(options.history)
    else:
        if options.daemon and not options.logfile:
            parser.error('LOGFILE is required when running as a daemon')
        if options.daemon and not options.pidfile:
            parser.error('PIDFILE is required when running as a daemon')
        
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
        
