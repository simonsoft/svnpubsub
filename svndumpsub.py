#!/usr/bin/env python3
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

from io import BytesIO
import subprocess
import threading
import sys
import stat
import os
import tempfile
import re
import json
import socket
import boto3
import logging.handlers
try:
  import queue
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

s3client = boto3.client('s3')

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
        self.shard_size = 'shard0'

    def get_key(self, rev):
        #/v1/Cloudid/reponame/shardX/0000001000/reponame-0000001000.svndump.gz
        return '%s/%s' % (self._get_s3_base(rev = rev), self.get_name(rev))

    def get_name(self, rev):
        revStr = str(rev)
        revStr = revStr.zfill(10)
        name = self.repo + '-' + revStr + '.svndump.gz'
        #reponame-0000001000.svndump.gz
        return name

    def _get_s3_base(self, rev):

        # Always using 1000 for folders, can not yet support >shard3.
        d = int(rev) / 1000
        d = str(int(d)) + '000'
        shard_number = d.zfill(10)

        version = 'v1'
        # v1/CLOUDID/demo1/shard0/0000000000
        return '%s/%s/%s/%s/%s' % (version, CLOUDID, self.repo, self.shard_size, shard_number)

    def _get_svn_dump_args(self, from_rev, to_rev):
        path = '%s/%s' % (SVNROOT, self.repo)
        dump_rev = '-r%s:%s' % (from_rev, to_rev)
        #svnadmin dump --incremental --deltas /srv/cms/svn/demo1 -r 237:237
        return [SVNADMIN, 'dump', '--incremental', '--deltas', path, dump_rev]


    #def _get_aws_cp_args(self, rev):
    #    # aws s3 cp - s3://cms-review-jandersson/v1/jandersson/demo1/shard0/0000000000/demo1-0000000363.svndump.gz
    #    return [AWS, 's3', 'cp', '-', 's3://%s/%s' % (BUCKET, self.get_key(rev))]

    def _validate_shard(self, rev):
        key = self.get_key(rev)
        try:
            response = s3client.head_object(Bucket=BUCKET, Key=key)
            logging.debug('Shard key exists: %s' % key)
            if (not response["ContentLength"] > 0):
                logging.warning('Dump file empty: %s' % key)
                return False
            #logging.info(response)
            return True
        except Exception as err:
            logging.debug("S3 exception: {0}".format(err))
            logging.info('Shard key does not exist: s3://%s/%s' % (BUCKET, key))
            return False


    #Will recursively check a bucket if (rev - 1) exists until it finds a rev dump.
    def validate_rev(self, rev):

        validate_to_rev = self._get_validate_to_rev()
        rev_to_validate = rev - 1

        if rev_to_validate < validate_to_rev:
            logging.info('At first possible rev in shard, will not validate further rev: %s', rev_to_validate)
            return True

        return self._validate_shard(rev_to_validate)


    def _get_validate_to_rev(self):
        rev_round_down = int((self.head - 1) / 1000)
        return rev_round_down * 1000

    def _backup_commit(self):
        logging.info('Dumping and uploading rev: %s from repo: %s' % (self.rev, self.repo))
        self.dump_zip_upload(self._get_svn_dump_args(self.rev, self.rev), self.rev)


    def dump_zip_upload(self, dump_args, rev):
        shard_key = self.get_key(rev)

        gz = '/bin/gzip'
        gz_args = [gz]

        # Svn admin dump
        p1 = subprocess.Popen((dump_args), stdout=subprocess.PIPE, env=self.env)
        # Zip stdout
        p2 = subprocess.Popen((gz_args), stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
        # Upload zip.stdout to s3
        s3client.upload_fileobj(p2.stdout, BUCKET, shard_key)
        #TODO: Do we need to close stuff?
        p2.communicate()[0]
        p1.poll()

        if p1.returncode != 0:
            logging.error('Dumping shard failed (rc=%s): %s', p1.returncode, shard_key)
            raise Exception('Dumping shard failed')

        if p2.returncode != 0:
            logging.error('Compressing shard failed (rc=%s): %s', p2.returncode, shard_key)
            raise Exception('Compressing shard failed')


# Processing one repo, specified in history option
class JobMulti(Job):

    def __init__(self, repo, shard_size):
        self.shard_size = shard_size
        self.env = {'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8'}
        self.repo = repo
        self.head = self._get_head(self.repo)

        if shard_size == 'shard3':
            self.shard_div = 1000
            self.rev_min = 0
        elif shard_size == 'shard0':
            self.shard_div = 1
            self.shard_div_next = 1000 # next larger shard
            self.rev_min = int(self.head / self.shard_div_next) * self.shard_div_next
            # shard0 revisions might be skipped when using only --history without svnpubsub
            # revision will be dumped in shard3 but can be a problem for slave servers loading single revisions
            if self.rev_min != 0:
                self.rev_min = self.rev_min - 100
        else:
            logging.error('Unsupported shard type: %s' % shard_size)
            raise Exception('Unsupported shard type')

        logging.info('Processing repo %s with head revision %s' % (self.repo, self.head))
        shards = self._get_shards(self.head)
        self._run(shards)

    def _run(self, shards):
        for shard in shards:
            dump_exists = self._validate_shard(shard)
            if not dump_exists:
                logging.info('Shard is missing will dump and upload shard %s' % shard)
                self._backup_shard(shard)

    def _get_head(self, repo):

        path = '%s/%s' % (SVNROOT, repo)

        # While using fs we can check that repo exists and provide a better error message.
        if not os.path.isdir(path):
            logging.error('Repository does not exist: %s' % repo)
            raise Exception('Repository does not exist')

        # Considered using svn to enable rdump in the future.
        #fqdn = socket.getfqdn()
        #url = 'http://%s/svn/%s' % (fqdn, repo)

        #args = [SVN, 'info', url]
        #grep_args = ['/bin/grep', 'Revision:']

        args = [SVNLOOK, 'youngest', path]
        grep_args = ['/bin/grep', '^[0-9]\+']

        p1 = subprocess.Popen((args), stdout=subprocess.PIPE)
        output = subprocess.check_output((grep_args), stdin=p1.stdout)

        rev = int(''.join(filter(str.isdigit, output.decode("utf-8"))))
        logging.info('Repository %s youngest: %s' % (repo, rev))
        return rev

    def _get_shards(self, head):
        shards = []
        number_of_shards = int(head / self.shard_div)
        # python range excludes second arg from range.
        # range(0, 2000, 1000) = [0, 1000]
        # The shard now specifies start rev for the shard.
        # rev_min has already been floored
        # Upper limit must be +1 before division (both shard3 and shard0).
        return list(range(self.rev_min, int((head + 1) / self.shard_div) * self.shard_div, self.shard_div))


    def _backup_shard(self, shard):
        logging.info('Dumping and uploading shard: %s from repo: %s' % (shard, self.repo))
        start_rev = str(shard)
        to_rev = str(((int(shard / self.shard_div) + 1) * self.shard_div) - 1)

        svn_args = self._get_svn_dump_args(start_rev, to_rev)
        self.dump_zip_upload(svn_args, start_rev)


class JobMultiLoad(JobMulti):
    def __init__(self, repo):
        self.shard_size = ''
        self.env = {'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8'}
        self.repo = repo
        self.head = self._get_head(self.repo)
        if self.head == 0:
            # Empty repository is a special case because the current head rev can be loaded.
            self.rev_min = 0
        else:
            self.rev_min = self.head + 1

        logging.info('Processing repo %s with head revision %s' % (self.repo, self.head))
        # First process large shards if local head is divisible with shard size.
        if self.rev_min % 1000 == 0:
            self.shard_size = 'shard3'
            self.shard_div = 1000
            shards = self._get_shards(self.rev_min + 1000 * self.shard_div)
            self._run(shards)


        # Refresh head after potentially loading large dumps.
        self.head = self._get_head(self.repo)
        if self.head == 0:
            # Empty repository is a special case because the current head rev can be loaded.
            self.rev_min = 0
        else:
            self.rev_min = self.head + 1

        # Then look for single revision dumps.
        self.shard_size = 'shard0'
        self.shard_div = 1

        shards = self._get_shards(self.rev_min + 999)
        self._run(shards)

    def _run(self, shards):
        logging.info('Shards length %s' % len(shards))
        for shard in shards:
            dump_exists = self._validate_shard(shard)
            if dump_exists:
                logging.info('Shard exists, will load shard %s' % shard)
                self._load_shard(shard)
                continue
            else:
                logging.info('Shard does not exist, done for now - %s' % shard)
                break
            logging.warning('Maximum number of shards processed, terminating.')
            # Restart or raise the maximum number of shards.
            raise Exception('Maximum number of shards processed')


    def _load_shard(self, shard):
        logging.info('Loading shard: %s from repo: %s' % (shard, self.repo))
        start_rev = str(shard)
        to_rev = str(((int(shard / self.shard_div) + 1) * self.shard_div) - 1)

        logging.info('Loading shard %s' % shard)
        self.load_zip(start_rev)


    def load_zip(self, rev):
        shard_key = self.get_key(rev)

        gz = '/bin/gunzip'
        gz_args = [gz, '-c']

        path = '%s/%s' % (SVNROOT, self.repo)
        load_args = [SVNADMIN, 'load', path]

        # Temporary file
        # TODO: Use specific tmp dir.
        prefix = '%s_%s_' % (self.repo, rev)
        fp = tempfile.NamedTemporaryFile(prefix=prefix, suffix='.svndump.gz', delete=True)
        logging.debug('Downloading shard to temporary file: %s', fp.name)
        # Download from s3
        s3client.download_fileobj(BUCKET, shard_key, fp)
        fp.seek(0)
        # gunzip
        p1 = subprocess.Popen((gz_args), stdin=fp, stdout=subprocess.PIPE, env=self.env)
        # svnadmin load
        p2 = subprocess.Popen((load_args), stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
        p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
        #TODO: Do we need to close stuff?
        p2.communicate()[0]
        logging.debug('Load return code: %s', p2.returncode)
        p1.poll()
        logging.debug('Gunzip return code: %s', p1.returncode)

        # Closing tmp file should delete it.
        fp.close()

        if p1.returncode != 0:
            logging.error('Decompressing shard failed (rc=%s): %s', p1.returncode, shard_key)
            raise Exception('Decompressing shard failed')

        if p2.returncode != 0:
            logging.error('Loading shard failed (rc=%s): %s', p2.returncode, shard_key)
            raise Exception('Loading shard failed')

        # TODO Analyze output, should conclude with (ensure revision is correct for shard):
        # ------- Committed revision 4999 >>>

class BigDoEverythingClasss(object):
    #removed the config object from __init__.
    def __init__(self):
        self.streams = ["http://%s:%d/commits" %(HOST, PORT)]

        self.hook = None
        self.svnbin = SVNADMIN
        self.worker = BackgroundWorker(self.svnbin, self.hook)
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
                logging.info('Commit in excluded repository, ignoring: %s' % commit.repositoryname)
                excluded = True

        if not excluded:
            job = Job(commit.repositoryname, commit.id, commit.id)
            self.worker.add_job(job)

# Start logging warnings if the work backlog reaches this many items
BACKLOG_TOO_HIGH = 500

class BackgroundWorker(threading.Thread):
    def __init__(self, svnbin, hook):
        threading.Thread.__init__(self)

        # The main thread/process should not wait for this thread to exit.
        ### compat with Python 2.5
        self.setDaemon(True)

        self.svnbin = svnbin
        self.hook = hook
        self.q = queue.PriorityQueue()

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

    if options.svnlook:
        global SVNLOOK
        SVNLOOK = options.svnlook

    if options.svn:
        global SVN
        SVN = options.svn

    if options.history and not options.svnlook:
        raise ValueError('A valid --svnlook has to be provided if combined with --history (path to svnlook executable)')

    # Set up the logging, then process the rest of the options.


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
        os.write(fd, b'%d\n' % pid)
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

    prepare_logging(options.logfile)

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
                    help='Will dump and backup repository in shard3 ranges (even thousands), e.g --history reponame')
    parser.add_option('--shardsize', default='shard3',
                    help='Shard size used by --history. Assumes that shard3 is executed before shard0.')
    parser.add_option('--load',
                    help='Will load repository from shards in size order (shard3 then shard0), e.g --load reponame')
    parser.add_option('--aws',
                    help='path to aws executable e.g /usr/bin/aws')
    parser.add_option('--svnadmin',
                    help='path to svnadmin executable e.g /usr/bin/svnadmin')
    parser.add_option('--svnlook',
                    help='path to svnlook executable e.g /usr/bin/svnlook')
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

    if options.history:
        JobMulti(options.history, options.shardsize)
    elif options.load:
        JobMultiLoad(options.load)
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
