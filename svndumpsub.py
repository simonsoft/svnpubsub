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

import os
import stat
import boto3
import tempfile
import argparse
import subprocess
import svnpubsub.util
import svnpubsub.logger
import logging.handlers
import svnpubsub.client
from svnpubsub import bgworker
from svnpubsub.util import execute
from svnpubsub.client import Commit
from svnpubsub.daemon import Daemon, DaemonTask

AWS = None
SVN = None
BUCKET = None
SVNROOT = None
CLOUDID = None
SVNLOOK = None
SVNADMIN = None

HOST = "127.0.0.1"
PORT = 2069

EXCLUDED_REPOS = [] # ['demo', 'repo']

s3client = boto3.client('s3')


class Job(bgworker.BackgroundJob):

    def __init__(self, commit: Commit = None, shard_size: str = 'shard0', repo: str = None, rev: int = None, head: int = None):
        if commit is not None:
            super().__init__(repo=commit.repositoryname,
                             rev=commit.id,
                             head=commit.id,
                             commit=commit,
                             shard_size=shard_size,
                             env={'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8'})
        elif repo is not None:
            super().__init__(repo=repo,
                             rev=rev,
                             head=head,
                             commit=None,
                             shard_size=shard_size,
                             env={'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8'})
        else:
            raise Exception('No commit or repo specified')


    def get_key(self, rev):
        # /v1/Cloudid/reponame/shardX/0000001000/reponame-0000001000.svndump.gz
        return '%s/%s' % (self.__get_s3_base(rev = rev), self.get_name(rev))

    def get_name(self, rev):
        rev_str = str(rev)
        rev_str = rev_str.zfill(10)
        name = self.repo + '-' + rev_str + '.svndump.gz'
        # reponame-0000001000.svndump.gz
        return name

    def __get_s3_base(self, rev):

        # Always using 1000 for folders, can not yet support >shard3.
        d = int(rev) / 1000
        d = str(int(d)) + '000'
        shard_number = d.zfill(10)

        version = 'v1'
        # v1/CLOUDID/demo1/shard0/0000000000
        return '%s/%s/%s/%s/%s' % (version, CLOUDID, self.repo, self.shard_size, shard_number)

    def __get_svn_dump_args(self, from_rev, to_rev):
        path = '%s/%s' % (SVNROOT, self.repo)
        dump_rev = '-r%s:%s' % (from_rev, to_rev)
        # svnadmin dump --incremental --deltas /srv/cms/svn/demo1 -r 237:237
        return [SVNADMIN, 'dump', '--incremental', '--deltas', path, dump_rev]

    def validate_shard(self, rev):
        key = self.get_key(rev)
        try:
            response = s3client.head_object(Bucket=BUCKET, Key=key)
            logging.debug('Shard key exists: %s' % key)
            if not response["ContentLength"] > 0:
                logging.warning('Dump file empty: %s' % key)
                return False
            # logging.info(response)
            return True
        except Exception as err:
            logging.debug("S3 exception: {0}".format(err))
            logging.info('Shard key does not exist: s3://%s/%s' % (BUCKET, key))
            return False

    def __get_validate_to_rev(self):
        rev_round_down = int((self.head - 1) / 1000)
        return rev_round_down * 1000

    def validate(self) -> bool:
        # Will recursively check a bucket if (rev - 1) exists until it finds a rev dump.
        validate_to_rev = self.__get_validate_to_rev()
        rev_to_validate = self.rev - 1

        if rev_to_validate < validate_to_rev:
            logging.info('At first possible rev in shard, will not validate further rev: %s', rev_to_validate)
            return True

        return self.validate_shard(rev_to_validate)

    def run(self):
        logging.info('Dumping and uploading rev: %s from repo: %s' % (self.rev, self.repo))
        self.dump_zip_upload(self.__get_svn_dump_args(self.rev, self.rev), self.rev)

    def dump_zip_upload(self, dump_args, rev):
        shard_key = self.get_key(rev)

        gz = '/bin/gzip'
        gz_args = [gz]

        # Svn admin dump
        p1 = subprocess.Popen(dump_args, stdout=subprocess.PIPE, env=self.env)
        # Zip stdout
        p2 = subprocess.Popen(gz_args, stdin=p1.stdout, stdout=subprocess.PIPE)
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


class HistoryDumpJob(Job):
    """
    Processing one repo as specified in the history option.
    """
    def __init__(self, repo, shard_size=None):
        super().__init__(shard_size=shard_size, repo=repo, head=self.get_head(repo))
        if shard_size is None:
            pass
        elif shard_size == 'shard3':
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

    def validate(self) -> bool:
        return True

    def run(self):
        logging.info('Processing repo %s with head revision %s' % (self.repo, self.head))
        shards = self.get_shards(self.head)
        for shard in shards:
            dump_exists = self.validate_shard(shard)
            if not dump_exists:
                logging.info('Shard is missing will dump and upload shard %s' % shard)
                self.__backup_shard(shard)

    def get_head(self, repo) -> int:
        path = '%s/%s' % (SVNROOT, repo)

        # While using fs we can check that repo exists and provide a better error message.
        if not os.path.isdir(path):
            logging.error('Repository does not exist: %s' % repo)
            raise Exception('Repository does not exist')

        # Considered using svn to enable rdump in the future.
        # fqdn = socket.getfqdn()
        # url = 'http://%s/svn/%s' % (fqdn, repo)

        # args = [SVN, 'info', url]
        # grep_args = ['/bin/grep', 'Revision:']

        # svnlook youngest outputs only the revision number, so grep is redundant.
        # Using execute() removes the manual pipe and decode handling.
        _, output, _ = execute(SVNLOOK, 'youngest', path)
        rev = int(output.strip())
        logging.info('Repository %s youngest: %s' % (repo, rev))
        return rev

    def get_shards(self, head):
        shards = []
        number_of_shards = int(head / self.shard_div)
        # python range excludes second arg from range.
        # range(0, 2000, 1000) = [0, 1000]
        # The shard now specifies start rev for the shard.
        # rev_min has already been floored
        # Upper limit must be +1 before division (both shard3 and shard0).
        return list(range(self.rev_min, int((head + 1) / self.shard_div) * self.shard_div, self.shard_div))

    def __backup_shard(self, shard):
        logging.info('Dumping and uploading shard: %s from repo: %s' % (shard, self.repo))
        start_rev = str(shard)
        to_rev = str(((int(shard / self.shard_div) + 1) * self.shard_div) - 1)

        svn_args = self.__get_svn_dump_args(start_rev, to_rev)
        self.dump_zip_upload(svn_args, start_rev)


class HistoryLoadJob(HistoryDumpJob):
    def __init__(self, repo):
        super().__init__(repo=repo)
        if self.head == 0:
            # Empty repository is a special case because the current head rev can be loaded.
            self.rev_min = 0
        else:
            self.rev_min = self.head + 1

    def validate(self) -> bool:
        return True

    def run(self):
        logging.info('Processing repo %s with head revision %s' % (self.repo, self.head))
        # First process large shards if local head is divisible with shard size.
        if self.rev_min % 1000 == 0:
            self.shard_size = 'shard3'
            self.shard_div = 1000
            shards = self.get_shards(self.rev_min + 1000 * self.shard_div)
            self.__run(shards)

        # Refresh head after potentially loading large dumps.
        self.head = self.get_head(self.repo)
        if self.head == 0:
            # Empty repository is a special case because the current head rev can be loaded.
            self.rev_min = 0
        else:
            self.rev_min = self.head + 1

        # Then look for single revision dumps.
        self.shard_size = 'shard0'
        self.shard_div = 1

        shards = self.get_shards(self.rev_min + 999)
        self.__run(shards)

    def __run(self, shards):
        logging.info('Shards length %s' % len(shards))
        for shard in shards:
            dump_exists = self.validate_shard(shard)
            if dump_exists:
                logging.info('Shard exists, will load shard %s' % shard)
                self.__load_shard(shard)
                continue
            else:
                logging.info('Shard does not exist, done for now - %s' % shard)
                break
            logging.warning('Maximum number of shards processed, terminating.')
            # Restart or raise the maximum number of shards.
            raise Exception('Maximum number of shards processed')

    def __load_shard(self, shard):
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


class Task(DaemonTask):

    def __init__(self):
        super().__init__(urls=["http://%s:%d/commits" % (HOST, PORT)], excluded_repos=EXCLUDED_REPOS, worker=bgworker.BackgroundWorker(recursive=True))

    def start(self):
        logging.info('Daemon started.')

    def commit(self, url: str, commit: Commit):
        try:
            job = Job(commit)
            self.worker.queue(job)
        except Exception:
            logging.exception('Failed to queue a job for r%s in: %s.', job.rev, job.repo)


def main():
    global AWS, SVNADMIN, SVNROOT, BUCKET, CLOUDID, SVNLOOK, SVN
    parser = argparse.ArgumentParser(description='An SvnPubSub client to keep working copies synchronized with a repository.', usage='Usage: %prog [options] CONFIG_FILE')

    parser.add_argument('--logfile', help='filename for logging')
    parser.add_argument('--pidfile', help="the process' PID will be written to this file")
    parser.add_argument('--uid', help='switch to this UID before running')
    parser.add_argument('--gid', help='switch to this GID before running')
    parser.add_argument('--daemon', action='store_true', help='run as a background daemon')
    parser.add_argument('--umask', help='set this (octal) umask before running')
    parser.add_argument('--history', help='Will dump and backup repository in shard3 ranges (even thousands), e.g --history reponame')
    parser.add_argument('--shardsize', default='shard3', help='Shard size used by --history. Assumes that shard3 is executed before shard0.')
    parser.add_argument('--load', help='Will load repository from shards in size order (shard3 then shard0), e.g --load reponame')
    parser.add_argument('--aws', help='path to aws executable e.g /usr/bin/aws')
    parser.add_argument('--svnadmin', help='path to svnadmin executable e.g /usr/bin/svnadmin')
    parser.add_argument('--svnlook', help='path to svnlook executable e.g /usr/bin/svnlook')
    parser.add_argument('--svnroot', help='path to repository locations /srv/cms/svn')
    parser.add_argument('--svn', help='path to svn executable only required when combined with --history e.g /usr/bin/svn')
    parser.add_argument('--bucket', help='name of S3 bucket where dumps will be stored')
    parser.add_argument('--cloudid', help='AWS cloud-id')
    parser.add_argument('--log-level', type=int, default=logging.INFO,
                        help='log level (DEBUG: %d | INFO: %d | WARNING: %d | ERROR: %d | CRITICAL: %d) (default: %d)' %
                             (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL, logging.INFO))

    args = parser.parse_args()

    if not args.aws:
        parser.error('A valid --aws has to be provided (path to aws executable)')

    AWS = args.aws

    if not args.svnadmin:
        parser.error('A valid --svnadmin has to be provided (path to svnadmin executable)')

    SVNADMIN = args.svnadmin

    if not args.svnroot:
        parser.error('A valid --svnroot has to be provided (path to location of svn repositories)')

    SVNROOT = args.svnroot

    if not args.bucket:
        parser.error('A valid --bucket has to be provided (bucket where dump files will be stored)')

    BUCKET = args.bucket

    if not args.cloudid:
        parser.error('A valid --cloudid has to be provided (aws cloudid)')

    CLOUDID = args.cloudid

    if args.svnlook:
        SVNLOOK = args.svnlook

    if args.svn:
        SVN = args.svn

    if args.history and not args.svnlook:
        parser.error('A valid --svnlook has to be provided if combined with --history (path to svnlook executable)')

    # Set up the logging, then process the rest of the args.

    # In daemon mode, we let the daemonize module handle the pidfile.
    # Otherwise, we should write this (foreground) PID into the file.
    if args.pidfile and not args.daemon:
        pid = os.getpid()
        # Be wary of symlink attacks
        try:
            os.remove(args.pidfile)
        except OSError:
            pass
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        mode = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
        fd = os.open(args.pidfile, flags, mode)
        with os.fdopen(fd, 'wb') as f:
            f.write(b'%d\n' % pid)
            logging.info('PID: %d -> %s', pid, args.pidfile)

    if args.gid:
        try:
            gid = int(args.gid)
        except ValueError:
            import grp
            gid = grp.getgrnam(args.gid)[2]
        logging.info('GID: %d', gid)
        os.setgid(gid)

    if args.uid:
        try:
            uid = int(args.uid)
        except ValueError:
            import pwd
            uid = pwd.getpwnam(args.uid)[2]
        logging.info('UID: %d', uid)
        os.setuid(uid)

    # Set up a new logging handler with the specified log level
    svnpubsub.logger.setup(logfile=args.logfile, level=args.log_level)

    if args.history:
        HistoryDumpJob(args.history, args.shardsize).run()
    elif args.load:
        HistoryLoadJob(args.load).run()
    else:
        if args.daemon and not args.logfile:
            parser.error('LOGFILE is required when running as a daemon')
        if args.daemon and not args.pidfile:
            parser.error('PIDFILE is required when running as a daemon')

        # We manage the logfile ourselves (along with possible rotation).
        # The daemon process can just drop stdout/stderr into /dev/null.
        daemon = Daemon(name=os.path.basename(__file__),
                        logfile='/dev/null',
                        pidfile=os.path.abspath(args.pidfile) if args.pidfile else None,
                        umask=args.umask,
                        task=Task())
        if args.daemon:
            # Daemonize the process and call sys.exit() with appropriate code
            daemon.daemonize_exit()
        else:
            # Just run in the foreground (the default)
            daemon.foreground()


if __name__ == "__main__":
    main()
