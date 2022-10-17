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
# SvnSQSSub - Subscribe to a SvnPubSub stream, dumps commits, posts them to AWS SQS.
#
# Example:
#  svnsqssub.py
#
# On startup SvnSQSSub starts listening to commits in all repositories.
#

import os
import stat
import boto3
import logging
import argparse
import svnpubsub.logger
from svnpubsub.client import Commit
from svnpubsub.daemon import Daemon, DaemonTask
from svnpubsub.bgworker import BackgroundJob
from botocore.exceptions import ClientError
from svnpubsub.util import execute

PORT = 2069
HOST = "127.0.0.1"
REPO_EXCLUDES = []
SVNBIN_DIR = "/usr/bin"
SVNROOT_DIR = "/srv/cms/svn"
SQS_MESSAGE_GROUP_ID = "commit"
SQS_MESSAGE_ATTRIBUTES = {}
SQS_QUEUES = []
CLOUDID = None


class Job(BackgroundJob):

    def __init__(self, commit: Commit):
        super().__init__(repo=commit.repositoryname, rev=commit.id, head=commit.id, commit=commit)

    def validate(self) -> bool:
        return True

    def run(self):
        global CLOUDID, SQS_QUEUES, SQS_MESSAGE_GROUP_ID, SQS_MESSAGE_ATTRIBUTES
        if not CLOUDID:
            CLOUDID = get_cloudid(self.repo)
        sqs = boto3.resource('sqs')
        for item in SQS_QUEUES:
            queue_name = "cms-{}-{}".format(CLOUDID, item)
            item = sqs.get_queue_by_name(QueueName=queue_name)
            try:
                logging.debug("Posting r%d from %s to: %s", self.rev, self.repo, queue_name)
                response = item.send_message(
                    MessageBody=self.commit.dumps(),
                    MessageGroupId=SQS_MESSAGE_GROUP_ID,
                    MessageAttributes=SQS_MESSAGE_ATTRIBUTES,
                    MessageDeduplicationId="{}/r{}".format(self.repo, self.rev)
                )
                logging.info("Posted r%d from %s to: %s", self.rev, self.repo, queue_name)
                logging.debug("Response: %s", response)
            except ClientError:
                logging.exception("Exception occurred while sending a queue message.")


class Task(DaemonTask):

    def __init__(self):
        super().__init__(urls=["http://%s:%d/commits" % (HOST, PORT)])

    def start(self):
        logging.info('Daemon started.')

    def commit(self, url: str, commit: Commit):
        if commit.type != 'svn' or commit.format != 1:
            logging.info("SKIP unknown commit format (%s.%d)", commit.type, commit.format)
            return
        logging.info("COMMIT r%d (%d paths) from %s" % (commit.id, len(commit.changed), url))

        excluded = False
        for repo in REPO_EXCLUDES:
            if commit.repositoryname.startswith(repo):
                logging.info('Commit in excluded repository, ignoring: %s' % commit.repositoryname)
                excluded = True

        if not excluded:
            job = Job(commit)
            self.worker.queue(job)


def get_cloudid(repo, rev=0):
    global SVNBIN_DIR, SVNROOT_DIR
    arguments = [
        os.path.join(SVNBIN_DIR, 'svnlook'),
        'propget', '--revision', str(rev), '--revprop',
        os.path.join(SVNROOT_DIR, repo),
        'cmsconfig:cloudid'
    ]
    return execute(*arguments)


def main():
    global CLOUDID, SVNROOT_DIR, SVNBIN_DIR, SQS_QUEUES

    parser = argparse.ArgumentParser(description='An SvnPubSub client that posts an SQS queue message for each commit.')

    parser.add_argument('--cloudid', help='aws cloud-id which overrides retrieving it from revprops or repository name')
    parser.add_argument('--logfile', help='a filename for logging if stdout is not the desired output')
    parser.add_argument('--pidfile', help='the PID file where the process PID will be written to')
    parser.add_argument('--uid', help='switch to this UID before running')
    parser.add_argument('--gid', help='switch to this GID before running')
    parser.add_argument('--daemon', action='store_true', help='run as a background daemon')
    parser.add_argument('--umask', help='set this (octal) UMASK before running')
    parser.add_argument('--svnroot', default=SVNROOT_DIR, help='the path to repositories (default: %s)' % SVNROOT_DIR)
    parser.add_argument('--svnbin', default=SVNBIN_DIR,
                        help='the path to svn, svnlook, svnadmin, ... binaries (default: %s)' % SVNBIN_DIR)
    parser.add_argument('--queues', default=[], type=str, nargs='+', metavar='SUFFIX',
                        help='one or more SQS queue suffix names to compile the queue names: "cms-CLOUDID-SUFFIX"')
    parser.add_argument('--log-level', type=int, default=logging.INFO,
                        help='log level (DEBUG: %d | INFO: %d | WARNING: %d | ERROR: %d | CRITICAL: %d) (default: %d)' %
                             (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL, logging.INFO))

    args = parser.parse_args()

    if args.cloudid:
        CLOUDID = args.cloudid
    elif args.svnbin and args.svnroot:
        SVNBIN_DIR = args.svnbin
        SVNROOT_DIR = args.svnroot
    else:
        parser.error('Either CLOUDID or SVNBIN and SVNROOT must be provided')

    if not len(args.queues):
        parser.error('At least one SQS queue name suffix must be provided')
    else:
        SQS_QUEUES = args.queues

    # In daemon mode, we let the daemonize module handle the pidfile.
    # Otherwise, we should write this (foreground) PID into the file.
    if args.pidfile and not args.daemon:
        pid = os.getpid()
        # Be wary of symlink attacks
        try:
            os.remove(args.pidfile)
        except OSError:
            pass
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
        with os.open(args.pidfile, flags) as f:
            os.write(f, b'%d\n' % pid)
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
        logging.info('Setting UID: %d', uid)
        os.setuid(uid)

    # Setup a new logging handler with the specified log level
    svnpubsub.logger.setup(logfile=args.logfile, level=args.log_level)

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
