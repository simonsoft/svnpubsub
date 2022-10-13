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
import argparse
import logging.handlers

from svnpubsub.client import Commit
from svnpubsub.daemon import Daemon, DaemonTask
from svndumpsub import prepare_logging
from svnpubsub.bgworker import BackgroundJob
from botocore.exceptions import ClientError

PORT = 2069
HOST = "127.0.0.1"
REPO_EXCLUDES = []
SQS_MESSAGE_ATTRIBUTES = {}
SQS_MESSAGE_GROUP_ID = 'commit'
SQS_QUEUES = []
CLOUDID = None

sqs = boto3.resource('sqs')


class Job(BackgroundJob):

    def __init__(self, commit: Commit):
        super().__init__(repo=commit.repositoryname, rev=commit.id, head=commit.id, commit=commit)

    def validate(self) -> bool:
        return True

    def run(self):
        for item in SQS_QUEUES:
            queue_name = "cms-{}-{}".format(CLOUDID, item)
            item = sqs.get_queue_by_name(QueueName=queue_name)
            try:
                logging.debug("Posting r%d from %s to: %s", self.rev, self.repo, queue_name)
                response = item.send_message(
                    MessageBody=self.commit.dumps(),
                    MessageGroupId=SQS_MESSAGE_GROUP_ID,
                    MessageAttributes=SQS_MESSAGE_ATTRIBUTES
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


def main():
    parser = argparse.ArgumentParser(description='An SvnPubSub client that posts an SQS queue message for each commit.')

    parser.add_argument('--logfile', help='filename for logging')
    parser.add_argument('--pidfile', help="the process' PID will be written to this file")
    parser.add_argument('--uid', help='switch to this UID before running')
    parser.add_argument('--gid', help='switch to this GID before running')
    parser.add_argument('--daemon', action='store_true', help='run as a background daemon')
    parser.add_argument('--umask', help='set this (octal) umask before running')
    parser.add_argument('--queue', default=[], type=str, nargs='+',
                        help='one or more SQS queue names to post the commits to')
    parser.add_argument('--cloudid', help='AWS cloud-id')

    args = parser.parse_args()

    if not args.cloudid:
        raise ValueError('A valid --cloudid has to be provided (aws cloudid)')
    else:
        global CLOUDID
        CLOUDID = args.cloudid

    if not len(args.queue):
        raise ValueError('At least one SQS queue name must be supplied')
    else:
        global SQS_QUEUES
        SQS_QUEUES = args.queue

    # In daemon mode, we let the daemonize module handle the pidfile.
    # Otherwise, we should write this (foreground) PID into the file.
    if args.pidfile and not args.daemon:
        pid = os.getpid()
        # Be wary of symlink attacks
        try:
            os.remove(args.pidfile)
        except OSError:
            pass
        fd = os.open(args.pidfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        os.write(fd, b'%d\n' % pid)
        os.close(fd)
        logging.info('pid %d written to %s', pid, args.pidfile)

    if args.gid:
        try:
            gid = int(args.gid)
        except ValueError:
            import grp
            gid = grp.getgrnam(args.gid)[2]
        logging.info('setting gid %d', gid)
        os.setgid(gid)

    if args.uid:
        try:
            uid = int(args.uid)
        except ValueError:
            import pwd
            uid = pwd.getpwnam(args.uid)[2]
        logging.info('setting uid %d', uid)
        os.setuid(uid)

    prepare_logging(args.logfile)

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
