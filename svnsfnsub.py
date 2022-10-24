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
# svnsfnsub - Subscribe to a SvnPubSub topic, start a Step Functions execution for each changed item.
#
# Example:
#  svnsfnsub.py
#
# On startup svnsfnsub starts listening to commits in all repositories.
#
import json
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
EXCLUDED_REPOS = []
SVNBIN_DIR = "/usr/bin"
SVNROOT_DIR = "/srv/cms/svn"
DOMAIN = "simonsoftcms.se"
ACCOUNT = None
CLOUDID = {}


class Job(BackgroundJob):

    def __init__(self, commit: Commit):
        super().__init__(repo=commit.repositoryname, rev=commit.id, head=commit.id, commit=commit)

    def validate(self) -> bool:
        return True

    def run(self):
        global ACCOUNT, DOMAIN, CLOUDID
        if isinstance(CLOUDID, str):
            cloudid = CLOUDID
        elif self.repo in CLOUDID:
            cloudid = CLOUDID[self.repo]
        else:
            cloudid = CLOUDID[self.repo] = get_cloudid(self.repo)
        if ACCOUNT is None:
            sts = boto3.client('sts')
            response = sts.get_caller_identity()
            ACCOUNT = response.get('Account')
            if ACCOUNT:
                logging.info("Account identifier retrieved: %s", ACCOUNT)
            else:
                logging.error("Failed to retrieve the account identifier.")
                return
        stepfunctions = boto3.client('stepfunctions')
        for item, details in self.commit.changed.items():
            name = "cms-{}-event-v1".format(cloudid)
            state_machine_arn = "arn:aws:states:eu-west-1:{}:stateMachine:{}".format(ACCOUNT, name)
            try:
                logging.debug("Starting a %s execution for: %s/%s (r%d)", name, self.repo, item, self.commit.id)
                response = stepfunctions.start_execution(
                    stateMachineArn=state_machine_arn,
                    input=json.dumps({
                        "action": "item-event",
                        "userid": self.commit.committer,
                        "itemid": "x-svn://{}.{}/svn/{}/{}?p={}".format(cloudid, DOMAIN, self.repo, item, self.commit.id)
                    })
                )
                logging.info("Successfully started %d %s execution(s)", len(self.commit.changed.keys()), name)
                logging.debug("Response: %s", response)
            except ClientError:
                logging.exception("Exception occurred while starting an execution.")


class Task(DaemonTask):

    def __init__(self):
        super().__init__(urls=["http://%s:%d/commits" % (HOST, PORT)], excluded_repos=EXCLUDED_REPOS)

    def start(self):
        logging.info('Daemon started.')

    def commit(self, url: str, commit: Commit):
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
    try:
        _, stdout, _ = execute(*arguments)
        return stdout
    except RuntimeError as e:
        logging.warning("%s, falling back to repository name: %s", str(e), repo)
        return repo


def main():
    global DOMAIN, CLOUDID, SVNROOT_DIR, SVNBIN_DIR

    parser = argparse.ArgumentParser(description='An SvnPubSub client that subscribes to a topic, starts a Step Functions execution for each changed item.')

    parser.add_argument('--cloudid', help='aws cloud-id which overrides retrieving it from revprops or repository name')
    parser.add_argument('--domain', help='domain name used to build the payload itemid (default: %s)' % DOMAIN)
    parser.add_argument('--logfile', help='a filename for logging if stdout is not the desired output')
    parser.add_argument('--pidfile', help='the PID file where the process PID will be written to')
    parser.add_argument('--uid', help='switch to this UID before running')
    parser.add_argument('--gid', help='switch to this GID before running')
    parser.add_argument('--daemon', action='store_true', help='run as a background daemon')
    parser.add_argument('--umask', help='set this (octal) UMASK before running')
    parser.add_argument('--svnroot', default=SVNROOT_DIR, help='the path to repositories (default: %s)' % SVNROOT_DIR)
    parser.add_argument('--svnbin', default=SVNBIN_DIR,
                        help='the path to svn, svnlook, svnadmin, ... binaries (default: %s)' % SVNBIN_DIR)
    parser.add_argument('--log-level', type=int, default=logging.INFO,
                        help='log level (DEBUG: %d | INFO: %d | WARNING: %d | ERROR: %d | CRITICAL: %d) (default: %d)' %
                             (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL, logging.INFO))

    args = parser.parse_args()

    if args.domain:
        DOMAIN = args.domain

    if args.cloudid:
        CLOUDID = args.cloudid
    elif args.svnbin and args.svnroot:
        SVNBIN_DIR = args.svnbin
        SVNROOT_DIR = args.svnroot
    else:
        parser.error('Either CLOUDID or SVNBIN and SVNROOT must be provided')

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
