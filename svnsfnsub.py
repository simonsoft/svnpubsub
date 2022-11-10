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
import os
import re
import stat
import json
import boto3
import logging
import argparse
import svnpubsub.logger
from time import sleep
from io import StringIO
from svnpubsub.client import Commit
from svnpubsub.daemon import Daemon, DaemonTask
from svnpubsub.bgworker import BackgroundJob
from botocore.exceptions import ClientError

PORT = 2069
HOST = "127.0.0.1"
SSM_PREFIX = None
DOMAIN = "simonsoftcms.se"
ACCOUNT = None
CLOUDID = {}
RETRY_DELAY = 30
RETRIES = 3


class Job(BackgroundJob):

    failed = []
    retrying = 0

    def __init__(self, commit: Commit):
        super().__init__(repo=commit.repositoryname, rev=commit.id, head=commit.id, commit=commit)

    def validate(self) -> bool:
        return True

    def run(self):
        global ACCOUNT, DOMAIN, CLOUDID
        if self.failed:
            self.retrying += 1
        if self.retrying > RETRIES:
            return
        if isinstance(CLOUDID, str):
            cloudid = CLOUDID
        elif self.repo in CLOUDID and CLOUDID[self.repo]:
            cloudid = CLOUDID[self.repo]
        else:
            cloudid = CLOUDID[self.repo] = get_cloudid(self.repo)
        if not cloudid:
            logging.warning("Commit skipped.")
            return
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
        items = self.failed if self.failed else self.commit.changed
        for item in items:
            name = "cms-{}-event-v1".format(cloudid)
            state_machine_arn = "arn:aws:states:eu-west-1:{}:stateMachine:{}".format(ACCOUNT, name)
            try:
                logging.debug("%s a %s execution for: %s/%s (r%d)",
                              "Restarting" if self.retrying else "Starting", name, self.repo, item, self.commit.id)
                response = stepfunctions.start_execution(
                    stateMachineArn=state_machine_arn,
                    input=json.dumps({
                        "action": "item-event",
                        "userid": self.commit.committer,
                        "itemid": "x-svn://{}.{}/svn/{}/{}?p={}".format(cloudid, DOMAIN, self.repo, encode(item), self.commit.id)
                    })
                )
                if item in self.failed:
                    self.failed.remove(item)
                logging.debug("Successfully started a %s execution for: %s/%s", name, self.repo, item)
                logging.debug("Response: %s", response)
            except ClientError:
                logging.exception("Exception occurred while starting an execution:")
                if item not in self.failed:
                    self.failed.append(item)
        failed = len(self.failed)
        succeeded = len(items) - failed
        if succeeded:
            logging.info("Successfully %s %d %s execution(s)", "restarted" if self.retrying else "started", succeeded, name)
        if failed:
            if self.retrying < RETRIES:
                logging.warning("Failed to start %d %s execution(s), will retry %d more time(s) every %d seconds.",
                                failed, name, RETRIES - self.retrying, RETRY_DELAY)
                sleep(RETRY_DELAY)
                self.run()
            else:
                logging.error("Failed to start %d %s execution(s) after %d retries.", failed, name, RETRIES)


class Task(DaemonTask):

    def __init__(self):
        super().__init__(urls=["http://%s:%d/commits" % (HOST, PORT)])

    def start(self):
        logging.info('Daemon started.')

    def commit(self, url: str, commit: Commit):
        job = Job(commit)
        self.worker.queue(job)


def get_cloudid(repo):
    global SSM_PREFIX
    ssm = boto3.client('ssm')
    name = os.path.join(SSM_PREFIX, repo, 'cloudid')
    try:
        cloudid = ssm.get_parameter(Name=name)['Parameter']['Value']
        if cloudid and re.match('^[a-z0-9-]{1,20}$', cloudid, re.MULTILINE):
            logging.info("Retrieved CloudId from SSM parameter store at %s: %s", name, cloudid)
            return cloudid
        else:
            logging.error("The retrieved CloudId from SSM parameter store at %s was invalid: %s", name, cloudid)
        return None
    except Exception as e:
        logging.warning("%s, falling back to repository name: %s", str(e), repo)
        return repo


def encode(path: str) -> str:
    output = StringIO()
    safe_characters = [
        *[chr(x) for x in range(ord('a'), ord('z') + 1)],   # a-z
        *[chr(x) for x in range(ord('A'), ord('Z') + 1)],   # A-Z
        *[chr(x) for x in range(ord('0'), ord('9') + 1)],   # 0-9
        # The following additions to safe rule was likely made by the Derby project:
        # The 'safe' rule
        '$', '-', '_', '.', '+',
        # The 'extra' rule
        '!', '*', '\'', '(', ')', ',',
        # Special characters common to http: file: and ftp: URLs ('fsegment' and 'hsegment' rules)
        '/', ':', '@', '&', '=',
        # Added for Svnkit compatibility
        '~',
    ]
    for i, c in enumerate(path):
        if c in safe_characters:
            output.write(c)
        else:
            output.write("".join(['%{:02X}'.format(byte) for byte in c.encode("utf-8")]))
    output.seek(0)
    return output.read()


def main():
    global DOMAIN, CLOUDID, SSM_PREFIX

    parser = argparse.ArgumentParser(description='An SvnPubSub client that subscribes to a topic, starts a Step Functions execution for each changed item.')

    parser.add_argument('--ssm-prefix', help='aws ssm prefix used to retrieve the CLOUDID from the parameter store')
    parser.add_argument('--cloudid', help='aws cloud-id which overrides retrieving it from ssm parameter store or repository name')
    parser.add_argument('--domain', help='domain name used to build the payload itemid (default: %s)' % DOMAIN)
    parser.add_argument('--logfile', help='a filename for logging if stdout is not the desired output')
    parser.add_argument('--pidfile', help='the PID file where the process PID will be written to')
    parser.add_argument('--uid', help='switch to this UID before running')
    parser.add_argument('--gid', help='switch to this GID before running')
    parser.add_argument('--daemon', action='store_true', help='run as a background daemon')
    parser.add_argument('--umask', help='set this (octal) UMASK before running')
    parser.add_argument('--log-level', type=int, default=logging.INFO,
                        help='log level (DEBUG: %d | INFO: %d | WARNING: %d | ERROR: %d | CRITICAL: %d) (default: %d)' %
                             (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL, logging.INFO))

    args = parser.parse_args()

    if args.domain:
        DOMAIN = args.domain

    if args.cloudid:
        CLOUDID = args.cloudid
    elif args.ssm_prefix:
        SSM_PREFIX = args.ssm_prefix
    else:
        parser.error('Either CLOUDID or SSM_PREFIX must be provided')

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
