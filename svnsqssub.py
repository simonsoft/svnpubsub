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
# SvnSQSSub - Subscribe to a SvnPubSub topic, posts commit events to AWS SQS.
#
# Example:
#  svnsqssub.py
#
# On startup SvnSQSSub starts listening to commits in all repositories.
#
import os
import re
import copy
import stat
import json
import boto3
import logging
import argparse
import svnpubsub.logger
from svnpubsub.client import Commit
from svnpubsub.daemon import Daemon, DaemonTask
from svnpubsub.bgworker import BackgroundJob
from botocore.exceptions import ClientError

PORT = 2069
HOST = "127.0.0.1"
SSM_PREFIX = None
EXCLUDED_REPOS = []
SQS_MESSAGE_GROUP_ID = "commit"
SQS_MESSAGE_ATTRIBUTES = {}
SQS_QUEUES = {}
CLOUDID = {}


class Job(BackgroundJob):

    def __init__(self, commit: Commit):
        super().__init__(repo=commit.repositoryname, rev=commit.id, head=commit.id, commit=commit)

    def validate(self) -> bool:
        return True

    def run(self):
        queue = None
        cloudid = None
        commit = copy.deepcopy(self.commit)
        global CLOUDID, SQS_QUEUES, SQS_MESSAGE_GROUP_ID, SQS_MESSAGE_ATTRIBUTES
        if isinstance(CLOUDID, str):
            cloudid = CLOUDID
        elif self.repo in CLOUDID and CLOUDID[self.repo]:
            cloudid = CLOUDID[self.repo]
        else:
            cloudid = CLOUDID[self.repo] = get_cloudid(self.repo)
        if not cloudid:
            logging.warning("Commit skipped.")
            return
        sqs = boto3.resource('sqs')
        for suffix, queues in SQS_QUEUES.items():
            config = SQS_QUEUES[suffix]
            message_type = config.get('type', 'commit')
            extensions = config.get('extensions', [])
            # Filter changed paths by extensions if specified
            if extensions:
                changed = getattr(commit, 'changed', {})
                commit.changed = { path: details for path, details in changed.items() if any(path.endswith(extension) for extension in extensions) }
                if not commit.changed:
                    logging.debug("Skipping r%d from %s for queue %s: no matching extensions.", self.rev, self.repo, suffix)
                    continue
            fifo = suffix.endswith('.fifo')
            name = "cms-{}-{}".format(cloudid, suffix)
            if self.repo in queues:
                queue = SQS_QUEUES[suffix][self.repo]
            else:
                queue = SQS_QUEUES[suffix][self.repo] = sqs.get_queue_by_name(QueueName=name)
            if message_type == 'item':
                # Send individual messages for each changed item
                for path, details in commit.changed.items():
                    self.send_message(queue, name, fifo, commit, { path: details })
            else:
                # Send the commit message
                self.send_message(queue, name, fifo, commit)

    def send_message(self, queue, name, fifo, commit, changed=None):
        commit = copy.deepcopy(commit)
        if changed is not None:
            commit.changed = changed
        try:
            if changed is not None:
                logging.debug("Posting %s of r%d from %s to: %s", list(changed.keys())[0], self.rev, self.repo, name)
            else:
                logging.debug("Posting r%d from %s to: %s", self.rev, self.repo, name)
            message = {
                'MessageBody': commit.dumps(),
                'MessageAttributes': SQS_MESSAGE_ATTRIBUTES,
            }
            if fifo:
                message['MessageGroupId'] = SQS_MESSAGE_GROUP_ID
                message['MessageDeduplicationId'] = "{}/r{}".format(self.repo, self.rev)
            response = queue.send_message(**message)
            if changed is not None:
                logging.info("Posted %s of r%d from %s to: %s", list(changed.keys())[0], self.rev, self.repo, name)
            else:
                logging.info("Posted r%d from %s to: %s", self.rev, self.repo, name)
            logging.debug("Response: %s", response)
        except ClientError:
            logging.exception("Exception occurred while sending a queue message.")


class Task(DaemonTask):

    def __init__(self):
        super().__init__(urls=["http://%s:%d/commits" % (HOST, PORT)], excluded_repos=EXCLUDED_REPOS)

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


def parse_queues(value):
    """Parse a JSON string or read from file if value starts with @."""
    if value.startswith('@'):
        filepath = value[1:]
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise argparse.ArgumentTypeError(f'Queues config file not found: {filepath}')
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(f'Invalid JSON in file {filepath}: {e}')
    else:
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(f'Invalid JSON: {e}')


def main():
    global CLOUDID, SSM_PREFIX, SQS_QUEUES

    parser = argparse.ArgumentParser(description='An SvnPubSub client that subscribes to a topic, posts commit events to AWS SQS.')

    parser.add_argument('--ssm-prefix', help='aws ssm prefix used to retrieve the CLOUDID from the parameter store')
    parser.add_argument('--cloudid', help='aws cloud-id which overrides retrieving it from ssm parameter store or repository name')
    parser.add_argument('--logfile', help='a filename for logging if stdout is not the desired output')
    parser.add_argument('--pidfile', help='the PID file where the process PID will be written to')
    parser.add_argument('--uid', help='switch to this UID before running')
    parser.add_argument('--gid', help='switch to this GID before running')
    parser.add_argument('--daemon', action='store_true', help='run as a background daemon')
    parser.add_argument('--umask', help='set this (octal) UMASK before running')
    parser.add_argument('--queues', type=parse_queues, required=True,
                        help='JSON object or @filepath mapping queue suffixes to configuration with "type" (commit/item) and optional "extensions" array')
    parser.add_argument('--log-level', type=int, default=logging.INFO,
                        help='log level (DEBUG: %d | INFO: %d | WARNING: %d | ERROR: %d | CRITICAL: %d) (default: %d)' %
                             (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL, logging.INFO))

    args = parser.parse_args()

    if args.cloudid:
        CLOUDID = args.cloudid
    elif args.ssm_prefix:
        SSM_PREFIX = args.ssm_prefix
    else:
        parser.error('Either CLOUDID or SSM_PREFIX must be provided')

    if not isinstance(args.queues, dict) or not args.queues:
        parser.error('--queues must be a non-empty JSON object')

    for queue, config in args.queues.items():
        if not isinstance(config, dict):
            parser.error(f'Queue "{queue}" configuration must be an object')

        message_type = config.get('type')
        if not message_type:
            parser.error(f'Queue "{queue}" must have a "type" field (commit or item)')
        if message_type not in ['commit', 'item']:
            parser.error(f'Queue "{queue}" type must be either "commit" or "item", got: {message_type}')

        extensions = config.get('extensions')
        if extensions is not None:
            if not isinstance(extensions, list):
                parser.error(f'Queue "{queue}" extensions must be an array')
            if not all(isinstance(extension, str) for extension in extensions):
                parser.error(f'Queue "{queue}" extensions must be an array of strings')

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

    # Set up a new logging handler with the specified log level
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
