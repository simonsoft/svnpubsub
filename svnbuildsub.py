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
# svnbuildsub - Subscribe to a SvnPubSub topic, start a CodeBuild build upon changes in *-application repositories.
#
# Example:
#  svnbuildsub.py
#
# On startup svnbuildsub starts listening to commits in all repositories.
#
import io
import os
import re
import stat
import zipfile

import boto3
import logging
import argparse
import svnpubsub.logger
from svnpubsub.util import execute
from svnpubsub.client import Commit
from svnpubsub.daemon import Daemon, DaemonTask
from svnpubsub.bgworker import BackgroundJob

PORT = 2069
HOST = "127.0.0.1"
BUCKET = "cms-codebuild-source"
SSM_PREFIX = "/cms/"
REPO_REGEX = "^[a-z0-9-]{1,20}-application$"
SVNBIN_DIR = "/usr/bin"
ACCOUNT = None


class Job(BackgroundJob):

    def __init__(self, commit: Commit):
        super().__init__(repo=commit.repositoryname, rev=commit.id, head=commit.id, commit=commit)

    def validate(self) -> bool:
        return True

    def run(self):
        global ACCOUNT, HOST, BUCKET, REPO_REGEX
        if ACCOUNT is None:
            ACCOUNT = get_account_identifier()
        if not re.match(REPO_REGEX, self.repo):
            logging.debug("Repository name mismatch: Commit skipped.")
            return
        if re.match('^(WIP)|(wip):?', self.commit.log):
            logging.debug("WIP: Commit skipped.")
            return
        """
        changes = {
            "demo-dev": {
                "DocumentTypes": [
                    "se.simonsoft.bogus"
                ]
            }
        }
        """
        changes = {}
        for item in self.commit.changed:
            # Format: cloudid/path2/qname/...
            # Example: demo-dev/DocumentTypes/se.simonsoft.bogus/repos.txt
            matches = re.match('^/?([a-z0-9-]{1,20})/(.+)/([a-z0-9.-]+)/', item)
            if matches and len(matches.groups()) == 3:
                cloudid = matches.group(1)
                path2 = matches.group(2)
                qname = matches.group(3)
                changes[cloudid] = changes.get(cloudid, {})
                changes[cloudid][path2] = changes[cloudid].get(path2, set())
                changes[cloudid][path2].add(qname)

        for cloudid, change in changes.items():
            for path2 in change:
                for qname in change[path2]:
                    zip_buffer = io.BytesIO()
                    folder = os.path.join(cloudid, path2, qname)
                    logging.debug("Processing: %s", folder)
                    try:
                        files = svn_list(repo=self.repo, rev=self.rev, path=folder)
                        for file in files:
                            path = os.path.join(folder, file)
                            data = svn_cat(repo=self.repo, rev=self.rev, path=path)
                            if data is not None:
                                add_to_archive(file=zip_buffer, path=os.path.relpath(path, folder), data=data)
                        # Add the revision as svnrevision.txt
                        add_to_archive(file=zip_buffer, path='svnrevision.txt', data="{}{}".format(self.rev, os.linesep).encode())
                        # Add the deploy:abx-secret property value as deploysecret.txt
                        secret = svn_propget(self.repo, os.path.join(cloudid, path2), 'deploy:abx-secret')
                        if secret:
                            add_to_archive(file=zip_buffer, path='deploysecret.txt', data=secret)
                        # Add the deploy:abx-suffix property value as deploysuffix.txt
                        suffix = svn_propget(self.repo, os.path.join(cloudid, path2), 'deploy:abx-suffix')
                        if suffix:
                            add_to_archive(file=zip_buffer, path='deploysuffix.txt', data=suffix)
                        key = "v1/{}/{}/{}.zip".format(cloudid, path2, qname)
                        version = upload_file(file=zip_buffer, bucket=BUCKET, key=key)
                        if not version:
                            logging.error("Failed to upload or retrieve the uploaded file version: %s/%s", BUCKET, key)
                            continue
                        path, parameters = get_build_names(cloudid=cloudid, path2=path2, qname=qname)
                        if parameters is None:
                            logging.error("Failed to retrieve the build names from: %s", path)
                            continue
                        for parameter in parameters:
                            _, project = os.path.split(parameter)
                            response = start_build(project=project, version=version)
                            build = response.get('build', {}).get('id')
                            if build:
                                logging.info("Build triggered: %s", build)
                            else:
                                logging.error("Failed to trigger a start build for: %s", project)
                    except Exception:
                        logging.exception("Exception while processing: %s", folder)
                        continue


class Task(DaemonTask):

    def __init__(self):
        super().__init__(urls=["http://%s:%d/commits" % (HOST, PORT)])

    def start(self):
        logging.info('Daemon started.')

    def commit(self, url: str, commit: Commit):
        job = Job(commit)
        self.worker.queue(job)


def svn_list(repo, rev, path):
    global SVNBIN_DIR, HOST
    arguments = [
        os.path.join(SVNBIN_DIR, 'svn'),
        'list', '-r', str(rev), '--depth', 'infinity',
        str.format('https://{}/svn/{}/{}', HOST, repo, path)
    ]
    try:
        _, stdout, _ = execute(*arguments)
        return stdout.splitlines()
    except Exception as e:
        logging.warning("%s, failed to retrieve the list of files at: %s/%s", str(e), repo, path)
        return []


def svn_cat(repo, rev, path):
    global SVNBIN_DIR, HOST
    arguments = [
        os.path.join(SVNBIN_DIR, 'svn'),
        'cat', '-r', str(rev),
        str.format('https://{}/svn/{}/{}', HOST, repo, path)
    ]
    try:
        _, stdout, _ = execute(*arguments, text=False)
        return stdout
    except Exception as e:
        logging.warning("%s, failed to retrieve the contents of the file at: %s/%s", str(e), repo, path)
        return None


def svn_propget(repo, path, property):
    global SVNBIN_DIR, HOST
    arguments = [
        os.path.join(SVNBIN_DIR, 'svn'),
        'propget', property,
        str.format('https://{}/svn/{}/{}', HOST, repo, path)
    ]
    try:
        _, stdout, _ = execute(*arguments, text=False)
        return stdout
    except Exception as e:
        logging.warning("%s, failed to retrieve the %s property from: %s/%s", str(e), property, repo, path)
        return None


def add_to_archive(file, path, data):
    with zipfile.ZipFile(file, 'a', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(path, data)
        logging.debug("Archived: %s", path)


def get_account_identifier():
    client = boto3.client('sts')
    response = client.get_caller_identity()
    account = response.get('Account')
    if account:
        logging.info("Account identifier retrieved: %s", account)
    else:
        logging.error("Failed to retrieve the account identifier.")
    return account


def get_build_names(cloudid, path2, qname):
    global SSM_PREFIX
    client = boto3.client('ssm')
    path = os.path.join(SSM_PREFIX, cloudid, 'application', path2, qname, 'codebuild')
    logging.debug("Retrieving SSM parameter: %s", path)
    response = client.get_parameters_by_path(Path=path, Recursive=False)
    return path, [parameter['Name'] for parameter in response.get('Parameters', []) if parameter.get('Value') == 'true']


def start_build(project, version):
    client = boto3.client('codebuild')
    logging.debug("Starting Codebuild build: %s (sourceVersion: %s)", project, version)
    response = client.start_build(
        projectName=project,
        sourceVersion=version
    )
    return response


def upload_file(file, bucket, key) -> str:
    """Uploads a file to the S3 storage from a physical file name or a file-like object
    Args:
        file (string/Fileobj): A physical file name or a fileobj (actual file or file-like object)
        bucket (string): The name of the destination S3 bucket
        key (string): The destination key on the destination bucket on the S3 storage
    Returns:
        The uploaded file's VersionId.
    """
    client = boto3.client('s3')
    if isinstance(file, str):
        client.upload_file(file, bucket, key)
    else:
        file.seek(0)
        client.upload_fileobj(file, bucket, key)
    response = client.get_object(
        Bucket=bucket,
        Key=key
    )
    version = response.get('VersionId')
    logging.info("File uploaded: %s/%s (VersionId: %s)", bucket, key, version)
    return version


def main():
    global HOST, BUCKET, SSM_PREFIX, REPO_REGEX, SVNBIN_DIR

    parser = argparse.ArgumentParser(description='An SvnPubSub client that subscribes to a topic, starts a CodeBuild build upon changes in *-application repositories.')

    parser.add_argument('--host', help='host name used to subscribe to events (default: %s)' % HOST)
    parser.add_argument('--bucket', help='the s3 bucket name where the qname archive is uploaded (default: %s)' % BUCKET)
    parser.add_argument('--ssm-prefix', help='aws ssm prefix used to retrieve the parameters from the parameter store')
    parser.add_argument('--repo-regex', help='the regex pattern used to recognize the applicable repo names (default: %s)' % REPO_REGEX)
    parser.add_argument('--logfile', help='a filename for logging if stdout is not the desired output')
    parser.add_argument('--pidfile', help='the PID file where the process PID will be written to')
    parser.add_argument('--uid', help='switch to this UID before running')
    parser.add_argument('--gid', help='switch to this GID before running')
    parser.add_argument('--daemon', action='store_true', help='run as a background daemon')
    parser.add_argument('--umask', help='set this (octal) UMASK before running')
    parser.add_argument('--svnbin', default=SVNBIN_DIR, help='the path to svn, svnlook, svnadmin, ... binaries (default: %s)' % SVNBIN_DIR)
    parser.add_argument('--log-level', type=int, default=logging.INFO,
                        help='log level (DEBUG: %d | INFO: %d | WARNING: %d | ERROR: %d | CRITICAL: %d) (default: %d)' %
                             (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL, logging.INFO))

    args = parser.parse_args()

    if args.host:
        HOST = args.host
    if args.bucket:
        BUCKET = args.bucket
    if args.ssm_prefix:
        SSM_PREFIX = args.ssm_prefix
    if args.repo_regex:
        REPO_REGEX = args.repo_regex
    if args.svnbin:
        SVNBIN_DIR = args.svnbin

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
