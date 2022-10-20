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
#  svnauthzsub - Subscribe to a SvnPubSub stream, monitor the access.accs changes and regenerate the apache config file.
#
# Example:
#   svnauthzsub.py
#
# On startup  svnauthzsub starts listening to commits in all repositories.
#

import os
import re
import stat
import shutil
import logging
import argparse
import configparser
import svnpubsub.logger
from io import StringIO
from textwrap import indent
from svnpubsub.client import Commit
from svnpubsub.daemon import Daemon, DaemonTask
from svnpubsub.bgworker import BackgroundJob
from svnpubsub.util import execute

PORT = 2069
HOST = "127.0.0.1"
EXCLUDED_REPOS = []
SVNBIN_DIR = "/usr/bin"
SVNROOT_DIR = "/srv/cms/svn"
OUTPUT_DIR = None
INDENTATION = 2


def generate(access_accs: str | list, repo):
    class ConfigParser(configparser.ConfigParser):
        """A custom ConfigParser sub-class that ensures the option cases are preserved."""

        def optionxform(self, optionstr):
            return optionstr

    def directive(name: str, content: str | list, parameters: str = None):
        result = "<{}{}>".format(name, " {} ".format(parameters) if parameters else "") + os.linesep
        if isinstance(content, list):
            result += indent("".join(content), "".ljust(INDENTATION))
        else:
            result += indent(content, "".ljust(INDENTATION))
        result += "</{}>".format(name) + os.linesep
        return result

    def location(name, content):
        return directive("Location", content, parameters="/svn/{}{}".format(repo, name.rstrip('/'))) + os.linesep

    def require(expression):
        return "Require {}".format(expression) + os.linesep

    def require_any(content):
        return directive("RequireAny", content)

    def require_all(content):
        return directive("RequireAll", content)

    output = StringIO()
    config_parser = ConfigParser()
    config_parser.read_string(os.linesep.join(access_accs) if isinstance(access_accs, list) else access_accs)
    # Ignore sections not containing a path such as 'groups', etc.
    paths = [section for section in config_parser.sections() if '/' in section]
    # Sort the paths putting parent before children as that is how it works as expected in apache
    paths.sort(key=lambda x: (os.path.dirname(x), os.path.basename(x)))
    for path in paths:
        section = config_parser[path]
        permissions = {section[role]: [] for role in section if role.startswith('*') or role.startswith('@')}
        for role in section:
            # Remove the starting @ from the role name
            matches = re.match("^(?:@)?([\\w*]+)$", role)
            if matches:
                permissions[section[role]].append(matches.group(1))
        if '' not in permissions or '*' not in permissions['']:
            if path != '/':
                logging.warning("Section [%s] is missing the '* = ' permission inheritance specifier.", path)
        # Now append the individual sections
        output.write(location(path, [
            # Add the common OPTIONS section
            require_all([
                require("valid-user"),
                require("method OPTIONS")
            ]),
            # Add the Read-Only section
            require_all([
                require("valid-user"),
                require("method GET PROPFIND OPTIONS REPORT"),
                require_any([
                    require("expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*{}(,[^,]+)*$/".format(role))
                    for role in permissions['r']
                ])
            ]) if 'r' in permissions else "",
            # Add the Read/Write section
            require_all([
                require("valid-user"),
                require_any([
                    require("expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*{}(,[^,]+)*$/".format(role))
                    for role in permissions['rw']
                ])
            ]) if 'rw' in permissions else ""
        ]))
    output.seek(0)
    return output


class Job(BackgroundJob):

    def __init__(self, commit: Commit):
        super().__init__(repo=commit.repositoryname, rev=commit.id, head=commit.id, commit=commit)

    def retrieve_access_accs(self, rev=0):
        global SVNBIN_DIR, SVNROOT_DIR
        svnlook = os.path.join(SVNBIN_DIR, 'svnlook')
        repository = os.path.join(SVNROOT_DIR, self.repo)
        arguments = [svnlook, 'cat', repository, '/access.accs']
        _, stdout, _ = execute(*arguments)
        return stdout

    def validate(self) -> bool:
        return True

    def run(self):
        global OUTPUT_DIR
        access_accs = self.retrieve_access_accs()
        output_file = os.path.join(OUTPUT_DIR, "svn-{}.conf".format(self.repo))
        try:
            exists = os.path.exists(output_file)
            config = generate(access_accs=access_accs, repo=self.repo)
            with open(output_file, 'w') as output:
                shutil.copyfileobj(config, output)
                logging.info("Config %s: %s", "regenerated" if exists else "generated", output_file)
        except Exception as e:
            logging.error("%s", str(e))


class Task(DaemonTask):

    def __init__(self):
        super().__init__(urls=["http://%s:%d/commits" % (HOST, PORT)], excluded_repos=EXCLUDED_REPOS)

    def start(self):
        logging.info('Daemon started.')

    def commit(self, url: str, commit: Commit):
        if "access.accs" in commit.changed:
            job = Job(commit)
            self.worker.queue(job)


def main():
    global SVNROOT_DIR, SVNBIN_DIR, OUTPUT_DIR

    parser = argparse.ArgumentParser(description='An SvnPubSub client that monitors the access.accs and regenerates the apache config file.')

    parser.add_argument('--input', help='process a local access.acss file, generate the configuration and exit')
    parser.add_argument('--repo', help='the repository name to use in combination with INPUT file as input')
    parser.add_argument('--output', help='the output file to write to when INPUT is supplied if not stdout')
    parser.add_argument('--output-dir', help='the path to place the generated apache configuration files')
    parser.add_argument('--svnroot', default=SVNROOT_DIR, help='the path to repositories (default: %s)' % SVNROOT_DIR)
    parser.add_argument('--svnbin', default=SVNBIN_DIR, help='the path to svn, svnlook, svnadmin, ... binaries (default: %s)' % SVNBIN_DIR)
    parser.add_argument('--umask', help='set this (octal) UMASK before running')
    parser.add_argument('--daemon', action='store_true', help='run as a background daemon')
    parser.add_argument('--uid', help='switch to this UID before running')
    parser.add_argument('--gid', help='switch to this GID before running')
    parser.add_argument('--pidfile', help='the PID file where the process PID will be written to')
    parser.add_argument('--logfile', help='a filename for logging if stdout is not the desired output')
    parser.add_argument('--log-level', type=int, default=logging.INFO,
                        help='log level (DEBUG: %d | INFO: %d | WARNING: %d | ERROR: %d | CRITICAL: %d) (default: %d)' %
                             (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL, logging.INFO))

    args = parser.parse_args()

    if args.input and not args.repo:
        parser.error('REPO is required when INPUT is supplied')

    if args.input:
        if not os.path.exists(args.input):
            parser.error('Input file not found: {}'.format(args.input))
        try:
            with open(args.input, 'r') as input:
                access_accs = input.read()
                config = generate(access_accs=access_accs, repo=args.repo)
                if args.output:
                    with open(args.output, 'w') as output:
                        shutil.copyfileobj(config, output)
                else:
                    print(config.read())
                exit(0)
        except Exception as e:
            logging.error("%s", str(e))
            exit(1)
    elif not args.output_dir:
        parser.error('OUTPUT_DIR must be provided')

    if args.output_dir:
        OUTPUT_DIR = args.output_dir

    if args.svnbin and args.svnroot:
        SVNBIN_DIR = args.svnbin
        SVNROOT_DIR = args.svnroot
    else:
        parser.error('SVNBIN and SVNROOT must be provided')

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
        logging.info('UID: %d', uid)
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
