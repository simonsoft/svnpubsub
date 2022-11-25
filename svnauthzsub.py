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
import sys
import stat
import shutil
import logging
import argparse
import configparser
import svnpubsub.logger
from io import StringIO
from textwrap import indent
from urllib.request import urlopen
from svnpubsub.client import Commit
from svnpubsub.daemon import Daemon, DaemonTask
from svnpubsub.bgworker import BackgroundJob

PORT = 2069
VPC_PORT = 8091
HOST = "127.0.0.1"
EXCLUDED_REPOS = []
OUTPUT_DIR = None
INDENTATION = 2


class ConfigParser(configparser.ConfigParser):
    """A custom ConfigParser sub-class that ensures the option cases are preserved."""

    def optionxform(self, optionstr):
        return optionstr


def validate(access_accs: str | list):
    result = True
    config_parser = ConfigParser()
    config_parser.read_string(os.linesep.join(access_accs) if isinstance(access_accs, list) else access_accs)
    paths = [section for section in config_parser.sections() if '/' in section]
    for path in paths:
        section = config_parser[path]
        roles = [role for role in section if role.startswith('@')]
        # Path may not contain extended characters as Apache doesn't support UTF8 locations
        # Accept alphanumeric characters plus the following: _./ -
        if not re.match("^([A-Za-z0-9_./ -]*)$", path):
            logging.error("The path provided in Section [%s] contains invalid characters.", path)
            result = False
        # Group/Role may only contain alphanumeric characters plus the following: _-
        for role in roles:
            if not re.match("^@([A-Za-z0-9_-]*)$", role):
                logging.error("The %s role provided in Section [%s] contains invalid characters.", role, path)
                result = False
    return result


def generate(access_accs: str | list, repo):
    def directive(name: str, content: str | list, parameters: str = None):
        result = "<{}{}>".format(name, " {} ".format(parameters) if parameters else "") + os.linesep
        if isinstance(content, list):
            result += indent("".join(content), "".ljust(INDENTATION))
        else:
            result += indent(content, "".ljust(INDENTATION))
        result += "</{}>".format(name) + os.linesep
        return result

    def location(name, content):
        return directive("Location", content, parameters="\"/svn/{}{}\"".format(repo, name.rstrip('/'))) + os.linesep

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
        if ('' not in permissions or '*' not in permissions['']) and path != '/':
            logging.warning("Section [%s] is missing a '* = ' permission inheritance specifier or it is invalid.", path)
        # Now append the individual sections
        output.write(location(path, [
            require("all denied")   # Special case when the only entry in a section is a "* = " statement
        ] if len(permissions) == 1 and list(permissions.values())[0] == ['*'] else [
            # Add the common OPTIONS section
            require_all([
                require("valid-user"),
                require("method OPTIONS MERGE")
            ]),
            # Add the Read-Only section
            require_all([
                require("valid-user"),
                require("method GET PROPFIND OPTIONS REPORT"),
                require_any([
                    require("expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*{}(,[^,]+)*$/".format(role))
                    for role in permissions['r'] if role != '*'
                ])
            ]) if 'r' in permissions else "",
            # Add the Read/Write section
            require_all([
                require("valid-user"),
                require_any([
                    require("expr req_novary('OIDC_CLAIM_roles') =~ /^([^,]+,)*{}(,[^,]+)*$/".format(role))
                    for role in permissions['rw'] if role != '*'
                ])
            ]) if 'rw' in permissions else ""
        ]))
    output.seek(0)
    return output


class Job(BackgroundJob):

    def __init__(self, commit: Commit):
        super().__init__(repo=commit.repositoryname, rev=commit.id, head=commit.id, commit=commit)

    def retrieve_access_accs(self, rev=0):
        global HOST, VPC_PORT
        url = "http://{}:{}/svn/{}/access.accs".format(HOST, VPC_PORT, self.repo)
        try:
            with urlopen(url=url) as response:
                return response.read().decode('utf-8')
        except Exception as e:
            logging.error("Failed to retrieve the access.accs file: %s", str(e))
        return None

    def validate(self) -> bool:
        return True

    def run(self):
        global OUTPUT_DIR
        access_accs = self.retrieve_access_accs()
        if not access_accs:
            logging.warning("Commit skipped.")
            return
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
    global OUTPUT_DIR
    access_accs = None

    parser = argparse.ArgumentParser(description='An SvnPubSub client that monitors the access.accs and regenerates the apache config file.')

    parser.add_argument('--validate', action='store_true', help='validate the access file provided via INPUT or through STDIN and exit')
    parser.add_argument('--stdin', action='store_true', help='read the access.acss file from stdin, generate the configuration or validate it and exit')
    parser.add_argument('--input', help='process a local access.acss file, generate the configuration or validate it and exit')
    parser.add_argument('--repo', help='the repository name to use in combination with INPUT file as input')
    parser.add_argument('--output', help='the output file to write to when INPUT is supplied if not stdout')
    parser.add_argument('--output-dir', help='the path to place the generated apache configuration files')
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

    if (args.input or args.stdin) and not args.validate and not args.repo:
        parser.error('--repo is required when --input or --stdin is used')

    try:
        # The input has been provided through the --input argument
        if args.input:
            if not os.path.exists(args.input):
                parser.error('Input file not found: {}'.format(args.input))
            with open(args.input, 'r') as input:
                access_accs = input.read()
        # The input has been provided through stdin
        elif args.stdin:
            access_accs = sys.stdin.read()
        # No explicit input has been provided, the --output-dir becomes mandatory
        elif not args.output_dir:
            parser.error('--output-dir must be provided')
        # Process the access file provided through --input or stdin
        if access_accs:
            if args.validate:
                exit(0 if validate(access_accs=access_accs) else 1)
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

    if args.output_dir:
        OUTPUT_DIR = args.output_dir

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
