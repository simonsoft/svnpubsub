#!/usr/bin/env python
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
# SvnWcSub - Subscribe to a SvnPubSub stream, and keep a set of working copy
# paths in sync
#
# Example:
#  svnwcsub.py svnwcsub.conf
#
# On startup svnwcsub checks the working copy's path, runs a single svn update
# and then watches for changes to that path.
#
# See svnwcsub.conf for more information on its contents.
#

# TODO:
# - bulk update at startup time to avoid backlog warnings
# - fold BDEC into Daemon
# - fold WorkingCopy._get_match() into __init__
# - remove wc_ready(). assume all WorkingCopy instances are usable.
#   place the instances into .watch at creation. the .update_applies()
#   just returns if the wc is disabled (eg. could not find wc dir)
# - figure out way to avoid the ASF-specific PRODUCTION_RE_FILTER
#   (a base path exclusion list should work for the ASF)
# - add support for SIGHUP to reread the config and reinitialize working copies
# - joes will write documentation for svnpubsub as these items become fulfilled
# - make LOGLEVEL configurable

import errno
import subprocess
import threading
import sys
import stat
import os
import re
import posixpath
try:
  import ConfigParser
except ImportError:
  import configparser as ConfigParser
import time
import logging.handlers
try:
  import Queue
except ImportError:
  import queue as Queue
import optparse
import functools
try:
  import urlparse
except ImportError:
  import urllib.parse as urlparse

import daemonize
import svnpubsub.client
import svnpubsub.util

HOST = "127.0.0.1"
PORT = 2069
AWS = "/home/vagrant/.local/bin/aws"
SVNADMIN = "/usr/bin/svnadmin"

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
        # TODO: log stdout too?
        logging.error('Command failed: returncode=%d command=%r stderr=%r',
                      pipe.returncode, cmd, errput)
        raise subprocess.CalledProcessError(pipe.returncode, args)
    return pipe.returncode # is EXIT_OK

def dump_cm_to_s3(dump_args, pipe_args):

    #TODO: check if pipe is needed
    pipe = subprocess.Popen((dump_args), stdout=subprocess.PIPE)
    output = subprocess.check_output((pipe_args), stdin=pipe.stdout)
    #TODO: Should use communicate instead of wait, wait will deadlock the Thread.
    pipe.wait()

### note: this runs synchronously. within the current Twisted environment,
### it is called from ._get_match() which is run on a thread so it won't
### block the Twisted main loop.
def svn_info(svnbin, env, path):
    "Run 'svn info' on the target path, returning a dict of info data."
    args = [svnbin, "info", "--non-interactive", "--", path]
    output = svnpubsub.util.check_output(args, env=env).strip()
    info = { }
    for line in output.split('\n'):
        idx = line.index(':')
        info[line[:idx]] = line[idx+1:].strip()
    return info

try:
    import glob
    glob.iglob
    def is_emptydir(path):
        # ### If the directory contains only dotfile children, this will readdir()
        # ### the entire directory.  But os.readdir() is not exposed to us...
        for x in glob.iglob('%s/*' % path):
            return False
        for x in glob.iglob('%s/.*' % path):
            return False
        return True
except (ImportError, AttributeError):
    # Python ≤2.4
    def is_emptydir(path):
        # This will read the entire directory list to memory.
        return not os.listdir(path)

class Commit(object):
    
    def __init__(self, bdec):
        self.loggEverything()
    
    def loggEverything(self):
        logging.info("Watching Commit at %s" % (self))

PRODUCTION_RE_FILTER = re.compile("/websites/production/[^/]+/")

class BigDoEverythingClasss(object):
    def __init__(self, config):
        self.svnbin = config.get_value('svnbin')
        self.env = config.get_env()
        self.tracking = config.get_track()
        self.hook = config.get_optional_value('hook')
        self.streams = config.get_value('streams').split()
        self.worker = BackgroundWorker(self.svnbin, self.env, self.hook)
        self.watch = [ ]

    def start(self):
        for path, url in self.tracking.items():
            # working copies auto-register with the BDEC when they are ready.
            WorkingCopy(self, path, url)

    def wc_ready(self, wc):
        # called when a working copy object has its basic info/url,
        # Add it to our watchers, and trigger an svn update.
        logging.info("Watching WC at %s <-> %s" % (wc.path, wc.url))
        self.watch.append(wc)
        self.worker.add_work(OP_BOOT, wc)

    def _normalize_path(self, path):
        if path[0] != '/':
            return "/" + path
        return posixpath.abspath(path)

    def commit(self, url, commit):
        if commit.type != 'svn' or commit.format != 1:
            logging.info("SKIP unknown commit format (%s.%d)",
                         commit.type, commit.format)
            return
        logging.info("COMMIT r%d (%d paths) from %s"
                     % (commit.id, len(commit.changed), url))
        
        
        rev = '-r%s:%s'% (commit.id, commit.id)
        svn_root = '/srv/cms/svn'
        path = '%s/%s' % (svn_root, commit.repositoryname)        
        svn_args = [SVNADMIN, 'dump', '--incremental', '--deltas', path, rev]
        
        #TODO: Path should look like:
        #/v1/Cloudid/reponame/shardX/0000001000/reponame-0000001000.svndump.gz
        s3_uri = 's3://cms-review-jandersson/dumps/'
        op, from_rev = decide_OP(s3_uri, commit.id)
        
        svn_root = '/srv/cms/svn'
        path = '%s/%s' % (svn_root, commit.repositoryname)
        
        #TODO: These should be done with the Operation stack, we find out what Operations that should be made and add them.
        while from_rev <= commit.id: 
            s3_uri_dump_key = '%s%s-%s.dump' % (s3_uri, commit.repositoryname, from_rev)
            logging.info('s3 key will be: %s' % s3_uri_dump_key)
            aws_args = [AWS, 's3', 'cp', '-', s3_uri_dump_key]
           
            rev = '-r%s:%s'% (from_rev, from_rev)
            svn_args = [SVNADMIN, 'dump', '--incremental', '--deltas', path, rev]
            logging.info('Dumping rev: %s in repo %s' % (rev, path))
            dump_cm_to_s3(svn_args, aws_args)
            from_rev = from_rev + 1
                
# Start logging warnings if the work backlog reaches this many items
BACKLOG_TOO_HIGH = 20
OP_BOOT = 'boot'
OP_UPDATE = 'update'
OP_CLEANUP = 'cleanup'

class BackgroundWorker(threading.Thread):
    def __init__(self, svnbin, env, hook):
        threading.Thread.__init__(self)

        # The main thread/process should not wait for this thread to exit.
        ### compat with Python 2.5
        self.setDaemon(True)

        self.svnbin = svnbin
        self.env = env
        self.hook = hook
        self.q = Queue.Queue()

        self.has_started = False

    def run(self):
        while True:
            # This will block until something arrives
            operation, wc = self.q.get()

            # Warn if the queue is too long.
            # (Note: the other thread might have added entries to self.q
            # after the .get() and before the .qsize().)
            qsize = self.q.qsize()+1
            if operation != OP_BOOT and qsize > BACKLOG_TOO_HIGH:
                logging.warn('worker backlog is at %d', qsize)

            try:
                if operation == OP_UPDATE:
                    self._update(wc)
                elif operation == OP_BOOT:
                    self._update(wc, boot=True)
                elif operation == OP_CLEANUP:
                    self._cleanup(wc)
                else:
                    logging.critical('unknown operation: %s', operation)
            except:
                logging.exception('exception in worker')

            # In case we ever want to .join() against the work queue
            self.q.task_done()

    def add_work(self, operation, wc):
        # Start the thread when work first arrives. Thread-start needs to
        # be delayed in case the process forks itself to become a daemon.
        if not self.has_started:
            self.start()
            self.has_started = True

        self.q.put((operation, wc))

    def _update(self, wc, boot=False):
        "Update the specified working copy."

        # For giggles, let's clean up the working copy in case something
        # happened earlier.
        self._cleanup(wc)

        logging.info("updating: %s", wc.path)

        ## Run the hook
        HEAD = svn_info(self.svnbin, self.env, wc.url)['Revision']
        if self.hook:
            hook_mode = ['pre-update', 'pre-boot'][boot]
            logging.info('running hook: %s at %s',
                         wc.path, hook_mode)
            args = [self.hook, hook_mode, wc.path, HEAD, wc.url]
            rc = check_call(args, env=self.env, __okayexits=[0, 1])
            if rc == 1:
                # TODO: log stderr
                logging.warn('hook denied update of %s at %s',
                             wc.path, hook_mode)
                return
            del rc

        ### we need to move some of these args into the config. these are
        ### still specific to the ASF setup.
        args = [self.svnbin, 'switch',
                '--quiet',
                '--non-interactive',
                '--trust-server-cert',
                '--ignore-externals',
                '--config-option',
                'config:miscellany:use-commit-times=on',
                '--',
                wc.url + '@' + HEAD,
                wc.path]
        check_call(args, env=self.env)

        ### check the loglevel before running 'svn info'?
        info = svn_info(self.svnbin, self.env, wc.path)
        assert info['Revision'] == HEAD
        logging.info("updated: %s now at r%s", wc.path, info['Revision'])

        ## Run the hook
        if self.hook:
            hook_mode = ['post-update', 'boot'][boot]
            logging.info('running hook: %s at revision %s due to %s',
                         wc.path, info['Revision'], hook_mode)
            args = [self.hook, hook_mode,
                    wc.path, info['Revision'], wc.url]
            check_call(args, env=self.env)

    def _cleanup(self, wc):
        "Run a cleanup on the specified working copy."

        ### we need to move some of these args into the config. these are
        ### still specific to the ASF setup.
        args = [self.svnbin, 'cleanup',
                '--non-interactive',
                '--trust-server-cert',
                '--config-option',
                'config:miscellany:use-commit-times=on',
                wc.path]
        check_call(args, env=self.env)


class ReloadableConfig(ConfigParser.SafeConfigParser):
    def __init__(self, fname):
        ConfigParser.SafeConfigParser.__init__(self)

        self.fname = fname
        self.read(fname)

        ### install a signal handler to set SHOULD_RELOAD. BDEC should
        ### poll this flag, and then adjust its internal structures after
        ### the reload.
        self.should_reload = False

    def reload(self):
        # Delete everything. Just re-reading would overlay, and would not
        # remove sections/options. Note that [DEFAULT] will not be removed.
        for section in self.sections():
            self.remove_section(section)

        # Now re-read the configuration file.
        self.read(fname)

    def get_value(self, which):
        return self.get(ConfigParser.DEFAULTSECT, which)

    def get_optional_value(self, which, default=None):
        if self.has_option(ConfigParser.DEFAULTSECT, which):
            return self.get(ConfigParser.DEFAULTSECT, which)
        else:
            return default

    def get_env(self):
        env = os.environ.copy()
        default_options = self.defaults().keys()
        for name, value in self.items('env'):
            if name not in default_options:
                env[name] = value
        return env

    def get_track(self):
        "Return the {PATH: URL} dictionary of working copies to track."
        track = dict(self.items('track'))
        for name in self.defaults().keys():
            del track[name]
        return track

    def optionxform(self, option):
        # Do not lowercase the option name.
        return str(option)


class Daemon(daemonize.Daemon):
    def __init__(self, logfile, pidfile, umask, bdec):
        daemonize.Daemon.__init__(self, logfile, pidfile)

        self.umask = umask
        self.bdec = bdec

    def setup(self):
        # There is no setup which the parent needs to wait for.
        pass

    def run(self):
        logging.info('svnwcsub started, pid=%d', os.getpid())

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
    # Set up the logging, then process the rest of the options.
    prepare_logging(options.logfile)

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
        os.write(fd, '%d\n' % pid)
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
    parser.add_option('--umask',
                      help='set this (octal) umask before running')
    parser.add_option('--daemon', action='store_true',
                      help='run as a background daemon')

    options, extra = parser.parse_args(args)

    if len(extra) != 1:
        parser.error('CONFIG_FILE is required')
    config_file = extra[0]

    if options.daemon and not options.logfile:
        parser.error('LOGFILE is required when running as a daemon')
    if options.daemon and not options.pidfile:
        parser.error('PIDFILE is required when running as a daemon')

    # Process any provided options.
    handle_options(options)

    c = ReloadableConfig(config_file)
    bdec = BigDoEverythingClasss(c)

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
