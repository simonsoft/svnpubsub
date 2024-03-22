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
import io
import os
import sys
import logging
import subprocess
from select import select
from threading import Thread
from shutil import copyfileobj
from subprocess import Popen, PIPE, CalledProcessError

# check_output() is only available in Python 2.7. Allow us to run with
# earlier versions
try:
    def check_output(args, env=None, universal_newlines=False):
        return subprocess.check_output(args, shell=False, env=env, universal_newlines=universal_newlines)
except AttributeError:
    def check_output(args, env=None, universal_newlines=False):
        # note: we only use these three args
        pipe = Popen(args, shell=False, env=env, stdout=PIPE, universal_newlines=universal_newlines)
        output, _ = pipe.communicate()
        if pipe.returncode:
            raise CalledProcessError(pipe.returncode, args)
        return output


def is_file_like(variable) -> bool:
    try:
        return hasattr(variable, 'fileno')
    except AttributeError:
        return False


def execute(*args, text=True, env=None, throw=True, stdin=None, stdout=None, stderr=None):
    rlist = []
    wlist = []
    xlist = []
    process = None
    arguments = [*args]
    chunk_size = 1 * 1024 * 1024    # 1 MB
    stdout_buffer = None if stdout is not None else [] if text else bytearray()
    stderr_buffer = None if stderr is not None else [] if text else bytearray()
    stdin_writer = None

    if stdin is not None and not is_file_like(stdin):
        raise ValueError("stdin must be a file-like object")
    if stdout is not None and not is_file_like(stdout):
        raise ValueError("stdout must be a file-like object")
    if stderr is not None and not is_file_like(stderr):
        raise ValueError("stderr must be a file-like object")

    logging.debug("Running: %s", " ".join(arguments))

    def write(source, destination):
        """
        Copy the source file-like object to the destination file-like object and close
        the destination file-like object.
        @param source: A source file-like object typically the source of the stdin
        @param destination: A destination file-like object typically the stdin stream of the process
        """
        source.seek(0)
        copyfileobj(source, destination, chunk_size)
        destination.close()

    def read(p, f, o, e) -> (bool, bool):
        """
        Reads a chunk or a line of the available data and add them to the stdout and stderr buffers.
        @param p: The process from which to read the stdout or stderr data
        @param f: The file descriptor of the corresponding stdout or stderr object
        @param o: The stdout buffer to write to
        @param e: The stderr buffer to write to
        @return: A boolean tuple indicating whether there's more to read for either stdout or stderr
        """
        more_o = False
        more_e = False
        if f == p.stdout.fileno():
            if stdout is None:
                if text:
                    line = p.stdout.readline()
                    if line:
                        o.append(line.rstrip())
                        more_o = True
                else:
                    chunk = p.stdout.read(chunk_size)
                    if chunk:
                        o += chunk
                        more_o = True
            else:
                chunk = p.stdout.read(chunk_size)
                if chunk:
                    stdout.write(chunk)
                    more_o = True
        if f == p.stderr.fileno():
            if stderr is None:
                if text:
                    line = p.stderr.readline()
                    if line:
                        e.append(line.rstrip())
                        more_e = True
                else:
                    chunk = p.stderr.read(chunk_size)
                    if chunk:
                        e += chunk
                        more_e = True
            else:
                chunk = p.stderr.read(chunk_size)
                if chunk:
                    stderr.write(chunk)
                    more_e = True

        return more_o, more_e

    try:
        process = Popen(arguments, text=text, universal_newlines=text, env=env,
                        stdin=PIPE if stdin is not None else None, stdout=PIPE, stderr=PIPE)

        if stdin is not None and process.poll() is None:
            # If stdin was supplied, copy its contents to the stdin stream of the process in a separate thread
            stdin_writer = Thread(target=write, args=[stdin, process.stdin])
            stdin_writer.start()
        while process.poll() is None:
            rlist += [process.stdout.fileno(), process.stderr.fileno()]
            for fd in [item for sublist in select(rlist, wlist, xlist) for item in sublist]:
                while True:
                    more_stdout, more_stderr = read(process, fd, stdout_buffer, stderr_buffer)
                    if not more_stdout and not more_stderr:
                        break
        if process.returncode and throw:
            raise subprocess.CalledProcessError(process.returncode, process.args, process.stdout, process.stderr)
    except Exception:
        _, value, traceback = sys.exc_info()
        raise RuntimeError(os.linesep.join(stderr_buffer) if text else stderr_buffer.decode('utf-8')).with_traceback(traceback)
    if stdin_writer is not None and stdin_writer.is_alive():
        raise ChildProcessError("The child stdin writer process did not terminate successfully")
    return (process,
            None if stdout is not None else os.linesep.join(stdout_buffer) if text else bytes(stdout_buffer),
            None if stderr is not None else os.linesep.join(stderr_buffer) if text else bytes(stderr_buffer))
