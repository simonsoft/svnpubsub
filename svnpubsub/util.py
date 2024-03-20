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


def execute(*args, text=True, env=None, throw=True, stdin=None):
    process = None
    arguments = [*args]
    rlist = wlist = xlist = []
    chunk_size = 1 * 1024 * 1024    # 1 MB
    stdout_buffer = [] if text else bytes()
    stderr_buffer = [] if text else bytes()
    stdin_writer = None

    logging.debug("Running: %s", " ".join(arguments))

    def writer(source, destination):
        """
        Copy the source file-like object to the destination file-like object and close
        the destination file-like object.
        @param source: A source file-like object typically the source of the stdin
        @param destination: A destination file-like object typically the stdin stream of the process
        """
        source.seek(0)
        while True:
            data = source.read(chunk_size)
            if not data:
                break
            destination.write(data)
            destination.flush()
        destination.close()

    try:
        process = Popen(arguments, text=text, universal_newlines=text, env=env,
                        stdin=PIPE if stdin is not None else None, stdout=PIPE, stderr=PIPE)
        if stdin is not None and process.poll() is None:
            # If stdin was supplied, copy its contents to the stdin stream of the process in a separate thread
            stdin_writer = Thread(target=writer, args=[stdin, process.stdin])
            stdin_writer.start()
        while process.poll() is None:
            rlist += [process.stdout.fileno(), process.stderr.fileno()]
            for fd in [item for sublist in select(rlist, wlist, xlist) for item in sublist]:
                if fd == process.stdout.fileno():
                    if text:
                        line = process.stdout.readline()
                        stdout_buffer.append(line.rstrip())
                    else:
                        chunk = process.stdout.read(chunk_size)
                        if chunk:
                            stdout_buffer += chunk
                if fd == process.stderr.fileno():
                    if text:
                        line = process.stderr.readline()
                        stderr_buffer.append(line.rstrip())
                    else:
                        chunk = process.stderr.read(chunk_size)
                        if chunk:
                            stdout_buffer += chunk
        if process.returncode and throw:
            raise subprocess.CalledProcessError(process.returncode, process.args, process.stdout, process.stderr)
    except Exception:
        _, value, traceback = sys.exc_info()
        raise RuntimeError(os.linesep.join(stderr_buffer) if text else stderr_buffer.decode('utf-8')).with_traceback(traceback)
    if stdin_writer is not None and stdin_writer.is_alive():
        raise ChildProcessError("The child stdin writer process did not terminate successfully")
    return process, os.linesep.join(stdout_buffer) if text else stdout_buffer, os.linesep.join(stderr_buffer) if text else stderr_buffer
