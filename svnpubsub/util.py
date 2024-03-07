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
from subprocess import CalledProcessError

# check_output() is only available in Python 2.7. Allow us to run with
# earlier versions
try:
    __check_output = subprocess.check_output
    def check_output(args, env=None, universal_newlines=False):
        return __check_output(args, shell=False, env=env,
                              universal_newlines=universal_newlines)
except AttributeError:
    def check_output(args, env=None, universal_newlines=False):
        # note: we only use these three args
        pipe = subprocess.Popen(args, shell=False, env=env,
                                stdout=subprocess.PIPE,
                                universal_newlines=universal_newlines)
        output, _ = pipe.communicate()
        if pipe.returncode:
            raise subprocess.CalledProcessError(pipe.returncode, args)
        return output


def execute(*args, text=True, env=None, stdin=None, throw=True):
    process = None
    arguments = [*args]
    stdout = [] if text else bytes()
    stderr = [] if text else bytes()

    logging.debug("Running: %s", " ".join(arguments))

    if hasattr(stdin, 'fileno'):
        stdin = stdin.read()

    try:
        process = subprocess.Popen(arguments, text=text, universal_newlines=text, env=env,
                                   stdin=subprocess.PIPE if stdin is not None else None,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if text:
            stdout_data, stderr_data = process.communicate(input=stdin)
            for line in stdout_data.splitlines():
                stdout.append(line.rstrip())
            logging.debug(os.linesep.join(stdout))
            for line in stderr_data.splitlines():
                stderr.append(line.rstrip())
        else:
            stdout, stderr = process.communicate(input=stdin)
        if process.returncode and throw:
            raise CalledProcessError(process.returncode, process.args, process.stdout, process.stderr)
    except Exception:
        _, value, traceback = sys.exc_info()
        raise RuntimeError(os.linesep.join(stderr) if text else stderr.decode('utf-8')).with_traceback(traceback)
    return process, os.linesep.join(stdout) if text else stdout, os.linesep.join(stderr) if text else stderr
