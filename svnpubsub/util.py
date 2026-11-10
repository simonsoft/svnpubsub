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

import os
import logging
import subprocess as __subprocess

# check_output() is only available in Python 2.7. Allow us to run with
# earlier versions
try:
    __check_output = __subprocess.check_output
    def check_output(args, env=None, universal_newlines=False):
        return __check_output(args, shell=False, env=env,
                              universal_newlines=universal_newlines)
except AttributeError:
    def check_output(args, env=None, universal_newlines=False):
        # note: we only use these three args
        pipe = __subprocess.Popen(args, shell=False, env=env,
                                  stdout=__subprocess.PIPE,
                                  universal_newlines=universal_newlines)
        output, _ = pipe.communicate()
        if pipe.returncode:
            raise __subprocess.CalledProcessError(pipe.returncode, args)
        return output


def execute(*args, text=True):
    """Run a command and return (process, stdout, stderr).

    stdout and stderr are str (lines right-stripped, joined with os.linesep) when text is True,
    otherwise bytes. Raises RuntimeError with the command's stderr when it exits with a non-zero code.
    """
    arguments = [*args]

    logging.debug("Running: %s", " ".join(arguments))

    process = __subprocess.Popen(arguments, text=text, stdout=__subprocess.PIPE, stderr=__subprocess.PIPE)
    # communicate() waits for the process (so returncode is set) and drains both pipes concurrently.
    stdout, stderr = process.communicate()

    if text:
        stdout = os.linesep.join(line.rstrip() for line in stdout.splitlines())
        stderr = os.linesep.join(line.rstrip() for line in stderr.splitlines())
        logging.debug(stdout)

    if process.returncode:
        message = stderr if text else stderr.decode('utf-8', errors='replace').rstrip()
        error = __subprocess.CalledProcessError(process.returncode, process.args, stdout, stderr)
        raise RuntimeError(message or str(error)) from error

    return process, stdout, stderr
