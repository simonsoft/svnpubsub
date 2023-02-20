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
import sys
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
    stdout = []
    stderr = []
    process = None
    arguments = [*args]

    logging.debug("Running: %s", " ".join(arguments))

    try:
        process = __subprocess.Popen(arguments, text=text, universal_newlines=text,
                                     stdout=__subprocess.PIPE, stderr=__subprocess.PIPE)
        if text:
            for line in process.stdout.readlines():
                stdout.append(line.rstrip())
            for line in process.stderr.readlines():
                stderr.append(line.rstrip())
            logging.debug(os.linesep.join(stdout))
        if process.returncode:
            raise __subprocess.CalledProcessError(process.returncode, process.args, process.stdout, process.stderr)
    except Exception:
        _, value, traceback = sys.exc_info()
        if not text:
            stderr.extend(line.decode('utf-8').rstrip() for line in process.stderr.readlines())
        raise RuntimeError(os.linesep.join(stderr)).with_traceback(traceback)
    return process, os.linesep.join(stdout) if text else process.stdout.read(), os.linesep.join(stderr) if text else process.stderr.read()
