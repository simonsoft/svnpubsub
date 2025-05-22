#!/usr/bin/env python3
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

SVNLOOK="/usr/bin/svnlook"
HOST="127.0.0.1"
PORT=2069

import argparse

try:
    import simplejson as json
except ImportError:
    import json

import svnpubsub.util
import urllib.request, urllib.error, urllib.parse

def svnlook(cmd, **kwargs):
    args = [SVNLOOK] + cmd
    return svnpubsub.util.check_output(args, **kwargs)

def svnlook_uuid(repo):
    cmd = ["uuid", "--", repo]
    # Unifi parameters to all svnlook calls, ensures the return type is string (got bytes on Ubuntu 22.04 / Python 3.10)
    data = svnlook(cmd, universal_newlines=True).split("\n")
    return data[0].strip()

def svnlook_info(repo, revision):
    cmd = ["info", "-r", revision, "--", repo]
    data = svnlook(cmd, universal_newlines=True).split("\n")
    return {'author': data[0].strip(),
            'date': data[1].strip(),
            'log': "\n".join(data[3:]).strip()}

def svnlook_changed(repo, revision):
    cmd = ["changed", "-r", revision, "--", repo]
    lines = svnlook(cmd, universal_newlines=True).split("\n")
    changed = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        (flags, filename) = (line[0:3], line[4:])
        changed[filename] = {'flags': flags}
    return changed

def svnlook_youngest(repo):
    cmd = ["youngest", repo]
    data = svnlook(cmd, universal_newlines=True).split("\n")
    return data[0].strip()

def do_put(body):
    opener = urllib.request.build_opener(urllib.request.HTTPHandler)
    request = urllib.request.Request("http://%s:%d/commits" % (HOST, PORT), data=body)
    request.add_header('Content-Type', 'application/json')
    request.get_method = lambda: 'PUT'
    url = opener.open(request)

def do_process(repo, revision):
    print(f"{repo} (r{revision})...", end="")
    revision = revision.lstrip('r')
    i = svnlook_info(repo, revision)
    last_slash_index = repo.rindex("/")
    data = {
        'type': 'svn',
        'format': 1,
        'id': int(revision),
        'changed': {},
        'repository': svnlook_uuid(repo),
        'repositoryname': repo[last_slash_index + 1:],
        'committer': i['author'],
        'log': i['log'],
        'date': i['date'],
    }
    data['changed'].update(svnlook_changed(repo, revision))
    body = str.encode(json.dumps(data))
    do_put(body)
    print("OK")

def main(repo, from_rev, to_rev):
    for i in range(from_rev, to_rev + 1):
        do_process(repo, str(i))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='An SvnPubSub client to replay previous commits of a repository.')

    parser.add_argument('--repo', required=True, help='the repository name')
    parser.add_argument('--from-rev', required=True, help='the revision to start from')
    parser.add_argument('--to-rev', help='the revision to end at (default: HEAD)')

    args = parser.parse_args()

    repo = args.repo
    from_rev = int(args.from_rev)
    to_rev = int(args.to_rev or svnlook_youngest(repo))

    if from_rev > to_rev:
        parser.error("--from-rev must be less than or equal to --to-rev")

    main(repo, from_rev, to_rev)


