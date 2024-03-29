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

#SVNLOOK="/usr/local/svn-install/current/bin/svnlook"
#SVNLOOK="/usr/local/bin/svnlook"
SVNLOOK="/usr/bin/svnlook"

HOST="127.0.0.1"
PORT=2069

import sys, base64
try:
    import simplejson as json
except ImportError:
    import json

import urllib.request, urllib.error, urllib.parse

import svnpubsub.util

def svnlook(cmd, **kwargs):
    args = [SVNLOOK] + cmd
    return svnpubsub.util.check_output(args, **kwargs)

def svnlook_uuid(repo):
    cmd = ["uuid", "--", repo]
    # Unifi parameters to all svnlook calls, ensures return type is string (got bytes on Ubuntu 22.04 / Python 3.10)
    data = svnlook(cmd, universal_newlines=True).split("\n")
    return data[0].strip()

def svnlook_info(repo, revision):
    cmd = ["info", "-r", revision, "--", repo]
    data = svnlook(cmd, universal_newlines=True).split("\n")
    #print data
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

def do_put(body):
    opener = urllib.request.build_opener(urllib.request.HTTPHandler)
    request = urllib.request.Request("http://%s:%d/commits" % (HOST, PORT), data=body)
    request.add_header('Content-Type', 'application/json')
    request.get_method = lambda: 'PUT'
    url = opener.open(request)

def do_post_commit_webapp(body):
    #Just a bogus auth, the webapp assumes user is already approved by apache.
    username = "postcommit"
    password = "password"
    base64Auth = base64.encodebytes(("%s:%s" % (username, password)).encode()).replace(b'\n', b'').decode()

    path = "cms/rest/hook/postcommit"
    port_webapp = 8080

    opener = urllib.request.build_opener(urllib.request.HTTPHandler)
    request = urllib.request.Request("http://%s:%d/%s" % (HOST, port_webapp, path), data=body)
    request.add_header('Content-Type', 'application/json')
    request.add_header("Authorization", "Basic %s" % base64Auth)
    request.add_header('X_AUTH_uid', username)
    request.get_method = lambda: 'PUT'
    url = opener.open(request)

def main(repo, revision):
    revision = revision.lstrip('r')
    i = svnlook_info(repo, revision)
    lastSlashIndex = repo.rindex("/")
    data = {'type': 'svn',
            'format': 1,
            'id': int(revision),
            'changed': {},
            'repository': svnlook_uuid(repo),
            'repositoryname': repo[lastSlashIndex + 1:],
            'committer': i['author'],
            'log': i['log'],
            'date': i['date'],
            }
    data['changed'].update(svnlook_changed(repo, revision))
    body = str.encode(json.dumps(data))
    do_put(body)
    # Replaced by svnsfnsub
    #do_post_commit_webapp(body)

if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        sys.stderr.write("invalid args\n")
        sys.exit(1)

    main(*sys.argv[1:3])
