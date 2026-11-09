Installation instructions:

1. Set up an svnpubsub service.

   This directory should be checked out to /usr/local/svnpubsub (or /opt/svnpubsub
   on Debian).

   There are init scripts for several OSes in the rc.d/ directory; add them
   to your OS boot process in the usual way for your OS.  (For example, via
   rc.conf(5) or update-rc.d(8).)

2. Run "commit-hook.py $REPOS $REV" from your post-commit hook.

   (As of 1.7, these are the same ordered arguments the post-commmit hook
   itself receives, so you can just symlink commit-hook.py as hooks/post-commit
   hook if you don't need any other hooks to run in the server process.  (This
   isn't as insane as it sounds --- post-commit email hooks could also feed of
   svnpubsub, and thus not be run within the committing server thread, but on
   any other process or box that listens to the svnpubsub stream!))

3. Set up svnpubsub clients.

   (eg svnwcsub.py, svnpubsub/client.py,
       'curl -sN http://${hostname}:2069/commits')

AWS subscriber state isolation:

The Step Functions subscriber previously shared its failed-path list between
jobs. After a job exhausted its retries, the next job could send an old path
with the new job's repository, revision and author, skipping its own paths.
Each Job now owns its failed-path list and retry counter. The existing payload
format, retry limit and delay are unchanged.

Retry rounds also iterate a snapshot of the failed paths. Previously, removing
successful paths from the list being iterated could skip other paths and exhaust
the retry limit without giving every failed path a retry.

The SQS subscriber previously reused one mutable commit copy across all
destinations. Filtering one destination could remove paths needed by another,
making delivery depend on destination order. Each destination now filters a
fresh copy of the original commit. Extension matching, commit/item message
formats and FIFO commit metadata are unchanged.

Unit tests:

Use Python 3.10 or 3.11 with boto3 installed. The existing subscriber client
imports asyncore/asynchat, which are unavailable in Python 3.12 and later.

    python -m unittest discover -p '*test.py' -v

The AWS subscriber tests replace SDK clients with mocks and require no AWS
credentials or service access. They cover cross-job failure isolation, retries
of eight failed paths without resending an initially successful path,
all six orders of three SQS destinations, empty filter results and FIFO commit
metadata, including the original commit contents and author/revision fields.
