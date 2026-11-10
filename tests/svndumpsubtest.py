import os
import gzip
import shutil
import unittest
from unittest import mock

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-1')  # boto3.client() is created at import time
import svndumpsub


class FakeS3(object):
    """Stores what upload_fileobj() streams, like S3 does even when the producer fails later."""

    def __init__(self, fail_upload=False):
        self.objects = {}
        self.deleted = []
        self.fail_upload = fail_upload

    def upload_fileobj(self, fileobj, bucket, key):
        if self.fail_upload:
            fileobj.read(1)
            raise RuntimeError('Upload failed')
        self.objects[key] = fileobj.read()

    def delete_object(self, Bucket, Key):
        self.deleted.append(Key)
        self.objects.pop(Key, None)


class DumpZipUploadTest(unittest.TestCase):

    def setUp(self):
        self.s3 = FakeS3()
        patches = [
            mock.patch.object(svndumpsub, 's3client', self.s3),
            mock.patch.object(svndumpsub, 'BUCKET', 'bucket'),
            mock.patch.object(svndumpsub, 'CLOUDID', 'cloud'),
            mock.patch.object(svndumpsub, 'GZIP', shutil.which('gzip')),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.job = svndumpsub.Job(repo='repo', rev=5, head=5)
        self.key = self.job._get_key(5)

    def test_successful_dump_is_uploaded(self):
        self.job.dump_zip_upload(['sh', '-c', 'echo dump-content'], 5)
        self.assertEqual(gzip.decompress(self.s3.objects[self.key]), b'dump-content\n')
        self.assertEqual(self.s3.deleted, [])

    def test_failed_dump_does_not_leave_a_partial_object(self):
        """svnadmin dump writes its header before failing; that partial output must not stay in S3."""
        with self.assertRaisesRegex(Exception, 'Dumping shard failed'):
            self.job.dump_zip_upload(['sh', '-c', 'echo SVN-fs-dump-format-version: 2; exit 1'], 5)
        self.assertEqual(self.s3.deleted, [self.key])
        self.assertNotIn(self.key, self.s3.objects)

    def test_failed_upload_stops_the_processes(self):
        self.s3.fail_upload = True
        processes = []
        real_popen = svndumpsub.subprocess.Popen

        def popen(*args, **kwargs):
            processes.append(real_popen(*args, **kwargs))
            return processes[-1]

        with mock.patch.object(svndumpsub.subprocess, 'Popen', side_effect=popen):
            with self.assertRaisesRegex(RuntimeError, 'Upload failed'):
                # Produces far more than a pipe buffer, so it would block forever if nobody stopped it.
                self.job.dump_zip_upload(['sh', '-c', 'yes dump-content | head -c 50000000'], 5)
        self.assertEqual(len(processes), 2)
        for process in processes:
            self.assertIsNotNone(process.returncode, '%s is still running' % process.args)


if __name__ == '__main__':
    unittest.main()
