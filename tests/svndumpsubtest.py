import os
import gzip
import shutil
import unittest
from unittest import mock

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-1')  # boto3.client() is created at import time
import svndumpsub


class FakeS3(object):
    """Stores whatever upload_fileobj() reads, like S3 does. Deleting is not allowed for the CMS role."""

    def __init__(self, fail_upload=False):
        self.objects = {}
        self.fail_upload = fail_upload

    def upload_fileobj(self, fileobj, bucket, key):
        if self.fail_upload:
            raise RuntimeError('Upload failed')
        self.objects[key] = fileobj.read()

    def delete_object(self, Bucket, Key):
        raise AssertionError('The CMS role has no s3:DeleteObject permission on the backup bucket')


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

    def test_large_dump_is_uploaded_intact(self):
        """More than a pipe buffer, so svnadmin and gzip must run concurrently with the temp file."""
        self.job.dump_zip_upload(['sh', '-c', 'yes dump-content | head -c 5000000'], 5)
        data = gzip.decompress(self.s3.objects[self.key])
        self.assertEqual(len(data), 5000000)
        self.assertTrue(data.startswith(b'dump-content\n'))

    def test_failed_dump_is_not_uploaded(self):
        """svnadmin dump writes its header before failing; that partial output must never reach S3."""
        with self.assertRaisesRegex(Exception, 'Dumping shard failed'):
            self.job.dump_zip_upload(['sh', '-c', 'echo SVN-fs-dump-format-version: 2; exit 1'], 5)
        self.assertEqual(self.s3.objects, {})

    def test_failed_compression_is_not_uploaded(self):
        with mock.patch.object(svndumpsub, 'GZIP', shutil.which('false')):
            with self.assertRaisesRegex(Exception, 'Compressing shard failed'):
                self.job.dump_zip_upload(['sh', '-c', 'echo dump-content'], 5)
        self.assertEqual(self.s3.objects, {})

    def test_failed_upload_raises_after_both_processes_finished(self):
        self.s3.fail_upload = True
        processes = []
        real_popen = svndumpsub.subprocess.Popen

        def popen(*args, **kwargs):
            processes.append(real_popen(*args, **kwargs))
            return processes[-1]

        with mock.patch.object(svndumpsub.subprocess, 'Popen', side_effect=popen):
            with self.assertRaisesRegex(RuntimeError, 'Upload failed'):
                self.job.dump_zip_upload(['sh', '-c', 'echo dump-content'], 5)
        self.assertEqual(len(processes), 2)
        for process in processes:
            self.assertEqual(process.returncode, 0, '%s did not finish cleanly' % process.args)


if __name__ == '__main__':
    unittest.main()
