import json
import unittest
from unittest.mock import patch
from botocore.exceptions import ClientError
from svnpubsub.client import Commit
from svnsfnsub import Job, encode


class SvnSfnSubTestCase(unittest.TestCase):

    def test_encode_1_to_256_range(self):
        # Svnkit encoding as of Svnkit 1.8.4
        expected = "%01%02%03%04%05%06%07%08%09%0B%0E%0F%10%11%12%13%14%15%16%17%18%19%1A%1B%1C%1D%1E%1F%20!%23$%25&'"\
                   "()+,-.0123456789%3B=@ABCDEFGHIJKLMNOPQRSTUVWXYZ%5B%5D%5E_%60abcdefghijklmnopqrstuvwxyz%7B%7D~%7F"\
                   "%C2%80%C2%81%C2%82%C2%83%C2%84%C2%85%C2%86%C2%87%C2%88%C2%89%C2%8A%C2%8B%C2%8C%C2%8D%C2%8E%C2%8F"\
                   "%C2%90%C2%91%C2%92%C2%93%C2%94%C2%95%C2%96%C2%97%C2%98%C2%99%C2%9A%C2%9B%C2%9C%C2%9D%C2%9E%C2%9F"\
                   "%C2%A0%C2%A1%C2%A2%C2%A3%C2%A4%C2%A5%C2%A6%C2%A7%C2%A8%C2%A9%C2%AA%C2%AB%C2%AC%C2%AD%C2%AE%C2%AF"\
                   "%C2%B0%C2%B1%C2%B2%C2%B3%C2%B4%C2%B5%C2%B6%C2%B7%C2%B8%C2%B9%C2%BA%C2%BB%C2%BC%C2%BD%C2%BE%C2%BF"\
                   "%C3%80%C3%81%C3%82%C3%83%C3%84%C3%85%C3%86%C3%87%C3%88%C3%89%C3%8A%C3%8B%C3%8C%C3%8D%C3%8E%C3%8F"\
                   "%C3%90%C3%91%C3%92%C3%93%C3%94%C3%95%C3%96%C3%97%C3%98%C3%99%C3%9A%C3%9B%C3%9C%C3%9D%C3%9E%C3%9F"\
                   "%C3%A0%C3%A1%C3%A2%C3%A3%C3%A4%C3%A5%C3%A6%C3%A7%C3%A8%C3%A9%C3%AA%C3%AB%C3%AC%C3%AD%C3%AE%C3%AF"\
                   "%C3%B0%C3%B1%C3%B2%C3%B3%C3%B4%C3%B5%C3%B6%C3%B7%C3%B8%C3%B9%C3%BA%C3%BB%C3%BC%C3%BD%C3%BE%C3%BF"\
                   "%C4%80"
        invalid_characters = [
            # New line characters
            '\n', '\r', '\f',
            # Adapter invalid chars: :*?"<>|^+@#&
            ':', '*', '?', '\"', '<', '>', '\\', '/', '|'
        ]
        path = "".join([chr(i) for i in range(1, 256 + 1) if chr(i) not in invalid_characters])
        result = encode(path)
        self.assertEquals(result, expected)

    def test_encode_multibyte_characters(self):
        # Svnkit encoding as of Svnkit 1.10.4
        expected = "%CE%B1%CE%B2%CE%B3%E0%B5%A6%E0%B5%AA%E0%B5%AC%E0%B5%AE%F0%9F%98%80%F0%9F%98%B2%F0%9F%99%82%F0%9F"\
                   "%98%95%F0%9F%98%A0"
        path = "".join([
            # 2-bytes characters
            "αβγ",
            # 3-bytes characters
            "൦൪൬൮",
            # 4-bytes characters
            "😀😲🙂😕😠"
        ])
        result = encode(path)
        self.assertEquals(result, expected)


class SvnSfnSubJobTestCase(unittest.TestCase):

    def setUp(self):
        config = patch.multiple('svnsfnsub', CLOUDID='test', ACCOUNT='123456789012', DOMAIN='example.invalid')
        config.start()
        self.addCleanup(config.stop)
        sdk = patch('svnsfnsub.boto3')
        boto3 = sdk.start()
        self.addCleanup(sdk.stop)
        boto3.session.Session.return_value.region_name = 'eu-west-1'
        self.start_execution = boto3.client.return_value.start_execution
        self.start_execution.return_value = {'executionArn': 'test-execution'}
        delay = patch('svnsfnsub.sleep')
        self.sleep = delay.start()
        self.addCleanup(delay.stop)
        self.error = ClientError({'Error': {'Code': 'ThrottlingException', 'Message': 'Test failure'}}, 'StartExecution')

    def test_failed_paths_do_not_leak_into_the_next_job(self):
        first = Job(Commit({'repositoryname': 'alpha', 'id': 7, 'committer': 'alice',
                            'changed': {'old.xml': {'flags': 'U  '}}}))
        self.start_execution.side_effect = self.error
        with self.assertLogs(level='WARNING'):
            first.run()

        self.assertEqual(self.start_execution.call_count, 4)
        self.assertEqual(self.sleep.call_count, 3)
        self.assertEqual(first.failed, ['old.xml'])
        self.assertEqual(first.retrying, 3)
        for call in self.start_execution.call_args_list:
            self.assertEqual(json.loads(call.kwargs['input']), {
                'action': 'item-event', 'userid': 'alice',
                'itemid': 'x-svn://test.example.invalid/svn/alpha/old.xml?p=7'
            })

        self.start_execution.reset_mock()
        self.start_execution.side_effect = None
        second = Job(Commit({'repositoryname': 'beta', 'id': 9, 'committer': 'bob',
                             'changed': {'new.xml': {'flags': 'A  '}}}))
        second.run()

        self.start_execution.assert_called_once()
        self.assertEqual(self.start_execution.call_args.kwargs['stateMachineArn'],
                         'arn:aws:states:eu-west-1:123456789012:stateMachine:cms-test-event-v1')
        self.assertEqual(json.loads(self.start_execution.call_args.kwargs['input']), {
            'action': 'item-event', 'userid': 'bob',
            'itemid': 'x-svn://test.example.invalid/svn/beta/new.xml?p=9'
        })
        self.assertEqual(first.failed, ['old.xml'])
        self.assertEqual(second.failed, [])
        self.assertEqual(second.retrying, 0)

    def test_retries_only_failed_paths(self):
        # Eight failures expose skipped paths even after all three retry rounds.
        retry_paths = ['retry-{}.xml'.format(i) for i in range(8)]
        changed = {'ok.xml': {'flags': 'U  '}}
        changed.update({path: {'flags': 'A  '} for path in retry_paths})
        job = Job(Commit({'repositoryname': 'alpha', 'id': 7, 'committer': 'alice',
                          'changed': changed}))
        self.start_execution.side_effect = ([{'executionArn': 'ok'}]
                                            + [self.error] * len(retry_paths)
                                            + [{'executionArn': 'retry'}] * len(retry_paths))
        with self.assertLogs(level='WARNING'):
            job.run()

        self.assertEqual(job.failed, [])
        payloads = [json.loads(call.kwargs['input']) for call in self.start_execution.call_args_list]
        self.assertEqual(payloads, [
            {'action': 'item-event', 'userid': 'alice',
             'itemid': 'x-svn://test.example.invalid/svn/alpha/{}?p=7'.format(path)}
            for path in ['ok.xml'] + retry_paths + retry_paths
        ])
        self.assertEqual(job.retrying, 1)
        self.sleep.assert_called_once_with(30)


if __name__ == '__main__':
    unittest.main()
