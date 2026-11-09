import json
import unittest
from itertools import permutations
from unittest.mock import Mock, patch
from svnpubsub.client import Commit
from svnsqssub import Job


class SvnSqsSubTestCase(unittest.TestCase):

    def setUp(self):
        config = patch.multiple('svnsqssub', CLOUDID='test', SQS_MESSAGE_GROUP_ID='commit', SQS_MESSAGE_ATTRIBUTES={})
        config.start()
        self.addCleanup(config.stop)
        sdk = patch('svnsqssub.boto3')
        boto3 = sdk.start()
        self.addCleanup(sdk.stop)
        self.sqs = boto3.resource.return_value
        self.commit = Commit({
            'type': 'svn', 'format': 1, 'repository': '12345678-1234-1234-1234-123456789012',
            'repositoryname': 'demo', 'id': 42, 'committer': 'alice',
            'date': '2026-11-03T12:00:00Z', 'log': 'Update documents and images',
            'changed': {'docs/a.png': {'flags': 'A  '}, 'docs/b.png': {'flags': 'U  '},
                        'docs/test.xml': {'flags': 'UU '}, 'docs/': {'flags': '_U '}}
        })

    def test_each_destination_filters_the_original_commit_regardless_of_order(self):
        configs = {
            'images': {'type': 'item', 'extensions': ['.png']},
            'xml': {'type': 'commit', 'extensions': ['.xml']},
            'all': {'type': 'commit'}
        }
        expected_paths = {
            'images': [['docs/a.png'], ['docs/b.png']],
            'xml': [['docs/test.xml']],
            'all': [list(self.commit.changed)]
        }
        original = json.loads(self.commit.dumps())
        for order in permutations(configs):
            with self.subTest(order=order):
                queues = {suffix: Mock() for suffix in order}
                self.sqs.get_queue_by_name.side_effect = lambda QueueName: queues[QueueName.removeprefix('cms-test-')]
                with patch('svnsqssub.SQS_QUEUES', {suffix: dict(configs[suffix]) for suffix in order}):
                    Job(self.commit).run()

                for suffix, paths_per_message in expected_paths.items():
                    expected = [dict(original, changed={path: original['changed'][path] for path in paths})
                                for paths in paths_per_message]
                    messages = [call.kwargs for call in queues[suffix].send_message.call_args_list]
                    self.assertEqual([json.loads(message['MessageBody']) for message in messages], expected)
                    for message in messages:
                        self.assertEqual(set(message), {'MessageBody', 'MessageAttributes'})
                        self.assertEqual(message['MessageAttributes'], {})
                self.assertEqual(json.loads(self.commit.dumps()), original)

    def test_empty_filter_result_does_not_affect_later_destinations(self):
        queue = self.sqs.get_queue_by_name.return_value
        with patch('svnsqssub.SQS_QUEUES', {
            'jpeg': {'type': 'item', 'extensions': ['.jpg']},
            'all': {'type': 'commit'}
        }):
            Job(self.commit).run()

        self.sqs.get_queue_by_name.assert_called_once_with(QueueName='cms-test-all')
        queue.send_message.assert_called_once_with(MessageBody=self.commit.dumps(), MessageAttributes={})

    def test_fifo_commit_keeps_message_metadata(self):
        queue = self.sqs.get_queue_by_name.return_value
        attributes = {'source': {'DataType': 'String', 'StringValue': 'svn'}}
        with patch('svnsqssub.SQS_QUEUES', {'events.fifo': {'type': 'commit'}}), \
                patch('svnsqssub.SQS_MESSAGE_ATTRIBUTES', attributes):
            Job(self.commit).run()

        self.sqs.get_queue_by_name.assert_called_once_with(QueueName='cms-test-events.fifo')
        queue.send_message.assert_called_once_with(
            MessageBody=self.commit.dumps(), MessageAttributes=attributes,
            MessageGroupId='commit', MessageDeduplicationId='demo/r42'
        )


if __name__ == '__main__':
    unittest.main()
