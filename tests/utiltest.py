import os
import unittest
from svnpubsub.util import execute


class ExecuteTest(unittest.TestCase):

    def test_returns_stripped_text_output(self):
        process, stdout, stderr = execute('sh', '-c', 'printf "a  \\nb\\n"; echo warn >&2')
        self.assertEqual(process.returncode, 0)
        self.assertEqual(stdout, 'a' + os.linesep + 'b')
        self.assertEqual(stderr, 'warn')

    def test_returns_bytes_when_text_is_false(self):
        _, stdout, _ = execute('printf', 'abc', text=False)
        self.assertEqual(stdout, b'abc')

    def test_raises_on_non_zero_exit_with_stderr(self):
        with self.assertRaises(RuntimeError) as context:
            execute('sh', '-c', 'echo boom >&2; exit 3')
        self.assertEqual(str(context.exception), 'boom')
        self.assertEqual(context.exception.__cause__.returncode, 3)

    def test_raises_on_non_zero_exit_when_text_is_false(self):
        with self.assertRaises(RuntimeError) as context:
            execute('sh', '-c', 'echo boom >&2; exit 1', text=False)
        self.assertEqual(str(context.exception), 'boom')

    def test_raises_on_non_zero_exit_without_stderr(self):
        with self.assertRaises(RuntimeError) as context:
            execute('false')
        self.assertIn('non-zero exit status', str(context.exception))


if __name__ == '__main__':
    unittest.main()
