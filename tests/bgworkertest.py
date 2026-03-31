import unittest
from svnpubsub.bgworker import BackgroundJob, BackgroundWorker


class ConcreteJob(BackgroundJob):
    def validate(self):
        return True
    def run(self):
        pass


class BackgroundWorkerTest(unittest.TestCase):

    def test_queue_two_jobs_same_rev(self):
        """Two jobs at the same revision must not raise a TypeError.
        heapq falls through to compare Job instances when rev is equal,
        which fails unless a tiebreaker is in the tuple."""
        worker = BackgroundWorker()
        job1 = ConcreteJob(repo='repo-a', rev=1, head=1)
        job2 = ConcreteJob(repo='repo-b', rev=1, head=1)
        worker.queue(job1)
        worker.queue(job2)  # would raise TypeError before the fix


if __name__ == '__main__':
    unittest.main()
