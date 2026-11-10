import threading
import unittest
from svnpubsub.bgworker import BackgroundJob, BackgroundWorker


class ConcreteJob(BackgroundJob):
    def validate(self):
        return True
    def run(self):
        pass


class RecursiveJob(BackgroundJob):
    """Only dumps a revision once its predecessor has been dumped."""

    dumped = set()
    done = threading.Event()

    def validate(self):
        # Rev 1 is the first revision; everything else needs rev - 1 dumped first.
        return self.rev == 1 or (self.repo, self.rev - 1) in RecursiveJob.dumped

    def run(self):
        RecursiveJob.dumped.add((self.repo, self.rev))
        if self.rev == 3:
            RecursiveJob.done.set()


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

    def test_recursive_worker_queues_missing_predecessors(self):
        """When rev - 1 is missing, the recursive worker must queue the predecessor
        and eventually process every revision instead of spinning on the same job."""
        RecursiveJob.dumped.clear()
        RecursiveJob.done.clear()
        worker = BackgroundWorker(recursive=True)
        worker.queue(RecursiveJob(repo='repo-a', rev=3, head=3))
        self.assertTrue(RecursiveJob.done.wait(timeout=5), 'Worker never processed r3')
        self.assertEqual(RecursiveJob.dumped, {('repo-a', 1), ('repo-a', 2), ('repo-a', 3)})


if __name__ == '__main__':
    unittest.main()
