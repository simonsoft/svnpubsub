import time
import threading
import unittest
from svnpubsub.bgworker import BackgroundJob, BackgroundWorker, MAX_REQUEUES


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


class FailingPredecessorJob(BackgroundJob):
    """r2 needs r1 dumped, but dumping r1 always fails."""

    validations = 0

    def validate(self):
        if self.rev == 2:
            FailingPredecessorJob.validations += 1
            return False
        return True

    def run(self):
        raise RuntimeError('Dump failed')


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

    def test_recursive_worker_gives_up_when_predecessor_keeps_failing(self):
        """A predecessor that always fails must not keep the worker requeueing forever."""
        FailingPredecessorJob.validations = 0
        worker = BackgroundWorker(recursive=True)
        worker.queue(FailingPredecessorJob(repo='repo-a', rev=2, head=2))
        deadline = time.monotonic() + 5
        while worker.q.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.1)  # Give a runaway loop the chance to show itself.
        self.assertEqual(worker.q.qsize(), 0)
        # The initial attempt plus one per requeue.
        self.assertEqual(FailingPredecessorJob.validations, MAX_REQUEUES + 1)


if __name__ == '__main__':
    unittest.main()
