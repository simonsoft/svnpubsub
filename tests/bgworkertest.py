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


class UnreachableStorageJob(BackgroundJob):
    """Every predecessor looks missing (as when every S3 request fails) and every dump fails."""

    validations = 0

    def validate(self):
        UnreachableStorageJob.validations += 1
        return self.rev == 0  # The shard boundary does not ask S3.

    def run(self):
        raise RuntimeError('Upload failed')


def wait_until_drained(worker, timeout=10):
    deadline = time.monotonic() + timeout
    while worker.q.unfinished_tasks and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.1)  # Give a runaway loop the chance to show itself.


class BackgroundWorkerTest(unittest.TestCase):

    def test_queue_two_jobs_same_rev(self):
        """Two jobs at the same revision must not raise a TypeError.
        heapq falls through to compare Job instances when rev is equal,
        which fails unless a tiebreaker is in the tuple."""
        worker = BackgroundWorker()
        # Keep the thread from starting, otherwise it may take job1 off the queue
        # before job2 is added and heapq never has to compare the two entries.
        worker.started = True
        job1 = ConcreteJob(repo='repo-a', rev=1, head=1)
        job2 = ConcreteJob(repo='repo-b', rev=1, head=1)
        worker.queue(job1)
        worker.queue(job2)  # would raise TypeError before the fix
        self.assertEqual(worker.q.qsize(), 2)
        queued = [entry[-1] for entry in worker.q.queue]
        self.assertCountEqual(queued, [job1, job2])

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
        wait_until_drained(worker)
        self.assertEqual(worker.q.qsize(), 0)
        # The initial attempt plus one per requeue.
        self.assertEqual(FailingPredecessorJob.validations, MAX_REQUEUES + 1)

    def test_recursive_worker_work_grows_linearly_with_missing_chain(self):
        """With every predecessor missing, each revision is retried a bounded number of times.
        Counting requeues per job object instead let the work grow as (MAX_REQUEUES + 1) ** depth."""
        depth = 50
        UnreachableStorageJob.validations = 0
        worker = BackgroundWorker(recursive=True)
        worker.queue(UnreachableStorageJob(repo='repo-a', rev=depth, head=depth))
        wait_until_drained(worker)
        self.assertEqual(worker.q.unfinished_tasks, 0, 'Worker never drained the queue')
        self.assertLessEqual(UnreachableStorageJob.validations, 2 * (MAX_REQUEUES + 1) * (depth + 1))
        self.assertEqual(worker.requeues, {})

    def test_requeue_counts_reset_when_the_queue_drains(self):
        """After a burst of work ends, the next commit gets a fresh set of attempts."""
        FailingPredecessorJob.validations = 0
        worker = BackgroundWorker(recursive=True)
        for _ in range(2):
            worker.queue(FailingPredecessorJob(repo='repo-a', rev=2, head=2))
            wait_until_drained(worker)
        self.assertEqual(FailingPredecessorJob.validations, 2 * (MAX_REQUEUES + 1))


if __name__ == '__main__':
    unittest.main()
