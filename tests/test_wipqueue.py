# -*- coding: utf-8 -*-

import contextlib
import time

from tests import RQTestCase
from rq import Queue
from rq.job import Job
from rq.queue import WIPQueue
from tests.fixtures import Number, div_by_zero, say_hello, do_nothing


@contextlib.contextmanager
def mock_time(timestamp):
    """A simple context manager for mocking time.time() useful for traveling
    immediately in the future or in the past in unit tests.

      >>> t0 = time.time()
      >>> with mock_time(t0 + 5.0):
      >>>   t1 = time.time()
      >>> t1 - t0
      5.0
    """
    def my_time():
        return timestamp
    sv_time = time.time
    time.time = my_time
    try:
        yield timestamp
    finally:
        time.time = sv_time


class TestWIPQueue(RQTestCase):
    def setUp(self):
        super(TestWIPQueue, self).setUp()
        self.q = Queue()
        self.wq = self.q.wip_queue

    def test_create_queue(self):
        """Each queue has its associated WIP Queue"""
        self.assertTrue(isinstance(self.wq, WIPQueue))

    def test_default_wq_name(self):
        """We have  default WIP queue too"""
        self.assertEqual(self.wq.name, 'default')
        self.assertEqual(self.wq.key, 'rq:wipqueue:default')

    def test_queue_mgmt(self):
        """WIP queue simple management"""
        job = Job.create(say_hello, timeout=5)
        job.save()
        self.wq.add_job(job)

        # We got exactly one element in our set
        self.assertEqual(len(self.testconn.zrange(self.wq.key, 0, -1)), 1)

        # Adding twice the same job does not add anything
        self.wq.add_job(job)
        self.assertEqual(len(self.testconn.zrange(self.wq.key, 0, -1)), 1)

        # Adding another job makes two jobs in the wip queue
        job = Job.create(do_nothing, timeout=4)
        job.save()
        self.wq.add_job(job)
        all_jobs = self.testconn.zrange(self.wq.key, 0, -1)

        # We got two running jobs
        self.assertEqual(len(all_jobs), 2)

        # We will now get the jobs and execute them by order
        job0 = Job.fetch(all_jobs[0], connection=self.testconn)
        job1 = Job.fetch(all_jobs[1], connection=self.testconn)

        self.assertEqual(job0, job)  # Same operation
        self.assertNotEqual(job1, job)

        # Removing jobs from WIP queue with job id
        self.wq.remove_job(all_jobs[0])  # job ID of job0
        self.assertEqual(self.testconn.zcount(self.wq.key, '-inf', '+inf'), 1)
        expect_jobid = all_jobs[1]
        all_jobs = self.testconn.zrange(self.wq.key, 0, -1)
        self.assertEqual(len(all_jobs), 1)
        self.assertEqual(all_jobs[0], expect_jobid)

        # Removing expired jobs

        # Nothing expires within 2 next seconds
        with mock_time(time.time() + 2.0):
            self.wq.remove_expired_jobs()

        all_jobs = self.testconn.zrange(self.wq.key, 0, -1)
        self.assertEqual(len(all_jobs), 1)

        # But everything expires after 10 seconds
        with mock_time(time.time() + 10.0):
            self.wq.remove_expired_jobs()

        all_jobs = self.testconn.zrange(self.wq.key, 0, -1)
        self.assertEqual(len(all_jobs), 0)
        return





