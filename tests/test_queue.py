# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from rq import Queue, get_failed_queue, release_job
from rq.exceptions import InvalidJobOperationError
from rq.job import Job, Status
from rq.worker import Worker

from tests import RQTestCase
from tests.fixtures import (div_by_zero, echo, Number, say_hello,
                            some_calculation)


class CustomJob(Job):
    pass


class TestQueue(RQTestCase):
    def test_create_queue(self):
        """Creating queues."""
        q = Queue('my-queue')
        self.assertEqual(q.name, 'my-queue')

    def test_create_default_queue(self):
        """Instantiating the default queue."""
        q = Queue()
        self.assertEqual(q.name, 'default')

    def test_equality(self):
        """Mathematical equality of queues."""
        q1 = Queue('foo')
        q2 = Queue('foo')
        q3 = Queue('bar')

        self.assertEqual(q1, q2)
        self.assertEqual(q2, q1)
        self.assertNotEqual(q1, q3)
        self.assertNotEqual(q2, q3)

    def test_empty_queue(self):
        """Emptying queues."""
        q = Queue('example')

        self.testconn.rpush('rq:queue:example', 'foo')
        self.testconn.rpush('rq:queue:example', 'bar')
        self.assertEqual(q.is_empty(), False)

        q.empty()

        self.assertEqual(q.is_empty(), True)
        self.assertIsNone(self.testconn.lpop('rq:queue:example'))

    def test_empty_removes_jobs(self):
        """Emptying a queue deletes the associated job objects"""
        q = Queue('example')
        job = q.enqueue(say_hello)
        self.assertTrue(Job.exists(job.id))
        q.empty()
        self.assertFalse(Job.exists(job.id))

    def test_queue_is_empty(self):
        """Detecting empty queues."""
        q = Queue('example')
        self.assertEqual(q.is_empty(), True)

        self.testconn.rpush('rq:queue:example', 'sentinel message')
        self.assertEqual(q.is_empty(), False)

    def test_remove(self):
        """Ensure queue.remove properly removes Job from queue."""
        q = Queue('example')
        job = q.enqueue(say_hello)
        self.assertIn(job.id, q.job_ids)
        q.remove(job)
        self.assertNotIn(job.id, q.job_ids)

        job = q.enqueue(say_hello)
        self.assertIn(job.id, q.job_ids)
        q.remove(job.id)
        self.assertNotIn(job.id, q.job_ids)

    def test_jobs(self):
        """Getting jobs out of a queue."""
        q = Queue('example')
        self.assertEqual(q.jobs, [])
        job = q.enqueue(say_hello)
        self.assertEqual(q.jobs, [job])

        # Fetching a deleted removes it from queue
        job.delete()
        self.assertEqual(q.job_ids, [job.id])
        q.jobs
        self.assertEqual(q.job_ids, [])

    def test_compact(self):
        """Queue.compact() removes non-existing jobs."""
        q = Queue()

        q.enqueue(say_hello, 'Alice')
        q.enqueue(say_hello, 'Charlie')
        self.testconn.lpush(q.key, '1', '2')

        self.assertEqual(q.count, 4)

        q.compact()

        self.assertEqual(q.count, 2)

    def test_enqueue(self):
        """Enqueueing job onto queues."""
        q = Queue()
        self.assertEqual(q.is_empty(), True)

        # say_hello spec holds which queue this is sent to
        job = q.enqueue(say_hello, 'Nick', foo='bar')
        job_id = job.id

        # Inspect data inside Redis
        q_key = 'rq:queue:default'
        self.assertEqual(self.testconn.llen(q_key), 1)
        self.assertEqual(
            self.testconn.lrange(q_key, 0, -1)[0].decode('ascii'),
            job_id)

    def test_enqueue_sets_metadata(self):
        """Enqueueing job onto queues modifies meta data."""
        q = Queue()
        job = Job.create(func=say_hello, args=('Nick',), kwargs=dict(foo='bar'))

        # Preconditions
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)

        # Action
        q.enqueue_job(job)

        # Postconditions
        self.assertEqual(job.origin, q.name)
        self.assertIsNotNone(job.enqueued_at)

    def test_pop_job_id(self):
        """Popping job IDs from queues."""
        # Set up
        q = Queue()
        uuid = '112188ae-4e9d-4a5b-a5b3-f26f2cb054da'
        q.push_job_id(uuid)

        # Pop it off the queue...
        self.assertEqual(q.count, 1)
        self.assertEqual(q.pop_job_id(), uuid)

        # ...and assert the queue count when down
        self.assertEqual(q.count, 0)

    def test_dequeue(self):
        """Dequeueing jobs from queues."""
        # Set up
        q = Queue()
        result = q.enqueue(say_hello, 'Rick', foo='bar')

        # Dequeue a job (not a job ID) off the queue
        self.assertEqual(q.count, 1)
        job = q.dequeue()
        self.assertEqual(job.id, result.id)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, q.name)
        self.assertEqual(job.args[0], 'Rick')
        self.assertEqual(job.kwargs['foo'], 'bar')

        # ...and assert the queue count when down
        self.assertEqual(q.count, 0)

    def test_dequeue_instance_method(self):
        """Dequeueing instance method jobs from queues."""
        q = Queue()
        n = Number(2)
        q.enqueue(n.div, 4)

        job = q.dequeue()

        # The instance has been pickled and unpickled, so it is now a separate
        # object. Test for equality using each object's __dict__ instead.
        self.assertEqual(job.instance.__dict__, n.__dict__)
        self.assertEqual(job.func.__name__, 'div')
        self.assertEqual(job.args, (4,))

    def test_dequeue_class_method(self):
        """Dequeueing class method jobs from queues."""
        q = Queue()
        q.enqueue(Number.divide, 3, 4)

        job = q.dequeue()

        self.assertEqual(job.instance.__dict__, Number.__dict__)
        self.assertEqual(job.func.__name__, 'divide')
        self.assertEqual(job.args, (3, 4))

    def test_dequeue_ignores_nonexisting_jobs(self):
        """Dequeuing silently ignores non-existing jobs."""

        q = Queue()
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)
        q.push_job_id(uuid)
        result = q.enqueue(say_hello, 'Nick', foo='bar')
        q.push_job_id(uuid)

        # Dequeue simply ignores the missing job and returns None
        self.assertEqual(q.count, 4)
        self.assertEqual(q.dequeue().id, result.id)
        self.assertIsNone(q.dequeue())
        self.assertEqual(q.count, 0)

    def test_dequeue_any(self):
        """Fetching work from any given queue."""
        fooq = Queue('foo')
        barq = Queue('bar')

        self.assertEqual(Queue.dequeue_any([fooq, barq], None), None)

        # Enqueue a single item
        barq.enqueue(say_hello)
        job, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(queue, barq)

        # Enqueue items on both queues
        barq.enqueue(say_hello, 'for Bar')
        fooq.enqueue(say_hello, 'for Foo')

        job, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEqual(queue, fooq)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, fooq.name)
        self.assertEqual(job.args[0], 'for Foo',
                          'Foo should be dequeued first.')

        job, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEqual(queue, barq)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, barq.name)
        self.assertEqual(job.args[0], 'for Bar',
                          'Bar should be dequeued second.')

    def test_dequeue_any_ignores_nonexisting_jobs(self):
        """Dequeuing (from any queue) silently ignores non-existing jobs."""

        q = Queue('low')
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)

        # Dequeue simply ignores the missing job and returns None
        self.assertEqual(q.count, 1)
        self.assertEqual(Queue.dequeue_any([Queue(), Queue('low')], None),  # noqa
                None)
        self.assertEqual(q.count, 0)

    def test_enqueue_sets_status(self):
        """Enqueueing a job sets its status to "queued"."""
        q = Queue()
        job = q.enqueue(say_hello)
        self.assertEqual(job.get_status(), Status.QUEUED)

    def test_enqueue_explicit_args(self):
        """enqueue() works for both implicit/explicit args."""
        q = Queue()

        # Implicit args/kwargs mode
        job = q.enqueue(echo, 1, timeout=1, result_ttl=1, bar='baz')
        self.assertEqual(job.timeout, 1)
        self.assertEqual(job.result_ttl, 1)
        self.assertEqual(
            job.perform(),
            ((1,), {'bar': 'baz'})
        )

        # Explicit kwargs mode
        kwargs = {
            'timeout': 1,
            'result_ttl': 1,
        }
        job = q.enqueue(echo, timeout=2, result_ttl=2, args=[1], kwargs=kwargs)
        self.assertEqual(job.timeout, 2)
        self.assertEqual(job.result_ttl, 2)
        self.assertEqual(
            job.perform(),
            ((1,), {'timeout': 1, 'result_ttl': 1})
        )

    def test_all_queues(self):
        """All queues"""
        q1 = Queue('first-queue')
        q2 = Queue('second-queue')
        q3 = Queue('third-queue')

        # Ensure a queue is added only once a job is enqueued
        self.assertEqual(len(Queue.all()), 0)
        q1.enqueue(say_hello)
        self.assertEqual(len(Queue.all()), 1)

        # Ensure this holds true for multiple queues
        q2.enqueue(say_hello)
        q3.enqueue(say_hello)
        names = [q.name for q in Queue.all()]
        self.assertEqual(len(Queue.all()), 3)

        # Verify names
        self.assertTrue('first-queue' in names)
        self.assertTrue('second-queue' in names)
        self.assertTrue('third-queue' in names)

        # Now empty two queues
        w = Worker([q2, q3])
        w.work(burst=True)

        # Queue.all() should still report the empty queues
        self.assertEqual(len(Queue.all()), 3)

    def test_enqueue_job_with_dependencies(self):
        """In enqueue_call(), jobs are enqueued iff all their dependencies are
        finished."""
        # Job with remaining dependency is not immediately enqueued
        parent_job1 = Job.create(func=say_hello)
        parent_job1.save()
        parent_job2 = Job.create(func=say_hello, status=Status.FINISHED)
        parent_job2.save()
        parent_job3 = Job.create(func=say_hello)
        parent_job3.save()

        q = Queue()

        q.empty()
        job = q.enqueue_call(say_hello)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, Queue.DEFAULT_TIMEOUT)

        q.empty()
        q.enqueue_call(say_hello, depends_on=parent_job1)
        self.assertEqual(q.job_ids, [])

        q.empty()
        q.enqueue_call(say_hello, depends_on=[parent_job1])
        self.assertEqual(q.job_ids, [])

        q.empty()
        q.enqueue_call(say_hello, depends_on=[parent_job1, parent_job2])
        parent_job1.set_status(Status.FINISHED)
        parent_job1.save()
        self.assertEqual(q.job_ids, [])
        job = q.enqueue_call(say_hello, depends_on=[parent_job1, parent_job2])
        self.assertEqual(q.job_ids, [job.id])

        q.empty()
        parent_job1.set_status(Status.FINISHED)
        parent_job1.save()
        self.assertEqual(q.job_ids, [])
        q.enqueue_call(say_hello, depends_on=[parent_job1, parent_job2, parent_job3])
        self.assertEqual(q.job_ids, [])
        parent_job3.set_status(Status.FINISHED)
        parent_job3.save()
        job2 = q.enqueue_call(say_hello, depends_on=[parent_job1, parent_job2, parent_job3])
        self.assertEqual(q.job_ids, [job2.id])

        # Jobs dependent on finished jobs are immediately enqueued
        q.empty()
        parent_job = Job.create(func=say_hello)
        parent_job.set_status(Status.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job)

        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, Queue.DEFAULT_TIMEOUT)

    def test_defer_job(self):
        """Test that a job created as deferred is not put in a queue"""
        q = Queue()
        job = q.enqueue(say_hello, deferred=True)
        self.assertEqual(job.status, Status.DEFERRED)
        self.assertNotIn(job.id, q.job_ids)

    def test_release_job(self):
        q = Queue()
        job = q.enqueue(say_hello, deferred=True)
        release_job(job)
        self.assertIn(job.id, q.job_ids)

    def test_release_job_with_multiple_dependencies(self):
        q = Queue()
        future_job1 = q.enqueue(say_hello, deferred=True)
        future_job2 = q.enqueue(say_hello, deferred=True)
        future_job3 = q.enqueue(say_hello, deferred=True)

        job1 = q.enqueue(say_hello, blocked_by=future_job1)
        job2 = q.enqueue(say_hello, blocked_by=[future_job1, future_job2, future_job3])

        self.assertEqual(q.job_ids, [])
        release_job(future_job1)
        self.assertEqual(q.job_ids, [future_job1.id])
        release_job(future_job2)
        self.assertEqual(q.job_ids, [future_job1.id, future_job2.id])
        release_job(future_job3)
        self.assertEqual(q.job_ids, [future_job1.id, future_job2.id, future_job3.id])

        self.assertNotIn(job1.id, q.job_ids)
        self.assertNotIn(job2.id, q.job_ids)

        self.assertEqual(job1.dependencies, [future_job1])
        self.assertEqual(job2.dependencies, [future_job1, future_job2, future_job3])

        self.assertEqual(2, len(future_job1.reverse_dependencies))
        self.assertEqual(1, len(future_job2.reverse_dependencies))
        self.assertEqual(1, len(future_job3.reverse_dependencies))

        self.assertIn(job1, future_job1.reverse_dependencies)
        self.assertNotIn(job1, future_job2.reverse_dependencies)
        self.assertNotIn(job1, future_job3.reverse_dependencies)

        self.assertIn(job2, future_job1.reverse_dependencies)
        self.assertIn(job2, future_job2.reverse_dependencies)
        self.assertIn(job2, future_job3.reverse_dependencies)

        self.assertIn(future_job1.id, q.job_ids)
        self.assertIn(future_job2.id, q.job_ids)
        self.assertIn(future_job3.id, q.job_ids)

    def test_release_job_in_other_queue(self):
        q = Queue(name="1")
        job = q.enqueue(say_hello, deferred=True)
        q2 = Queue(name="2")
        release_job(job, queue_or_name=q2)
        self.assertIn(job.id, q2.job_ids)

    def test_release_job_here(self):
        q = Queue(name="1")
        job = q.enqueue(say_hello, deferred=True)
        q2 = Queue(name="2")
        q2.release_job_here(job)
        self.assertIn(job.id, q2.job_ids)

    def test_enqueue_job_with_dependency_and_timeout(self):
        """Jobs still know their specified timeout after being scheduled as reverse_dependencies."""
        parent_job = Job.create(func=say_hello)
        q = Queue()

        # Job with remaining dependency is not immediately enqueued
        q.empty()
        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=123)
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.timeout, 123)

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(Status.FINISHED)
        parent_job.save()

        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=456)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, 456)


class TestFailedQueue(RQTestCase):
    def test_requeue_job(self):
        """Requeueing existing jobs."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))  # noqa

        self.assertEqual(Queue.all(), [get_failed_queue()])  # noqa
        self.assertEqual(get_failed_queue().count, 1)

        get_failed_queue().requeue(job.id)

        self.assertEqual(get_failed_queue().count, 0)
        self.assertEqual(Queue('fake').count, 1)

    def test_requeue_nonfailed_job_fails(self):
        """Requeueing non-failed jobs raises error."""
        q = Queue()
        job = q.enqueue(say_hello, 'Nick', foo='bar')

        # Assert that we cannot requeue a job that's not on the failed queue
        with self.assertRaises(InvalidJobOperationError):
            get_failed_queue().requeue(job.id)

    def test_quarantine_preserves_timeout(self):
        """Quarantine preserves job timeout."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.timeout = 200
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))

        self.assertEqual(job.timeout, 200)

    def test_requeueing_preserves_timeout(self):
        """Requeueing preserves job timeout."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.timeout = 200
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))
        get_failed_queue().requeue(job.id)

        job = Job.fetch(job.id)
        self.assertEqual(job.timeout, 200)

    def test_requeue_sets_status_to_queued(self):
        """Requeueing a job should set its status back to QUEUED."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))
        get_failed_queue().requeue(job.id)

        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), Status.QUEUED)

    def test_enqueue_preserves_result_ttl(self):
        """Enqueueing persists result_ttl."""
        q = Queue()
        job = q.enqueue(div_by_zero, args=(1, 2, 3), result_ttl=10)
        self.assertEqual(job.result_ttl, 10)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(int(job_from_queue.result_ttl), 10)

    def test_async_false(self):
        """Executes a job immediately if async=False."""
        q = Queue(async=False)
        job = q.enqueue(some_calculation, args=(2, 3))
        self.assertEqual(job.return_value, 6)

    def test_custom_job_class(self):
        """Ensure custom job class assignment works as expected."""
        q = Queue(job_class=CustomJob)
        self.assertEqual(q.job_class, CustomJob)
