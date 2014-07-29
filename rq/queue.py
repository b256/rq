# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time
import uuid

from .connections import resolve_connection
from .job import Job, Status
from .utils import import_attribute, utcnow

from .exceptions import (DequeueTimeout, InvalidJobOperationError,
                         NoSuchJobError, UnpickleError)
from .compat import total_ordering, string_types, as_text


def release_job(job_or_id, queue_or_name=None, connection=None):
    """ Release a job that was previously created as "deferred", either in its original queue or in the queue passed in the queue_or_name parameter.
    """
    connection = resolve_connection(connection)
    if not isinstance(job_or_id, Job):
        assert isinstance(job_or_id, (str, unicode))
        # OK, we got a job id
        if Job.exists(job_or_id, connection=connection):
            job = Job.fetch(job_or_id, connection=connection)
        else:
            raise NoSuchJobError("There is no job with id '{0}'".format(job_or_id))
    else:
        job = job_or_id

    if not job.is_deferred:
        raise InvalidJobOperationError("Job id '{0}' status is {1} and not 'deferred'".format(job_or_id, job.status))

    # Get associated queue
    if queue_or_name is None:
        queue = Queue(name=job.origin, connection=connection)
    elif isinstance(queue_or_name, Queue):
        queue = queue_or_name
    else:  # A string (?)
        assert isinstance(queue_or_name, (str, unicode))
        queue = Queue(name=queue_or_name, connection=connection)
    result = connection.srem('rq:deferred', job.id)
    if result == 0:
        raise NoSuchJobError('No such blocked job: %s' % job.id)

    job.status = Status.QUEUED
    job.save()
    queue.enqueue_job(job)

    return


def get_failed_queue(connection=None):
    """Returns a handle to the special failed queue."""
    return FailedQueue(connection=connection)


def compact(lst):
    return [item for item in lst if item is not None]


@total_ordering
class Queue(object):
    job_class = Job
    DEFAULT_TIMEOUT = 180  # Default timeout seconds.
    redis_queue_namespace_prefix = 'rq:queue:'
    redis_queues_keys = 'rq:queues'

    @classmethod
    def all(cls, connection=None):
        """Returns an iterable of all Queues.
        """
        connection = resolve_connection(connection)

        def to_queue(queue_key):
            return cls.from_queue_key(as_text(queue_key),
                                      connection=connection)
        return [to_queue(rq_key) for rq_key in connection.smembers(cls.redis_queues_keys) if rq_key]

    @classmethod
    def from_queue_key(cls, queue_key, connection=None):
        """Returns a Queue instance, based on the naming conventions for naming
        the internal Redis keys.  Can be used to reverse-lookup Queues by their
        Redis keys.
        """
        prefix = cls.redis_queue_namespace_prefix
        if not queue_key.startswith(prefix):
            raise ValueError('Not a valid RQ queue key: %s' % (queue_key,))
        name = queue_key[len(prefix):]
        return cls(name, connection=connection)

    def __init__(self, name='default', default_timeout=None, connection=None,
                 async=True, job_class=None):
        self.connection = resolve_connection(connection)
        prefix = self.redis_queue_namespace_prefix
        self.name = name
        self._key = '%s%s' % (prefix, name)
        self._default_timeout = default_timeout
        self._async = async
        if self.__class__ is Queue:
            # No such features for inheriting queues types
            self.done_queue = DoneQueue.of_parent(self)
            self.wip_queue = WIPQueue.of_parent(self)

        if job_class is not None:
            if isinstance(job_class, string_types):
                job_class = import_attribute(job_class)
            self.job_class = job_class

    @property
    def key(self):
        """Returns the Redis key for this Queue."""
        return self._key

    def empty(self):
        """Removes all messages on the queue."""
        script = b"""
            local prefix = "rq:job:"
            local q = KEYS[1]
            local count = 0
            while true do
                local job_id = redis.call("lpop", q)
                if job_id == false then
                    break
                end

                -- Delete the relevant keys
                redis.call("del", prefix..job_id)
                redis.call("del", prefix..job_id..":dependents")
                count = count + 1
            end
            return count
        """
        script = self.connection.register_script(script)
        return script(keys=[self.key])

    def is_empty(self):
        """Returns whether the current queue is empty."""
        return self.count == 0

    def fetch_job(self, job_id):
        try:
            return self.job_class.fetch(job_id, connection=self.connection)
        except NoSuchJobError:
            self.remove(job_id)

    def get_job_ids(self, offset=0, length=-1):
        """Returns a slice of job IDs in the queue."""
        start = offset
        if length >= 0:
            end = offset + (length - 1)
        else:
            end = length
        return [as_text(job_id) for job_id in
                self.connection.lrange(self.key, start, end)]

    def get_jobs(self, offset=0, length=-1):
        """Returns a slice of jobs in the queue."""
        job_ids = self.get_job_ids(offset, length)
        return compact([self.fetch_job(job_id) for job_id in job_ids])

    @property
    def job_ids(self):
        """Returns a list of all job IDS in the queue."""
        return self.get_job_ids()

    @property
    def jobs(self):
        """Returns a list of all (valid) jobs in the queue."""
        return self.get_jobs()

    @property
    def count(self):
        """Returns a count of all messages in the queue."""
        return self.connection.llen(self.key)

    def release_job_here(self, job_or_id):
        """Release a job that was deferred into *this* queue (i.e. change the job initialy declared queue is needed)
           If you don't want to affect the job originally chosen queue use the module function release_job
           :param job_or_id: the job instance or the job_id of the job to be release from its deferred state i.e. put back into this queue.
           """
        release_job(job_or_id=job_or_id, queue_or_name=self)

    def remove(self, job_or_id, pipeline=None):
        """Removes Job from queue, accepts either a Job instance or ID."""
        job_id = job_or_id.id if isinstance(job_or_id, self.job_class) else job_or_id
        
        if pipeline is not None:
            pipeline.lrem(self.key, 0, job_id)
        
        return self.connection._lrem(self.key, 0, job_id)

    def compact(self):
        """Removes all "dead" jobs from the queue by cycling through it, while
        guarantueeing FIFO semantics.
        """
        COMPACT_QUEUE = '{0}_compact:{1}'.format(self.redis_queue_namespace_prefix, uuid.uuid4())

        self.connection.rename(self.key, COMPACT_QUEUE)
        while True:
            job_id = as_text(self.connection.lpop(COMPACT_QUEUE))
            if job_id is None:
                break
            if self.job_class.exists(job_id, self.connection):
                self.connection.rpush(self.key, job_id)

    def push_job_id(self, job_id):
        """Pushes a job ID on the corresponding Redis queue."""
        self.connection.rpush(self.key, job_id)

    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, description=None, depends_on=None,
                     blocked_by=None, deferred=False):
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """
        timeout = timeout or self._default_timeout

        # TODO: job with dependency shouldn't have "queued" as status
        init_status = Status.DEFERRED if (deferred or blocked_by) else Status.QUEUED

        # blocked_by some job implies depends_on some job
        if blocked_by:
            depends_on = blocked_by

        job = self.job_class.create(func, args, kwargs, connection=self.connection,
                                    result_ttl=result_ttl, status=init_status,
                                    description=description, depends_on=depends_on, timeout=timeout)

        # If job depends on an unfinished job, register itself on it's
        # parent's dependents instead of enqueueing it.
        if depends_on is not None:
            if isinstance(depends_on, Job):
                depends_on = [depends_on]
            remaining_dependencies = job.register_dependencies(depends_on)
            if remaining_dependencies:
                job.save()
                return job

        if init_status == Status.DEFERRED:
            return self.defer_job(job)
        else:
            return self.enqueue_job(job)

    def enqueue(self, f, *args, **kwargs):
        """Creates a job to represent the delayed function call and enqueues
        it.

        Expects the function to call, along with the arguments and keyword
        arguments.

        The function argument `f` may be any of the following:

        * A reference to a function
        * A reference to an object's instance method
        * A string, representing the location of a function (must be
          meaningful to the import context of the workers)
        """
        if not isinstance(f, string_types) and f.__module__ == '__main__':
            raise ValueError('Functions from the __main__ module cannot be processed '
                             'by workers.')

        # Detect explicit invocations, i.e. of the form:
        #     q.enqueue(foo, args=(1, 2), kwargs={'a': 1}, timeout=30)
        result_ttl = kwargs.pop('result_ttl', None)
        depends_on = kwargs.pop('depends_on', None)
        deferred = kwargs.pop('deferred', False)
        blocked_by = kwargs.pop('blocked_by', None)
        timeout = kwargs.pop('timeout', None)
        description = kwargs.pop('description', None)

        if 'args' in kwargs or 'kwargs' in kwargs:
            assert args == (), 'Extra positional arguments cannot be used when using explicit args and kwargs.'  # noqa
            args = kwargs.pop('args', None)
            kwargs = kwargs.pop('kwargs', None)

        return self.enqueue_call(func=f, args=args, kwargs=kwargs,
                                 timeout=timeout, result_ttl=result_ttl,
                                 description=description, depends_on=depends_on,
                                 deferred=deferred, blocked_by=blocked_by)

    def defer_job(self, job, set_meta_data=True):
        """Enqueues a job for a deferred execution (conditioned by a future release)

        If the `set_meta_data` argument is `True` (default), it will update
        the properties `origin` and `enqueued_at`.
        """
        value = job.id
        self.connection.sadd('rq:deferred', value)

        if set_meta_data:
            job.origin = self.name
            job.enqueued_at = utcnow()

        if job.timeout is None:
            job.timeout = self.DEFAULT_TIMEOUT
        job.save()
        return job

    def enqueue_job(self, job, set_meta_data=True):
        """Enqueues a job for delayed execution.

        If the `set_meta_data` argument is `True` (default), it will update
        the properties `origin` and `enqueued_at`.

        If Queue is instantiated with async=False, job is executed immediately.
        """
        # Add Queue key set
        self.connection.sadd(self.redis_queues_keys, self.key)

        if set_meta_data:
            job.origin = self.name
            job.enqueued_at = utcnow()

        if job.timeout is None:
            job.timeout = self.DEFAULT_TIMEOUT
        job.save()

        if self._async:
            self.push_job_id(job.id)
        else:
            job.perform()
            job.save()
        return job

    def pop_job_id(self):
        """Pops a given job ID from this Redis queue."""
        return as_text(self.connection.lpop(self.key))

    @classmethod
    def lpop(cls, queue_keys, timeout, connection=None):
        """Helper method.  Intermediate method to abstract away from some
        Redis API details, where LPOP accepts only a single key, whereas BLPOP
        accepts multiple.  So if we want the non-blocking LPOP, we need to
        iterate over all queues, do individual LPOPs, and return the result.

        Until Redis receives a specific method for this, we'll have to wrap it
        this way.

        The timeout parameter is interpreted as follows:
            None - non-blocking (return immediately)
             > 0 - maximum number of seconds to block
        """
        connection = resolve_connection(connection)
        if timeout is not None:  # blocking variant
            if timeout == 0:
                raise ValueError('RQ does not support indefinite timeouts. Please pick a timeout value > 0.')
            result = connection.blpop(queue_keys, timeout)
            if result is None:
                raise DequeueTimeout(timeout, queue_keys)
            queue_key, job_id = result
            return queue_key, job_id
        else:  # non-blocking variant
            for queue_key in queue_keys:
                blob = connection.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    def dequeue(self):
        """Dequeues the front-most job from this queue.

        Returns a job_class instance, which can be executed or inspected.
        """
        # ISSUE: This method does not seem to be used anywhere except in tests
        job_id = self.pop_job_id()
        if job_id is None:
            return None
        try:
            job = self.job_class.fetch(job_id, connection=self.connection)
        except NoSuchJobError as e:
            # Silently pass on jobs that don't exist (anymore),
            # and continue by reinvoking itself recursively
            return self.dequeue()
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting
            e.job_id = job_id
            e.queue = self
            raise e
        return job

    @classmethod
    def dequeue_any(cls, queues, timeout, connection=None):
        """Class method returning the job_class instance at the front of the given
        set of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `timeout` argument,
        either blocks execution of this function for the duration of the
        timeout or until new messages arrive on any of the queues, or returns
        None.

        See the documentation of cls.lpop for the interpretation of timeout.
        """
        queue_keys = [q.key for q in queues]
        result = cls.lpop(queue_keys, timeout, connection=connection)
        if result is None:
            return None
        queue_key, job_id = map(as_text, result)
        queue = cls.from_queue_key(queue_key, connection=connection)
        try:
            job = cls.job_class.fetch(job_id, connection=connection)
        except NoSuchJobError:
            # Silently pass on jobs that don't exist (anymore),
            # and continue by reinvoking the same function recursively
            return cls.dequeue_any(queues, timeout, connection=connection)
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting
            e.job_id = job_id
            e.queue = queue
            raise e
        return job, queue

    # Total ordering defition (the rest of the required Python methods are
    # auto-generated by the @total_ordering decorator)
    def __eq__(self, other):  # noqa
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects.')
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects.')
        return self.name < other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):  # noqa
        return 'Queue(%r)' % (self.name,)

    def __str__(self):
        return '<Queue \'%s\'>' % (self.name,)


class FailedQueue(Queue):
    def __init__(self, connection=None):
        super(FailedQueue, self).__init__(Status.FAILED, connection=connection)

    def quarantine(self, job, exc_info):
        """Puts the given Job in quarantine (i.e. put it on the failed
        queue).

        This is different from normal job enqueueing, since certain meta data
        must not be overridden (e.g. `origin` or `enqueued_at`) and other meta
        data must be inserted (`ended_at` and `exc_info`).
        """
        job.ended_at = utcnow()
        job.exc_info = exc_info
        return self.enqueue_job(job, set_meta_data=False)

    def requeue(self, job_id):
        """Requeues the job with the given job ID."""
        try:
            job = self.job_class.fetch(job_id, connection=self.connection)
        except NoSuchJobError:
            # Silently ignore/remove this job and return (i.e. do nothing)
            self.remove(job_id)
            return

        # Delete it from the failed queue (raise an error if that failed)
        if self.remove(job) == 0:
            raise InvalidJobOperationError('Cannot requeue non-failed jobs.')

        job.set_status(Status.QUEUED)
        job.exc_info = None
        q = Queue(job.origin, connection=self.connection)
        q.enqueue_job(job)


class ChildQueue(object):
    """This is a mixin class for classes inheriting from :class:`Queue` that only provides
    a factory classmethod that initialises main attributes from its parent object
    (which is supposed to be a :class:`Queue` or subclass of it)
    """
    @classmethod
    def of_parent(cls, parent):
        """Factory method
        """
        #queue = cls.from_queue_key(parent.name, connection=parent.connection)
        queue = cls(parent.name, parent._default_timeout, parent.connection, parent._async)
        queue.parent = parent
        return queue


class WIPQueue(Queue, ChildQueue):
    """This queue will handle "work in process" jobs

    :param name: Name of the WIP queue (the same as the associated jobs queue)
    :type name: :class:`str`
    :param default_timeout: timeout of the associated job
    :param default_timeout: :class:`float`
    :param connection: Redis connection to be used
    :type connection: :class:`redis.StrictRedis`
    """
    redis_queue_namespace_prefix = 'rq:wipqueue:'

    def add_job(self, job):
        """Adding a job in positional slot (latest timeout first)
        """
        timeout = job.timeout
        if timeout is None:
            timeout = self._default_timeout or self.DEFAULT_TIMEOUT
        rank = time.time() + timeout
        self.connection.zadd(self.key, rank, job.id)
        return

    def remove_job(self, job):
        """Removes a job from the WIP queue

        :param job: a :class:`rq.job.Job` object or a job id
        :return: The count of removed jobs, thus 0 or 1.
        """
        if isinstance(job, Job):
            job = job.id
        return self.connection.zrem(self.key, job)

    def remove_expired_jobs(self):
        """Removes the oldest expired in the queue
        """
        to_remove = self.connection.zrangebyscore(self.key, '-inf', time.time())
        if len(to_remove) > 0:
            self.connection.zrem(self.key, *to_remove)
        return


class DoneQueue(Queue, ChildQueue):
    """This queue will handle "successfully done jobs"
    """
    redis_queue_namespace_prefix = 'rq:donequeue:'
    redis_queues_keys = 'rq:donequeues'

    def add_job(self, job):
        """Adding a job in positional slot (latest timeout first)
        """
        timeout = job.timeout
        if timeout is None:
            timeout = self._default_timeout or self.DEFAULT_TIMEOUT
        rank = time.time() + timeout
        self.connection.zadd(self.key, rank, job.id)
        return

    def requeue_job(self, job_id):
        """Puts back in the parent queue the job id

        Issue: take care of job dependency. Means that requeuing a job requires its dependent jobs are
        in the same Done queue too.
        In that case, we must requeue the jobs in the dependency reverse order.
        """
        pass

