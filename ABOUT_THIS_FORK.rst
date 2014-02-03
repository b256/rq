===============
About this fork
===============

This fork is hosted at https://github.com/glenfant/rq/tree/wip-queue

Windows support
===============

RQ may work under windows.

Multiprocessing in place of Unix fork
=====================================

Jobs are always executed in a distinct process. In original rq, jobs are
executed using Unix fork() that's not available under Windows. From this
version, when executed under Windows, rq workers use automatically
multiprocessing when executing jobs in a distinct process. When under Unix,
workers execute as before the jobs in a forked process.

But the "-- multiprocessing" option of "rqworker" command enables workers to
execute jobs with multiprocessing.

In process queue
================

Added the WIP queue that records actually executed jobs.

rq enhancement. Support for deferred job executions
===================================================

rq has already a support for dependent jobs. At a given time, you have two
jobs J1 and J2 :

- they need to be executed in a particular order (J1, then J2)
- the parameters and execution contexts for running these jobs are known
  before execution. i.e. : you can create them at the same time / sequence
  in your code

So far, so good, we can use the "depends_on" parameter for this. In example ::

  >>> bread_slices = queue.enqueue(cut_bread, kwargs={parts: 4})
  >>> queue.enqueue(add_marmalade, args=(strawberry,), depends_on=bread_slices)

In that case the ``cut_bread`` job can have been already executed when the
``add_marmalade`` job is enqueued.

But we have sometimes situations where we need to enqueue a task with known
parameters which depends on future tasks that cannot be known (enumeration,
name, parameters, ...) before this, or we can have a task that should not be
started before other tasks are created, or we need to ensure execution order.

- because we  need to be sure that all those tasks start together or never.
- because each task is the result of a process that update an underlying state
  (i.e. a database of sort) and all tasks should be executed only when the
  (underlying state is coherent.

In other words, and expressed as API ::

  >>> future_job = queue.enqueue(some_callable, ..., deferred=True)

This create a new job with the DEFERRED status (and **not** the QUEUED
status as usual). The workers will ignore such jobs at the moment, because
they are not put in their designated underlying queue yet.

We shall now add new jobs which **must** be executed before this
``future_job`` ::

  >>> queue.enqueue(some_callable_1, ..., blocked_by=future_job)
  >>> queue.enqueue(some_callable_2, ..., blocked_by=future_job)

This register new jobs which execution is conditioned by the execution of
``future_job``. The corresponding jobs are created with the DEFERRED status
too. Like above, those jobs are created but not yet enqueued in their
designated queue, thus, they won't be picked up for execution by any worker
yet.

**A job that is ``blocked_by`` is** *also* **set as depends_on.**

In fact if the ``depends_on`` parameter is present it **must** be the
same as ``blocked_by`` parameter.

If the ``depends_on`` parameter is **not** present it **will** be set at the
same value as the ``blocked_by`` parameter.

Technically speaking, each job enqueued with the ``blocked_by`` argument is
appended to a Redis list ::

  key : "rq:deferred:<id of future_job>"
  value : ["target_queue_for_job1/id of job some_callable_1",
           "target_queue_for_job2/id of job some_callable_2"]

The workers will ignore such jobs since those jobs are not even really
enqueued, i.e. their job_id are not in the redis list that represent a given
queue.

The structure ``target_queue_for_job1/id of job some_callable_1`` allow the re-
insertion of a job in its queue without unpicking it.

When all dependent jobs are pushed, we are ready to execute them all ::

  >>> future_job.release()

or... ::

  >>> rq.release_job(future_job)

or... ::

  >>> rq.release_job(future_job_id)

The effect of this is:

- Change the state of all dependent jobs to QUEUED and put them in their
  destinated Queues.

  -> The workers may execute these jobs when possible, taking depedencies into account.

- Change the state of ``future_job`` to ``QUEUED`` when last of these jobs is
  executed and enqueue it. Executing "de facto" the job after the former
  ``blocked_by`` jobs thanks to the regular ``depends_on`` mechanism.

If a ``future_job`` is cancelled or removed from RQ before having been
released, all dependend jobs (the one registered in the ``rq:deferred:<id of
future_job>`` data structure) are cancelled or removed too.

If ``future_job`` has been released the former depending jobs becomes
"independent" in their lifecycle and are thus not affected by status changes
or deletion of the ``future_job``
