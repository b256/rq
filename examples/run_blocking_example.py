import os
import time
from rq import Queue, Connection, release_job
import sys
if '' not in sys.path:
    sys.path.insert(0, '')

from fib import slow_fib


def main():
    # Range of Fibonacci numbers to compute
    fib_range = range(20, 34)

    # Kick off the tasks asynchronously
    async_results = {}
    q = Queue()
    result = None
    for x in fib_range:
        if result is None:
            async_results[x] = result = q.enqueue(slow_fib, args=(x,), deferred=True)
        else:
            async_results[x] = q.enqueue(slow_fib, args=(x,), blocked_by=result)

    raw_input("All jobs enqueued and blocked by {}\nHit return to release this job then dependants:".format(result.id))
    start_time = time.time()
    release_job(result, queue_or_name=q)

    # Otherwise you may choose any of these
    #release_job(result, queue_or_name='default')
    #release_job(result.id)
    #release_job(result.id)

    done = False
    while not done:
        os.system('clear')
        print 'Asynchronously: (now = %.2f)' % (time.time() - start_time)
        done = True
        for x in fib_range:
            result = async_results[x].return_value
            if result is None:
                done = False
                result = '(calculating)'
            print 'fib(%d) = %s' % (x, result)
        print ''
        print 'To start the actual in the background, run a worker:'
        print '    python examples/run_worker.py'
        time.sleep(0.2)

    print 'Done'


if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    with Connection():
        main()
