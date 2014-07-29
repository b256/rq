import os
import time
import sys
if '' not in sys.path:
    sys.path.insert(0, '')

from rq import Queue, Connection
from fib import slow_fib


RESULT_TTL = 5000  # seconds

def main():
    # Range of Fibonacci numbers to compute
    fib_range = range(20, 34)

    # Kick off the tasks asynchronously
    async_results = {}
    low_q = Queue('low')
    medium_q = Queue('medium')
    high_q = Queue('high')
    queues = (low_q, medium_q, high_q)
    last_result = None
    for i, x in enumerate(fib_range):
        q = queues[i % len(queues)]
        if last_result is None:
            last_result = q.enqueue(slow_fib, args=(x,), result_ttl=RESULT_TTL)
        else:
            last_result = q.enqueue(slow_fib, args=(x,), result_ttl=RESULT_TTL)
        async_results[x] = last_result

    start_time = time.time()
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
        print '    python examples/run_multi_worker.py'
        time.sleep(0.2)

    print 'Done'


if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    with Connection():
        main()
