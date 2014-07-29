import sys
if '' not in sys.path:
    sys.path.insert(0, '')

from rq import Queue, Worker, Connection


if __name__ == '__main__':
    # Tell rq what Redis connection to use
    with Connection():
        qs = Queue('low'), Queue('medium'), Queue('high')
        Worker(qs).work()
