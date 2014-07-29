from .config import HAVE_WINDOWS



class JobTimeoutException(Exception):
    """Raised when a job takes longer to complete than the allowed maximum
    timeout value.
    """
    pass


if HAVE_WINDOWS:
    # Thanks to Gabriel Ahtune
    # http://gahtune.blogspot.fr/2013/08/a-timeout-context-manager.html
    #
    # Thanks to Eli Bendersky
    # http://eli.thegreenplace.net/2011/08/22/how-not-to-set-a-timeout-on-a-computation-in-python/
    #
    # This one is cross-platforms and thread safe, but due to the GIL management,
    # the timeout control is not as accurate as the above one using signals, and may
    # wait the end of a long blocking Python atomic instruction to take effect.
    import ctypes
    import threading


    def ctype_async_raise(target_tid, exception):
        # See http://docs.python.org/c-api/init.html#PyThreadState_SetAsyncExc
        # ensuring and releasing GIL are useless since we're not in C
        # gil_state = ctypes.pythonapi.PyGILState_Ensure()
        ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(target_tid), ctypes.py_object(exception))
        # ctypes.pythonapi.PyGILState_Release(gil_state)
        if ret == 0:
            raise ValueError("Invalid thread ID {}".format(target_tid))
        elif ret > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(target_tid), None)
            raise SystemError("PyThreadState_SetAsyncExc failed")


    class death_penalty_after():
        """Context manager for limiting in the time the execution of a block"""

        EXECUTED = 0
        EXECUTING = 1
        TIMED_OUT = -1
        INTERRUPTED = -2
        CANCELED = -3

        def __init__(self, seconds, swallow_exc=False):
            self.seconds = seconds
            self.swallow_exc = swallow_exc
            self.state = death_penalty_after.EXECUTED
            self.target_tid = threading.current_thread().ident

        def __bool__(self):
            return self.state in (death_penalty_after.EXECUTED, death_penalty_after.EXECUTING)

        def stop(self):
            self.state = death_penalty_after.TIMED_OUT
            # Raise a Timeout exception in the caller thread
            ctype_async_raise(self.target_tid, JobTimeoutException)

        def __enter__(self):
            self.timer = threading.Timer(self.seconds, self.stop)  # noqa
            self.timer.start()
            return self

        def __exit__(self, exctype, excinst, exctb):
            if exctype is JobTimeoutException:
                if self.state == death_penalty_after.TIMED_OUT:
                    return self.swallow_exc
                else:
                    self.state = death_penalty_after.INTERRUPTED
                    self.timer.cancel()
            else:
                if exctype is None:
                    self.state = death_penalty_after.EXECUTED
                self.timer.cancel()
            return self.swallow_exc

        def __repr__(self):
            return "< Timeout in state: {}>".format(self.state)

        def cancel(self):
            """In case in the block you realize you don't need anymore
           limitation"""
            self.state = death_penalty_after.CANCELED
            self.timer.cancel()

else:
    # Timeout context manager based on signals, available only on Unix platforms.
    # Does not work on pure Windows plaform that ignores SIGALRM

    import signal


    class death_penalty_after(object):
        def __init__(self, timeout):
            self._timeout = timeout

        def __enter__(self):
            self.setup_death_penalty()

        def __exit__(self, type, value, traceback):
            # Always cancel immediately, since we're done
            try:
                self.cancel_death_penalty()
            except JobTimeoutException:
                # Weird case: we're done with the with body, but now the alarm is
                # fired.  We may safely ignore this situation and consider the
                # body done.
                pass

            # __exit__ may return True to supress further exception handling.  We
            # don't want to suppress any exceptions here, since all errors should
            # just pass through, JobTimeoutException being handled normally to the
            # invoking context.
            return False

        def handle_death_penalty(self, signum, frame):
            raise JobTimeoutException('Job exceeded maximum timeout '
                                      'value (%d seconds).' % self._timeout)

        def setup_death_penalty(self):
            """Sets up an alarm signal and a signal handler that raises
            a JobTimeoutException after the timeout amount (expressed in
            seconds).
            """
            signal.signal(signal.SIGALRM, self.handle_death_penalty)
            signal.alarm(self._timeout)

        def cancel_death_penalty(self):
            """Removes the death penalty alarm and puts back the system into
            default signal handling.
            """
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
