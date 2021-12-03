
import random
import sys
import time
import logging

from eventlet import event
from eventlet import greenthread
from oslo_utils import eventletutils
from oslo_utils import reflection
from oslo_utils import timeutils

LOG = logging.getLogger(__name__)


class LoopingCallDone(Exception):
    """Exception to break out and stop a LoopingCallBase.

    The poll-function passed to LoopingCallBase can raise this exception to
    break out of the loop normally. This is somewhat analogous to
    StopIteration.

    An optional return-value can be included as the argument to the exception;
    this return-value will be returned by LoopingCallBase.wait()

    """

    def __init__(self, retvalue=True):
        """:param retvalue: Value that LoopingCallBase.wait() should return."""
        self.retvalue = retvalue


class LoopingCallTimeOut(Exception):
    """Exception for a timed out LoopingCall.

    The LoopingCall will raise this exception when a timeout is provided
    and it is exceeded.
    """
    pass


def _safe_wrapper(f, kind, func_name):
    """Wrapper that calls into wrapped function and logs errors as needed."""

    def func(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except LoopingCallDone:
            raise  # let the outer handler process this
        except Exception:
            LOG.error('%(kind)s %(func_name)r failed',
                      {'kind': kind, 'func_name': func_name},
                      exc_info=True)
            return 0

    return func


class LoopingCallBase(object):
    _KIND = "Unknown looping call"

    _RUN_ONLY_ONE_MESSAGE = "A looping call can only run one function " \
                            "at a time"

    def __init__(self, f=None, *args, **kw):
        self.args = args
        self.kw = kw
        self.f = f
        self._thread = None
        self.done = None
        self._abort = eventletutils.EventletEvent()

    @property
    def _running(self):
        return not self._abort.is_set()

    def stop(self):
        if self._running:
            self._abort.set()

    def wait(self):
        return self.done.wait()

    def _on_done(self, gt, *args, **kwargs):
        self._thread = None

    def _sleep(self, timeout):
        self._abort.wait(timeout)

    def _start(self, idle_for, initial_delay=None, stop_on_exception=True):
        """Start the looping

        :param idle_for: Callable that takes two positional arguments, returns
                         how long to idle for. The first positional argument is
                         the last result from the function being looped and the
                         second positional argument is the time it took to
                         calculate that result.
        :param initial_delay: How long to delay before starting the looping.
                              Value is in seconds.
        :param stop_on_exception: Whether to stop if an exception occurs.
        :returns: eventlet event instance
        """
        if self._thread is not None:
            raise RuntimeError(self._RUN_ONLY_ONE_MESSAGE)
        self.done = event.Event()
        self._abort.clear()
        self._thread = greenthread.spawn(
            self._run_loop, idle_for,
            initial_delay=initial_delay, stop_on_exception=stop_on_exception)
        self._thread.link(self._on_done)
        return self.done

    def _elapsed(self, watch):
        return watch.elapsed()

    def _run_loop(self, idle_for_func,
                  initial_delay=None, stop_on_exception=True):
        kind = self._KIND
        func_name = reflection.get_callable_name(self.f)
        func = self.f if stop_on_exception else _safe_wrapper(self.f, kind,
                                                              func_name)
        if initial_delay:
            self._sleep(initial_delay)
        try:
            watch = timeutils.StopWatch()
            while self._running:
                watch.restart()
                result = func(*self.args, **self.kw)
                watch.stop()
                if not self._running:
                    break
                idle = idle_for_func(result, self._elapsed(watch))
                LOG.warn('%(kind)s %(func_name)r sleeping '
                         'for %(idle).02f seconds',
                         {'func_name': func_name, 'idle': idle,
                          'kind': kind})
                self._sleep(idle)
        except LoopingCallDone as e:
            self.done.send(e.retvalue)
        except Exception:
            exc_info = sys.exc_info()
            try:
                LOG.error('%(kind)s %(func_name)r failed',
                          {'kind': kind, 'func_name': func_name},
                          exc_info=exc_info)
                self.done.send_exception(*exc_info)
            finally:
                del exc_info
            return
        else:
            self.done.send(True)


class FixedIntervalLoopingCall(LoopingCallBase):
    """A fixed interval looping call."""

    _RUN_ONLY_ONE_MESSAGE = "A fixed interval looping call can only run" \
                            " one function at a time"

    _KIND = 'Fixed interval looping call'

    def start(self, interval, initial_delay=None, stop_on_exception=True):
        def _idle_for(result, elapsed):
            delay = round(elapsed - interval, 2)
            if delay > 0:
                func_name = reflection.get_callable_name(self.f)
                LOG.warning('Function %(func_name)r run outlasted '
                            'interval by %(delay).2f sec',
                            {'func_name': func_name, 'delay': delay})
            return -delay if delay < 0 else 0
        return self._start(_idle_for, initial_delay=initial_delay,
                           stop_on_exception=stop_on_exception)


class FixedIntervalWithTimeoutLoopingCall(LoopingCallBase):
    """A fixed interval looping call with timeout checking mechanism."""

    _RUN_ONLY_ONE_MESSAGE = "A fixed interval looping call with timeout" \
                            " checking and can only run one function at" \
                            " at a time"

    _KIND = 'Fixed interval looping call with timeout checking.'

    def start(self, interval, initial_delay=None,
              stop_on_exception=True, timeout=0):
        start_time = time.time()

        def _idle_for(result, elapsed):
            delay = round(elapsed - interval, 2)
            if delay > 0:
                func_name = reflection.get_callable_name(self.f)
                LOG.warning('Function %(func_name)r run outlasted '
                            'interval by %(delay).2f sec',
                            {'func_name': func_name, 'delay': delay})
            elapsed_time = time.time() - start_time
            if timeout > 0 and elapsed_time > timeout:
                raise LoopingCallTimeOut(
                    'Looping call timed out after %.02f seconds'
                    % elapsed_time)
            return -delay if delay < 0 else 0

        return self._start(_idle_for, initial_delay=initial_delay,
                           stop_on_exception=stop_on_exception)


class DynamicLoopingCall(LoopingCallBase):
    """A looping call which sleeps until the next known event.

    The function called should return how long to sleep for before being
    called again.
    """

    _RUN_ONLY_ONE_MESSAGE = "A dynamic interval looping call can only run" \
                            " one function at a time"
    _TASK_MISSING_SLEEP_VALUE_MESSAGE = "A dynamic interval looping call should supply either an" \
                                        " interval or periodic_interval_max"

    _KIND = 'Dynamic interval looping call'

    def start(self, initial_delay=None, periodic_interval_max=None,
              stop_on_exception=True):
        def _idle_for(suggested_delay, elapsed):
            delay = suggested_delay
            if delay is None:
                if periodic_interval_max is not None:
                    delay = periodic_interval_max
                else:
                    # Note(suro-patz): An application used to receive a
                    #     TypeError thrown from eventlet layer, before
                    #     this RuntimeError was introduced.
                    raise RuntimeError(
                        self._TASK_MISSING_SLEEP_VALUE_MESSAGE)
            else:
                if periodic_interval_max is not None:
                    delay = min(delay, periodic_interval_max)
            return delay

        return self._start(_idle_for, initial_delay=initial_delay,
                           stop_on_exception=stop_on_exception)


class BackOffLoopingCall(LoopingCallBase):
    """Run a method in a loop with backoff on error.

    The passed in function should return True (no error, return to
    initial_interval),
    False (error, start backing off), or raise LoopingCallDone(retvalue=None)
    (quit looping, return retvalue if set).

    When there is an error, the call will backoff on each failure. The
    backoff will be equal to double the previous base interval times some
    jitter. If a backoff would put it over the timeout, it halts immediately,
    so the call will never take more than timeout, but may and likely will
    take less time.

    When the function return value is True or False, the interval will be
    multiplied by a random jitter. If min_jitter or max_jitter is None,
    there will be no jitter (jitter=1). If min_jitter is below 0.5, the code
    may not backoff and may increase its retry rate.

    If func constantly returns True, this function will not return.

    To run a func and wait for a call to finish (by raising a LoopingCallDone):

        timer = BackOffLoopingCall(func)
        response = timer.start().wait()

    :param initial_delay: delay before first running of function
    :param starting_interval: initial interval in seconds between calls to
                              function. When an error occurs and then a
                              success, the interval is returned to
                              starting_interval
    :param timeout: time in seconds before a LoopingCallTimeout is raised.
                    The call will never take longer than timeout, but may quit
                    before timeout.
    :param max_interval: The maximum interval between calls during errors
    :param jitter: Used to vary when calls are actually run to avoid group of
                   calls all coming at the exact same time. Uses
                   random.gauss(jitter, 0.1), with jitter as the mean for the
                   distribution. If set below .5, it can cause the calls to
                   come more rapidly after each failure.
    :param min_interval: The minimum interval in seconds between calls to
                         function.
    :raises: LoopingCallTimeout if time spent doing error retries would exceed
             timeout.
    """

    _RNG = random.SystemRandom()
    _KIND = 'Dynamic backoff interval looping call'
    _RUN_ONLY_ONE_MESSAGE = "A dynamic backoff interval looping call can" \
                            " only run one function at a time"

    def __init__(self, f=None, *args, **kw):
        super(BackOffLoopingCall, self).__init__(f=f, *args, **kw)
        self._error_time = 0
        self._interval = 1

    def start(self, initial_delay=None, starting_interval=1, timeout=300,
              max_interval=300, jitter=0.75, min_interval=0.001):
        if self._thread is not None:
            raise RuntimeError(self._RUN_ONLY_ONE_MESSAGE)

        # Reset any prior state.
        self._error_time = 0
        self._interval = starting_interval

        def _idle_for(success, _elapsed):
            random_jitter = abs(self._RNG.gauss(jitter, 0.1))
            if success:
                # Reset error state now that it didn't error...
                self._interval = starting_interval
                self._error_time = 0
                return self._interval * random_jitter
            else:
                # Perform backoff, random jitter around the next interval
                # bounded by min_interval and max_interval.
                idle = max(self._interval * 2 * random_jitter, min_interval)
                idle = min(idle, max_interval)
                # Calculate the next interval based on the mean, so that the
                # backoff grows at the desired rate.
                self._interval = max(self._interval * 2 * jitter, min_interval)
                # Don't go over timeout, end early if necessary. If
                # timeout is 0, keep going.
                if timeout > 0 and self._error_time + idle > timeout:
                    raise LoopingCallTimeOut(
                        'Looping call timed out after %.02f seconds'
                        % self._error_time)
                self._error_time += idle
                return idle

        return self._start(_idle_for, initial_delay=initial_delay)
