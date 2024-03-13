import collections
import errno
import functools
import inspect
import io
import logging
import os
import signal
import sys

import eventlet
import greenlet
from oslo_config import cfg
import six
from werkzeug import serving

from virtcdp.api import app
from virtcdp import config
from virtcdp import log
from virtcdp.conductor import manager
from virtcdp import utils

if six.PY2:
    from SimpleXMLRPCServer import SimpleXMLRPCServer
else:
    from xmlrpc.server import SimpleXMLRPCServer

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def _get_binary_name():
    return os.path.basename(inspect.stack()[-1][1])


def prepare_service(argv=None, binary=None):
    if argv is None:
        argv = []
    config.parse_args(argv,
                      default_config_files=['../../etc/virtcdp.conf'])
    if binary is None:
        binary = _get_binary_name()
    log.setup_logging(cfg.CONF.debug, binary)

    LOG.debug("Configuration:")
    CONF.log_opt_values(LOG, logging.DEBUG)


class RPCService(object):

    def __init__(self, host=None, port=None):
        self.host = host
        self.port = port
        self.endpoints = [
            manager.BackupConductor(),
        ]
        self._server = None

    def start(self, workers=1):
        self._server = SimpleXMLRPCServer((self.host, self.port),
                                          allow_none=True)
        self._server.register_introspection_functions()
        for e in self.endpoints:
            self._server.register_instance(e)
        self._server.serve_forever()

    # def create_periodic_tasks(self):
    #     if CONF.periodic_enable:
    #         periodic.setup(CONF, self.tg)
    #     servicegroup.setup(CONF, self.binary, self.tg)
    #
    # def stop(self):
    #     if self._server:
    #         self._server.stop()
    #         self._server.wait()
    #     super(Service, self).stop()

    @classmethod
    def create(cls, host=None, port=None):
        if host is None:
            host = CONF.rpc_server_listen
        if port is None:
            port = CONF.rpc_server_listen_port
        service_obj = cls(host, port)
        return service_obj

    def stop(self):
        if self._server:
            self._server.shutdown()

    def wait(self):
        pass


class WSGIService(object):

    def __init__(self, binary, use_ssl=False):
        self.bin = binary
        self.use_ssl = use_ssl
        self.app = app.setup_app()
        self.host = CONF.virtcdp_server_listen
        self.port = CONF.virtcdp_server_listen_port
        self.workers = CONF.virtcdp_server_workers
        self._server = None

    def start(self, workers):
        LOG.info('Starting server in PID %s', os.getpid())
        LOG.info('Serving on %(proto)s://%(host)s:%(port)s',
                 dict(proto="https" if self.use_ssl else "http",
                      host=self.host, port=self.port))
        # serving.run_simple(self.host,
        #                    self.port,
        #                    self.app,
        #                    processes=workers)
        self._server = utils.spawn(serving.run_simple,
                                   self.host, self.port, self.app,
                                   processes=workers)

    def reset(self):
        pass

    def stop(self):
        LOG.info("Stopping WSGI server.")

        if self._server is not None:
            self._server.kill()

    def wait(self):
        try:
            if self._server is not None:
                self._server.wait()
        except greenlet.GreenletExit:
            LOG.info("WSGI server has stopped.")


class SignalHandler(object):

    def __init__(self, *args, **kwargs):
        super(SignalHandler, self).__init__(*args, **kwargs)

        self.__setup_signal_interruption()

        # Map all signal names to signal integer values and create a
        # reverse mapping (for easier + quick lookup).
        self._ignore_signals = ('SIG_DFL', 'SIG_IGN')
        self._signals_by_name = dict((name, getattr(signal, name))
                                     for name in dir(signal)
                                     if name.startswith("SIG") and
                                     name not in self._ignore_signals)
        self.signals_to_name = dict(
            (sigval, name)
            for (name, sigval) in self._signals_by_name.items())
        self._signal_handlers = collections.defaultdict(set)
        self.clear()

    def clear(self):
        for sig in self._signal_handlers:
            signal.signal(sig, signal.SIG_DFL)
        self._signal_handlers.clear()

    def add_handlers(self, signals, handler):
        for sig in signals:
            self.add_handler(sig, handler)

    def add_handler(self, sig, handler):
        if not self.is_signal_supported(sig):
            return
        signo = self._signals_by_name[sig]
        self._signal_handlers[signo].add(handler)
        signal.signal(signo, self._handle_signal)

    def _handle_signal(self, signo, frame):
        # This method can be called anytime, even between two Python
        # instructions. It's scheduled by the C signal handler of Python using
        # Py_AddPendingCall().
        #
        # We only do one thing: schedule a call to _handle_signal_cb() later.
        # eventlet.spawn() is not signal-safe: _handle_signal() can be called
        # during a call to eventlet.spawn(). This case is supported, it is
        # ok to schedule multiple calls to _handle_signal() with the same
        # signal number.
        #
        # To call to _handle_signal_cb() is delayed to avoid reentrant calls to
        # _handle_signal_cb(). It avoids race conditions like reentrant call to
        # clear(): clear() is not reentrant (bug #1538204).
        eventlet.spawn(self._handle_signal_cb, signo, frame)

        # On Python >= 3.5, ensure that eventlet's poll() or sleep() call is
        # interrupted by raising an exception. If the signal handler does not
        # raise an exception then due to PEP 475 the call will not return until
        # an event is detected on a file descriptor or the timeout is reached,
        # and thus eventlet will not wake up and notice that there has been a
        # new thread spawned.
        if self.__force_interrupt_on_signal:
            try:
                interrupted_frame = inspect.stack(context=0)[1]
            except IndexError:
                pass
            else:
                if ((interrupted_frame.function == 'do_poll' and
                     interrupted_frame.filename == self.__hub_module_file) or
                    (interrupted_frame.function == 'do_sleep' and
                     interrupted_frame.filename == __file__)):
                    raise IOError(errno.EINTR, 'Interrupted')

    def __setup_signal_interruption(self):
        """Set up to do the Right Thing with signals during poll() and sleep().

        For Python 3.5 and later, deal with the changes in PEP 475 that prevent
        a signal from interrupting eventlet's call to poll() or sleep().
        """
        select_module = eventlet.patcher.original('select')
        self.__force_interrupt_on_signal = (sys.version_info >= (3, 5) and
                                            hasattr(select_module, 'poll'))

        if self.__force_interrupt_on_signal:
            try:
                from eventlet.hubs import poll as poll_hub
            except ImportError:
                pass
            else:
                # This is a function we can test for in the stack when handling
                # a signal - it's safe to raise an IOError with EINTR anywhere
                # in this function.
                def do_sleep(time_sleep_func, seconds):
                    return time_sleep_func(seconds)

                time_sleep = eventlet.patcher.original('time').sleep

                # Wrap time.sleep to ignore the interruption error we're
                # injecting from the signal handler. This makes the behaviour
                # the same as sleep() in Python 2, where EINTR causes the
                # sleep to be interrupted (and not resumed), but no exception
                # is raised.
                @functools.wraps(time_sleep)
                def sleep_wrapper(seconds):
                    try:
                        return do_sleep(time_sleep, seconds)
                    except (IOError, InterruptedError) as err:
                        if err.errno != errno.EINTR:
                            raise

                poll_hub.sleep = sleep_wrapper

            hub = eventlet.hubs.get_hub()
            self.__hub_module_file = sys.modules[hub.__module__].__file__

    def _handle_signal_cb(self, signo, frame):
        for handler in self._signal_handlers[signo]:
            handler(signo, frame)

    def is_signal_supported(self, sig_name):
        return sig_name in self._signals_by_name


def _is_daemon():
    # The process group for a foreground process will match the
    # process group of the controlling terminal. If those values do
    # not match, or ioctl() fails on the stdout file handle, we assume
    # the process is running in the background as a daemon.
    # http://www.gnu.org/software/bash/manual/bashref.html#Job-Control-Basics
    try:
        is_daemon = os.getpgrp() != os.tcgetpgrp(sys.stdout.fileno())
    except io.UnsupportedOperation:
        # Could not get the fileno for stdout, so we must be a daemon.
        is_daemon = True
    except OSError as err:
        if err.errno == errno.ENOTTY:
            # Assume we are a daemon because there is no terminal.
            is_daemon = True
        else:
            raise
    return is_daemon


def _is_sighup_and_daemon(signo):
    if not (SignalHandler().is_signal_supported('SIGHUP') and
            signo == signal.SIGHUP):
        # Avoid checking if we are a daemon, because the signal isn't
        # SIGHUP.
        return False
    return _is_daemon()


class SignalExit(SystemExit):
    def __init__(self, signo, exccode=1):
        super(SignalExit, self).__init__(exccode)
        self.signo = signo


class ServiceLauncher(object):
    def __init__(self):
        self.service = None
        self.signal_handler = SignalHandler()

    def handle_signal(self):
        """Set self._handle_signal as a signal handler."""
        self.signal_handler.clear()
        # self.signal_handler.add_handler('SIGTERM', self._graceful_shutdown)
        # self.signal_handler.add_handler('SIGINT', self._fast_exit)
        # self.signal_handler.add_handler('SIGHUP', self._reload_service)
        # self.signal_handler.add_handler('SIGALRM', self._on_timeout_exit)
        self.signal_handler.add_handlers(['SIGTERM', 'SIGINT'],
                                         self._graceful_shutdown)
        self.signal_handler.add_handler("SIGHUP", signal.SIG_IGN)

    def _graceful_shutdown(self, *args):
        self.signal_handler.clear()
        # if (self.conf.graceful_shutdown_timeout and
        #         self.signal_handler.is_signal_supported('SIGALRM')):
        #     signal.alarm(self.conf.graceful_shutdown_timeout)
        self.stop()

    def _reload_service(self, *args):
        self.signal_handler.clear()
        raise SignalExit(signal.SIGHUP)

    def _fast_exit(self, *args):
        LOG.info('Caught SIGINT signal, instantaneous exiting')
        os._exit(1)

    def _on_timeout_exit(self, *args):
        LOG.info('Graceful shutdown timeout exceeded, '
                 'instantaneous exiting')
        os._exit(1)

    def serve(self, service, workers=1):
        self.service = service
        if workers is None or workers < 1:
            workers = 1
        service.start(workers)
        return self

    def _wait_for_exit_or_signal(self):
        status = None
        signo = 0

        try:
            self.service.wait()
        except SignalExit as exc:
            signame = self.signal_handler.signals_to_name[exc.signo]
            LOG.info('Caught %s, handling', signame)
            status = exc.code
            signo = exc.signo
        except SystemExit as exc:
            self.stop()
            status = exc.code
        except Exception:
            self.stop()
        return status, signo

    def wait(self):
        self.signal_handler.clear()
        self.handle_signal()
        self._wait_for_exit_or_signal()
        # while True:
        #     self.handle_signal()
        #     status, signo = self._wait_for_exit_or_signal()
        #     if not _is_sighup_and_daemon(signo):
        #         break
        #     # self.restart()
        #
        # if self.service:
        #     self.service.wait()

    def stop(self):
        """Stop all services which are currently running.

        :returns: None

        """
        self.service.stop()
