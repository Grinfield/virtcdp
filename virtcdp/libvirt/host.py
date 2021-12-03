import logging
import operator
import os
import socket
import six

import libvirt
from eventlet import greenio
from eventlet import greenthread
from eventlet import patcher

from virtcdp import utils
from virtcdp.libvirt import event as virtevent
from virtcdp.libvirt import connection

LOG = logging.getLogger(__name__)

native_socket = patcher.original('socket')
native_Queue = patcher.original("Queue" if six.PY2 else "queue")

# This list is for libvirt hypervisor drivers that need special handling.
# This is *not* the complete list of supported hypervisor drivers.
HV_DRIVER_QEMU = "QEMU"


class Host(object):

    def __init__(self, conn_event_handler=None,
                 lifecycle_event_handler=None):
        self.wrapped_conn = None
        self._initial_connection = True
        self._conn_event_handler = conn_event_handler
        self._conn_event_handler_queue = six.moves.queue.Queue()
        self._lifecycle_event_handler = lifecycle_event_handler
        self._caps = None
        self._hostname = None

        self._event_queue = None
        self._events_delayed = {}
        self._lifecycle_delay = 15

    def _dispatch_thread(self):
        """Dispatches async events coming in from libvirtd.

        This is a green thread which waits for events to
        arrive from the libvirt event loop thread. This
        then dispatches the events to the compute manager.
        """

        while True:
            self._dispatch_events()

    def _conn_event_thread(self):
        """Dispatches async connection events"""
        # This thread doesn't need to jump through the same
        # hoops as _dispatch_thread because it doesn't interact directly
        # with the libvirt native thread.

        while True:
            self._dispatch_conn_event()

    def _dispatch_conn_event(self):
        # Splitting out this loop looks redundant, but it
        # means we can easily dispatch events synchronously from tests and
        # it isn't completely awful.
        handler = self._conn_event_handler_queue.get()

        try:
            handler()
        except Exception:
            LOG.exception('Exception handling connection event')
        finally:
            self._conn_event_handler_queue.task_done()

    @staticmethod
    def _event_lifecycle_callback(conn, dom, event, detail, opaque):
        """Receives lifecycle events from libvirt.

        NB: this method is executing in a native thread, not
        an eventlet coroutine. It can only invoke other libvirt
        APIs, or use self._queue_event(). Any use of logging APIs
        in particular is forbidden.
        """
        self = opaque

        uuid = dom.UUIDString()
        transition = None
        if event == libvirt.VIR_DOMAIN_EVENT_STOPPED:
            transition = virtevent.EVENT_LIFECYCLE_STOPPED
        elif event == libvirt.VIR_DOMAIN_EVENT_STARTED:
            transition = virtevent.EVENT_LIFECYCLE_STARTED
        elif event == libvirt.VIR_DOMAIN_EVENT_SUSPENDED:
            transition = virtevent.EVENT_LIFECYCLE_PAUSED
        elif event == libvirt.VIR_DOMAIN_EVENT_RESUMED:
            transition = virtevent.EVENT_LIFECYCLE_RESUMED

        if transition is not None:
            event_compact = virtevent.LifecycleEvent(uuid, transition)
            self._queue_event(event_compact)

            LOG.debug("myDomainEventCallback%s EVENT: Domain %s(%s) %s" % (
                opaque, dom.name(), dom.ID(), event_compact.get_name()))

    def _close_callback(self, conn, reason, opaque):
        close_info = {'conn': conn, 'reason': reason}
        self._queue_event(close_info)

    def _queue_event(self, event):
        """Puts an event on the queue for dispatch.

        This method is called by the native event thread to
        put events on the queue for later dispatch by the
        green thread. Any use of logging APIs is forbidden.
        """

        if self._event_queue is None:
            return

        # Queue the event...
        self._event_queue.put(event)

        # ...then wakeup the green thread to dispatch it
        c = ' '.encode()
        self._event_notify_send.write(c)
        self._event_notify_send.flush()

    def _dispatch_events(self):
        """Wait for & dispatch events from native thread

        Blocks until native thread indicates some events
        are ready. Then dispatches all queued events.
        """

        # Wait to be notified that there are some
        # events pending
        try:
            _c = self._event_notify_recv.read(1)
            assert _c
        except ValueError:
            return  # will be raised when pipe is closed

        # Process as many events as possible without
        # blocking
        last_close_event = None
        while not self._event_queue.empty():
            try:
                event = self._event_queue.get(block=False)
                if isinstance(event, virtevent.LifecycleEvent):
                    # call possibly with delay
                    self._event_emit_delayed(event)

                elif 'conn' in event and 'reason' in event:
                    last_close_event = event
            except native_Queue.Empty:
                pass
        if last_close_event is None:
            return

        conn = last_close_event['conn']
        self.wrapped_conn.dispatch_conn_closed_event(conn,
                                                     last_close_event)

    def _event_emit_delayed(self, event):
        """Emit events - possibly delayed."""

        def event_cleanup(gt, *args, **kwargs):
            """Callback function for greenthread. Called
            to cleanup the _events_delayed dictionary when an event
            was called.
            """
            event = args[0]
            self._events_delayed.pop(event.uuid, None)

        # Cleanup possible delayed stop events.
        if event.uuid in self._events_delayed.keys():
            self._events_delayed[event.uuid].cancel()
            self._events_delayed.pop(event.uuid, None)
            LOG.debug("Removed pending event for %s due to "
                      "lifecycle event", event.uuid)

        if event.transition == virtevent.EVENT_LIFECYCLE_STOPPED:
            # Delay STOPPED event, as they may be followed by a STARTED
            # event in case the instance is rebooting
            id_ = greenthread.spawn_after(self._lifecycle_delay,
                                          self._event_emit, event)
            self._events_delayed[event.uuid] = id_
            # add callback to cleanup self._events_delayed dict after
            # event was called
            id_.link(event_cleanup, event)
        else:
            self._event_emit(event)

    def _event_emit(self, event):
        if self._lifecycle_event_handler is not None:
            self._lifecycle_event_handler(event)

    def _init_events_pipe(self):
        """Create a self-pipe for the native thread to synchronize on.
        """
        self._event_queue = native_Queue.Queue()
        try:
            rpipe, wpipe = os.pipe()
            self._event_notify_send = greenio.GreenPipe(wpipe, 'wb', 0)
            self._event_notify_recv = greenio.GreenPipe(rpipe, 'rb', 0)
        except (ImportError, NotImplementedError):
            # This is Windows compatibility -- use a socket instead
            #  of a pipe because pipes don't really exist on Windows.
            sock = native_socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('localhost', 0))
            sock.listen(50)
            csock = native_socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            csock.connect(('localhost', sock.getsockname()[1]))
            nsock, addr = sock.accept()
            self._event_notify_send = nsock.makefile('wb', 0)
            gsock = greenio.GreenSocket(csock)
            self._event_notify_recv = gsock.makefile('rb', 0)

    def _init_events(self):
        """Initializes the libvirt events subsystem.

        This requires running a native thread to provide the
        libvirt event loop integration. This forwards events
        to a green thread which does the actual dispatching.
        """
        self._init_events_pipe()

        LOG.debug("Starting green dispatch thread")
        utils.spawn(self._dispatch_thread)

        LOG.debug("Starting connection event dispatch thread")
        utils.spawn(self._conn_event_thread)

    @staticmethod
    def _libvirt_error_handler(context, err):
        # Just ignore instead of default outputting to stderr.
        pass

    def init_host(self):
        self._init_events()

        # Get a new connection, then register lifecycle and
        # connection close event callback function
        self.wrapped_conn = connection.LibvirtConnection(
            conn_event_handler=self._conn_event_handler,
            conn_event_handler_queue=self._conn_event_handler_queue)
        self.wrapped_conn.get_connection()
        self._register_event_callback()

    def _version_check(self, lv_ver=None, hv_ver=None, hv_type=None,
                       op=operator.lt):
        """Check libvirt version, hypervisor version, and hypervisor type

        :param hv_type: hypervisor driver from the top of this file.
        """
        conn = self.wrapped_conn.get_connection()
        try:
            if lv_ver is not None:
                libvirt_version = conn.getLibVersion()
                if op(libvirt_version,
                      utils.convert_version_to_int(lv_ver)):
                    return False

            if hv_ver is not None:
                hypervisor_version = conn.getVersion()
                if op(hypervisor_version,
                      utils.convert_version_to_int(hv_ver)):
                    return False

            if hv_type is not None:
                hypervisor_type = conn.getType()
                if hypervisor_type != hv_type:
                    return False

            return True
        except Exception:
            return False

    def has_min_version(self, lv_ver=None, hv_ver=None, hv_type=None):
        return self._version_check(
            lv_ver=lv_ver, hv_ver=hv_ver, hv_type=hv_type, op=operator.lt)

    def has_version(self, lv_ver=None, hv_ver=None, hv_type=None):
        return self._version_check(
            lv_ver=lv_ver, hv_ver=hv_ver, hv_type=hv_type, op=operator.ne)

    def _register_event_callback(self):
        conn = self.wrapped_conn.get_connection()

        try:
            LOG.debug("Registering for lifecycle events: %s", self)
            conn.domainEventRegisterAny(
                None,
                libvirt.VIR_DOMAIN_EVENT_ID_LIFECYCLE,
                self._event_lifecycle_callback,
                self)
        except Exception as e:
            LOG.warning("URI %(uri)s does not support events: %(error)s",
                        {'uri': self.wrapped_conn.uri, 'error': e})

        try:
            LOG.debug("Registering for connection events: %s", str(self))
            conn.registerCloseCallback(self._close_callback, None)
        except (TypeError, AttributeError) as e:
            # NOTE: The registerCloseCallback of python-libvirt 1.0.1+
            # is defined with 3 arguments, and the above registerClose-
            # Callback succeeds. However, the one of python-libvirt 1.0.0
            # is defined with 4 arguments and TypeError happens here.
            # Then python-libvirt 0.9 does not define a method register-
            # CloseCallback.
            LOG.debug("The version of python-libvirt does not support "
                      "registerCloseCallback or is too old: %s", e)
        except libvirt.libvirtError as e:
            LOG.warning("URI %(uri)s does not support connection"
                        " events: %(error)s",
                        {'uri': self.wrapped_conn.uri, 'error': e})
