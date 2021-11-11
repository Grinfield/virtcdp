import logging
import operator
import os
import socket
import sys
import threading
import six

import libvirt
import libvirt_qemu
from eventlet import greenio
from eventlet import greenthread
from eventlet import patcher
from eventlet import tpool

from bitcdp import exception
from bitcdp import utils
from bitcdp import config
from bitcdp.libvirt import event as virtevent

LOG = logging.getLogger(__name__)

native_socket = patcher.original('socket')
native_threading = patcher.original("threading")
native_Queue = patcher.original("Queue" if six.PY2 else "queue")

CONF = config.CONF

# This list is for libvirt hypervisor drivers that need special handling.
# This is *not* the complete list of supported hypervisor drivers.
HV_DRIVER_QEMU = "QEMU"
HV_DRIVER_XEN = "Xen"


class Host(object):

    def __init__(self, uri, read_only=False,
                 conn_event_handler=None,
                 lifecycle_event_handler=None):

        self._uri = uri
        self._read_only = read_only
        self._initial_connection = True
        self._conn_event_handler = conn_event_handler
        self._conn_event_handler_queue = six.moves.queue.Queue()
        self._lifecycle_event_handler = lifecycle_event_handler
        self._caps = None
        self._hostname = None

        self._wrapped_conn = None
        self._wrapped_conn_lock = threading.Lock()
        self._event_queue = None

        self._events_delayed = {}
        self._lifecycle_delay = 15

    def _native_thread(self):
        """Receives async events coming in from libvirtd.

        This is a native thread which runs the default
        libvirt event loop implementation. This processes
        any incoming async events from libvirtd and queues
        them for later dispatch. This thread is only
        permitted to use libvirt python APIs, and the
        driver.queue_event method. In particular any use
        of logging is forbidden, since it will confuse
        eventlet's greenthread integration
        """

        while True:
            libvirt.virEventRunDefaultImpl()

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
        # NOTE(mdbooth): This thread doesn't need to jump through the same
        # hoops as _dispatch_thread because it doesn't interact directly
        # with the libvirt native thread.
        while True:
            self._dispatch_conn_event()

    def _dispatch_conn_event(self):
        # NOTE(mdbooth): Splitting out this loop looks redundant, but it
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
            event_compat = virtevent.LifecycleEvent(uuid, transition)
            self._queue_event(event_compat)

            LOG.debug("myDomainEventCallback%s EVENT: Domain %s(%s) %s" % (
                opaque, dom.name(), dom.ID(), event_compat.get_name()))

    def _close_callback(self, conn, reason, opaque):
        close_info = {'conn': conn, 'reason': reason}
        self._queue_event(close_info)

    @staticmethod
    def _test_connection(conn):
        try:
            conn.getLibVersion()
            return True
        except libvirt.libvirtError as e:
            if (e.get_error_code() in (libvirt.VIR_ERR_SYSTEM_ERROR,
                                       libvirt.VIR_ERR_INTERNAL_ERROR) and
                    e.get_error_domain() in (libvirt.VIR_FROM_REMOTE,
                                             libvirt.VIR_FROM_RPC)):
                LOG.debug('Connection to libvirt broke')
                return False
            raise

    @staticmethod
    def _connect_auth_cb(creds, opaque):
        if len(creds) == 0:
            return 0
        raise exception.InternalError(
            "Can not handle authentication request for %d credentials"
            % len(creds))

    @staticmethod
    def _connect(uri, read_only):
        auth = [[libvirt.VIR_CRED_AUTHNAME,
                 libvirt.VIR_CRED_ECHOPROMPT,
                 libvirt.VIR_CRED_REALM,
                 libvirt.VIR_CRED_PASSPHRASE,
                 libvirt.VIR_CRED_NOECHOPROMPT,
                 libvirt.VIR_CRED_EXTERNAL],
                Host._connect_auth_cb,
                None]

        flags = 0
        if read_only:
            flags = libvirt.VIR_CONNECT_RO
        # tpool.proxy_call creates a native thread. Due to limitations
        # with eventlet locking we cannot use the logging API inside
        # the called function.
        return tpool.proxy_call(
            (libvirt.virDomain, libvirt.virConnect),
            libvirt.openAuth, uri, auth, flags)

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
        # get_new_connection may already have disabled the host,
        # in which case _wrapped_conn is None.
        with self._wrapped_conn_lock:
            if conn == self._wrapped_conn:
                reason = str(last_close_event['reason'])
                msg = "Connection to libvirt lost: %s" % reason
                self._wrapped_conn = None
                self._queue_conn_event_handler(False, msg)

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

        LOG.debug("Starting native event thread")
        self._event_thread = native_threading.Thread(
            target=self._native_thread)
        self._event_thread.setDaemon(True)
        self._event_thread.start()

        LOG.debug("Starting green dispatch thread")
        utils.spawn(self._dispatch_thread)
        # self._disp_thread = threading.Thread(
        #     target=self._dispatch_thread)
        # self._disp_thread.start()

    def _get_new_connection(self):
        # call with _wrapped_conn_lock held
        LOG.debug('Connecting to libvirt: %s', self._uri)

        # This will raise an exception on failure
        wrapped_conn = self._connect(self._uri, self._read_only)

        try:
            LOG.debug("Registering for lifecycle events %s", self)
            wrapped_conn.domainEventRegisterAny(
                None,
                libvirt.VIR_DOMAIN_EVENT_ID_LIFECYCLE,
                self._event_lifecycle_callback,
                self)
        except Exception as e:
            LOG.warning("URI %(uri)s does not support events: %(error)s",
                        {'uri': self._uri, 'error': e})

        try:
            LOG.debug("Registering for connection events: %s", str(self))
            wrapped_conn.registerCloseCallback(self._close_callback, None)
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
                        {'uri': self._uri, 'error': e})

        return wrapped_conn

    def _queue_conn_event_handler(self, *args, **kwargs):
        if self._conn_event_handler is None:
            return

        def handler():
            return self._conn_event_handler(*args, **kwargs)

        self._conn_event_handler_queue.put(handler)

    def _get_connection(self, new_conn=False):
        if new_conn:
            LOG.debug("Get a newly connection to libvirt.")
            return self._connect(self._uri, self._read_only)

        # multiple concurrent connections are protected by _wrapped_conn_lock
        with self._wrapped_conn_lock:
            # Drop the existing connection if it is not usable
            if (self._wrapped_conn is not None and
                    not self._test_connection(self._wrapped_conn)):
                self._wrapped_conn = None
                # Connection was previously up, and went down
                self._queue_conn_event_handler(
                    False, 'Connection to libvirt lost')

            if self._wrapped_conn is None:
                try:
                    # This will raise if it fails to get a connection
                    self._wrapped_conn = self._get_new_connection()
                except Exception as ex:
                    # with excutils.save_and_reraise_exception():
                    # If we previously had a connection and it went down,
                    # we generated a down event for that above.
                    # We also want to generate a down event for an initial
                    # failure, which won't be handled above.
                    if self._initial_connection:
                        self._queue_conn_event_handler(
                            False,
                            'Failed to connect to libvirt: %(msg)s' %
                            {'msg': ex})
                    raise ex

                finally:
                    self._initial_connection = False

                self._queue_conn_event_handler(True, None)

        return self._wrapped_conn

    def get_connection(self, new_conn=False):
        """Returns a connection to the hypervisor

        This method should be used to create and return a well
        configured connection to the hypervisor.

        :returns: a libvirt.virConnect object
        """
        try:
            conn = self._get_connection(new_conn)
        except libvirt.libvirtError as ex:
            LOG.exception("Connection to libvirt failed: %s", ex)
            raise exception.HypervisorUnavailable(host=CONF.host)

        return conn

    @staticmethod
    def _libvirt_error_handler(context, err):
        # Just ignore instead of default outputting to stderr.
        pass

    def initialize(self):
        libvirt.registerErrorHandler(self._libvirt_error_handler, None)
        libvirt.virEventRegisterDefaultImpl()
        self._init_events()

        LOG.debug("Starting connection event dispatch thread")
        utils.spawn(self._conn_event_thread)

        self._initialized = True

    def _version_check(self, lv_ver=None, hv_ver=None, hv_type=None,
                       op=operator.lt):
        """Check libvirt version, hypervisor version, and hypervisor type

        :param hv_type: hypervisor driver from the top of this file.
        """
        conn = self.get_connection()
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

    def get_domain(self, uuid, new_conn=False):
        """Retrieve libvirt domain object for an instance.

        All libvirt error handling should be handled in this method and
        relevant nova exceptions should be raised in response.

        :param instance: a nova.objects.Instance object

        :returns: a libvirt.Domain object
        :raises exception.InstanceNotFound: The domain was not found
        :raises exception.InternalError: A libvirt error occured
        """
        try:
            conn = self.get_connection(new_conn)
            return conn.lookupByUUIDString(uuid)
        except libvirt.libvirtError as ex:
            error_code = ex.get_error_code()
            if error_code == libvirt.VIR_ERR_NO_DOMAIN:
                raise exception.InstanceNotFound(instance_id=uuid)

            msg = ('Error from libvirt while looking up %(uuid)s: '
                   '[Error Code %(error_code)s] %(ex)s' %
                   {'uuid': uuid,
                    'error_code': error_code,
                    'ex': ex})
            raise exception.InternalError(msg)

    def list_instance_domains(self, only_running=True, only_guests=True):
        """Get a list of libvirt.Domain objects for nova instances

        :param only_running: True to only return running instances
        :param only_guests: True to filter out any host domain (eg Dom-0)

        Query libvirt to a get a list of all libvirt.Domain objects
        that correspond to nova instances. If the only_running parameter
        is true this list will only include active domains, otherwise
        inactive domains will be included too. If the only_guests parameter
        is true the list will have any "host" domain (aka Xen Domain-0)
        filtered out.

        :returns: list of libvirt.Domain objects
        """
        flags = libvirt.VIR_CONNECT_LIST_DOMAINS_ACTIVE
        if not only_running:
            flags = flags | libvirt.VIR_CONNECT_LIST_DOMAINS_INACTIVE
        alldoms = self.get_connection().listAllDomains(flags)

        doms = []
        for dom in alldoms:
            if only_guests and dom.ID() == 0:
                continue
            doms.append(dom)

        return doms

    def get_online_cpus(self):
        """Get the set of CPUs that are online on the host

        Method is only used by NUMA code paths which check on
        libvirt version >= 1.0.4. getCPUMap() was introduced in
        libvirt 1.0.0.

        :returns: set of online CPUs, raises libvirtError on error

        """

        (cpus, cpu_map, online) = self.get_connection().getCPUMap()

        online_cpus = set()
        for cpu in range(cpus):
            if cpu_map[cpu]:
                online_cpus.add(cpu)

        return online_cpus

    def get_driver_type(self):
        """Get hypervisor type.

        :returns: hypervisor type (ex. qemu)

        """

        return self.get_connection().getType()

    def get_version(self):
        """Get hypervisor version.

        :returns: hypervisor version (ex. 12003)

        """

        return self.get_connection().getVersion()

    def get_hostname(self):
        """Returns the hostname of the hypervisor."""
        hostname = self.get_connection().getHostname()
        if self._hostname is None:
            self._hostname = hostname
        elif hostname != self._hostname:
            LOG.error('Hostname has changed from %(old)s '
                      'to %(new)s. A restart is required to take effect.',
                      {'old': self._hostname,
                       'new': hostname})
        return self._hostname

    def _qemu_monitor_event_callback(self, conn, dom, event,
                                     seconds, micros, details,
                                     opaque):
        qemu_mon_q = opaque
        if qemu_mon_q is None:
            return

        uuid = dom.UUIDString()
        job_status = event
        # if event == libvirt.VIR_DOMAIN_JOB_COMPLETED:
        #     job_status = libvirt.VIR_DOMAIN_JOB_COMPLETED

        # if job_status is not None:
        #     qm_event = virtevent.QemuMonitorEvent(uuid, job_status, seconds)
        #     qemu_monitor_queue.put(qm_event)

        qm_event = virtevent.QemuMonitorEvent(uuid, job_status, seconds, details)
        qemu_mon_q.put(qm_event)
        # LOG.debug("qemu_monitor_event_callback Queue: %s, EVENT: "
        #           "Domain %s(%s) %s, DETAILS:%s" %
        #           (opaque, dom.name(), dom.ID(), qm_event.get_status(),
        #            qm_event.get_details()))

    def register_qemu_monitor_event(self, dom=None, event_q=None):
        opaque = event_q
        conn = dom.connect()
        try:
            LOG.info("Registering for qemu monitor events: %s for domain %s",
                     self, dom.UUIDString())
            id = libvirt_qemu.qemuMonitorEventRegister(
                conn,
                dom,
                None,
                self._qemu_monitor_event_callback,
                opaque
            )
        except Exception as e:
            LOG.warning("URI %(uri)s does not support connection"
                        " events: %(error)s",
                        {'uri': self._uri, 'error': e})

        return id

    def deregister_qemu_monitor_event(self, domain, callback_id):
        conn = domain.connect()
        try:
            LOG.info("Deregistering for qemu monitor events callback: %s",
                     callback_id)
            libvirt_qemu.qemuMonitorEventDeregister(conn, callback_id)
        except Exception as ex:
            LOG.exception("Failed to deregister qemu monitor event "
                          "callback id %(id)s, error: %(error)s",
                          {'id': callback_id, 'error': ex})
