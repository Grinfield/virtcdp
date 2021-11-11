import contextlib
import logging
import six

from collections import namedtuple
from oslo_config import cfg
from eventlet import patcher

from bitcdp import exception
from bitcdp.libvirt import monitor
from bitcdp.libvirt import host
from bitcdp.libvirt import event as virtevent
from bitcdp.libvirt import power_state
from bitcdp import utils
from bitcdp.libvirt import config as vconfig

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

native_Queue = patcher.original("Queue" if six.PY2 else "queue")

# Disable reason for the service which was enabled or disabled without reason
DISABLE_REASON_UNDEFINED = None
MIN_LIBVIRT_VERSION = (1, 2, 1)
MIN_QEMU_VERSION = (1, 5, 3)


class LibvirtDriver(object):

    def __init__(self, read_only=False):
        self._host = host.Host(self._uri(), read_only,
                               conn_event_handler=self._handle_conn_event,
                               lifecycle_event_handler=self.emit_event)
        self._monitor = monitor.QemuMonitor()
        self._compute_event_callback = None

    def init_host(self):
        self.init_virt_events()
        self._host.initialize()

        if not self._host.has_min_version(MIN_LIBVIRT_VERSION):
            raise exception.InternalError(
                'Nova requires libvirt version %s or greater.' %
                self._version_to_string(MIN_LIBVIRT_VERSION))

        if (CONF.libvirt.virt_type in ("qemu", "kvm") and
            not self._host.has_min_version(hv_ver=MIN_QEMU_VERSION)):
            raise exception.InternalError(
                'Nova requires QEMU version %s or greater.' %
                self._version_to_string(MIN_QEMU_VERSION))

    @staticmethod
    def _uri():
        uri = CONF.libvirt.connection_uri or 'qemu:///system'
        return uri

    def emit_event(self, event):
        """Dispatches an event to the compute manager.

        Invokes the event callback registered by the
        compute manager to dispatch the event. This
        must only be invoked from a green thread.
        """

        if not self._compute_event_callback:
            LOG.debug("Discarding event %s", six.text_type(event))
            return

        if not isinstance(event, virtevent.Event):
            raise ValueError("Event must be an instance of nova.virt.event.Event")

        try:
            LOG.debug("Emitting event %s", six.text_type(event))
            self._compute_event_callback(event)
        except Exception as ex:
            LOG.error("Exception dispatching event %(event)s: %(ex)s",
                      {'event': event, 'ex': ex})

    def handle_events(self, event):
        if isinstance(event, virtevent.LifecycleEvent):
            try:
                self.handle_lifecycle_event(event)
            except Exception:
                LOG.exception("some error occurred.")
                LOG.debug("Event %s arrived for non-existent instance. The "
                          "instance was probably deleted.", event)
        else:
            LOG.debug("Ignoring event %s", event)

    def init_virt_events(self):
        if CONF.libvirt.handle_virt_lifecycle_events:
            self.register_event_listener(self.handle_events)
        else:
            # NOTE(mriedem): If the _sync_power_states periodic task is
            # disabled we should emit a warning in the logs.
            if CONF.sync_power_state_interval < 0:
                LOG.warning('Instance lifecycle events from the compute '
                            'driver have been disabled. Note that lifecycle '
                            'changes to an instance outside of the compute '
                            'service will not be synchronized '
                            'automatically since the _sync_power_states '
                            'periodic task is also disabled.')
            else:
                LOG.info('Instance lifecycle events from the compute '
                         'driver have been disabled. Note that lifecycle '
                         'changes to an instance outside of the compute '
                         'service will only be synchronized by the '
                         '_sync_power_states periodic task.')

    def register_event_listener(self, callback):
        """Register a callback to receive events.

        Register a callback to receive asynchronous event
        notifications from hypervisors. The callback will
        be invoked with a single parameter, which will be
        an instance of the nova.virt.event.Event class.
        """
        self._compute_event_callback = callback

    def handle_lifecycle_event(self, event):
        LOG.info("VM %(uuid)s received %(state)s (Lifecycle Event)",
                 {'uuid': event.get_instance_uuid(),
                  'state': event.get_name()})
        uuid = event.get_instance_uuid()
        vm_power_state = None
        if event.get_transition() == virtevent.EVENT_LIFECYCLE_STOPPED:
            vm_power_state = power_state.SHUTDOWN
        elif event.get_transition() == virtevent.EVENT_LIFECYCLE_STARTED:
            vm_power_state = power_state.RUNNING
        elif event.get_transition() == virtevent.EVENT_LIFECYCLE_PAUSED:
            vm_power_state = power_state.PAUSED
        elif event.get_transition() == virtevent.EVENT_LIFECYCLE_RESUMED:
            vm_power_state = power_state.RUNNING
        elif event.get_transition() == virtevent.EVENT_LIFECYCLE_SUSPENDED:
            vm_power_state = power_state.SUSPENDED
        else:
            LOG.warning("Unexpected power state %d",
                        event.get_transition())

        LOG.info("VM %s translate to state: %s.", uuid, vm_power_state)
        # TODO: received VM stopped event, then stop backup

    def _handle_conn_event(self, enabled, reason):
        LOG.info("Connection event '%(enabled)d' reason '%(reason)s'",
                 {'enabled': enabled, 'reason': reason})
        # self._set_host_enabled(enabled, reason)

    def _version_to_string(self, version):
        return '.'.join([str(x) for x in version])

    def register_qemu_monitor_event(self, domain, event_queue):
        return self._host.register_qemu_monitor_event(domain, event_queue)

    def deregister_qemu_monitor_event(self, domain, cbId):
        return self._host.deregister_qemu_monitor_event(domain, cbId)

    @contextlib.contextmanager
    def wait_qemu_monitor_event(self, domain, event_queue, timeout=None):

        event = event_queue.get(True, timeout)
        LOG.debug("Emitting qemu monitor event %s", six.text_type(event))

        yield event

    def _do_instance_backup(self, domain, event_queue, **kwargs):
        # do a full backup first
        # self._full_backup()

        # cmd = "drive_backup"
        cmd = "query-block"
        # TODO: Then do periodic incremental backup
        ret = self._monitor.sendCommand(domain, cmd, **kwargs)
        # handling result

        # waiting for command completed event
        # utils.spawn(self.wait_qemu_monitor_event, domain)
        try:
            with self.wait_qemu_monitor_event(domain, event_queue, timeout=30) as event:
                LOG.debug("Received qemu monitor event from domain %s", event)
        except Exception as e:
            raise e

    @staticmethod
    def _close_connection(domain=None, conn=None):
        if not any((domain, conn)):
            return

        if domain:
            conn = domain.connect()
        if conn:
            LOG.info("Closing connection %s", conn)
            conn.close()
        return

    def drive_backup(self, uuid, target=None, disk=None, interval=10):
        # event_q = six.moves.queue.Queue()
        event_q = native_Queue.Queue()
        kwargs = {}

        domain = self._host.get_domain(uuid, new_conn=True)
        devs = self.get_all_devices(domain)

        # register qemu monitor events for domain
        cb_id = self.register_qemu_monitor_event(domain, event_q)

        # Check if bitmap exists, or create a new dirty bitmap
        self._check_or_create_bitmap(domain)

        # Clear dirty bitmap
        self._clear_bitmap()

        def _finalize_task():
            self.deregister_qemu_monitor_event(domain, cb_id)
            self._close_connection(domain=domain)

        def _inner_do_instance_backup(domain, event_q, finalize_func, **kwargs):
            try:
                self._do_instance_backup(domain, event_q, **kwargs)
            except Exception as ex:
                LOG.exception("Error occurred during backup: %s", ex)
                raise ex
            finally:
                finalize_func()

        utils.spawn_n(_inner_do_instance_backup,
                      domain, event_q,
                      _finalize_task,
                      **kwargs)

        return "OK"

    def _query_block(self, domain):
        cmd = "query-block"
        ret = self._monitor.sendCommand(domain, cmd)
        return self._get_block_devices(ret)

    @staticmethod
    def _get_block_devices(blockinfo):
        """Get a list of block devices that we can create a bitmap for,
           currently we only get inserted qcow based images
        """
        BlockDev = namedtuple('BlockDev', [
            'node', 'format', 'filename', 'backing_image', 'has_bitmap', 'bitmaps'
        ])
        blockdevs = []

        for device in blockinfo:
            backing_image = False
            has_bitmap = False
            bitmaps = None

            try:
                inserted = device['inserted']
                # if inserted['drv'] == 'raw':
                #     continue

                try:
                    if len(device['dirty-bitmaps']) > 0:
                        has_bitmap = True
                        bitmaps = device['dirty-bitmaps']
                except KeyError:
                    pass

                try:
                    bi = inserted['image']['backing-image']
                    backing_image = True
                except KeyError:
                    pass

                blockdevs.append(BlockDev(
                    device['device'],
                    inserted['image']['format'],
                    inserted['image']['filename'],
                    backing_image,
                    has_bitmap,
                    bitmaps)
                )
            except KeyError:
                continue

        if len(blockdevs) == 0:
            return None

        return blockdevs

    @staticmethod
    def _check_bitmap_state(node, bitmaps):
        """
        Check if the bitmap state is ready for backup
            active  -> Ready for backup
            frozen  -> backup in progress
            disabled-> migration might be going on
        """
        for bitmap in bitmaps:
            LOG.debug('Node %s, Bitmap: %s', node, bitmap)
            match = "%s-%s" % ('bitcdp', node)
            if bitmap["name"] == match and bitmap['status'] == "active":
                return True

    def _clear_bitmap(self):
        pass

    def _check_or_create_bitmap(self, domain):
        blockdevs = self._query_block(domain)
        if not blockdevs:
            LOG.error("Instance %(instance_id)s has no any block device suitable for backup.",
                      {"instance_id": domain.UUIDString()})
            raise exception.NoBlockdevsFound(instance_id=domain.UUIDString())

        for dev in blockdevs:
            if dev.has_bitmap is True:
                state = self._check_bitmap_state(dev.node, dev.bitmaps)
                if state is not True:
                    LOG.warn("Bitmap for device %(device)s is in state %(state)s.",
                             {"device": dev.node,
                              "state": state})

    def _full_backup(self):
        pass

    def _inc_backup(self):
        pass

    def _periodic_task(self):
        pass

    def drive_restore(self):
        pass

    @staticmethod
    def get_all_devices(domain, devtype=None):
        """Returns all devices for a guest
        :param devtype: a LibvirtConfigGuestDevice subclass class
        :returns: a list of LibvirtConfigGuestDevice instances
        """
        try:
            config = vconfig.LibvirtConfigGuest()
            config.parse_str(
                domain.XMLDesc(0))
        except Exception:
            return []

        devs = []
        for dev in config.devices:
            if (devtype is None or
                    isinstance(dev, devtype)):
                devs.append(dev)
        return devs
