import logging
import six
import time
import os
import collections

from eventlet import patcher
from oslo_config import cfg

from bitcdp import exception
from bitcdp.libvirt import host
from bitcdp.libvirt import event as virtevent
from bitcdp.libvirt import power_state
from bitcdp.libvirt import eventhandler
from bitcdp.libvirt import connection
from bitcdp.common import loopingcall
from bitcdp import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

native_Queue = patcher.original("Queue" if six.PY2 else "queue")

# Disable reason for the service which was enabled or disabled without reason
DISABLE_REASON_UNDEFINED = None
MIN_LIBVIRT_VERSION = (1, 2, 1)
MIN_QEMU_VERSION = (1, 5, 3)


class LibvirtDriver(object):

    def __init__(self):
        self.event_handler = eventhandler.EventHandler()
        self._host = host.Host(conn_event_handler=self._handle_conn_event,
                               lifecycle_event_handler=self.emit_event)
        self._compute_event_callback = None
        self.timers = collections.defaultdict(dict)

    def init_host(self):
        self.event_handler.initialize()
        self.init_virt_events()
        self._host.init_host()

        if not self._host.has_min_version(MIN_LIBVIRT_VERSION):
            raise exception.InternalError(
                'Bitcdp requires libvirt version %s or greater.' %
                self._version_to_string(MIN_LIBVIRT_VERSION))

        if (CONF.libvirt.virt_type in ("qemu", "kvm") and
            not self._host.has_min_version(hv_ver=MIN_QEMU_VERSION)):
            raise exception.InternalError(
                'Bitcdp requires QEMU version %s or greater.' %
                self._version_to_string(MIN_QEMU_VERSION))

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

    def _do_instance_backup(self, guest, targetdir, format,
                            interval, disk=None, **kwargs):
        devices = guest.qm_query_block()

        def _inc_backup(device):
            # Do incremental backup for device
            # target: /targdetdir/domain/node/INC-timestamp
            target = os.path.join(targetdir, guest.uuid,
                                  device.node, "INC-%s" % int(time.time()))
            if not os.path.exists(os.path.dirname(target)):
                os.makedirs(os.path.dirname(target))
            guest.qm_inc_backup(device, target,
                                format=format,
                                sync="incremental")

        for dev in devices:
            # if device and dev.node == device:
            # /targdetdir/domain/node/FULL-timestamp
            target = os.path.join(targetdir, guest.uuid,
                                  dev.node, "FULL-%s" % int(time.time()))
            if not os.path.exists(os.path.dirname(target)):
                os.makedirs(os.path.dirname(target))
            # do a full backup with dirty bitmap
            guest.qm_full_backup_with_bitmap(dev, target,
                                             format=format,
                                             sync="full")

            # Create a looping call to do periodic incremental backup
            timer = loopingcall.FixedIntervalLoopingCall(_inc_backup,
                                                         dev)
            self.timers[guest.uuid][dev.node] = timer
            # `initial_delay` means sleep for `interval` duration,
            # then start the loop and wait the done event that tell
            # the loop to exit
            timer.start(interval=interval, initial_delay=interval).wait()
            LOG.debug("Received stop backup request.")

    def drive_backup(self, uuid, disk=None, targetdir=None,
                     format="qcow2", interval=10):
        if targetdir is None:
            raise ValueError("Target directory of backup shouldn't be NULL.")
        LOG.info("Starting backup for instance %s...", uuid)

        kwargs = {}
        wrapped_conn = connection.LibvirtConnection()

        # domain = wrapped_conn.get_domain(uuid)
        # guest_obj = guest.Guest(domain)
        guest_obj = wrapped_conn.get_guest(uuid)
        devs = guest_obj.get_all_disks()

        # register qemu monitor events for domain
        cb_id = guest_obj.register_qemu_monitor_event()

        def _finalize_task():
            guest_obj.deregister_qemu_monitor_event(cb_id)
            wrapped_conn.close_connection()

        def _inner_do_instance_backup(guest, finalize_func,
                                      *args, **kwargs):
            try:
                self._do_instance_backup(guest, *args, **kwargs)
            except Exception as ex:
                LOG.exception("Error occurred during backup: %s", ex)
                raise ex
            finally:
                finalize_func()

        utils.spawn_n(_inner_do_instance_backup,
                      guest_obj,
                      _finalize_task,
                      targetdir,
                      format,
                      interval,
                      **kwargs)

        return "OK"

    def stop_backup(self, uuid, disk=None):
        if uuid not in self.timers:
            raise exception.InstanceNotInBackup(uuid=uuid)

        if disk and disk not in self.timers[uuid]:
            raise exception.DiskNotInBackup(uuid=uuid,
                                            disk=disk)

        # If disk is None, stop all the disk timers of the instance
        if disk is None:
            for timer in self.timers[uuid].items():
                timer.stop()
        else:
            # Send an event to the timer to stop backup looping call
            timer = self.timers[uuid][disk]
            timer.stop()

        return "OK"

    def drive_restore(self, uuid, timestamp, disk=None):
        pass

    def stop_restore(self):
        pass

    def query_blockjob(self):
        pass
