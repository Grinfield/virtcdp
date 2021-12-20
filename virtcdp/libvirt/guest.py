import logging
from lxml import etree
import six
import contextlib

import libvirt
import libvirt_qemu
from eventlet import patcher

from virtcdp import exception
from virtcdp.libvirt import event as virtevent
from virtcdp.libvirt import config as vconfig
from virtcdp.libvirt import power_state
from virtcdp.libvirt import monitor

LOG = logging.getLogger(__name__)
native_Queue = patcher.original("Queue" if six.PY2 else "queue")

VIR_DOMAIN_NOSTATE = 0
VIR_DOMAIN_RUNNING = 1
VIR_DOMAIN_BLOCKED = 2
VIR_DOMAIN_PAUSED = 3
VIR_DOMAIN_SHUTDOWN = 4
VIR_DOMAIN_SHUTOFF = 5
VIR_DOMAIN_CRASHED = 6
VIR_DOMAIN_PMSUSPENDED = 7

LIBVIRT_POWER_STATE = {
    VIR_DOMAIN_NOSTATE: power_state.NOSTATE,
    VIR_DOMAIN_RUNNING: power_state.RUNNING,
    # The DOMAIN_BLOCKED state is only valid in Xen.  It means that
    # the VM is running and the vCPU is idle. So, we map it to RUNNING
    VIR_DOMAIN_BLOCKED: power_state.RUNNING,
    VIR_DOMAIN_PAUSED: power_state.PAUSED,
    # The libvirt API doc says that DOMAIN_SHUTDOWN means the domain
    # is being shut down. So technically the domain is still
    # running. SHUTOFF is the real powered off state.  But we will map
    # both to SHUTDOWN anyway.
    # http://libvirt.org/html/libvirt-libvirt.html
    VIR_DOMAIN_SHUTDOWN: power_state.SHUTDOWN,
    VIR_DOMAIN_SHUTOFF: power_state.SHUTDOWN,
    VIR_DOMAIN_CRASHED: power_state.CRASHED,
    VIR_DOMAIN_PMSUSPENDED: power_state.SUSPENDED,
}


class Guest(object):

    def __init__(self, domain):
        self._event_queue = None
        self._domain = domain
        self._qemu_mon = monitor.QemuMonitor(self._domain)

    def __repr__(self):
        return "<Guest %(id)d %(name)s %(uuid)s>" % {
            'id': self.id,
            'name': self.name,
            'uuid': self.uuid
        }

    @property
    def id(self):
        return self._domain.ID()

    @property
    def uuid(self):
        return self._domain.UUIDString()

    @property
    def name(self):
        return self._domain.name()

    @property
    def domain(self):
        return self._domain

    def get_disk(self, device):
        """Returns the disk mounted at device

        :returns LivirtConfigGuestDisk: mounted at device or None
        """
        try:
            doc = etree.fromstring(self._domain.XMLDesc(0))
        except Exception:
            return None

        node = doc.find("./devices/disk/target[@dev='%s'].." % device)
        if node is None:
            node = doc.find("./devices/disk/source[@file='%s'].." % device)

        if node is not None:
            conf = vconfig.LibvirtConfigGuestDisk()
            conf.parse_dom(node)
            return conf

    def get_all_disks(self):
        """Returns all the disks for a guest

        :returns: a list of LibvirtConfigGuestDisk instances
        """

        return self.get_all_devices(vconfig.LibvirtConfigGuestDisk)

    def get_all_devices(self, devtype=None):
        """Returns all devices for a guest

        :param devtype: a LibvirtConfigGuestDevice subclass class

        :returns: a list of LibvirtConfigGuestDevice instances
        """

        try:
            config = vconfig.LibvirtConfigGuest()
            config.parse_str(
                self._domain.XMLDesc(0))
        except Exception:
            return []

        devs = []
        for dev in config.devices:
            if (devtype is None or
                    isinstance(dev, devtype)):
                devs.append(dev)
        return devs

    def get_xml_desc(self, dump_inactive=False, dump_sensitive=False,
                     dump_migratable=False):
        """Returns xml description of guest.

        :param dump_inactive: Dump inactive domain information
        :param dump_sensitive: Dump security sensitive information
        :param dump_migratable: Dump XML suitable for migration

        :returns string: XML description of the guest
        """
        flags = dump_inactive and libvirt.VIR_DOMAIN_XML_INACTIVE or 0
        flags |= dump_sensitive and libvirt.VIR_DOMAIN_XML_SECURE or 0
        flags |= dump_migratable and libvirt.VIR_DOMAIN_XML_MIGRATABLE or 0
        return self._domain.XMLDesc(flags=flags)

    def get_block_device(self, disk):
        """Returns a block device wrapper for disk."""
        return BlockDevice(self, disk)

    def set_user_password(self, user, new_pass):
        """Configures a new user password."""
        self._domain.setUserPassword(user, new_pass, 0)

    def _get_domain_info(self, host):
        """Returns information on Guest

        :param host: a host.Host object with current
                     connection. Unfortunately we need to pass it
                     because of a workaround with < version 1.2..11

        :returns list: [state, maxMem, memory, nrVirtCpu, cpuTime]
        """
        return compat.get_domain_info(libvirt, host, self._domain)

    def get_info(self, host):
        """Retrieve information from libvirt for a specific instance name.

        If a libvirt error is encountered during lookup, we might raise a
        NotFound exception or Error exception depending on how severe the
        libvirt error is.

        :returns hardware.InstanceInfo:
        """
        try:
            dom_info = self._get_domain_info(host)
        except libvirt.libvirtError as ex:
            error_code = ex.get_error_code()
            if error_code == libvirt.VIR_ERR_NO_DOMAIN:
                raise exception.InstanceNotFound(instance_id=self.uuid)

            msg = (_('Error from libvirt while getting domain info for '
                     '%(instance_name)s: [Error Code %(error_code)s] %(ex)s') %
                   {'instance_name': self.name,
                    'error_code': error_code,
                    'ex': ex})
            raise exception.InternalError(msg)

        return hardware.InstanceInfo(
            state=LIBVIRT_POWER_STATE[dom_info[0]],
            max_mem_kb=dom_info[1],
            mem_kb=dom_info[2],
            num_cpu=dom_info[3],
            cpu_time_ns=dom_info[4],
            id=self.id)

    def get_power_state(self, host):
        return self.get_info(host).state

    def is_active(self):
        "Determines whether guest is currently running."
        return self._domain.isActive()

    def freeze_filesystems(self):
        """Freeze filesystems within guest."""
        self._domain.fsFreeze()

    def thaw_filesystems(self):
        """Thaw filesystems within guest."""
        self._domain.fsThaw()

    def abort_job(self):
        """Requests to abort current background job"""
        self._domain.abortJob()

    def get_job_info(self):
        """Get job info for the domain

        Query the libvirt job info for the domain (ie progress
        of migration, or snapshot operation)

        :returns: a JobInfo of guest
        """
        if JobInfo._have_job_stats:
            try:
                stats = self._domain.jobStats()
                return JobInfo(**stats)
            except libvirt.libvirtError as ex:
                if ex.get_error_code() == libvirt.VIR_ERR_NO_SUPPORT:
                    # Remote libvirt doesn't support new API
                    LOG.debug("Missing remote virDomainGetJobStats: %s", ex)
                    JobInfo._have_job_stats = False
                    return JobInfo._get_job_stats_compat(self._domain)
                elif ex.get_error_code() in (
                        libvirt.VIR_ERR_NO_DOMAIN,
                        libvirt.VIR_ERR_OPERATION_INVALID):
                    # Transient guest finished migration, so it has gone
                    # away completclsely
                    LOG.debug("Domain has shutdown/gone away: %s", ex)
                    return JobInfo(type=libvirt.VIR_DOMAIN_JOB_COMPLETED)
                else:
                    LOG.debug("Failed to get job stats: %s", ex)
                    raise
            except AttributeError as ex:
                # Local python binding doesn't support new API
                LOG.debug("Missing local virDomainGetJobStats: %s", ex)
                JobInfo._have_job_stats = False
                return JobInfo._get_job_stats_compat(self._domain)
        else:
            return JobInfo._get_job_stats_compat(self._domain)

    def register_qemu_monitor_event(self):
        # event_q = six.moves.queue.Queue()
        if self._event_queue is None:
            self._event_queue = native_Queue.Queue()

        conn = self._domain.connect()
        try:
            LOG.info("Registering for qemu monitor events: %s for domain %s",
                     self, self.uuid)
            id = libvirt_qemu.qemuMonitorEventRegister(
                conn,
                self._domain,
                None,
                self._qemu_monitor_event_callback,
                self._event_queue
            )
        except Exception as e:
            LOG.error("URI %(uri)s does not support qemu monitor"
                      " events: %(error)s",
                      {'uri': conn.getURI(), 'error': e})
            raise

        return id

    def _qemu_monitor_event_callback(self, conn, dom, event,
                                     seconds, micros, details,
                                     opaque):
        if id(opaque) != id(self._event_queue):
            return

        uuid = dom.UUIDString()
        job_status = event
        # if event == libvirt.VIR_DOMAIN_JOB_COMPLETED:
        #     job_status = libvirt.VIR_DOMAIN_JOB_COMPLETED

        # if job_status is not None:
        #     qm_event = virtevent.QemuMonitorEvent(uuid, job_status, seconds)
        #     qemu_monitor_queue.put(qm_event)

        qm_event = virtevent.QemuMonitorEvent(uuid, job_status, seconds, details)
        self._event_queue.put(qm_event)
        # LOG.debug("qemu_monitor_event_callback Queue: %s, EVENT: "
        #           "Domain %s(%s) %s, DETAILS:%s" %
        #           (opaque, dom.name(), dom.ID(), qm_event.get_status(),
        #            qm_event.get_details()))

    @staticmethod
    def _event_match(event, match=None):
        if match is None:
            return True
        if not isinstance(event, virtevent.QemuMonitorEvent):
            return False

        if event.get_status() == match:
            return True
        else:
            return False

    @contextlib.contextmanager
    def wait_qemu_monitor_event(self, timeout=None, match=None):
        while True:
            event = self._event_queue.get(True, timeout)
            LOG.debug("Emitting qemu monitor event %s", six.text_type(event))
            if self._event_match(event, match):
                break

        yield event

    def deregister_qemu_monitor_event(self, callback_id):
        conn = self._domain.connect()
        try:
            LOG.info("Deregistering for qemu monitor events callback: %s",
                     callback_id)
            libvirt_qemu.qemuMonitorEventDeregister(conn, callback_id)
        except Exception as ex:
            LOG.exception("Failed to deregister qemu monitor event "
                          "callback id %(id)s, error: %(error)s",
                          {'id': callback_id, 'error': ex})

    def qmp_query_block(self, name=None):
        return self._qemu_mon.query_block(name)

    def qmp_full_backup_with_bitmap(self, dev, target, format="qcow2",
                                    sync="full", **kwargs):
        LOG.debug("Begin to do full backup for instance %(domain)s at target %(target)s.",
                  {"domain": self.uuid, "target": target})
        self._qemu_mon.full_backup_with_bitmap(dev, target, format, sync)

        try:
            with self.wait_qemu_monitor_event(
                    timeout=600, match="BLOCK_JOB_COMPLETED") as event:
                LOG.debug("Received full backup COMPLETED event "
                          "from domain, %s", event)
        except Exception as e:
            raise e

    def qmp_inc_backup(self, dev, target, format="qcow2",
                       sync="incremental", **kwargs):
        blocks = self.qmp_query_block(dev.node)
        new_dev = None
        for block in blocks:
            if block.node == dev.node:
                new_dev = block

        if new_dev is not None:
            dev = new_dev
        else:
            LOG.warning("Pending block %s disappeared before doing backup.", dev.node)
            return False

        LOG.debug("Begin to do incremental backup for instance %(domain)s "
                  "at target %(target)s.",
                  {"domain": self.uuid, "target": target})
        self._qemu_mon.inc_backup(dev, target, format, sync)

        with self.wait_qemu_monitor_event(
                timeout=600, match="BLOCK_JOB_COMPLETED") as event:
            LOG.debug("Received inc backup COMPLETED event from domain, %s", event)


class BlockDevice(object):
    """Wrapper around block device API"""

    REBASE_DEFAULT_BANDWIDTH = 0  # in MiB/s - 0 unlimited
    COMMIT_DEFAULT_BANDWIDTH = 0  # in MiB/s - 0 unlimited

    def __init__(self, guest, disk):
        self._guest = guest
        self._disk = disk

    def abort_job(self, async=False, pivot=False):
        """Request to cancel a live block device job

        :param async: Cancel the block device job (e.g. 'copy' or
                      'commit'), and return as soon as possible, without
                      waiting for job completion
        :param pivot: Pivot to the destination image when ending a
                      'copy' or "active commit" (meaning: merging the
                      contents of current active disk into its backing
                      file) job
        """
        flags = async and libvirt.VIR_DOMAIN_BLOCK_JOB_ABORT_ASYNC or 0
        flags |= pivot and libvirt.VIR_DOMAIN_BLOCK_JOB_ABORT_PIVOT or 0
        self._guest._domain.blockJobAbort(self._disk, flags=flags)

    def get_job_info(self):
        """Returns information about job currently running

        :returns: BlockDeviceJobInfo, or None if no job exists
        :raises: libvirt.libvirtError on error fetching block job info
        """

        # libvirt's blockJobInfo() raises libvirt.libvirtError if there was an
        # error. It returns {} if the job no longer exists, or a fully
        # populated dict if the job exists.
        status = self._guest._domain.blockJobInfo(self._disk, flags=0)

        # The job no longer exists
        if not status:
            return None

        return BlockDeviceJobInfo(
            job=status['type'],
            bandwidth=status['bandwidth'],
            cur=status['cur'],
            end=status['end'])

    def rebase(self, base, shallow=False, reuse_ext=False,
               copy=False, relative=False, copy_dev=False):
        """Copy data from backing chain into a new disk

        This copies data from backing file(s) into overlay(s), giving
        control over several aspects like what part of a disk image
        chain to be copied, whether to reuse an existing destination
        file, etc.  And updates the backing file to the new disk

        :param shallow: Limit copy to top of the source backing chain
        :param reuse_ext: Reuse an existing external file that was
                          pre-created
        :param copy: Start a copy job
        :param relative: Keep backing chain referenced using relative names
        :param copy_dev: Treat the destination as type="block"
        """
        flags = shallow and libvirt.VIR_DOMAIN_BLOCK_REBASE_SHALLOW or 0
        flags |= reuse_ext and libvirt.VIR_DOMAIN_BLOCK_REBASE_REUSE_EXT or 0
        flags |= copy and libvirt.VIR_DOMAIN_BLOCK_REBASE_COPY or 0
        flags |= copy_dev and libvirt.VIR_DOMAIN_BLOCK_REBASE_COPY_DEV or 0
        flags |= relative and libvirt.VIR_DOMAIN_BLOCK_REBASE_RELATIVE or 0
        return self._guest._domain.blockRebase(
            self._disk, base, self.REBASE_DEFAULT_BANDWIDTH, flags=flags)

    def commit(self, base, top, relative=False):
        """Merge data from overlays into backing file

        This live merges (or "commits") contents from backing files into
        overlays, thus reducing the length of a disk image chain.

        :param relative: Keep backing chain referenced using relative names
        """
        flags = relative and libvirt.VIR_DOMAIN_BLOCK_COMMIT_RELATIVE or 0
        return self._guest._domain.blockCommit(
            self._disk, base, top, self.COMMIT_DEFAULT_BANDWIDTH, flags=flags)

    def resize(self, size_kb):
        """Resize block device to KiB size"""
        self._guest._domain.blockResize(self._disk, size_kb)

    def is_job_complete(self):
        """Return True if the job is complete, False otherwise

        :returns: True if the job is complete, False otherwise
        :raises: libvirt.libvirtError on error fetching block job info
        """
        # NOTE(mdbooth): This method polls for block job completion. It returns
        # true if either we get a status which indicates completion, or there
        # is no longer a record of the job. Ideally this method and its
        # callers would be rewritten to consume libvirt events from the job.
        # This would provide a couple of advantages. Firstly, as it would no
        # longer be polling it would notice completion immediately rather than
        # at the next 0.5s check, and would also consume fewer resources.
        # Secondly, with the current method we only know that 'no job'
        # indicates completion. It does not necessarily indicate successful
        # completion: the job could have failed, or been cancelled. When
        # polling for block job info we have no way to detect this, so we
        # assume success.

        status = self.get_job_info()

        # If the job no longer exists, it is because it has completed
        # NOTE(mdbooth): See comment above: it may not have succeeded.
        if status is None:
            return True

        # NOTE(slaweq): because of bug in libvirt, which is described in
        # http://www.redhat.com/archives/libvir-list/2016-September/msg00017.html
        # if status.end == 0 job is not started yet so it is not finished
        # NOTE(mdbooth): The fix was committed upstream here:
        #   http://libvirt.org/git/?p=libvirt.git;a=commit;h=988218c
        # The earliest tag which contains this commit is v2.3.0-rc1, so we
        # should be able to remove this workaround when MIN_LIBVIRT_VERSION
        # reaches 2.3.0, or we move to handling job events instead.
        # NOTE(lyarwood): Use the mirror element to determine if we can pivot
        # to the new disk once blockjobinfo reports progress as complete.
        if status.end != 0 and status.cur == status.end:
            disk = self._guest.get_disk(self._disk)
            if disk and disk.mirror:
                return disk.mirror.ready == 'yes'

        return False


class BlockDeviceJobInfo(object):
    def __init__(self, job, bandwidth, cur, end):
        """Structure for information about running job.

        :param job: The running job (0 placeholder, 1 pull,
                      2 copy, 3 commit, 4 active commit)
        :param bandwidth: Used in MiB/s
        :param cur: Indicates the position between 0 and 'end'
        :param end: Indicates the position for this operation
        """
        self.job = job
        self.bandwidth = bandwidth
        self.cur = cur
        self.end = end


class JobInfo(object):
    """Information about libvirt background jobs

    This class encapsulates information about libvirt
    background jobs. It provides a mapping from either
    the old virDomainGetJobInfo API which returned a
    fixed list of fields, or the modern virDomainGetJobStats
    which returns an extendable dict of fields.
    """

    _have_job_stats = True

    def __init__(self, **kwargs):

        self.type = kwargs.get("type", libvirt.VIR_DOMAIN_JOB_NONE)
        self.time_elapsed = kwargs.get("time_elapsed", 0)
        self.time_remaining = kwargs.get("time_remaining", 0)
        self.downtime = kwargs.get("downtime", 0)
        self.setup_time = kwargs.get("setup_time", 0)
        self.data_total = kwargs.get("data_total", 0)
        self.data_processed = kwargs.get("data_processed", 0)
        self.data_remaining = kwargs.get("data_remaining", 0)
        self.memory_total = kwargs.get("memory_total", 0)
        self.memory_processed = kwargs.get("memory_processed", 0)
        self.memory_remaining = kwargs.get("memory_remaining", 0)
        self.memory_iteration = kwargs.get("memory_iteration", 0)
        self.memory_constant = kwargs.get("memory_constant", 0)
        self.memory_normal = kwargs.get("memory_normal", 0)
        self.memory_normal_bytes = kwargs.get("memory_normal_bytes", 0)
        self.memory_bps = kwargs.get("memory_bps", 0)
        self.disk_total = kwargs.get("disk_total", 0)
        self.disk_processed = kwargs.get("disk_processed", 0)
        self.disk_remaining = kwargs.get("disk_remaining", 0)
        self.disk_bps = kwargs.get("disk_bps", 0)
        self.comp_cache = kwargs.get("compression_cache", 0)
        self.comp_bytes = kwargs.get("compression_bytes", 0)
        self.comp_pages = kwargs.get("compression_pages", 0)
        self.comp_cache_misses = kwargs.get("compression_cache_misses", 0)
        self.comp_overflow = kwargs.get("compression_overflow", 0)

    @classmethod
    def _get_job_stats_compat(cls, dom):
        # Make the old virDomainGetJobInfo method look similar to the
        # modern virDomainGetJobStats method
        try:
            info = dom.jobInfo()
        except libvirt.libvirtError as ex:
            # When migration of a transient guest completes, the guest
            # goes away so we'll see NO_DOMAIN error code
            #
            # When migration of a persistent guest completes, the guest
            # merely shuts off, but libvirt unhelpfully raises an
            # OPERATION_INVALID error code
            #
            # Lets pretend both of these mean success
            if ex.get_error_code() in (libvirt.VIR_ERR_NO_DOMAIN,
                                       libvirt.VIR_ERR_OPERATION_INVALID):
                LOG.debug("Domain has shutdown/gone away: %s", ex)
                return cls(type=libvirt.VIR_DOMAIN_JOB_COMPLETED)
            else:
                LOG.debug("Failed to get job info: %s", ex)
                raise

        return cls(
            type=info[0],
            time_elapsed=info[1],
            time_remaining=info[2],
            data_total=info[3],
            data_processed=info[4],
            data_remaining=info[5],
            memory_total=info[6],
            memory_processed=info[7],
            memory_remaining=info[8],
            disk_total=info[9],
            disk_processed=info[10],
            disk_remaining=info[11])
