import logging
import threading

import libvirt
from eventlet import tpool

from virtcdp import exception
from virtcdp import config
from virtcdp.libvirt import guest

LOG = logging.getLogger(__name__)
CONF = config.CONF


class LibvirtConnection(object):

    def __init__(self, read_only=False,
                 conn_event_handler=None,
                 conn_event_handler_queue=None,):
        self.uri = self._uri()
        self._read_only = read_only
        self._initial_connection = True

        self._conn_event_handler = conn_event_handler
        self._conn_event_handler_queue = conn_event_handler_queue

        self._conn = None
        self._conn_lock = threading.Lock()

    @staticmethod
    def _uri():
        uri = CONF.libvirt.connection_uri or 'qemu:///system'
        return uri

    @staticmethod
    def _connect_auth_cb(creds, opaque):
        if len(creds) == 0:
            return 0
        raise exception.InternalError(
            "Can not handle authentication request for %d credentials"
            % len(creds))

    def _connect(self):
        auth = [[libvirt.VIR_CRED_AUTHNAME,
                 libvirt.VIR_CRED_ECHOPROMPT,
                 libvirt.VIR_CRED_REALM,
                 libvirt.VIR_CRED_PASSPHRASE,
                 libvirt.VIR_CRED_NOECHOPROMPT,
                 libvirt.VIR_CRED_EXTERNAL],
                self._connect_auth_cb,
                None]

        flags = 0
        if self._read_only:
            flags = libvirt.VIR_CONNECT_RO
        # tpool.proxy_call creates a native thread. Due to limitations
        # with eventlet locking we cannot use the logging API inside
        # the called function.
        return tpool.proxy_call(
            (libvirt.virDomain, libvirt.virConnect),
            libvirt.openAuth, self.uri, auth, flags)

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

    def _get_new_connection(self):
        # call with _conn_lock held
        LOG.debug('Connecting to libvirt: %s', self.uri)

        # This will raise an exception on failure
        # conn = self._connect(self._uri, self._read_only)
        conn = self._connect()

        return conn

    def _get_connection(self):
        # multiple concurrent connections are protected by _conn_lock
        with self._conn_lock:
            # Drop the existing connection if it is not usable
            if (self._conn is not None and
                    not self._test_connection(self._conn)):
                self._conn = None
                # Connection was previously up, and went down
                self._queue_conn_event_handler(
                    False, 'Connection to libvirt lost')

            if self._conn is None:
                try:
                    # This will raise if it fails to get a connection
                    self._conn = self._get_new_connection()
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

        return self._conn

    def get_connection(self):
        """Returns a connection to the hypervisor

        This method should be used to create and return a well
        configured connection to the hypervisor.

        :returns: a libvirt.virConnect object
        """
        try:
            conn = self._get_connection()
        except libvirt.libvirtError as ex:
            LOG.exception("Connection to libvirt failed: %s", ex)
            raise exception.HypervisorUnavailable(host=CONF.host)

        return conn

    def close_connection(self):
        conn = self.get_connection()
        if conn:
            LOG.info("Closing connection %s", conn)
            conn.close()
        return

    def get_domain(self, uuid):
        """Retrieve libvirt domain object for an instance.

        :param uuid: a libvirt domain uuid

        :returns: a libvirt.Domain object
        :raises exception.InstanceNotFound: The domain was not found
        :raises exception.InternalError: A libvirt error occured
        """
        try:
            conn = self.get_connection()
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

    def get_guest(self, uuid):
        return guest.Guest(self.get_domain(uuid))

    def _queue_conn_event_handler(self, *args, **kwargs):
        if self._conn_event_handler is None:
            return

        def handler():
            return self._conn_event_handler(*args, **kwargs)

        self._conn_event_handler_queue.put(handler)

    def dispatch_conn_closed_event(self, conn, event):
        """
        dispatch connection closed abruptly event
        :param conn:
        :param event:
        """
        with self._conn_lock:
            if conn == self._conn:
                reason = str(event['reason'])
                msg = "Connection to libvirt lost: %s" % reason
                self._conn = None
                self._queue_conn_event_handler(False, msg)
