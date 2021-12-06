import logging
import nbd

LOG = logging.getLogger(__name__)


class NBDClient(object):
    """Helper functions for NBD"""

    def __init__(self, export_name, meta_context, backup_socket):
        """ Parameters:
        :exportName: name of nbd export
        :backupSocket: ndb server endpoint
        """
        self._socket = backup_socket
        self._export = export_name
        if meta_context is None:
            self._metaContext = nbd.CONTEXT_BASE_ALLOCATION
        else:
            self._metaContext = meta_context

        self.max_req_size = 33554432  # default 32MB
        self.min_req_size = 65536

        self._connectionHandle = None

        self._nbd_handle = nbd.NBD()

        self.version()

    def version(self):
        LOG.info("libnbd version: %s", nbd.__version__)

    def get_block_info(self):
        """Read maximum request/block size as advertised by the nbd
        server. This is the value which will then be used by default
        """
        max_size = self._nbd_handle.get_block_size(nbd.SIZE_MAXIMUM)
        if max_size != 0:
            self.max_req_size = max_size

        LOG.info("Using Maximum Block size supported by nbd server: %s", max_size)

    def block_step(self, offset, length, max_req_size):
        """Process block and ensure to not exceed the maximum request size
        from NBD server.

        If length parameter is dict, compression was enabled during
        backup, thus we cannot use the offsets and sizes for the
        original data, but must use the compressed offsets and sizes
        to read the correct lz4 frames from the stream.
        """
        block_offset = offset
        if isinstance(length, dict):
            item = next(iter(length))
            for step in length[item]:
                block_offset += step
                yield step, block_offset
        else:
            while block_offset < offset + length:
                blocklen = min(offset + length - block_offset, max_req_size)
                yield blocklen, block_offset
                block_offset += blocklen

    def connect(self):
        """Setup connection to NBD server endpoint, return
        connection handle
        """
        try:
            self._nbd_handle.add_meta_context(self._metaContext)
            self._nbd_handle.set_export_name(self._export)
            self._nbd_handle.connect_unix(self._socket)
        except Exception as e:
            LOG.exception("Unable to connect ndb server:")
            raise e

        self.get_block_info()

        return self._nbd_handle

    def disconnect(self):
        self._nbd_handle.shutdown()
