
import logging
import os
import string
import six
if six.PY2:
    from SimpleXMLRPCServer import list_public_methods
else:
    from xmlrpc.server import list_public_methods

from virtcdp import exception
from virtcdp.libvirt import driver


LOG = logging.getLogger(__name__)


class BackupConductor(object):

    def __init__(self):
        # make all of the string functions available through
        # string.func_name
        self.string = string
        self.driver = driver.LibvirtDriver()
        self.driver.init_host()

    def _dispatch(self, method, param):
        LOG.debug("Calling method: %s, params: %s.", method, param)
        func = getattr(self, method)
        return func(*param)

    def _listMethods(self):
        # implement this method so that system.listMethods
        # knows to advertise the strings methods
        return list_public_methods(self) + \
               ['string.' + method for method in list_public_methods(self.string)]

    @exception.wrap_exception()
    def start_backup(self, uuid, kwargs):
        """start backup task"""
        kw = {"targetdir": kwargs.get("target_dir"),
              "format": kwargs.get("format"),
              "disk": kwargs.get("disk"),
              "interval": kwargs.get("interval")}
        LOG.info("Starting backup for instance %s...", uuid)
        ret = self.driver.drive_backup(uuid, **kw)
        return ret

    @exception.wrap_exception()
    def stop_backup(self, uuid, disk):
        """stop backup task"""

        LOG.debug("Going to stop backup %s", uuid)
        ret = self.driver.stop_backup(uuid, disk)
        return ret

    @exception.wrap_exception()
    def restore(self, uuid, kwargs):
        """Restore image from backup data"""
        # TODO: disk should change to 'vda, vdb...
        disk = kwargs.get("disk")
        data_dir = kwargs.get("data_dir")
        # date time format: "2021-12-05T17:45:08+0800"
        util_ts = kwargs.get("util")
        restore_dir = kwargs.get("restore_dir")

        if not os.path.exists(data_dir):
            LOG.error("Data directory input '%s' does not exist,"
                      " please do further check.", data_dir)
            raise exception.InvalidInput(
                reason="Data directory %s doesn't exist." % data_dir)

        if restore_dir and not os.path.exists(restore_dir):
            LOG.error("Restore directory input '%s' does not exist,"
                      " please do further check.", restore_dir)
            raise exception.InvalidInput(
                reason="Restore directory %s doesn't exist." % restore_dir)

        LOG.debug("Going to restore image from backed data.")
        try:
            ret = self.driver.drive_restore(uuid, data_dir, util_ts,
                                            disk=disk,
                                            restore_dir=restore_dir)
        except Exception as e:
            LOG.error("Failed to restore: %s.", e)
            raise e

        return ret

    def stop_restore(self):
        # stop restore task
        pass
