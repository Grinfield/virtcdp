
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
from virtcdp import utils


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

    def backup(self, uuid, *args):
        # name = 'cirros_test1'
        uuid = 'e50874d7-3cb8-4b51-aba2-a76ab270d656'
        targetdir = "/tmp/backup/"
        format = "qcow2"
        interval = 10

        kwargs = {"targetdir": targetdir,
                  "format": format,
                  "disk": None,
                  "interval": interval}
        ret = self.driver.drive_backup(uuid, **kwargs)
        return ret

    def stop_backup(self, uuid):
        """stop backup task"""

        uuid = 'e50874d7-3cb8-4b51-aba2-a76ab270d656'
        disk = "drive-ide0-0-0"

        LOG.debug("Going to stop backup %s", uuid)
        ret = self.driver.stop_backup(uuid, disk)
        return ret

    def restore(self, *args, **kwargs):
        """restore from backup data"""

        uuid = 'e50874d7-3cb8-4b51-aba2-a76ab270d656'
        # TODO: disk should change to 'vda, vdb...
        disk = "drive-ide0-0-0"
        data_dir = "/tmp/backup/"
        dt_str = "2021-12-03T17:45:08+0800"
        restore_dir = "/tmp/restore/"

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

        util_ts = utils.convert_datetime_to_ts(dt_str)

        LOG.debug("Going to restore image from backed data.")
        try:
            ret = self.driver.drive_restore(uuid, data_dir, util_ts,
                                            disk=disk, restore_dir=restore_dir)
        except Exception as e:
            LOG.error("Failed to restore: %s.", e)
            raise e

        return ret

    def stop_restore(self):
        # stop restore task
        pass
