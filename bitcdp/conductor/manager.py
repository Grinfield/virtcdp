
import logging
import string
from SimpleXMLRPCServer import list_public_methods

from bitcdp.libvirt import driver

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
        # stop backup task
        uuid = 'e50874d7-3cb8-4b51-aba2-a76ab270d656'
        disk = "drive-ide0-0-0"

        LOG.debug("Going to stop backup %s", uuid)
        ret = self.driver.stop_backup(uuid, disk)
        return ret

    def restore(self, *args, **kwargs):
        # restore from backup data
        pass

    def stop_restore(self):
        # stop restore task
        pass
