
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
        uuid = '4b371dcb-ee9c-4109-b2eb-5768e6d3c84a'
        ret = self.driver.drive_backup(uuid, disk=None)
        return ret

    def stop_backup(self, uuid):
        # stop backup task
        pass

    def restore(self, *args, **kwargs):
        # restore from backup data
        pass

    def stop_restore(self):
        # stop restore task
        pass
