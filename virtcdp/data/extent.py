
import logging
import six

LOG = logging.getLogger(__name__)


class Extent(object):
    def __init__(self):
        self.data = False
        self.zero = False
        self.length = 0
        self.offset = 0

    def __repr__(self):
        return "<data: %s, zero: %s, length: %s, offset: %s>" % \
               (self.data, self.zero, self.length, self.offset)


class ExtentHandler(object):

    def __init__(self, driver):
        self.driver = driver

    def query_extents(self, target):
        """Use qemu-img map to query extents from nbd
        server
        """
        extents = []
        for extent in self.driver.map(target):
            ext = Extent()
            ext.data = bool(extent["data"])
            ext.zero = bool(extent["zero"])
            ext.offset = extent["start"]
            ext.length = extent["length"]
            LOG.debug("Got ext: %s", six.text_type(ext))

            extents.append(ext)

        LOG.info("Got %s extents from qemu command.", len(extents))

        return extents

    def get_size(self, target):
        return self.driver.get_size(target)
