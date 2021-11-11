
from lxml import etree
import six

from bitcdp import exception
from bitcdp import utils


# Namespace to use for Nova specific metadata items in XML
NOVA_NS = "http://openstack.org/xmlns/libvirt/nova/1.0"


class LibvirtConfigObject(object):

    def __init__(self, **kwargs):
        super(LibvirtConfigObject, self).__init__()

        self.root_name = kwargs.get("root_name")
        self.ns_prefix = kwargs.get('ns_prefix')
        self.ns_uri = kwargs.get('ns_uri')

    def _new_node(self, node_name, **kwargs):
        if self.ns_uri is None:
            return etree.Element(node_name, **kwargs)
        else:
            return etree.Element("{" + self.ns_uri + "}" + node_name,
                                 nsmap={self.ns_prefix: self.ns_uri},
                                 **kwargs)

    def _text_node(self, node_name, value, **kwargs):
        child = self._new_node(node_name, **kwargs)
        child.text = six.text_type(value)
        return child

    def format_dom(self):
        return self._new_node(self.root_name)

    def parse_str(self, xmlstr):
        self.parse_dom(etree.fromstring(xmlstr))

    def parse_dom(self, xmldoc):
        if self.root_name != xmldoc.tag:
            msg = ("Root element name should be '%(name)s' not '%(tag)s'" %
                   {'name': self.root_name, 'tag': xmldoc.tag})
            raise exception.InvalidInput(msg)

    def to_xml(self, pretty_print=True):
        root = self.format_dom()
        xml_str = etree.tostring(root, pretty_print=pretty_print)
        if six.PY3 and isinstance(xml_str, six.binary_type):
            xml_str = xml_str.decode("utf-8")
        return xml_str


class LibvirtConfigGuestDevice(LibvirtConfigObject):

    def __init__(self, **kwargs):
        super(LibvirtConfigGuestDevice, self).__init__(**kwargs)


class LibvirtConfigGuestDisk(LibvirtConfigGuestDevice):

    def __init__(self, **kwargs):
        super(LibvirtConfigGuestDisk, self).__init__(root_name="disk",
                                                     **kwargs)

        self.source_type = "file"
        self.source_device = "disk"
        self.driver_name = None
        self.driver_format = None
        self.driver_cache = None
        self.driver_discard = None
        self.driver_io = None
        self.source_path = None
        self.source_protocol = None
        self.source_name = None
        self.source_hosts = []
        self.source_ports = []
        self.target_dev = None
        self.target_path = None
        self.target_bus = None
        self.auth_username = None
        self.auth_secret_type = None
        self.auth_secret_uuid = None
        self.serial = None
        self.disk_read_bytes_sec = None
        self.disk_read_iops_sec = None
        self.disk_write_bytes_sec = None
        self.disk_write_iops_sec = None
        self.disk_total_bytes_sec = None
        self.disk_total_iops_sec = None
        self.logical_block_size = None
        self.physical_block_size = None
        self.readonly = False
        self.shareable = False
        self.snapshot = None
        self.backing_store = None
        self.device_addr = None
        self.boot_order = None
        self.mirror = None

    def format_dom(self):
        dev = super(LibvirtConfigGuestDisk, self).format_dom()

        dev.set("type", self.source_type)
        dev.set("device", self.source_device)
        if (self.driver_name is not None or
            self.driver_format is not None or
            self.driver_cache is not None or
                self.driver_discard is not None):
            drv = etree.Element("driver")
            if self.driver_name is not None:
                drv.set("name", self.driver_name)
            if self.driver_format is not None:
                drv.set("type", self.driver_format)
            if self.driver_cache is not None:
                drv.set("cache", self.driver_cache)
            if self.driver_discard is not None:
                drv.set("discard", self.driver_discard)
            if self.driver_io is not None:
                drv.set("io", self.driver_io)
            dev.append(drv)

        if self.source_type == "file":
            dev.append(etree.Element("source", file=self.source_path))
        elif self.source_type == "block":
            dev.append(etree.Element("source", dev=self.source_path))
        elif self.source_type == "mount":
            dev.append(etree.Element("source", dir=self.source_path))
        elif self.source_type == "network" and self.source_protocol:
            source = etree.Element("source", protocol=self.source_protocol)
            if self.source_name is not None:
                source.set('name', self.source_name)
            hosts_info = zip(self.source_hosts, self.source_ports)
            for name, port in hosts_info:
                host = etree.Element('host', name=name)
                if port is not None:
                    host.set('port', port)
                source.append(host)
            dev.append(source)

        if self.auth_secret_type is not None:
            auth = etree.Element("auth")
            auth.set("username", self.auth_username)
            auth.append(etree.Element("secret", type=self.auth_secret_type,
                                      uuid=self.auth_secret_uuid))
            dev.append(auth)

        if self.source_type == "mount":
            dev.append(etree.Element("target", dir=self.target_path))
        else:
            dev.append(etree.Element("target", dev=self.target_dev,
                                     bus=self.target_bus))

        if self.serial is not None:
            dev.append(self._text_node("serial", self.serial))

        iotune = etree.Element("iotune")

        if self.disk_read_bytes_sec is not None:
            iotune.append(self._text_node("read_bytes_sec",
                self.disk_read_bytes_sec))

        if self.disk_read_iops_sec is not None:
            iotune.append(self._text_node("read_iops_sec",
                self.disk_read_iops_sec))

        if self.disk_write_bytes_sec is not None:
            iotune.append(self._text_node("write_bytes_sec",
                self.disk_write_bytes_sec))

        if self.disk_write_iops_sec is not None:
            iotune.append(self._text_node("write_iops_sec",
                self.disk_write_iops_sec))

        if self.disk_total_bytes_sec is not None:
            iotune.append(self._text_node("total_bytes_sec",
                self.disk_total_bytes_sec))

        if self.disk_total_iops_sec is not None:
            iotune.append(self._text_node("total_iops_sec",
                self.disk_total_iops_sec))

        if len(iotune) > 0:
            dev.append(iotune)

        # Block size tuning
        if (self.logical_block_size is not None or
                self.physical_block_size is not None):

            blockio = etree.Element("blockio")
            if self.logical_block_size is not None:
                blockio.set('logical_block_size', self.logical_block_size)

            if self.physical_block_size is not None:
                blockio.set('physical_block_size', self.physical_block_size)

            dev.append(blockio)

        if self.readonly:
            dev.append(etree.Element("readonly"))
        if self.shareable:
            dev.append(etree.Element("shareable"))

        if self.boot_order:
            dev.append(etree.Element("boot", order=self.boot_order))

        if self.device_addr:
            dev.append(self.device_addr.format_dom())

        return dev

    def parse_dom(self, xmldoc):
        super(LibvirtConfigGuestDisk, self).parse_dom(xmldoc)

        self.source_type = xmldoc.get('type')
        self.snapshot = xmldoc.get('snapshot')

        for c in xmldoc.getchildren():
            if c.tag == 'driver':
                self.driver_name = c.get('name')
                self.driver_format = c.get('type')
                self.driver_cache = c.get('cache')
                self.driver_discard = c.get('discard')
                self.driver_io = c.get('io')
            elif c.tag == 'source':
                if self.source_type == 'file':
                    self.source_path = c.get('file')
                elif self.source_type == 'block':
                    self.source_path = c.get('dev')
                elif self.source_type == 'mount':
                    self.source_path = c.get('dir')
                elif self.source_type == 'network':
                    self.source_protocol = c.get('protocol')
                    self.source_name = c.get('name')
                    for sub in c.getchildren():
                        if sub.tag == 'host':
                            self.source_hosts.append(sub.get('name'))
                            self.source_ports.append(sub.get('port'))

            elif c.tag == 'serial':
                self.serial = c.text
            elif c.tag == 'target':
                if self.source_type == 'mount':
                    self.target_path = c.get('dir')
                else:
                    self.target_dev = c.get('dev')

                self.target_bus = c.get('bus', None)
            elif c.tag == 'backingStore':
                b = LibvirtConfigGuestDiskBackingStore()
                b.parse_dom(c)
                self.backing_store = b
            elif c.tag == 'readonly':
                self.readonly = True
            elif c.tag == 'shareable':
                self.shareable = True
            elif c.tag == 'address':
                obj = LibvirtConfigGuestDeviceAddress.parse_dom(c)
                self.device_addr = obj
            elif c.tag == 'boot':
                self.boot_order = c.get('order')
            elif c.tag == 'mirror':
                m = LibvirtConfigGuestDiskMirror()
                m.parse_dom(c)
                self.mirror = m


class LibvirtConfigGuestDiskMirror(LibvirtConfigObject):

    def __init__(self, **kwargs):
        super(LibvirtConfigGuestDiskMirror, self).__init__(**kwargs)
        self.ready = None

    def parse_dom(self, xmldoc):
        self.ready = xmldoc.get('ready')


class LibvirtConfigGuestDeviceAddress(LibvirtConfigObject):
    def __init__(self, type=None, **kwargs):
        super(LibvirtConfigGuestDeviceAddress, self).__init__(
            root_name='address', **kwargs)
        self.type = type

    def format_dom(self):
        xml = super(LibvirtConfigGuestDeviceAddress, self).format_dom()
        xml.set("type", self.type)
        return xml

    @staticmethod
    def parse_dom(xmldoc):
        addr_type = xmldoc.get('type')
        if addr_type == 'pci':
            obj = LibvirtConfigGuestDeviceAddressPCI()
        elif addr_type == 'drive':
            obj = LibvirtConfigGuestDeviceAddressDrive()
        else:
            return None
        obj.parse_dom(xmldoc)
        return obj


class LibvirtConfigGuestDeviceAddressDrive(LibvirtConfigGuestDeviceAddress):
    def __init__(self, **kwargs):
        super(LibvirtConfigGuestDeviceAddressDrive, self).\
                __init__(type='drive', **kwargs)
        self.controller = None
        self.bus = None
        self.target = None
        self.unit = None

    def format_dom(self):
        xml = super(LibvirtConfigGuestDeviceAddressDrive, self).format_dom()

        if self.controller is not None:
            xml.set("controller", str(self.controller))
        if self.bus is not None:
            xml.set("bus", str(self.bus))
        if self.target is not None:
            xml.set("target", str(self.target))
        if self.unit is not None:
            xml.set("unit", str(self.unit))

        return xml

    def parse_dom(self, xmldoc):
        self.controller = xmldoc.get('controller')
        self.bus = xmldoc.get('bus')
        self.target = xmldoc.get('target')
        self.unit = xmldoc.get('unit')

    def format_address(self):
        return None


class LibvirtConfigGuestDeviceAddressPCI(LibvirtConfigGuestDeviceAddress):
    def __init__(self, **kwargs):
        super(LibvirtConfigGuestDeviceAddressPCI, self).\
                __init__(type='pci', **kwargs)
        self.domain = None
        self.bus = None
        self.slot = None
        self.function = None

    def format_dom(self):
        xml = super(LibvirtConfigGuestDeviceAddressPCI, self).format_dom()

        if self.domain is not None:
            xml.set("domain", str(self.domain))
        if self.bus is not None:
            xml.set("bus", str(self.bus))
        if self.slot is not None:
            xml.set("slot", str(self.slot))
        if self.function is not None:
            xml.set("function", str(self.function))

        return xml

    def parse_dom(self, xmldoc):
        self.domain = xmldoc.get('domain')
        self.bus = xmldoc.get('bus')
        self.slot = xmldoc.get('slot')
        self.function = xmldoc.get('function')

    def format_address(self):
        if self.domain is not None:
            return utils.get_pci_address(self.domain[2:],
                                             self.bus[2:],
                                             self.slot[2:],
                                             self.function[2:])


class LibvirtConfigGuestDiskBackingStore(LibvirtConfigObject):
    def __init__(self, **kwargs):
        super(LibvirtConfigGuestDiskBackingStore, self).__init__(
            root_name="backingStore", **kwargs)

        self.index = None
        self.source_type = None
        self.source_file = None
        self.source_protocol = None
        self.source_name = None
        self.source_hosts = []
        self.source_ports = []
        self.driver_name = None
        self.driver_format = None
        self.backing_store = None

    def parse_dom(self, xmldoc):
        super(LibvirtConfigGuestDiskBackingStore, self).parse_dom(xmldoc)

        self.source_type = xmldoc.get('type')
        self.index = xmldoc.get('index')

        for c in xmldoc.getchildren():
            if c.tag == 'driver':
                self.driver_name = c.get('name')
                self.driver_format = c.get('type')
            elif c.tag == 'source':
                self.source_file = c.get('file')
                self.source_protocol = c.get('protocol')
                self.source_name = c.get('name')
                for d in c.getchildren():
                    if d.tag == 'host':
                        self.source_hosts.append(d.get('name'))
                        self.source_ports.append(d.get('port'))
            elif c.tag == 'backingStore':
                if c.getchildren():
                    self.backing_store = LibvirtConfigGuestDiskBackingStore()
                    self.backing_store.parse_dom(c)


class LibvirtConfigGuest(LibvirtConfigObject):

    def __init__(self, **kwargs):
        super(LibvirtConfigGuest, self).__init__(root_name="domain",
                                                 **kwargs)

        self.virt_type = None
        self.uuid = None
        self.name = None
        self.memory = 500 * utils.UNITS.Mi
        self.membacking = None
        self.memtune = None
        self.numatune = None
        self.vcpus = 1
        self.cpuset = None
        self.cpu = None
        self.cputune = None
        self.features = []
        self.clock = None
        self.sysinfo = None
        self.os_type = None
        self.os_loader = None
        self.os_loader_type = None
        self.os_kernel = None
        self.os_initrd = None
        self.os_cmdline = None
        self.os_root = None
        self.os_init_path = None
        self.os_boot_dev = []
        self.os_smbios = None
        self.os_mach_type = None
        self.os_bootmenu = False
        self.devices = []
        self.metadata = []
        self.idmaps = []
        self.perf_events = []

    def _format_basic_props(self, root):
        root.append(self._text_node("uuid", self.uuid))
        root.append(self._text_node("name", self.name))
        root.append(self._text_node("memory", self.memory))
        if self.membacking is not None:
            root.append(self.membacking.format_dom())
        if self.memtune is not None:
            root.append(self.memtune.format_dom())
        if self.numatune is not None:
            root.append(self.numatune.format_dom())
        if self.cpuset is not None:
            vcpu = self._text_node("vcpu", self.vcpus)
            vcpu.set("cpuset", utils.format_cpu_spec(self.cpuset))
            root.append(vcpu)
        else:
            root.append(self._text_node("vcpu", self.vcpus))

        if len(self.metadata) > 0:
            metadata = etree.Element("metadata")
            for m in self.metadata:
                metadata.append(m.format_dom())
            root.append(metadata)

    def _format_os(self, root):
        os = etree.Element("os")
        type_node = self._text_node("type", self.os_type)
        if self.os_mach_type is not None:
            type_node.set("machine", self.os_mach_type)
        os.append(type_node)
        if self.os_kernel is not None:
            os.append(self._text_node("kernel", self.os_kernel))
        if self.os_loader is not None:
            # Generate XML nodes for UEFI boot.
            if self.os_loader_type == "pflash":
                loader = self._text_node("loader", self.os_loader)
                loader.set("type", "pflash")
                loader.set("readonly", "yes")
                os.append(loader)
            else:
                os.append(self._text_node("loader", self.os_loader))
        if self.os_initrd is not None:
            os.append(self._text_node("initrd", self.os_initrd))
        if self.os_cmdline is not None:
            os.append(self._text_node("cmdline", self.os_cmdline))
        if self.os_root is not None:
            os.append(self._text_node("root", self.os_root))
        if self.os_init_path is not None:
            os.append(self._text_node("init", self.os_init_path))

        for boot_dev in self.os_boot_dev:
            os.append(etree.Element("boot", dev=boot_dev))

        if self.os_smbios is not None:
            os.append(self.os_smbios.format_dom())

        if self.os_bootmenu:
            os.append(etree.Element("bootmenu", enable="yes"))
        root.append(os)

    def _format_features(self, root):
        if len(self.features) > 0:
            features = etree.Element("features")
            for feat in self.features:
                features.append(feat.format_dom())
            root.append(features)

    def _format_devices(self, root):
        if len(self.devices) == 0:
            return
        devices = etree.Element("devices")
        for dev in self.devices:
            devices.append(dev.format_dom())
        root.append(devices)

    def _format_idmaps(self, root):
        if len(self.idmaps) == 0:
            return
        idmaps = etree.Element("idmap")
        for idmap in self.idmaps:
            idmaps.append(idmap.format_dom())
        root.append(idmaps)

    def _format_perf_events(self, root):
        if len(self.perf_events) == 0:
            return
        perfs = etree.Element("perf")
        for pe in self.perf_events:
            event = etree.Element("event", name=pe, enabled="yes")
            perfs.append(event)
        root.append(perfs)

    def format_dom(self):
        root = super(LibvirtConfigGuest, self).format_dom()

        root.set("type", self.virt_type)

        self._format_basic_props(root)

        if self.sysinfo is not None:
            root.append(self.sysinfo.format_dom())

        self._format_os(root)
        self._format_features(root)

        if self.cputune is not None:
            root.append(self.cputune.format_dom())

        if self.clock is not None:
            root.append(self.clock.format_dom())

        if self.cpu is not None:
            root.append(self.cpu.format_dom())

        self._format_devices(root)

        self._format_idmaps(root)

        self._format_perf_events(root)

        return root

    def parse_dom(self, xmldoc):
        # Note: This cover only for: LibvirtConfigGuestDisks
        for c in xmldoc.getchildren():
            if c.tag == 'devices':
                for d in c.getchildren():
                    if d.tag == 'disk':
                        obj = LibvirtConfigGuestDisk()
                        obj.parse_dom(d)
                        self.devices.append(obj)

    def add_device(self, dev):
        self.devices.append(dev)

    def add_perf_event(self, event):
        self.perf_events.append(event)

    def set_clock(self, clk):
        self.clock = clk
