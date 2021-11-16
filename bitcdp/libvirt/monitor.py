
from collections import namedtuple
import json
import logging

import libvirt
import libvirt_qemu
from libvirt_qemu import qemuMonitorCommand

from bitcdp import exception

LOG = logging.getLogger(__name__)


class QMPCmd(object):
    @staticmethod
    def tojson(qmp_cmd):
        return json.dumps(qmp_cmd)

    @staticmethod
    def makecmd(cmd, cmdid=None, *args, **kwargs):
        qmp_cmd = {'execute': cmd}
        if kwargs:
            qmp_cmd['arguments'] = kwargs
        if cmdid:
            qmp_cmd['id'] = cmdid
        return QMPCmd.tojson(qmp_cmd)


class QemuMonitor(object):
    """
    Send an arbitrary command to domain via Libvirt method `qemuMonitorCommand`
    through Qemu Monitor Protocol.
    """

    def __init__(self, domain):
        self._domain = domain
        self._conn = None

    def qmp_cmd(self, cmd, cmdid=None, **kwargs):
        LOG.debug("->>> domain '%s' command '%s'.", self._domain.UUIDString(), cmd)
        LOG.debug("->>> args: %s", kwargs)

        try:
            result = qemuMonitorCommand(self._domain,
                                        QMPCmd.makecmd(cmd, cmdid, **kwargs),
                                        flags=libvirt_qemu.VIR_DOMAIN_QEMU_MONITOR_COMMAND_DEFAULT)
            print "<<<- Get result: %s." % result
            LOG.debug("<<<- Get result: %s.", result)

            if isinstance(result, str):
                result = json.loads(result)

            if "error" in result:
                LOG.error("<<<- Qemu monitor command failure, class: %(class)s,"
                          " desc: %(desc)s.",
                          {"class": result["error"]["class"],
                           "desc": result["error"]["desc"]})
                raise exception.QemuMonitorCommandError(
                    cmd=cmd,
                    domain=self._domain.UUIDString(),
                    args=kwargs,
                    cls=result["error"]["class"],
                    desc=result['error']['desc']
                )

        except libvirt.libvirtError as e:
            LOG.exception("Libvirt error occurred via QMP, "
                          "domain: %(dom)s, command: %(cmd)s, args: %(args)s",
                          {"dom": self._domain.UUIDString(), "cmd": cmd, "args": kwargs})
            raise e
        except Exception as e:
            raise e

        return result['return']

    @staticmethod
    def transaction_action(action, **kwargs):
        return {
            'type': action,
            'data': dict((k.replace('_', '-'), v) for k, v in kwargs.iteritems())
        }

    def transaction_bitmap_clear(self, node, name, **kwargs):
        """Return transaction action object for bitmap clear """
        return self.transaction_action('block-dirty-bitmap-clear',
                                       node=node,
                                       name=name,
                                       **kwargs)

    def transaction_bitmap_add(self, node, name, **kwargs):
        """Return transaction action object for bitmap add """
        return self.transaction_action('block-dirty-bitmap-add',
                                       node=node,
                                       name=name,
                                       **kwargs)

    def full_backup_with_bitmap(self, dev, target, format="qcow2", sync="full"):
        actions = []
        bitmap = "bitcdp-%s" % dev.node
        if dev.has_bitmap:
            actions.append(self.transaction_bitmap_clear(dev.node, bitmap))
        else:
            actions.append(self.transaction_bitmap_add(dev.node, bitmap))

        actions.append(self.transaction_action("drive-backup",
                                               device=dev.node,
                                               target=target,
                                               format=format,
                                               sync=sync))
        # actions.append(self.transaction_bitmap_clear(dev, dev.bitmap))

        reply = self.qmp_cmd("transaction", actions=actions)

        return reply

    def inc_backup(self, dev, target, format="qcow2", sync="incremental"):
        bitmap = "bitcdp-%s" % dev.node
        if not dev.has_bitmap:
            raise exception.IncBackupNoBitmapException(
                dev=dev.node,
                uuid=self._domain.UUIDString())

        kwargs = {"device": dev.node,
                  "target": target,
                  "format": format,
                  "bitmap": bitmap,
                  "sync": sync}
        reply = self.qmp_cmd("drive-backup", **kwargs)

        return reply

    def query_block(self):
        blockdevs = self._query_block()
        if not blockdevs:
            LOG.error("Instance %(instance_id)s has no any block device suitable for backup.",
                      {"instance_id": self._domain.UUIDString()})
            raise exception.NoBlockdevsFound(instance_id=self._domain.UUIDString())

        for dev in blockdevs:
            if dev.has_bitmap is True:
                state = self._check_bitmap_state(dev.node, dev.bitmaps)
                if state is not True:
                    LOG.warn("Bitmap for device %(device)s is in state %(state)s.",
                             {"device": dev.node,
                              "state": state})
        return blockdevs

    def _query_block(self):
        ret = self.qmp_cmd("query-block")
        return self._get_block_devices(ret)

    @staticmethod
    def _check_bitmap_state(node, bitmaps):
        """
        Check if the bitmap state is ready for backup
            active  -> Ready for backup
            frozen  -> backup in progress
            disabled-> migration might be going on
        """
        for bitmap in bitmaps:
            LOG.debug('Node %s, Bitmap: %s', node, bitmap)
            match = "%s-%s" % ('bitcdp', node)
            if bitmap["name"] == match and bitmap['status'] == "active":
                return True

    @staticmethod
    def _get_block_devices(blockinfo):
        """Get a list of block devices that we can create a bitmap for,
           currently we only get inserted qcow based images
        """
        BlockDev = namedtuple('BlockDev', [
            'node', 'format', 'filename', 'backing_image', 'has_bitmap', 'bitmaps'
        ])
        blockdevs = []

        for device in blockinfo:
            backing_image = False
            has_bitmap = False
            bitmaps = None

            try:
                inserted = device['inserted']
                # if inserted['drv'] == 'raw':
                #     continue

                try:
                    if len(device['dirty-bitmaps']) > 0:
                        has_bitmap = True
                        bitmaps = device['dirty-bitmaps']
                except KeyError:
                    pass

                try:
                    bi = inserted['image']['backing-image']
                    backing_image = True
                except KeyError:
                    pass

                blockdevs.append(BlockDev(
                    device['device'],
                    inserted['image']['format'],
                    inserted['image']['filename'],
                    backing_image,
                    has_bitmap,
                    bitmaps)
                )
            except KeyError:
                continue

        if len(blockdevs) == 0:
            return None

        return blockdevs
