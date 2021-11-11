
import json
import logging
import six

import libvirt
import libvirt_qemu
from libvirt_qemu import qemuMonitorCommand
from eventlet import patcher

from bitcdp import exception
from bitcdp.libvirt import event as virtevent

LOG = logging.getLogger(__name__)
native_Queue = patcher.original("Queue" if six.PY2 else "queue")


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
    def __init__(self):
        event_q = None

    def sendCommand(self, dom, cmd, cmdid=None, *args, **kwargs):
        LOG.debug("->>> domain '%s' command '%s'.", dom.UUIDString, cmd)
        LOG.debug("->>> args: %s", kwargs)

        try:
            result = qemuMonitorCommand(dom,
                                        QMPCmd.makecmd(cmd, cmdid, **kwargs),
                                        flags=libvirt_qemu.VIR_DOMAIN_QEMU_MONITOR_COMMAND_DEFAULT)
            print "<<<- Get result: %s." % result
            LOG.debug("<<<- Get result: %s.", result)

            if isinstance(result, str):
                result = json.loads(result)

            if "error" in result:
                LOG.error("<<<- Qemu monitor command failure: class %(class)s, %(desc)s.",
                          {"class": result["error"]["class"],
                           "desc": result["error"]["desc"]})
                raise exception.QemuMonitorCommandError(
                    cmd=cmd,
                    domain=dom.UUIDString,
                    args=kwargs,
                    desc=(result['error']['desc'])
                )

        except libvirt.libvirtError as e:
            LOG.exception("Libvirt error occurred via QMP, "
                          "domain: %(dom)s, command: %(cmd)s, args: %(args)s",
                          {"dom": dom.UUIDString, "cmd": cmd, "args": kwargs})
            raise e
        except Exception as e:
            raise e

        return result['return']

    def transaction_command(self):
        pass

    def register_qemu_monitor_event(self, dom=None, event_q=None):
        opaque = event_q
        conn = dom.connect()
        try:
            LOG.info("Registering for qemu monitor events: %s for domain %s",
                     self, dom.UUIDString())
            id = libvirt_qemu.qemuMonitorEventRegister(
                conn,
                dom,
                None,
                self._qemu_monitor_event_callback,
                opaque
            )
        except Exception as e:
            LOG.warning("URI %(uri)s does not support connection"
                        " events: %(error)s",
                        {'uri': self._uri, 'error': e})

        return id

    def _qemu_monitor_event_callback(self, conn, dom, event,
                                     seconds, micros, details,
                                     opaque):
        qemu_mon_q = opaque
        if qemu_mon_q is None:
            return

        uuid = dom.UUIDString()
        job_status = event
        # if event == libvirt.VIR_DOMAIN_JOB_COMPLETED:
        #     job_status = libvirt.VIR_DOMAIN_JOB_COMPLETED

        # if job_status is not None:
        #     qm_event = virtevent.QemuMonitorEvent(uuid, job_status, seconds)
        #     qemu_monitor_queue.put(qm_event)

        qm_event = virtevent.QemuMonitorEvent(uuid, job_status, seconds, details)
        qemu_mon_q.put(qm_event)
        # LOG.debug("qemu_monitor_event_callback Queue: %s, EVENT: "
        #           "Domain %s(%s) %s, DETAILS:%s" %
        #           (opaque, dom.name(), dom.ID(), qm_event.get_status(),
        #            qm_event.get_details()))

    def deregister_qemu_monitor_event(self, domain, callback_id):
        conn = domain.connect()
        try:
            LOG.info("Deregistering for qemu monitor events callback: %s",
                     callback_id)
            libvirt_qemu.qemuMonitorEventDeregister(conn, callback_id)
        except Exception as ex:
            LOG.exception("Failed to deregister qemu monitor event "
                          "callback id %(id)s, error: %(error)s",
                          {'id': callback_id, 'error': ex})
