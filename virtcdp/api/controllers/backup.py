# -*- encoding:utf-8 -*-
from pecan import expose

from virtcdp.api.controllers import BaseController
from virtcdp import exception


class BackupController(BaseController):
    _custom_actions = {
        'add': ['POST'],
        'stop': ['POST'],
        'query': ['GET']
    }

    # 下发备份任务
    @expose(template='json')
    def add(self, **kw):
        uuid = kw.get("uuid")
        target_dir = kw.get("target_dir")
        format = kw.get("format", 'qcow2')
        interval = kw.get("interval", 10)
        disk = kw.get("disk")

        if uuid is None:
            raise exception.InvalidUUID(uuid=uuid)
        if target_dir is None:
            raise exception.InvalidParamValue(param="target_dir",
                                              value=target_dir)
        if format not in ("qcow2", "raw"):
            raise exception.InvalidImageFormat(format=format)

        if interval < 1:
            raise exception.InvalidIntervalValue(interval=interval)

        kwargs = {"target_dir": target_dir,
                  "format": format,
                  "disk": disk,
                  "interval": interval}
        rst = self.call('start_backup', uuid, kwargs)
        return rst

    # 查看备份任务
    @expose('json')
    def index(self):
        return

    # 更新备份任务

    # 查看所有备份任务

    # 停止备份任务
    @expose(template='json')
    def stop(self, **kw):
        uuid = kw.get("uuid")
        disk = kw.get("disk")
        if uuid is None:
            raise exception.InvalidUUID(uuid=uuid)

        result = self.call("stop_backup", uuid, disk)
        return result
