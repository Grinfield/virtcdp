# -*- encoding:utf-8 -*-
from pecan import expose

from virtcdp.api.controllers import BaseController
from virtcdp import exception
from virtcdp import utils


class RestoreController(BaseController):
    _custom_actions = {
        'add': ['POST']
    }

    # 执行恢复任务
    @expose(template='json')
    def add(self, **kwargs):
        uuid = kwargs.get("uuid")
        disk = kwargs.get("disk")
        data_dir = kwargs.get("data_dir")
        # date time format: "2021-12-05T17:45:08+0800"
        dt_str = kwargs.get("datetime")
        restore_dir = kwargs.get("restore_dir")
        if uuid is None:
            raise exception.InvalidUUID(uuid=uuid)
        if data_dir is None:
            raise exception.InvalidParamValue(param="target_dir",
                                              value=data_dir)

        util_ts = None
        try:
            if dt_str:
                util_ts = utils.convert_datetime_to_ts(dt_str)
        except ValueError as e:
            msg = "Param 'datetime' %(dt)s is not ISO 8601 format: %(exc)s" \
                  % {"dt": dt_str, "exc": str(e)}
            raise exception.InvalidInput(reason=msg)

        kw = {
            "data_dir": data_dir,
            "util": util_ts,
            "restore_dir": restore_dir,
            "disk": disk
        }
        rst = self.call('restore', uuid, kw)
        return rst

    # 停止恢复任务

    # 查看恢复任务
    @expose('json')
    def index(self):
        return

    # 查看所有的恢复任务

    # 更新恢复任务
