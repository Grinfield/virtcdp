# -*- encoding:utf-8 -*-
from pecan import expose

from virtcdp.api.controllers import BaseController


class RestoreController(BaseController):

    # 执行恢复任务

    # 停止恢复任务

    # 查看恢复任务
    @expose('json')
    def index(self):
        rst = self.call('restore', 'instanceA', 'disk1')
        dom1 = {'instance1': {'disk': rst}}
        return dom1

    # 查看所有的恢复任务

    # 更新恢复任务


class MergeController(object):
    pass
