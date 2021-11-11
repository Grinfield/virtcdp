# -*- encoding:utf-8 -*-
from pecan import expose

from bitcdp.api.controllers import BaseController


class BackupController(BaseController):

    # 下发备份任务

    # 查看备份任务
    @expose('json')
    def index(self):
        rst = self.call('backup', 'instanceA', 'disk1')
        print 'rst: %s' % rst
        dom1 = {'instance1': {'disk': rst}}
        return dom1

    # 更新备份任务

    # 查看所有备份任务

    # 停止备份任务
