
from pecan import expose, request

from virtcdp.api.controllers import backup as bkp
from virtcdp.api.controllers import restore as rst


class RootController(object):

    backup = bkp.BackupController()
    restore = rst.RestoreController()

    @expose('json')
    def index(self):
        base_url = request.application_url
        versions = {'versions': {'values': base_url}}
        return versions
