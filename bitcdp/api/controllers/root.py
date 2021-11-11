

from pecan import expose, request

from bitcdp.api.controllers import backup
from bitcdp.api.controllers import restore


class RootController(object):

    bkp = backup.BackupController()
    rst = restore.RestoreController()

    @expose('json')
    def index(self):
        base_url = request.application_url
        versions = {'versions': {'values': base_url}}
        return versions
