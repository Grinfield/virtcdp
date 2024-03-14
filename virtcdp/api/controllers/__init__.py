
from pecan import request


class BaseController(object):
    @staticmethod
    def rpccall(method, *args):
        return request.rpcapi.call(method, *args)
