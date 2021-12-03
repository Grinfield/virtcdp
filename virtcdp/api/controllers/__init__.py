
from pecan import request


class BaseController(object):
    @staticmethod
    def call(method, *args, **kwargs):
        return request.rpcapi.call(method, *args, **kwargs)
