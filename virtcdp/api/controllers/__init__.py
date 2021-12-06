
from pecan import request


class BaseController(object):
    @staticmethod
    def call(method, *args):
        return request.rpcapi.call(method, *args)
