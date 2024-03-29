
import six
if six.PY2:
    import xmlrpclib as client
else:
    from xmlrpc import client

from oslo_config import cfg

CONF = cfg.CONF


class RPCApi(object):
    def __init__(self, context=None, topic=None, server=None,
                 timeout=None):
        # self._context = context
        # if topic is None:
        #     topic = ''
        # target = messaging.Target(topic=topic, server=server)
        # self._client = rpc.get_client(
        #     target,
        #     serializer=objects_base.MagnumObjectSerializer(),
        #     timeout=timeout
        # )
        self._rpc_server = CONF.rpc_server_listen
        self._rpc_port = CONF.rpc_server_listen_port
        url = "http://%s:%s/" % (self._rpc_server, self._rpc_port)
        self._proxy = client.ServerProxy(url, allow_none=True)

    def call(self, method, *args):
        # return self._client.call(self._context, method, *args, **kwargs)
        func = getattr(self._proxy, method)
        return func(*args)
