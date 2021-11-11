
from bitcdp.common import rpcclient


class API(rpcclient.RPCApi):
    def __init__(self, context=None):
        super(API, self).__init__(context=context, topic=None)

    def backup_server(self):
        pass
