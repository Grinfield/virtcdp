
# Copyright Shenzhen Mulang Cloud Data Co.,Ltd
# All Rights Reserved.

import os
import sys
parent = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent)
import eventlet

from bitcdp import service


def main():
    service.prepare_service(sys.argv, binary='bitagent')
    eventlet.monkey_patch()

    # should_use_ssl = 'osapi_compute' in cfg.CONF.enabled_ssl_apis
    server = service.RPCService.create()
    # service.serve(server, workers=server.workers)
    service.ServiceWrapper.serve(server)
    # service.wait()


if __name__ == "__main__":
    sys.exit(main())
