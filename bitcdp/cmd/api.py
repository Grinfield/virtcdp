# Copyright Shenzhen Mulang Cloud Data Co.,Ltd
# All Rights Reserved.

import sys

from bitcdp import service


def main():
    service.prepare_service(sys.argv, binary='api')

    # should_use_ssl = 'osapi_compute' in cfg.CONF.enabled_ssl_apis
    server = service.WSGIService('osapi_compute')
    # service.serve(server, workers=server.workers)
    service.ServiceWrapper.serve(server, workers=server.workers)
    # service.wait()


if __name__ == "__main__":
    sys.exit(main())