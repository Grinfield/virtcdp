
import os
import sys
parent = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent)
import eventlet
eventlet.monkey_patch()

from virtcdp import service


def main():
    service.prepare_service(sys.argv, binary='api')

    # should_use_ssl = 'virtcdp_api' in cfg.CONF.enabled_ssl_apis
    server = service.WSGIService('virtcdp_api')
    # service.serve(server, workers=server.workers)
    launcher = service.ServiceLauncher().serve(server, workers=server.workers)
    launcher.wait()
    return 0


if __name__ == "__main__":
    sys.exit(main())
