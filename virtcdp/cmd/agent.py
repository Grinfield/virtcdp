
import os
import sys
parent = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent)
import eventlet
eventlet.monkey_patch()

from virtcdp import service


def main():
    service.prepare_service(sys.argv, binary='agent')

    server = service.RPCService.create()
    # service.serve(server, workers=server.workers)
    launcher = service.ServiceLauncher().serve(server)
    launcher.wait()
    return 0


if __name__ == "__main__":
    sys.exit(main())
