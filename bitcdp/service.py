
import inspect
import logging
import os
from SimpleXMLRPCServer import SimpleXMLRPCServer

from oslo_config import cfg
from werkzeug import serving

from bitcdp.api import app
from bitcdp import config
from bitcdp import log
from bitcdp.conductor import manager

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def _get_binary_name():
    return os.path.basename(inspect.stack()[-1][1])


def prepare_service(argv=None, binary=None):
    if argv is None:
        argv = []
    config.parse_args(argv,
                      default_config_files=['../../etc/bitcdp.conf'])
    if binary is None:
        binary = _get_binary_name()
    log.setup_logging(cfg.CONF.debug, binary)

    LOG.debug("Configuration:")
    CONF.log_opt_values(LOG, logging.DEBUG)


class RPCService(object):

    def __init__(self, host=None, port=None):
        self.host = host
        self.port = port
        self.endpoints = [
            manager.BackupConductor(),
        ]
        self._server = None

    def start(self, workers=1):
        self._server = SimpleXMLRPCServer((self.host, self.port))
        self._server.register_introspection_functions()
        for e in self.endpoints:
            self._server.register_instance(e)
        self._server.serve_forever()

    # def create_periodic_tasks(self):
    #     if CONF.periodic_enable:
    #         periodic.setup(CONF, self.tg)
    #     servicegroup.setup(CONF, self.binary, self.tg)
    #
    # def stop(self):
    #     if self._server:
    #         self._server.stop()
    #         self._server.wait()
    #     super(Service, self).stop()

    @classmethod
    def create(cls, host=None, port=None):
        if host is None:
            host = CONF.rpc_server_listen
        if port is None:
            port = CONF.rpc_server_listen_port
        service_obj = cls(host, port)
        return service_obj

    def stop(self):
        if self._server:
            self._server.shutdown()


class WSGIService(object):

    def __init__(self, binary, use_ssl=False):
        self.bin = binary
        self.use_ssl = use_ssl
        self.app = app.setup_app()
        self.host = CONF.bitcdp_server_listen
        self.port = CONF.bitcdp_server_listen_port
        self.workers = CONF.bitcdp_server_workers

    def start(self, workers):
        LOG.info('Starting server in PID %s', os.getpid())
        LOG.info('Serving on %(proto)s://%(host)s:%(port)s',
                 dict(proto="https" if self.use_ssl else "http",
                      host=self.host, port=self.port))
        serving.run_simple(self.host,
                           self.port,
                           self.app,
                           processes=workers)

    def reset(self):
        pass

    def stop(self):
        pass

    def wait(self):
        pass


class ServiceWrapper(object):
    @staticmethod
    def serve(service, workers=1):
        service.start(workers)

    @staticmethod
    def wait(service):
        service.wait()
