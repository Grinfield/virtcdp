
import logging

from oslo_config import cfg

Logger = logging.getLogger(__name__)
CONF = cfg.CONF


def setup_logging(debug, binary=None):
    if not debug:
        return

    log_path = CONF.log_file % binary

    fmt = "%(asctime)s %(process)d %(levelname)s %(funcName)s " \
          "(%(module)s:%(lineno)d): %(message)s"
    formatter = logging.Formatter(fmt)
    root = logging.getLogger("bitcdp")
    handler = logging.FileHandler(log_path)
    handler.setFormatter(formatter)
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)
