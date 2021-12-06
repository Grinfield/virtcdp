import logging
import json
import re

from oslo_config import cfg
from pecan import hooks
import webob
from oslo_utils import importutils

from virtcdp.conductor import api as conductor_api
from virtcdp import exception

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class RPCHook(hooks.PecanHook):
    """Attach the rpcapi object to the request so controllers can get to it."""

    def before(self, state):
        state.request.rpcapi = conductor_api.API(context=state.request.context)


class ExceptionHook(hooks.PecanHook):
    def on_error(self, state, e):
        status = 500
        ret = {"code": 1}
        if CONF.debug:
            LOG.exception("Uncaught exception: %s", str(e))
        if isinstance(e, exception.VirtcdpException):
            ret["message"] = e.message
            status = e.code
        else:
            faultString = e.faultString
            marker = r"^<class '[\w\.*]+'>"
            match = re.match(marker, faultString)
            if match:
                ret["message"] = faultString.rsplit(":", maxsplit=1)[1]
                s = re.findall(r"'[\w\.*]+'", match.group())[0]
                cls = s.split("'")[1]
                try:
                    obj = importutils.import_object(
                        cls,
                        faultString.rsplit(":", maxsplit=1)[1])
                    if obj:
                        status = obj.code
                        ret["message"] = obj.message
                except ImportError:
                    LOG.warning("Can't import object: %s", cls)
                    pass
            else:
                ret["message"] = str(e)
        return webob.Response(body=json.dumps(ret), status=status)

    def after(self, state):
        # Omit empty body. Some errors may not have body at this level yet.
        if not state.response.body:
            return

        # Do nothing if there is no error.
        if 200 <= state.response.status_int < 400:
            return
