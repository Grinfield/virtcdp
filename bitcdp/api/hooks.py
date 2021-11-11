
from pecan import hooks

from bitcdp.conductor import api as conductor_api


class RPCHook(hooks.PecanHook):
    """Attach the rpcapi object to the request so controllers can get to it."""

    def before(self, state):
        state.request.rpcapi = conductor_api.API(context=state.request.context)


class SimpleHook(hooks.PecanHook):

    def on_route(self, state):
        print 'it is on route'

    def before(self, state):
        print 'it is before exec'

    def after(self, state):
        print 'it is after exec'

    def on_error(self, state, exc):
        print 'it is on error'
