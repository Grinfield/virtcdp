
import pecan

from bitcdp.api import hooks
from bitcdp.api import middleware


def setup_app(pecan_config=None):
    app_hooks = [
        #               hooks.ConfigHook(conf),
        hooks.SimpleHook(),
        hooks.RPCHook(),
        #              hooks.NotifierHook(conf),
        #              hooks.TranslationHook()
    ]

    pecan_config = pecan_config or {
        "app": {
            'root': 'bitcdp.api.controllers.root.RootController',
            'modules': ['bitcdp.api'],
        }
    }

    pecan.configuration.set_config(dict(pecan_config), overwrite=True)

    app = pecan.make_app(
        pecan_config['app']['root'],
        hooks=app_hooks,
        logging=getattr(pecan_config, 'logging', {}),
        wrap_app=middleware.ParsableErrorMiddleware,
        guess_content_type_from_ext=False
    )

    return app
