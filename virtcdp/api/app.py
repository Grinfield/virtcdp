
import pecan

from virtcdp.api import hooks
from virtcdp.api import middleware


def setup_app(pecan_config=None):
    app_hooks = [
        #               hooks.ConfigHook(conf),
        hooks.RPCHook(),
        hooks.ExceptionHook(),
    ]

    pecan_config = pecan_config or {
        "app": {
            'root': 'virtcdp.api.controllers.root.RootController',
            'modules': ['virtcdp.api'],
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
