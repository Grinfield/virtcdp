
import sys

from bitcdp import service
from bitcdp.api import app

# Initialize the oslo configuration library and logging
service.prepare_service(sys.argv)
application = app.setup_app()