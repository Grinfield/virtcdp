
import sys

from virtcdp import service
from virtcdp.api import app

# Initialize the oslo configuration library and logging
service.prepare_service(sys.argv)
application = app.setup_app()