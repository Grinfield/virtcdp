"""
Startup the application following by an uWSGI server
eg: uwsgi --http :8663 --wsgi-file app.wsgi -p 4
"""


import sys

from virtcdp import service
from virtcdp.api import app

# Initialize the oslo configuration library and logging
service.prepare_service(sys.argv)
application = app.setup_app()