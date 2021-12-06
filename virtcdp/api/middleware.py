
import six
import json


class ParsableErrorMiddleware(object):
    """Replace error body with something the client can parse."""

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        # Request for this state, modified by replace_start_response()
        # and used when an error is being reported.
        state = {}

        def replacement_start_response(status, headers, exc_info=None):
            """Overrides the default response to make errors parsable."""
            try:
                status_code = int(status.split(' ')[0])
                state['status_code'] = status_code
            except (ValueError, TypeError):  # pragma: nocover
                raise Exception((
                    'ErrorDocumentMiddleware received an invalid '
                    'status %s' % status
                ))
            else:
                if (state['status_code'] // 100) not in (2, 3):
                    # Remove some headers so we can replace them later
                    # when we have the full error message and can
                    # compute the length.
                    headers = [(h, v)
                               for (h, v) in headers
                               if h not in ('Content-Length', 'Content-Type')
                               ]
                # Save the headers in case we need to modify them.
                state['headers'] = headers
                return start_response(status, headers, exc_info)

        app_iter = self.app(environ, replacement_start_response)
        if (state['status_code'] // 100) not in (2, 3):
            content_type = 'application/json'
            app_data = b'\n'.join(app_iter)
            if six.PY3:
                app_data = app_data.decode('utf-8')
            try:
                fault = json.loads(app_data)
            except ValueError as err:
                fault = {'error_message': app_data}
            body = json.dumps(fault)
            if six.PY3:
                body = body.encode('utf-8')

            state['headers'].append(('Content-Length', str(len(body))))
            state['headers'].append(('Content-Type', content_type))
            body = [body]
        else:
            body = app_iter
        return body
