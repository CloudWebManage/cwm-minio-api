import os

from uvicorn_worker import UvicornWorker


class MyUvicornWorker(UvicornWorker):
    CONFIG_KWARGS = {"factory": True}


# https://docs.gunicorn.org/en/stable/settings.html#logging
accesslog = os.getenv('GUNICORN_ACCESSLOG', None)  # set to "-" to log to stdout

# https://docs.gunicorn.org/en/stable/settings.html#security
limit_request_line = int(os.getenv('GUNICORN_LIMIT_REQUEST_LINE', '4094'))
limit_request_fields = int(os.getenv('GUNICORN_LIMIT_REQUEST_FIELDS', '100'))
limit_request_field_size = int(os.getenv('GUNICORN_LIMIT_REQUEST_FIELD_SIZE', '8190'))

# https://docs.gunicorn.org/en/stable/settings.html#server-socket
bind = os.getenv('GUNICORN_BIND', "0.0.0.0:8000")
backlog = int(os.getenv('GUNICORN_BACKLOG', '2048'))

# https://docs.gunicorn.org/en/stable/settings.html#worker-processes
workers = int(os.getenv('GUNICORN_WORKERS', '4'))
worker_class = MyUvicornWorker
max_requests = int(os.getenv('GUNICORN_MAX_REQUESTS', '0'))
max_requests_jitter = int(os.getenv('GUNICORN_MAX_REQUESTS_JITTER', '0'))
timeout = int(os.getenv('GUNICORN_TIMEOUT', '30'))
graceful_timeout = int(os.getenv('GUNICORN_GRACEFUL_TIMEOUT', '30'))
keepalive = int(os.getenv('GUNICORN_KEEPALIVE', '2'))
