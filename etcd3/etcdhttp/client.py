import requests


class HTTPClient(requests.Session):

    def __init__(self, base_url, certs=(), timeout=None):
        super(HTTPClient, self).__init__()
        self.base_url = base_url
        if certs:
            self.cert = certs
        self.timeout = timeout
