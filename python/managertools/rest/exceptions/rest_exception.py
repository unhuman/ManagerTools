import json


class RESTException(Exception):
    def __init__(self, status_code: int, message: str, url: str):
        super().__init__(message)
        self.status_code = status_code
        self.url = url

    def __str__(self):
        return json.dumps({
            "statusCode": self.status_code,
            "message": self.args[0] if self.args else "",
            "url": self.url
        })
