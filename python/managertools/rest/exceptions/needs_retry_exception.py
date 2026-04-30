from .rest_exception import RESTException


class NeedsRetryException(RESTException):
    def __init__(self, status_code: int, message: str, url: str, retry_after: int):
        super().__init__(status_code, message, url)
        self.retry_after = retry_after

    def get_retry_after(self) -> int:
        return self.retry_after

    def __str__(self):
        return (f"NeedsRetryException{{"
                f"statusCode={self.status_code}, "
                f"message='{self.args[0] if self.args else ''}', "
                f"url='{self.url}', "
                f"retryAfter={self.retry_after}"
                f"}}")
