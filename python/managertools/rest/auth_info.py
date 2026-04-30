import base64
import re
from enum import Enum
from typing import List, Tuple


class AuthType(Enum):
    Basic = "Basic"
    Cookies = "Cookies"
    Bearer = "Bearer"


class AuthInfo:
    EXTRACT_COOKIE_INFO = re.compile(r"([^=]*)=([^;]*)")

    def __init__(self, auth_type_or_username, password_or_token=None):
        if isinstance(auth_type_or_username, str) and password_or_token is not None:
            # Constructor(username, password)
            self.auth_type = AuthType.Basic
            self.authentication = self._get_basic_auth(auth_type_or_username, password_or_token)
        elif isinstance(auth_type_or_username, AuthType):
            # Constructor(AuthType, token_or_cookies)
            if auth_type_or_username not in [AuthType.Cookies, AuthType.Bearer]:
                raise RuntimeError(f"Invalid AuthType provided: {auth_type_or_username}")
            self.auth_type = auth_type_or_username
            self.authentication = (self._get_bearer(password_or_token)
                                 if auth_type_or_username == AuthType.Bearer
                                 else password_or_token)
        else:
            raise TypeError("Invalid arguments to AuthInfo constructor")

    def update_cookies(self, cookies: List[Tuple[str, str]]):
        if self.auth_type == AuthType.Cookies:
            for cookie_name, cookie_value in cookies:
                pattern = re.compile(f"{re.escape(cookie_name)}=([^;]*)")
                self.authentication = pattern.sub(f"{cookie_name}={cookie_value}", self.authentication)

    def get_auth_header(self) -> Tuple[str, str]:
        if self.auth_type in [AuthType.Basic, AuthType.Bearer]:
            return "Authorization", self.authentication
        elif self.auth_type == AuthType.Cookies:
            return "Cookie", self.authentication
        else:
            raise RuntimeError("Invalid AuthInfo")

    @staticmethod
    def _get_basic_auth(username: str, password: str) -> str:
        credentials = f"{username}:{password}".encode('utf-8')
        encoded = base64.b64encode(credentials).decode('utf-8')
        return f"Basic {encoded}"

    @staticmethod
    def _get_bearer(token: str) -> str:
        return f"Bearer {token}"
