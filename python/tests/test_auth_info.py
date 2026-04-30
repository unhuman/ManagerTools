import base64
import pytest
from managertools.rest import AuthInfo
from managertools.rest.auth_info import AuthType


class TestAuthInfo:
    def test_basic_auth_creation(self):
        auth = AuthInfo("user", "pass")
        assert auth.auth_type == AuthType.Basic
        header_name, header_value = auth.get_auth_header()
        assert header_name == "Authorization"
        expected = base64.b64encode(b"user:pass").decode('utf-8')
        assert header_value == f"Basic {expected}"

    def test_bearer_auth_creation(self):
        auth = AuthInfo(AuthType.Bearer, "token123")
        assert auth.auth_type == AuthType.Bearer
        header_name, header_value = auth.get_auth_header()
        assert header_name == "Authorization"
        assert header_value == "Bearer token123"

    def test_cookie_auth_creation(self):
        auth = AuthInfo(AuthType.Cookies, "session=abc123; path=/")
        assert auth.auth_type == AuthType.Cookies
        header_name, header_value = auth.get_auth_header()
        assert header_name == "Cookie"
        assert header_value == "session=abc123; path=/"

    def test_update_cookies(self):
        auth = AuthInfo(AuthType.Cookies, "session=old123; path=/")
        auth.update_cookies([("session", "new456")])
        _, header_value = auth.get_auth_header()
        assert "session=new456" in header_value

    def test_invalid_auth_type(self):
        with pytest.raises(RuntimeError):
            AuthInfo(AuthType.Basic, "token")
