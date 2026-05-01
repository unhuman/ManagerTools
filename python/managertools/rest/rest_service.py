import json
import re
import sys
import time
from abc import ABC
from socket import timeout as SocketTimeoutException
from typing import Optional, Any, Dict
from urllib.parse import urlparse, parse_qs

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from .auth_info import AuthInfo
from .exceptions import RESTException, NeedsRetryException


class RestService(ABC):
    def __init__(self, auth_info: AuthInfo):
        self.auth_info = auth_info
        self._sessions: Dict[str, requests.Session] = {}
        self._session_lock = {}

    def _get_session(self, host: str) -> requests.Session:
        if host not in self._sessions:
            session = requests.Session()
            # Configure connection pooling
            adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self._sessions[host] = session
        return self._sessions[host]

    def get_request(self, uri: str, **params) -> Any:
        return self._execute_request("GET", uri, None, params)

    def put_request(self, uri: str, content: str, **params) -> Any:
        return self._execute_request("PUT", uri, content, params)

    def _execute_request(self, method: str, uri: str, content: Optional[str], params: Dict) -> Any:
        timeout_retries = 0
        max_timeout_retries = 3
        timeout_backoff = 5  # seconds

        while True:
            request_start_time = time.time()
            try:
                parsed = urlparse(uri)
                host = parsed.hostname
                session = self._get_session(host)

                headers = {
                    "Accept": "application/json;charset=UTF-8",
                    "Content-Type": "application/json;charset=UTF-8"
                }
                auth_header_name, auth_header_value = self.auth_info.get_auth_header()
                headers[auth_header_name] = auth_header_value

                # Prepare request kwargs
                kwargs = {
                    "headers": headers,
                    "timeout": (2, 60),  # (connect_timeout, read_timeout) in seconds
                    "params": params if params else None
                }

                if content is not None:
                    kwargs["data"] = content

                # Execute request
                response = session.request(method, uri, **kwargs)

                # Handle rate limiting (429 is always rate limit, 403 only if rate limit headers indicate it)
                if response.status_code == 429:
                    # 429 Too Many Requests is always a rate limit
                    rate_limit_headers = {k: v for k, v in response.headers.items()
                                        if 'ratelimit' in k.lower()}
                    if rate_limit_headers:
                        sys.stderr.write(f"Rate Limit Response Headers: {rate_limit_headers}\n")

                    retry_after = 0
                    # Check Retry-After header
                    if 'Retry-After' in response.headers:
                        try:
                            retry_after = int(response.headers['Retry-After'])
                        except ValueError:
                            pass
                    raise NeedsRetryException(response.status_code, response.text, uri, retry_after)

                elif response.status_code == 403:
                    # 403 Forbidden might be rate limit or permission error
                    # Only treat as rate limit if X-RateLimit-Remaining shows we're out
                    x_remaining = response.headers.get('X-RateLimit-Remaining')
                    is_rate_limited = False
                    if x_remaining:
                        try:
                            if int(x_remaining) == 0:
                                is_rate_limited = True
                                x_reset = response.headers.get('X-RateLimit-Reset')
                                if x_reset:
                                    try:
                                        retry_after = max(0, int(x_reset) - int(time.time()))
                                        raise NeedsRetryException(response.status_code, response.text, uri, retry_after)
                                    except ValueError:
                                        pass
                        except ValueError:
                            pass

                    # Not a rate limit issue—it's a permission error
                    sso_header = response.headers.get('x-github-sso')
                    sso_msg = f" (SSO authorization required: {sso_header})" if sso_header else ""
                    raise RESTException(response.status_code, f"Forbidden{sso_msg}: {dict(response.headers)}", uri)

                # Handle other HTTP errors
                if not (200 <= response.status_code <= 299):
                    raise RESTException(response.status_code,
                                      f"Unable to retrieve requested url {response.reason}",
                                      uri)

                # Update cookies if Set-Cookie headers present
                if 'Set-Cookie' in response.headers:
                    cookies = []
                    for cookie_header in response.headers.getlist('Set-Cookie') if hasattr(response.headers, 'getlist') else [response.headers.get('Set-Cookie')]:
                        if cookie_header:
                            cookie_kv = cookie_header.split(';')[0]
                            if '=' in cookie_kv:
                                k, v = cookie_kv.split('=', 1)
                                cookies.append((k.strip(), v.strip()))
                    if cookies:
                        self.auth_info.update_cookies(cookies)

                # Parse and return JSON response
                return response.json()

            except NeedsRetryException as nre:
                sys.stderr.write(f"Rate limit exceeded. Details: {str(nre)}\n")
                reset_timestamp = time.time() + nre.get_retry_after()

                while True:
                    current_time = time.time()
                    remaining_seconds = int(reset_timestamp - current_time)

                    if remaining_seconds <= 0:
                        sys.stderr.write("\r" + " " * 100 + "\r")
                        sys.stderr.write("Rate limit reset. Resuming requests...\n")
                        break

                    hours = remaining_seconds // 3600
                    minutes = (remaining_seconds % 3600) // 60
                    seconds = remaining_seconds % 60
                    sys.stderr.write(f"\rWaiting for rate limit reset... Time remaining: {hours:02d}:{minutes:02d}:{seconds:02d}")
                    sys.stderr.flush()
                    time.sleep(1)

            except (SocketTimeoutException, requests.exceptions.Timeout) as ste:
                request_duration = (time.time() - request_start_time) * 1000
                timeout_retries += 1
                sys.stderr.write(f"Timeout exceeded (attempt {timeout_retries}/{max_timeout_retries})\n")
                sys.stderr.write(f"  Endpoint: {uri}\n")
                sys.stderr.write(f"  Duration: {request_duration:.0f}ms ({request_duration / 1000:.1f}s)\n")
                sys.stderr.write(f"  Details: {str(ste)}\n")

                if timeout_retries >= max_timeout_retries:
                    sys.stderr.write(f"Max timeout retries ({max_timeout_retries}) exceeded. "
                                   f"Giving up on request: {uri}\n")
                    raise RESTException(408, f"Request timeout after {max_timeout_retries} attempts", uri)

                sys.stderr.write(f"Retrying in {timeout_backoff} seconds...\n")
                time.sleep(timeout_backoff)
                timeout_backoff *= 2

            except Exception as e:
                sys.stderr.write(f"Request Error: {str(e)}\n")
                raise
