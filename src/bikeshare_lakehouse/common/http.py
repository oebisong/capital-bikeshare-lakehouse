"""HTTP session with timeouts and limited retries for the public data sources."""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT_SECONDS = 30
USER_AGENT = "bikeshare-lakehouse/1.0 (portfolio data engineering project)"


class TimeoutSession(requests.Session):
    def __init__(self, timeout: float):
        super().__init__()
        self.timeout = timeout

    def request(self, method, url, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return super().request(method, url, **kwargs)


def build_session(retries: int = 3, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> requests.Session:
    retry = Retry(
        total=retries,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    session = TimeoutSession(timeout)
    session.headers["User-Agent"] = USER_AGENT
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session
