"""Auth client for QueryStation API key exchange."""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class QueryStationAuth:
    """Exchanges an API key for a short-lived JWT, with lazy refresh."""

    def __init__(
        self,
        api_key: str | None = None,
        auth_url: str | None = None,
    ) -> None:
        self._api_key = api_key or os.environ.get("QUERYSTATION_API_KEY", "")
        if not self._api_key:
            raise ValueError(
                "No API key: pass api_key= or set QUERYSTATION_API_KEY"
            )
        self._auth_url = (
            auth_url
            or os.environ.get("AUTH_URL", "https://auth-dev.querystation.app")
        )
        self._token: str | None = None
        self._remote_url: str | None = None
        self._expires_at: float = 0.0

    @property
    def remote_url(self) -> str:
        """Remote DuckDB endpoint URL. Triggers key exchange on first access."""
        if not self._remote_url:
            self._exchange()
        assert self._remote_url is not None
        return self._remote_url

    def get_token(self) -> str:
        """Return a valid JWT, re-exchanging if expired or within 60s of expiry."""
        if self._token is None or time.time() > (self._expires_at - 60):
            self._exchange()
        assert self._token is not None
        return self._token

    def force_refresh(self) -> None:
        """Force a token re-exchange on the next get_token() call."""
        self._expires_at = 0.0

    def _exchange(self) -> None:
        try:
            import httpx
        except ImportError as exc:
            raise ImportError(
                "httpx is required for remote DuckDB. "
                "Install with: pip install data-consumers[remote]"
            ) from exc

        r = httpx.post(
            f"{self._auth_url}/api/auth/api-keys/exchange",
            headers={"Authorization": f"Bearer {self._api_key}"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()

        self._token = data["token"]
        self._remote_url = data["remoteUrl"]

        expires_at_str = data.get("expiresAt")
        if expires_at_str:
            # Python 3.10 fromisoformat doesn't handle trailing Z
            self._expires_at = datetime.fromisoformat(
                expires_at_str.replace("Z", "+00:00")
            ).timestamp()
        else:
            self._expires_at = time.time() + 3300

        logger.debug(
            "Token exchanged, expires in %.0fs",
            self._expires_at - time.time(),
        )
