import hashlib
import hmac
from typing import Any, Dict, Tuple

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class BiconomyAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str):
        self._api_key = api_key
        self._secret_key = secret_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if "/private/" not in request.url:
            return request

        payload = dict(request.data or {})
        payload["api_key"] = self._api_key
        signature = self._generate_signature(payload)
        payload["sign"] = signature
        request.data = payload
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def generate_ws_sign_params(self, timestamp_ms: int) -> Tuple[str, str, int]:
        """Helper used by the user stream data source to build server.sign payload."""
        payload = {
            "api_key": self._api_key,
            "timestamp": timestamp_ms,
        }
        signature = self._generate_signature(payload)
        return self._api_key, signature, timestamp_ms

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        sorted_items = sorted((key, self._to_str(value)) for key, value in params.items())
        encoded = "&".join(f"{key}={value}" for key, value in sorted_items)
        signing_string = f"{encoded}&secret_key={self._secret_key}"
        digest = hmac.new(self._secret_key.encode(), signing_string.encode(), hashlib.sha256).hexdigest()
        return digest.upper()

    @staticmethod
    def _to_str(value: Any) -> str:
        if value is None:
            return ""
        return str(value)
