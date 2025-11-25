from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

from hummingbot.connector.exchange.biconomy import biconomy_constants as CONSTANTS
from hummingbot.connector.exchange.biconomy.biconomy_auth import BiconomyAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class BiconomyAPIUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: BiconomyAuth,
        trading_pairs: Optional[List[str]],
        domain: str,
        api_factory: WebAssistantsFactory,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs or []
        self._domain = domain
        self._api_factory = api_factory
        self._message_id = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_PUBLIC_URL, ping_timeout=CONSTANTS.HEARTBEAT_INTERVAL)
        await self._authenticate(ws)
        return ws

    async def _authenticate(self, ws: WSAssistant):
        timestamp = int(time.time() * 1e3)
        api_key, sign, ts = self._auth.generate_ws_sign_params(timestamp)
        sign_request = WSJSONRequest(
            payload={
                "method": "server.sign",
                "params": [api_key, sign, ts],
                "id": self._next_message_id(),
            }
        )
        await ws.send(sign_request)
        response: WSResponse = await ws.receive()
        message = await self._validated_message(response.data)
        if isinstance(message, dict) and message.get("code") not in (None, 0):
            raise ConnectionError(f"Error authenticating user stream: {message}")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        order_request = WSJSONRequest(
            payload={
                "method": "order.subscribe",
                "params": [],
                "id": self._next_message_id(),
            }
        )
        asset_request = WSJSONRequest(
            payload={
                "method": "asset.subscribe",
                "params": [],
                "id": self._next_message_id(),
            }
        )
        await websocket_assistant.send(order_request)
        await websocket_assistant.send(asset_request)

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        async for ws_response in websocket_assistant.iter_messages():
            try:
                message = await self._validated_message(ws_response.data)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().warning("Invalid message received on user stream", exc_info=True)
                continue

            if not message:
                continue

            if isinstance(message, dict):
                method = message.get("method")
                if method == "server.ping":
                    await self._respond_to_ping(websocket_assistant, message)
                    continue
                if method is None and "result" in message:
                    # subscription acknowledgement, ignore
                    continue

            await self._process_event_message(event_message=message, queue=queue)

    async def _respond_to_ping(self, ws: WSAssistant, message: Dict[str, Any]):
        pong_request = WSJSONRequest(
            payload={
                "method": "server.pong",
                "params": [],
                "id": message.get("id", self._next_message_id()),
            }
        )
        await ws.send(pong_request)

    async def _validated_message(self, raw_message: Any) -> Any:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode()
        if isinstance(raw_message, str):
            raw_message = raw_message.strip()
            if not raw_message:
                return None
            return json.loads(raw_message)
        return raw_message

    async def _send_ping(self, websocket_assistant: WSAssistant):
        ping_request = WSJSONRequest(
            payload={
                "method": "server.ping",
                "params": [],
                "id": self._next_message_id(),
            }
        )
        await websocket_assistant.send(ping_request)

    def _next_message_id(self) -> int:
        self._message_id += 1
        return self._message_id
