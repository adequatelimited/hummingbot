from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import suppress
from typing import Any, Dict, List, Optional

from . import biconomy_constants as CONSTANTS
from .biconomy_auth import BiconomyAuth
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
        self._last_event_timestamp = 0.0
        self._ping_task: Optional[asyncio.Task] = None
        self._last_client_ping_ts = 0.0
        self._last_server_ping_ts = 0.0
        self._last_pong_sent_ts = 0.0
        self._client_ping_interval = float(CONSTANTS.CLIENT_PING_INTERVAL)

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WS_PUBLIC_URL,
            ping_timeout=CONSTANTS.HEARTBEAT_INTERVAL,
            ws_headers=dict(CONSTANTS.WS_REQUEST_HEADERS),
        )
        self._log_ws_event(
            "connected",
            heartbeat=CONSTANTS.HEARTBEAT_INTERVAL,
            client_ping_interval=self._client_ping_interval,
        )
        await self._authenticate(ws)
        await self._start_ping_loop(ws)
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
                "params": self._assets_to_subscribe(),
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

            self._last_event_timestamp = time.time()

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
        self._last_server_ping_ts = time.time()
        pong_request = WSJSONRequest(
            payload={
                "method": "server.pong",
                "params": [],
                "id": message.get("id", self._next_message_id()),
            }
        )
        await ws.send(pong_request)
        self._last_pong_sent_ts = time.time()
        self._log_ws_event("server_ping", ping_id=message.get("id"))

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
        self._last_client_ping_ts = time.time()
        ping_request = WSJSONRequest(
            payload={
                "method": "server.ping",
                "params": [],
                "id": self._next_message_id(),
            }
        )
        await websocket_assistant.send(ping_request)
        self._log_ws_event("client_ping", ping_id=self._message_id)

    async def _start_ping_loop(self, websocket_assistant: WSAssistant):
        await self._cancel_ping_task()
        self._ping_task = asyncio.create_task(self._ping_loop(websocket_assistant))

    async def _ping_loop(self, websocket_assistant: WSAssistant):
        interval = self._client_ping_interval
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self._send_ping(websocket_assistant)
                except asyncio.CancelledError:
                    raise
                except Exception:  # pragma: no cover - network hiccups
                    self.logger().warning("Failed to send Biconomy user-stream ping", exc_info=True)
        except asyncio.CancelledError:
            return

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        await self._safe_close_websocket(websocket_assistant)
        self._log_disconnect_summary()
        await self._cancel_ping_task()
        await super()._on_user_stream_interruption(None)

    async def _cancel_ping_task(self):
        if self._ping_task is not None:
            self._ping_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._ping_task
            self._ping_task = None

    async def stop(self):
        websocket_assistant: Optional[WSAssistant] = getattr(self, "_ws_assistant", None)
        await self._safe_close_websocket(websocket_assistant)
        await self._cancel_ping_task()
        await super().stop()

    def _next_message_id(self) -> int:
        self._message_id += 1
        return self._message_id

    def _assets_to_subscribe(self) -> List[str]:
        assets = set()
        for trading_pair in self._trading_pairs:
            try:
                base, quote = trading_pair.split("-")
            except ValueError:
                continue
            assets.add(base)
            assets.add(quote)

        return sorted(assets)

    @property
    def last_event_timestamp(self) -> float:
        return self._last_event_timestamp

    def _log_ws_event(self, event: str, **metadata: Any):
        if self.logger().isEnabledFor(logging.DEBUG):
            details = " ".join(f"{key}={metadata[key]}" for key in sorted(metadata)) if metadata else ""
            suffix = f" {details}" if details else ""
            self.logger().debug(f"[biconomy-user-ws] {event}{suffix}")

    def _log_disconnect_summary(self):
        now = time.time()
        last_event_age = now - self._last_event_timestamp if self._last_event_timestamp else None
        server_ping_age = now - self._last_server_ping_ts if self._last_server_ping_ts else None
        client_ping_age = now - self._last_client_ping_ts if self._last_client_ping_ts else None
        pong_age = now - self._last_pong_sent_ts if self._last_pong_sent_ts else None
        summary_parts = [
            "summary=user-stream",
            f"last_event_age={last_event_age:.2f}s" if last_event_age is not None else "last_event_age=n/a",
            f"last_server_ping_age={server_ping_age:.2f}s" if server_ping_age is not None else "last_server_ping_age=n/a",
            f"last_client_ping_age={client_ping_age:.2f}s" if client_ping_age is not None else "last_client_ping_age=n/a",
            f"last_pong_age={pong_age:.2f}s" if pong_age is not None else "last_pong_age=n/a",
        ]
        self.logger().info("Biconomy user stream disconnect %s", " ".join(summary_parts))

    async def _safe_close_websocket(self, websocket_assistant: Optional[WSAssistant]):
        if websocket_assistant is None:
            return
        try:
            await websocket_assistant.disconnect()
            self._log_ws_event("client_close")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().warning("Failed to close Biconomy user websocket cleanly", exc_info=True)
