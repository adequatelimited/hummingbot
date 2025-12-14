from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from . import biconomy_constants as CONSTANTS, biconomy_web_utils as web_utils
from .biconomy_order_book import BiconomyOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from .biconomy_exchange import BiconomyExchange


class BiconomyAPIOrderBookDataSource(OrderBookTrackerDataSource):

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "BiconomyExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory
        self._message_id = 0
        self._ping_task: Optional[asyncio.Task] = None
        self._last_client_ping_ts = 0.0
        self._last_server_ping_ts = 0.0
        self._last_pong_sent_ts = 0.0
        self._last_event_timestamp = 0.0
        self._active_ws: Optional[WSAssistant] = None
        self._client_ping_interval = float(CONSTANTS.CLIENT_PING_INTERVAL)

    async def get_last_traded_prices(
        self, trading_pairs: List[str], domain: Optional[str] = None
    ) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        params = {"symbol": symbol, "size": 100}
        snapshot = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL, domain=self._domain),
            method=RESTMethod.GET,
            params=params,
            throttler_limit_id=CONSTANTS.DEPTH_PATH_URL,
        )
        return snapshot

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot = await self._request_order_book_snapshot(trading_pair)
        return BiconomyOrderBook.snapshot_message_from_rest(snapshot=snapshot, trading_pair=trading_pair)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        params = raw_message.get("params") or []
        if len(params) < 2:
            return
        symbol = params[0]
        trades = params[1] or []
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        trade_messages = BiconomyOrderBook.trade_messages_from_ws(trades=trades, trading_pair=trading_pair)
        self._last_event_timestamp = self._time()
        for message in trade_messages:
            message_queue.put_nowait(message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        params = raw_message.get("params") or []
        if len(params) < 3:
            return
        symbol = params[2]
        depth_payload = params[1]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        event_time = self._time()
        diff_message = BiconomyOrderBook.diff_message_from_ws(
            depth_payload=depth_payload,
            trading_pair=trading_pair,
            timestamp=event_time,
        )
        self._last_event_timestamp = event_time
        message_queue.put_nowait(diff_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        params = raw_message.get("params") or []
        if len(params) < 3:
            return
        symbol = params[2]
        depth_payload = params[1]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        event_time = self._time()
        snapshot_message = BiconomyOrderBook.snapshot_message_from_ws(
            depth_payload=depth_payload,
            trading_pair=trading_pair,
            timestamp=event_time,
        )
        self._last_event_timestamp = event_time
        message_queue.put_nowait(snapshot_message)

    async def _subscribe_channels(self, ws: WSAssistant):
        for trading_pair in self._trading_pairs:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
            depth_payload = {
                "method": "depth.subscribe",
                "params": [symbol, 50, "0.01"],
                "id": self._next_message_id(),
            }
            trades_payload = {
                "method": "deals.subscribe",
                "params": [symbol],
                "id": self._next_message_id(),
            }
            await ws.send(WSJSONRequest(payload=depth_payload))
            await ws.send(WSJSONRequest(payload=trades_payload))

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WS_PUBLIC_URL,
            ping_timeout=CONSTANTS.HEARTBEAT_INTERVAL,
            ws_headers=dict(CONSTANTS.WS_REQUEST_HEADERS),
        )
        self._active_ws = ws
        self._log_ws_event(
            "connected",
            heartbeat=CONSTANTS.HEARTBEAT_INTERVAL,
            client_ping_interval=self._client_ping_interval,
        )
        await self._start_ping_loop(ws)
        return ws

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        method = event_message.get("method")
        if method == "depth.update":
            params = event_message.get("params") or []
            if params and params[0]:
                return self._snapshot_messages_queue_key
            return self._diff_messages_queue_key
        if method == "deals.update":
            return self._trade_messages_queue_key
        return "unknown"

    async def _process_message_for_unknown_channel(
        self, event_message: Dict[str, Any], websocket_assistant: WSAssistant
    ):
        method = event_message.get("method")
        if method == "server.ping":
            self._last_server_ping_ts = self._time()
            pong_request = WSJSONRequest(
                payload={"method": "server.pong", "params": [], "id": event_message.get("id", self._next_message_id())}
            )
            await websocket_assistant.send(pong_request)
            self._last_pong_sent_ts = self._time()
            self._log_ws_event("server_ping", ping_id=event_message.get("id"))
        elif "result" in event_message:
            return
        elif event_message.get("error"):
            self.logger().warning(f"Error message received from Biconomy public stream: {event_message}")

    def _next_message_id(self) -> int:
        self._message_id += 1
        return self._message_id

    async def _start_ping_loop(self, websocket_assistant: WSAssistant):
        await self._cancel_ping_task()
        self._ping_task = asyncio.create_task(self._ping_loop(websocket_assistant))

    async def _cancel_ping_task(self):
        if self._ping_task is not None:
            self._ping_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._ping_task
            self._ping_task = None

    async def _ping_loop(self, websocket_assistant: WSAssistant):
        interval = self._client_ping_interval
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    self._last_client_ping_ts = self._time()
                    ping_request = WSJSONRequest(
                        payload={"method": "server.ping", "params": [], "id": self._next_message_id()},
                    )
                    await websocket_assistant.send(ping_request)
                    self._log_ws_event("client_ping", ping_id=self._message_id)
                except asyncio.CancelledError:
                    raise
                except Exception:  # pragma: no cover - network hiccups
                    self.logger().warning("Failed to send Biconomy order-book ping", exc_info=True)
        except asyncio.CancelledError:
            return

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None):
        await self._safe_close_websocket(websocket_assistant)
        self._log_disconnect_summary()
        await self._cancel_ping_task()
        await super()._on_order_stream_interruption(None)
        if websocket_assistant is self._active_ws:
            self._active_ws = None

    def _log_ws_event(self, event: str, **metadata: Any):
        if self.logger().isEnabledFor(logging.DEBUG):
            details = " ".join(f"{key}={metadata[key]}" for key in sorted(metadata)) if metadata else ""
            suffix = f" {details}" if details else ""
            self.logger().debug(f"[biconomy-public-ws] {event}{suffix}")

    def _log_disconnect_summary(self):
        now = self._time()
        last_event_age = now - self._last_event_timestamp if self._last_event_timestamp else None
        server_ping_age = now - self._last_server_ping_ts if self._last_server_ping_ts else None
        client_ping_age = now - self._last_client_ping_ts if self._last_client_ping_ts else None
        pong_age = now - self._last_pong_sent_ts if self._last_pong_sent_ts else None
        summary_parts = [
            "summary=order-book",
            f"last_event_age={last_event_age:.2f}s" if last_event_age is not None else "last_event_age=n/a",
            f"last_server_ping_age={server_ping_age:.2f}s" if server_ping_age is not None else "last_server_ping_age=n/a",
            f"last_client_ping_age={client_ping_age:.2f}s" if client_ping_age is not None else "last_client_ping_age=n/a",
            f"last_pong_age={pong_age:.2f}s" if pong_age is not None else "last_pong_age=n/a",
        ]
        self.logger().info("Biconomy public stream disconnect %s", " ".join(summary_parts))

    async def _safe_close_websocket(self, websocket_assistant: Optional[WSAssistant]):
        if websocket_assistant is None:
            return
        try:
            await websocket_assistant.disconnect()
            self._log_ws_event("client_close")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().warning("Failed to close Biconomy public websocket cleanly", exc_info=True)
