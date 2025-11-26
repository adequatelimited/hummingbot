from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.biconomy import biconomy_constants as CONSTANTS, biconomy_web_utils as web_utils
from hummingbot.connector.exchange.biconomy.biconomy_order_book import BiconomyOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.biconomy.biconomy_exchange import BiconomyExchange


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
        for message in trade_messages:
            message_queue.put_nowait(message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        params = raw_message.get("params") or []
        if len(params) < 3:
            return
        symbol = params[2]
        depth_payload = params[1]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        diff_message = BiconomyOrderBook.diff_message_from_ws(
            depth_payload=depth_payload,
            trading_pair=trading_pair,
            timestamp=self._time(),
        )
        message_queue.put_nowait(diff_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        params = raw_message.get("params") or []
        if len(params) < 3:
            return
        symbol = params[2]
        depth_payload = params[1]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        snapshot_message = BiconomyOrderBook.snapshot_message_from_ws(
            depth_payload=depth_payload,
            trading_pair=trading_pair,
            timestamp=self._time(),
        )
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
        await ws.connect(ws_url=CONSTANTS.WS_PUBLIC_URL, ping_timeout=CONSTANTS.HEARTBEAT_INTERVAL)
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
            pong_request = WSJSONRequest(
                payload={"method": "server.pong", "params": [], "id": event_message.get("id", self._next_message_id())}
            )
            await websocket_assistant.send(pong_request)
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
        interval = max(30.0, float(CONSTANTS.HEARTBEAT_INTERVAL) - 30.0)
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    ping_request = WSJSONRequest(
                        payload={"method": "server.ping", "params": [], "id": self._next_message_id()},
                    )
                    await websocket_assistant.send(ping_request)
                except asyncio.CancelledError:
                    raise
                except Exception:  # pragma: no cover - network hiccups
                    self.logger().warning("Failed to send Biconomy order-book ping", exc_info=True)
        except asyncio.CancelledError:
            return

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None):
        await self._cancel_ping_task()
        await super()._on_order_stream_interruption(websocket_assistant)
