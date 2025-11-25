from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BiconomyOrderBook(OrderBook):
    @classmethod
    def snapshot_message_from_rest(cls, snapshot: Dict[str, Any], trading_pair: str) -> OrderBookMessage:
        timestamp = time.time()
        return cls._snapshot_message_from_levels(snapshot.get("bids", []), snapshot.get("asks", []), trading_pair, timestamp)

    @classmethod
    def snapshot_message_from_ws(
        cls,
        depth_payload: Dict[str, Any],
        trading_pair: str,
        timestamp: Optional[float] = None,
    ) -> OrderBookMessage:
        ts = timestamp or time.time()
        return cls._snapshot_message_from_levels(depth_payload.get("bids", []), depth_payload.get("asks", []), trading_pair, ts)

    @classmethod
    def diff_message_from_ws(
        cls,
        depth_payload: Dict[str, Any],
        trading_pair: str,
        timestamp: Optional[float] = None,
    ) -> OrderBookMessage:
        ts = timestamp or time.time()
        update_id = int(ts * 1e3)
        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "first_update_id": update_id,
            "bids": cls._format_price_levels(depth_payload.get("bids", [])),
            "asks": cls._format_price_levels(depth_payload.get("asks", [])),
        }
        return OrderBookMessage(OrderBookMessageType.DIFF, content, timestamp=ts)

    @classmethod
    def trade_messages_from_ws(cls, trades: List[Dict[str, Any]], trading_pair: str) -> List[OrderBookMessage]:
        messages: List[OrderBookMessage] = []
        for trade in trades:
            try:
                trade_timestamp = float(trade.get("time", time.time()))
            except (TypeError, ValueError):
                trade_timestamp = time.time()
            update_id = int(trade_timestamp * 1e3)
            trade_type = TradeType.BUY if str(trade.get("type", "")).lower() == "buy" else TradeType.SELL
            try:
                trade_id = int(trade.get("id"))
            except (TypeError, ValueError):
                trade_id = update_id
            try:
                price = float(trade.get("price", 0))
            except (TypeError, ValueError):
                price = 0.0
            try:
                amount = float(trade.get("amount", 0))
            except (TypeError, ValueError):
                amount = 0.0

            content = {
                "trading_pair": trading_pair,
                "trade_type": float(trade_type.value),
                "trade_id": trade_id,
                "update_id": update_id,
                "price": price,
                "amount": amount,
            }
            messages.append(OrderBookMessage(OrderBookMessageType.TRADE, content, timestamp=trade_timestamp))
        return messages

    @classmethod
    def _snapshot_message_from_levels(
        cls,
        bids: List[List[Any]],
        asks: List[List[Any]],
        trading_pair: str,
        timestamp: float,
    ) -> OrderBookMessage:
        update_id = int(timestamp * 1e3)
        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": cls._format_price_levels(bids),
            "asks": cls._format_price_levels(asks),
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=timestamp)

    @staticmethod
    def _format_price_levels(levels: List[List[Any]]) -> List[List[float]]:
        formatted: List[List[float]] = []
        for level in levels:
            if len(level) < 2:
                continue
            price, amount = level[0], level[1]
            try:
                price_float = float(price)
            except (TypeError, ValueError):
                price_float = 0.0
            try:
                amount_float = float(amount)
            except (TypeError, ValueError):
                amount_float = 0.0
            formatted.append([price_float, amount_float])
        return formatted
