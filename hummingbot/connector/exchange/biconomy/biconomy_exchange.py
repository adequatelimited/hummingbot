from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Tuple

from async_timeout import timeout
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from . import (
    biconomy_auth,
    biconomy_constants as CONSTANTS,
    biconomy_web_utils as web_utils,
)
from .biconomy_api_order_book_data_source import BiconomyAPIOrderBookDataSource
from .biconomy_api_user_stream_data_source import BiconomyAPIUserStreamDataSource
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def _split_trading_pair(trading_pair: str) -> Tuple[str, str]:
    """Split on the last hyphen to support tokens that include hyphens"""
    if "-" not in trading_pair:
        raise ValueError(f"Invalid trading pair {trading_pair}")
    base, quote = trading_pair.rsplit("-", 1)
    if not base or not quote:
        raise ValueError(f"Invalid trading pair {trading_pair}")
    return base, quote


def _combine_trading_pair(base: str, quote: str) -> str:
    base = base.strip()
    quote = quote.strip()
    if not base or not quote:
        raise ValueError("Base and quote must be provided")
    return f"{base}-{quote}"


class BiconomyExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    BULK_CANCEL_MAX_SIZE = 10
    _CLIENT_ID_FIELDS = (
        "client_id",
        "clientId",
        "clientOrderId",
        "client_oid",
        "clientOid",
    )
    _EXCHANGE_ID_FIELDS = (
        "order_id",
        "orderId",
        "id",
    )
    _AMOUNT_FIELDS = ("amount", "number", "deal_stock", "size")
    _PRICE_FIELDS = ("price", "deal_price", "avg_price")
    _ORDER_MATCH_TOLERANCE = Decimal("0.0001")

    web_utils = web_utils

    def __init__(
        self,
        biconomy_api_key: str,
        biconomy_api_secret: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._api_key = biconomy_api_key
        self._secret_key = biconomy_api_secret
        self._trading_pairs = trading_pairs or []
        self._trading_required = trading_required
        self._domain = domain
        self._last_poll_timestamp = 0
        self._exchange_trading_pair_map: bidict[str, str] = bidict()
        self._empty_balance_logged = False

        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def authenticator(self):
        return biconomy_auth.BiconomyAuth(api_key=self._api_key, secret_key=self._secret_key)

    @property
    def name(self) -> str:
        return "biconomy"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.PAIRS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.PAIRS_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.TICKERS_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        response = await self._api_get(path_url=CONSTANTS.TICKERS_PATH_URL)
        return response.get("ticker", []) if isinstance(response, dict) else response

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            domain=self._domain,
            auth=self._auth,
        )

    def _create_order_book_data_source(self):
        return BiconomyAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BiconomyAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            domain=self._domain,
            api_factory=self._web_assistants_factory,
        )

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        side = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        payload: Dict[str, Any] = {
            "market": symbol,
            "side": side,
            "amount": f"{amount:f}",
            "client_id": order_id,
            "clientOrderId": order_id,
            "client_oid": order_id,
        }
        endpoint = CONSTANTS.MARKET_ORDER_PATH_URL
        if order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER):
            payload["price"] = f"{price:f}"
            endpoint = CONSTANTS.LIMIT_ORDER_PATH_URL

        response = await self._api_post(path_url=endpoint, data=payload, is_auth_required=True)
        order_data = self._extract_result(response, context="place order")
        exchange_order_id = str(order_data.get("id"))
        update_timestamp = float(order_data.get("ctime", time.time()))
        return exchange_order_id, update_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        start_ts = self.current_timestamp
        refresh_attempted = False
        exchange_order_id = tracked_order.exchange_order_id
        if exchange_order_id is None:
            refresh_attempted = True
            self.logger().warning(
                "Cancel instrumentation: missing exchange id for %s; attempting refresh.",
                tracked_order.client_order_id,
            )
            exchange_order_id = await self._refresh_exchange_order_id(tracked_order)
        if exchange_order_id is None:
            self.logger().error(
                "Cancel instrumentation: refresh failed for %s (age=%.1fs).",
                tracked_order.client_order_id,
                max(0.0, start_ts - tracked_order.creation_timestamp),
            )
            raise IOError("Cannot cancel order without exchange order id on Biconomy.")
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        payload = {
            "market": symbol,
            "order_id": exchange_order_id,
        }
        self.logger().info(
            "Cancel instrumentation: submitting cancel order=%s exchange_id=%s symbol=%s refresh_attempted=%s age=%.1fs",
            tracked_order.client_order_id,
            exchange_order_id,
            symbol,
            refresh_attempted,
            max(0.0, start_ts - tracked_order.creation_timestamp),
        )
        try:
            response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH_URL, data=payload, is_auth_required=True)
            self._extract_result(response, context="cancel order")
            self.logger().info(
                "Cancel instrumentation: cancel acknowledged order=%s exchange_id=%s elapsed=%.3fs",
                tracked_order.client_order_id,
                exchange_order_id,
                max(0.0, self.current_timestamp - start_ts),
            )
            return True
        except IOError as exc:
            self.logger().warning(
                "Cancel instrumentation: cancel request errored order=%s exchange_id=%s reason=%s",
                tracked_order.client_order_id,
                exchange_order_id,
                exc,
            )
            if self._is_missing_order_error(exc):
                self._finalize_missing_cancel(tracked_order, str(exc))
                return True
            raise

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        incomplete_orders = [order for order in self.in_flight_orders.values() if not order.is_done]
        if not incomplete_orders:
            return []

        order_ids_pending = {order.client_order_id for order in incomplete_orders}
        successful_cancellations: List[CancellationResult] = []

        try:
            async with timeout(timeout_seconds):
                batch_candidates, fallback_orders = await self._build_batch_cancel_candidates(incomplete_orders)
                batch_successful, batch_failed = await self._execute_batch_cancel(batch_candidates)

                for order in batch_successful:
                    successful_cancellations.append(CancellationResult(order.client_order_id, True))
                    order_ids_pending.discard(order.client_order_id)

                fallback_orders.extend(batch_failed)
                fallback_successes = await self._execute_individual_cancels(fallback_orders)
                for client_order_id in fallback_successes:
                    successful_cancellations.append(CancellationResult(client_order_id, True))
                    order_ids_pending.discard(client_order_id)
        except Exception:
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order. Check API key and network connection.",
            )

        failed_cancellations = [CancellationResult(order_id, False) for order_id in order_ids_pending]
        return successful_cancellations + failed_cancellations

    async def _build_batch_cancel_candidates(
        self, orders: List[InFlightOrder]
    ) -> Tuple[List[Tuple[InFlightOrder, Dict[str, str]]], List[InFlightOrder]]:
        candidates: List[Tuple[InFlightOrder, Dict[str, str]]] = []
        fallback_orders: List[InFlightOrder] = []
        for order in orders:
            exchange_order_id = order.exchange_order_id
            if exchange_order_id is None:
                try:
                    exchange_order_id = await self._refresh_exchange_order_id(order)
                except Exception as exc:
                    self.logger().warning(
                        "Cancel instrumentation: failed to refresh exchange id for %s during batch prep: %s",
                        order.client_order_id,
                        exc,
                    )
                    fallback_orders.append(order)
                    continue
            if exchange_order_id is None:
                fallback_orders.append(order)
                continue
            try:
                symbol = await self.exchange_symbol_associated_to_pair(order.trading_pair)
            except Exception as exc:
                self.logger().warning(
                    "Cancel instrumentation: unable to derive symbol for %s: %s",
                    order.client_order_id,
                    exc,
                )
                fallback_orders.append(order)
                continue
            payload = {
                "market": symbol,
                "order_id": str(exchange_order_id),
            }
            candidates.append((order, payload))
        return candidates, fallback_orders

    async def _execute_batch_cancel(
        self, candidates: List[Tuple[InFlightOrder, Dict[str, str]]]
    ) -> Tuple[List[InFlightOrder], List[InFlightOrder]]:
        successful: List[InFlightOrder] = []
        failed: List[InFlightOrder] = []
        if not candidates:
            return successful, failed

        chunk_count = max(1, (len(candidates) + self.BULK_CANCEL_MAX_SIZE - 1) // self.BULK_CANCEL_MAX_SIZE)
        self.logger().info(
            "Cancel instrumentation: attempting batch cancel for %d orders across %d chunk(s).",
            len(candidates),
            chunk_count,
        )
        for chunk in self._chunk_list(candidates, self.BULK_CANCEL_MAX_SIZE):
            payload_entries = [payload for _, payload in chunk]
            order_lookup = {payload["order_id"]: order for order, payload in chunk}
            try:
                response = await self._api_post(
                    path_url=CONSTANTS.BULK_CANCEL_PATH_URL,
                    data={"orders_json": json.dumps(payload_entries)},
                    is_auth_required=True,
                )
                result_entries = self._extract_entries(
                    self._extract_result(response, context="bulk cancel orders")
                )
            except Exception as exc:
                self.logger().warning(
                    "Cancel instrumentation: batch cancel request failed for %d orders: %s",
                    len(chunk),
                    exc,
                )
                failed.extend(order_lookup.values())
                continue

            if not result_entries:
                self.logger().warning(
                    "Cancel instrumentation: batch cancel response was empty for %d orders; falling back to single cancels.",
                    len(chunk),
                )
                failed.extend(order_lookup.values())
                continue

            acknowledged_ids = set()
            for entry in result_entries:
                if not isinstance(entry, dict):
                    continue
                exchange_order_id = str(entry.get("order_id") or "")
                if not exchange_order_id:
                    continue
                acknowledged_ids.add(exchange_order_id)
                order = order_lookup.get(exchange_order_id)
                if order is None:
                    continue
                if self._is_truthy(entry.get("result")):
                    self._apply_cancel_update(order)
                    successful.append(order)
                else:
                    self.logger().warning(
                        "Cancel instrumentation: batch cancel rejected order=%s exchange_id=%s",
                        order.client_order_id,
                        order.exchange_order_id,
                    )
                    failed.append(order)

            missing_ids = set(order_lookup.keys()) - acknowledged_ids
            for missing_id in missing_ids:
                order = order_lookup[missing_id]
                self.logger().warning(
                    "Cancel instrumentation: batch cancel response missing order=%s exchange_id=%s",
                    order.client_order_id,
                    order.exchange_order_id,
                )
                failed.append(order)

        return successful, failed

    def _apply_cancel_update(self, order: InFlightOrder):
        update_timestamp = self.current_timestamp
        if update_timestamp is None or update_timestamp != update_timestamp:
            update_timestamp = time.time()
        order_update = OrderUpdate(
            client_order_id=order.client_order_id,
            trading_pair=order.trading_pair,
            update_timestamp=update_timestamp,
            new_state=(
                OrderState.CANCELED
                if self.is_cancel_request_in_exchange_synchronous
                else OrderState.PENDING_CANCEL
            ),
        )
        self._order_tracker.process_order_update(order_update)

    async def _execute_individual_cancels(self, orders: List[InFlightOrder]) -> List[str]:
        if not orders:
            return []
        tasks = [self._execute_cancel(order.trading_pair, order.client_order_id) for order in orders]
        results = await safe_gather(*tasks, return_exceptions=True)
        successful_ids: List[str] = []
        for result in results:
            if isinstance(result, Exception) or result is None:
                continue
            successful_ids.append(result)
        return successful_ids

    @staticmethod
    def _chunk_list(source: List[Tuple[InFlightOrder, Dict[str, str]]], size: int):
        for start in range(0, len(source), size):
            yield source[start:start + size]

    @staticmethod
    def _is_truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "success"}
        if isinstance(value, (int, float)):
            return value != 0
        return False

    async def _refresh_exchange_order_id(self, tracked_order: InFlightOrder) -> Optional[str]:
        """Attempt to reload the exchange order id from REST before raising on cancel."""
        self.logger().info(
            "Cancel instrumentation: refreshing exchange id for %s (current=%s)",
            tracked_order.client_order_id,
            tracked_order.exchange_order_id,
        )
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        except Exception:
            self.logger().debug(
                "Cancel instrumentation: unable to derive symbol for %s while refreshing exchange id.",
                tracked_order.client_order_id,
            )
            return None
        order_entry = await self._find_order_entry_via_rest(tracked_order=tracked_order, symbol=symbol)
        if order_entry is not None:
            exchange_order_id = self._extract_exchange_order_id(order_entry)
            if exchange_order_id is not None:
                tracked_order.update_exchange_order_id(exchange_order_id)
                self.logger().info(
                    "Cancel instrumentation: refreshed exchange id for %s -> %s",
                    tracked_order.client_order_id,
                    exchange_order_id,
                )
            self._sync_tracked_order_with_remote_entry(tracked_order, order_entry)
            return tracked_order.exchange_order_id
        order_age = max(0.0, self.current_timestamp - tracked_order.creation_timestamp)
        self.logger().warning(
            "Cancel instrumentation: REST refresh could not find %s on Biconomy (age=%.1fs).",
            tracked_order.client_order_id,
            order_age,
        )
        if order_age > 30:
            self.logger().warning(
                "Unable to refresh exchange_order_id for %s after %.1fs; marking order as failed.",
                tracked_order.client_order_id,
                order_age,
            )
            self._update_order_after_failure(
                order_id=tracked_order.client_order_id,
                trading_pair=tracked_order.trading_pair,
                exception=IOError("order not found while refreshing exchange id"),
            )
        return None

    async def _find_order_entry_via_rest(self, tracked_order: InFlightOrder, symbol: str) -> Optional[Dict[str, Any]]:
        endpoints = (
            (CONSTANTS.PENDING_ORDERS_PATH_URL, "list pending orders"),
            (CONSTANTS.FINISHED_ORDERS_PATH_URL, "list finished orders"),
        )
        for path_url, context in endpoints:
            payload = {
                "market": symbol,
                "page": 1,
                "page_size": 200,
            }
            try:
                response = await self._api_post(path_url=path_url, data=payload, is_auth_required=True)
                entries = self._extract_entries(self._extract_result(response, context=context))
                self.logger().debug(
                    "Cancel instrumentation: fetched %d entries from %s for order=%s",
                    len(entries),
                    context,
                    tracked_order.client_order_id,
                )
            except Exception as exc:
                self.logger().debug(
                    "Failed to fetch %s for %s: %s",
                    context,
                    tracked_order.client_order_id,
                    exc,
                )
                continue
            matched_entry = self._locate_tracked_order_entry(tracked_order, entries)
            if matched_entry is not None:
                self.logger().info(
                    "Cancel instrumentation: located %s for order=%s exchange_id=%s status=%s",
                    context,
                    tracked_order.client_order_id,
                    matched_entry.get("id") or matched_entry.get("order_id"),
                    matched_entry.get("status"),
                )
                return matched_entry
        return None

    def _locate_tracked_order_entry(self, tracked_order: InFlightOrder, entries: List[Any]) -> Optional[Dict[str, Any]]:
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if self._entry_matches_tracked_order(tracked_order, entry):
                return entry
        return None

    def _entry_matches_tracked_order(self, tracked_order: InFlightOrder, entry: Dict[str, Any]) -> bool:
        client_id = self._extract_client_order_id(entry)
        if client_id is not None and client_id == tracked_order.client_order_id:
            return True
        exchange_order_id = self._extract_exchange_order_id(entry)
        if exchange_order_id is not None and tracked_order.exchange_order_id is not None:
            if exchange_order_id == tracked_order.exchange_order_id:
                return True
        matches = 0
        expected_side = CONSTANTS.SIDE_BUY if tracked_order.trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        entry_side = self._coerce_entry_side(entry.get("side") or entry.get("type"))
        if entry_side is not None:
            if entry_side != expected_side:
                return False
            matches += 1
        entry_amount = self._decimal_from_entry_fields(entry, self._AMOUNT_FIELDS)
        if entry_amount > Decimal("0") and tracked_order.amount > Decimal("0"):
            if self._values_close(entry_amount, tracked_order.amount):
                matches += 1
            else:
                return False
        entry_price = self._decimal_from_entry_fields(entry, self._PRICE_FIELDS)
        if tracked_order.price is not None and tracked_order.price > Decimal("0") and entry_price > Decimal("0"):
            if self._values_close(entry_price, tracked_order.price):
                matches += 1
            else:
                return False
        return matches >= 2

    def _extract_client_order_id(self, entry: Dict[str, Any]) -> Optional[str]:
        for field in self._CLIENT_ID_FIELDS:
            value = entry.get(field)
            if isinstance(value, str) and value:
                return value
        return None

    def _extract_exchange_order_id(self, entry: Dict[str, Any]) -> Optional[str]:
        for field in self._EXCHANGE_ID_FIELDS:
            value = entry.get(field)
            if value is None:
                continue
            try:
                return str(value)
            except Exception:
                continue
        return None

    def _sync_tracked_order_with_remote_entry(self, tracked_order: InFlightOrder, entry: Dict[str, Any]):
        try:
            order_state = self._order_state_from_status(entry)
        except Exception:
            return
        if order_state in {OrderState.CANCELED, OrderState.FILLED, OrderState.FAILED}:
            update = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self._coerce_timestamp(entry.get("mtime") or entry.get("update_time") or entry.get("ctime")),
                new_state=order_state,
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
            )
            self._order_tracker.process_order_update(update)

    def _coerce_entry_side(self, side_value: Any) -> Optional[int]:
        if side_value is None:
            return None
        if isinstance(side_value, str):
            lowered = side_value.lower()
            if lowered == "buy":
                return CONSTANTS.SIDE_BUY
            if lowered == "sell":
                return CONSTANTS.SIDE_SELL
            if lowered.isdigit():
                try:
                    return int(lowered)
                except Exception:
                    return None
        if isinstance(side_value, (int, float)):
            try:
                return int(side_value)
            except Exception:
                return None
        return None

    def _decimal_from_entry_fields(self, entry: Dict[str, Any], fields: Tuple[str, ...]) -> Decimal:
        for field in fields:
            if field in entry and entry[field] is not None:
                return self._decimal_from_value(entry[field])
        return Decimal("0")

    def _values_close(self, lhs: Decimal, rhs: Decimal) -> bool:
        if rhs == Decimal("0"):
            return lhs == rhs
        tolerance = max(Decimal("1e-8"), rhs.copy_abs() * self._ORDER_MATCH_TOLERANCE)
        return abs(lhs - rhs) <= tolerance

    def _is_missing_order_error(self, exception: Exception) -> bool:
        message = str(exception).lower()
        return "order not found" in message or "does not exist" in message

    def _finalize_missing_cancel(self, tracked_order: InFlightOrder, reason: str):
        self.logger().warning(
            "Cancel fallback: order %s not present on exchange (%s). Treating as canceled.",
            tracked_order.client_order_id,
            reason,
        )
        update = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=OrderState.CANCELED,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
        )
        self._order_tracker.process_order_update(update)

    async def _update_balances(self):
        response = await self._api_post(path_url=CONSTANTS.USER_BALANCES_PATH_URL, is_auth_required=True)
        balances = self._extract_result(response, context="fetch balances") or {}
        local_assets = set(self._account_balances.keys())
        remote_assets = set()
        balance_entries = self._collect_balance_entries(balances)

        if not balance_entries:
            self.logger().warning(
                "Balance endpoint returned no usable assets. raw_payload=%s",
                balances,
            )
            balance_entries = self._build_balance_fallback_entries()
        else:
            self._empty_balance_logged = False

        for entry in balance_entries:
            asset = (entry.get("asset") or entry.get("currency") or entry.get("coin") or entry.get("symbol") or "").upper()
            if not asset:
                continue
            available = Decimal(str(entry.get("available") or entry.get("free") or entry.get("available_balance") or "0"))
            frozen = Decimal(str(entry.get("freeze") or entry.get("frozen") or entry.get("locked") or "0"))
            other_frozen = Decimal(str(entry.get("other_freeze") or entry.get("other_locked") or "0"))
            total = available + frozen + other_frozen
            self._account_available_balances[asset] = available
            self._account_balances[asset] = total
            remote_assets.add(asset)
        if not remote_assets:
            self.logger().warning(
                "Balance endpoint parsed entries without assets. entries=%s raw_payload=%s",
                balance_entries,
                balances,
            )
            fallback_entries = self._build_balance_fallback_entries()
            for entry in fallback_entries:
                asset = entry["asset"]
                available = Decimal(str(entry.get("available", "0")))
                self._account_available_balances[asset] = available
                self._account_balances[asset] = available
            remote_assets = {entry["asset"] for entry in fallback_entries}
        for asset in local_assets.difference(remote_assets):
            self._account_available_balances.pop(asset, None)
            self._account_balances.pop(asset, None)

    def _collect_balance_entries(self, payload: Any, parent_asset: Optional[str] = None) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            for child in payload:
                entries.extend(self._collect_balance_entries(child, parent_asset))
        elif isinstance(payload, Mapping):
            if self._is_balance_entry(payload):
                entry_dict = dict(payload)
                if parent_asset:
                    entry_dict.setdefault("asset", parent_asset)
                entries.append(entry_dict)
            # Always continue descending so nested asset maps inside balance entries
            for key, child in payload.items():
                entries.extend(self._collect_balance_entries(child, str(key)))
        return entries

    @staticmethod
    def _is_balance_entry(candidate: Mapping[str, Any]) -> bool:
        balance_keys = (
            "available",
            "available_balance",
            "balance",
            "free",
            "total",
            "total_balance",
            "freeze",
            "frozen",
            "locked",
            "other_freeze",
        )
        for key in balance_keys:
            if key in candidate:
                value = candidate.get(key)
                if not isinstance(value, Mapping):
                    return True
        return False

    def _build_balance_fallback_entries(self) -> List[Dict[str, str]]:
        fallback_assets = set()
        for trading_pair in self._trading_pairs:
            try:
                base, quote = _split_trading_pair(trading_pair)
                fallback_assets.update([base, quote])
            except Exception:
                continue
        entries: List[Dict[str, str]] = []
        if fallback_assets:
            if not self._empty_balance_logged:
                self.logger().warning(
                    "Balance endpoint returned no assets; defaulting to zero for configured pairs."
                )
                self._empty_balance_logged = True
            entries = [{"asset": asset, "available": "0"} for asset in sorted(fallback_assets)]
        return entries

    async def _create_order_tracker_entry(self, order_id: str, trading_pair: str):
        return None

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        result: Dict[str, float] = {}
        symbol_map: Dict[str, str] = {}
        for trading_pair in trading_pairs:
            try:
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
            except Exception as exc:
                self.logger().debug("Failed to derive exchange symbol for %s: %s", trading_pair, exc)
                continue
            symbol_map[symbol.upper()] = trading_pair

        ticker_entries: List[Mapping[str, Any]] = []
        try:
            raw_prices = await self.get_all_pairs_prices()
        except Exception as exc:
            self.logger().warning("Ticker request failed, will attempt fallbacks: %s", exc)
            raw_prices = []

        if isinstance(raw_prices, Mapping):
            maybe_entries = raw_prices.get("ticker") or raw_prices.get("data") or []
        else:
            maybe_entries = raw_prices

        for entry in maybe_entries:
            if isinstance(entry, Mapping):
                ticker_entries.append(entry)

        for entry in ticker_entries:
            symbol = (entry.get("symbol") or entry.get("market") or entry.get("pair") or "").replace("-", "_").upper()
            trading_pair = symbol_map.get(symbol)
            if trading_pair is None:
                continue
            price_value = entry.get("last") or entry.get("close") or entry.get("price")
            parsed_price = self._safe_float(price_value)
            if parsed_price is None or parsed_price <= 0:
                continue
            result[trading_pair] = parsed_price

        missing_pairs = [tp for tp in trading_pairs if tp not in result]
        for trading_pair in missing_pairs:
            try:
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
            except Exception:
                symbol = None
            fallback_price = None
            if symbol:
                fallback_price = await self._get_last_trade_price(symbol)
            if fallback_price is None:
                fallback_price = self._get_mid_price(trading_pair)
            if fallback_price is not None:
                result[trading_pair] = fallback_price

        return result

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        prices = await self.get_last_traded_prices([trading_pair])
        price = prices.get(trading_pair)
        if price is None:
            return 0.0
        return float(price)

    async def _get_last_trade_price(self, symbol: str) -> Optional[float]:
        params = {"symbol": symbol, "size": "1"}
        try:
            response = await self._api_get(CONSTANTS.TRADES_PATH_URL, params=params)
        except Exception as exc:
            self.logger().debug("Trade request failed for %s: %s", symbol, exc)
            return None

        trades: Any
        if isinstance(response, Mapping):
            trades = response.get("data") or response.get("result") or []
        else:
            trades = response

        if isinstance(trades, list):
            for trade in trades:
                if not isinstance(trade, Mapping):
                    continue
                price = self._safe_float(trade.get("price"))
                if price is not None and price > 0:
                    return price
        return None

    def _get_mid_price(self, trading_pair: str) -> Optional[float]:
        try:
            tracker = self.order_book_tracker
            order_book = tracker.order_book(trading_pair) if tracker is not None else None
        except Exception:
            order_book = None
        if order_book is None:
            return None
        mid_price = order_book.get_mid_price()
        if mid_price is None or mid_price <= 0:
            return None
        return float(mid_price)

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        symbol = self._exchange_trading_pair_map.inverse.get(trading_pair)
        if symbol is None:
            base, quote = trading_pair.rsplit("-", 1)
            symbol = f"{base}_{quote}"
            self._exchange_trading_pair_map[symbol] = trading_pair
        return symbol

    async def trading_pair_associated_to_exchange_symbol(self, symbol: str) -> str:
        trading_pair = self._exchange_trading_pair_map.get(symbol)
        if trading_pair is None:
            base, quote = symbol.split("_", 1)
            trading_pair = _combine_trading_pair(base, quote)
            self._exchange_trading_pair_map[symbol] = trading_pair
        return trading_pair

    async def _api_get(self, path_url: str, params: Optional[Dict[str, str]] = None, **kwargs):
        return await super()._api_get(path_url=path_url, params=params, **kwargs)

    async def _api_post(self, path_url: str, data: Optional[Dict[str, str]] = None, **kwargs):
        return await super()._api_post(path_url=path_url, data=data, **kwargs)

    def _extract_result(self, response: Dict[str, Any], context: str) -> Any:
        if not isinstance(response, dict):
            raise IOError(f"{context} failed. Unexpected response: {response}")
        code = response.get("code")
        if code not in (0, "0", None):
            message = response.get("message", "")
            raise IOError(f"{context} failed ({code}): {message}")
        return response.get("result", response)

    @property
    def order_book_tracker(self):
        return super().order_book_tracker

    def supported_events(self) -> List[MarketEvent]:
        return [MarketEvent.TradeUpdate, MarketEvent.OrderUpdate]

    async def _update_trading_fees(self):
        from hummingbot.client.config.trade_fee_schema_loader import TradeFeeSchemaLoader

        fee_schema = TradeFeeSchemaLoader.configured_schema_for_exchange(self.name)
        for trading_pair in self._trading_pairs:
            self._trading_fees[trading_pair] = fee_schema

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                await self._handle_user_stream_event(event_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error while handling user event from Biconomy stream.")
                await self._sleep(5.0)

    async def _handle_user_stream_event(self, event_message: Any):
        if isinstance(event_message, dict):
            method = event_message.get("method")
            params = event_message.get("params") or []
            if method == "asset.update":
                self._process_balance_event(params)
            elif method == "order.update":
                await self._process_order_event(params)
        elif isinstance(event_message, list) and len(event_message) >= 2:
            method = event_message[0]
            payload = event_message[1]
            await self._handle_user_stream_event({"method": method, "params": payload})

    def _process_balance_event(self, payload: Any):
        entries = payload if isinstance(payload, list) else [payload]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            asset = (entry.get("asset") or entry.get("currency") or entry.get("coin") or "").upper()
            if not asset:
                continue
            available = self._decimal_from_value(entry.get("available") or entry.get("available_balance") or entry.get("free") or "0")
            total = self._decimal_from_value(entry.get("total") or entry.get("total_balance") or entry.get("balance") or available)
            self._account_available_balances[asset] = available
            self._account_balances[asset] = total

    async def _process_order_event(self, payload: Any):
        entries = payload if isinstance(payload, list) else [payload]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            symbol = entry.get("market") or entry.get("symbol")
            if not symbol:
                continue
            trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol)
            exchange_order_id = str(entry.get("order_id") or entry.get("id") or "")
            client_order_id = entry.get("client_id") or entry.get("clientOrderId") or entry.get("client_oid")
            order_state = self._order_state_from_status(entry)
            timestamp = self._coerce_timestamp(entry.get("mtime") or entry.get("update_time") or entry.get("ctime"))
            order_update = OrderUpdate(
                trading_pair=trading_pair,
                update_timestamp=timestamp,
                new_state=order_state,
                client_order_id=client_order_id if isinstance(client_order_id, str) else None,
                exchange_order_id=exchange_order_id or None,
            )
            self._order_tracker.process_order_update(order_update)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []
        symbol = await self.exchange_symbol_associated_to_pair(order.trading_pair)
        payload = {"market": symbol, "order_id": order.exchange_order_id}
        response = await self._api_post(
            path_url=CONSTANTS.ORDER_DEALS_PATH_URL,
            data=payload,
            is_auth_required=True,
        )
        deals = self._extract_result(response, context="fetch order deals")
        deal_entries = self._extract_entries(deals)
        trade_updates: List[TradeUpdate] = []
        for idx, deal in enumerate(deal_entries):
            if not isinstance(deal, dict):
                continue
            base_amount = self._decimal_from_value(deal.get("amount") or deal.get("number") or deal.get("deal_stock"))
            price = self._decimal_from_value(deal.get("price"))
            if base_amount <= Decimal("0"):
                continue
            if price <= Decimal("0"):
                price = self._decimal_from_value(deal.get("avg_price") or deal.get("deal_money")) / base_amount if base_amount != 0 else Decimal("0")
            quote_amount = self._decimal_from_value(deal.get("deal_money") or (price * base_amount))
            fee_amount = self._decimal_from_value(deal.get("fee"))
            fee_asset = deal.get("fee_asset") or deal.get("fee_currency") or order.quote_asset
            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=order.trade_type,
                flat_fees=[TokenAmount(token=fee_asset, amount=fee_amount)] if fee_amount > 0 else None,
            )
            trade_update = TradeUpdate(
                trade_id=str(deal.get("id") or deal.get("deal_id") or f"{order.exchange_order_id}-{idx}"),
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                trading_pair=order.trading_pair,
                fill_timestamp=self._coerce_timestamp(deal.get("ctime") or deal.get("time")),
                fill_price=price,
                fill_base_amount=base_amount,
                fill_quote_amount=quote_amount,
                fee=fee,
            )
            trade_updates.append(trade_update)
        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        if tracked_order.exchange_order_id is None:
            raise IOError("There is no exchange order id on the tracked order.")
        symbol = await self.exchange_symbol_associated_to_pair(tracked_order.trading_pair)
        payload = {"market": symbol, "order_id": tracked_order.exchange_order_id}
        pending_response = await self._api_post(
            path_url=CONSTANTS.PENDING_ORDER_DETAIL_PATH_URL,
            data=payload,
            is_auth_required=True,
        )
        order_data = self._extract_order_entry(self._extract_result(pending_response, context="pending order detail"))
        if order_data is None:
            finished_response = await self._api_post(
                path_url=CONSTANTS.FINISHED_ORDER_DETAIL_PATH_URL,
                data=payload,
                is_auth_required=True,
            )
            order_data = self._extract_order_entry(self._extract_result(finished_response, context="finished order detail"))
        if order_data is None:
            raise IOError(f"Order {tracked_order.client_order_id} not found on Biconomy.")
        order_state = self._order_state_from_status(order_data)
        timestamp = self._coerce_timestamp(order_data.get("mtime") or order_data.get("update_time") or order_data.get("ctime"))
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=timestamp,
            new_state=order_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order_data.get("id") or tracked_order.exchange_order_id),
        )

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        trading_rules: List[TradingRule] = []
        entries = self._extract_entries(exchange_info_dict)
        for raw_entry in entries:
            if not isinstance(raw_entry, dict):
                continue
            entry = {k.strip() if isinstance(k, str) else k: v for k, v in raw_entry.items()}
            base_asset = (entry.get("baseAsset") or entry.get("base") or entry.get("base_currency") or "").upper()
            quote_asset = (entry.get("quoteAsset") or entry.get("quote") or entry.get("quote_currency") or "").upper()
            if not base_asset or not quote_asset:
                continue
            status = entry.get("status")
            if isinstance(status, str) and status.lower() not in ("trading", "enabled", "online", "active", "1"):
                continue
            if "-" in base_asset or "-" in quote_asset:
                self.logger().debug(
                    "Skipping trading rule for %s_%s because token contains hyphen",
                    base_asset,
                    quote_asset,
                )
                continue
            trading_pair = _combine_trading_pair(base_asset, quote_asset)
            min_price = self._decimal_from_value(entry.get("minPrice") or entry.get("tickSize") or entry.get("min_price") or Decimal("1e-8"))
            min_amount = self._decimal_from_value(
                entry.get("minAmount")
                or entry.get("minQty")
                or entry.get("min_amount")
                or entry.get("minQuantity")
                or entry.get("min_quantity")
                or Decimal("1e-8")
            )
            try:
                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_amount,
                    min_price_increment=min_price,
                    min_base_amount_increment=min_amount,
                    min_quote_amount_increment=min_price,
                )
            except Exception as exc:
                self.logger().warning("Skipping trading rule for %s: %s", trading_pair, exc)
                continue
            trading_rules.append(trading_rule)
        return trading_rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        entries = self._extract_entries(exchange_info)
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            base_asset = (entry.get("baseAsset") or entry.get("base") or entry.get("base_currency") or "").upper()
            quote_asset = (entry.get("quoteAsset") or entry.get("quote") or entry.get("quote_currency") or "").upper()
            symbol = entry.get("symbol") or f"{base_asset}_{quote_asset}"
            if not base_asset or not quote_asset or not symbol:
                continue
            trading_pair = _combine_trading_pair(base_asset, quote_asset)
            self._exchange_trading_pair_map[symbol] = trading_pair
        if len(self._exchange_trading_pair_map) > 0:
            self._set_trading_pair_symbol_map(bidict(self._exchange_trading_pair_map))

    def _extract_entries(self, exchange_info: Any) -> List[Any]:
        if isinstance(exchange_info, dict):
            for key in ("result", "data", "pairs", "symbols"):
                if key in exchange_info:
                    value = exchange_info[key]
                    if isinstance(value, list):
                        return value
                    if isinstance(value, dict):
                        return self._extract_entries(value)
            return [exchange_info]
        if isinstance(exchange_info, list):
            return exchange_info
        return []

    def _extract_order_entry(self, data: Any) -> Optional[Dict[str, Any]]:
        if data is None:
            return None
        if isinstance(data, dict):
            for key in ("order", "orders", "result", "data"):
                if key in data:
                    return self._extract_order_entry(data[key])  # type: ignore
            return data
        if isinstance(data, list):
            return data[0] if data else None
        return None

    def _order_state_from_status(self, order_payload: Dict[str, Any]) -> OrderState:
        status = order_payload.get("status")
        if isinstance(status, int) and status in CONSTANTS.ORDER_STATE_MAP:
            return CONSTANTS.ORDER_STATE_MAP[status]
        if isinstance(status, str):
            lowered = status.lower()
            if lowered in {"filled", "finished", "done", "success"}:
                return OrderState.FILLED
            if lowered in {"cancelled", "canceled", "cancel"}:
                return OrderState.CANCELED
        if order_payload.get("is_cancel") in (True, 1):
            return OrderState.CANCELED
        remaining_value = (
            order_payload.get("left")
            or order_payload.get("remain")
            or order_payload.get("unfilled_amount")
            or order_payload.get("remain_amount")
        )
        if remaining_value is not None:
            remaining = self._decimal_from_value(remaining_value)
            if remaining == Decimal("0"):
                return OrderState.FILLED
        return OrderState.OPEN

    def _decimal_from_value(self, value: Any) -> Decimal:
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    def _coerce_timestamp(self, value: Optional[Any]) -> float:
        if value is None:
            return time.time()
        try:
            timestamp = float(value)
        except Exception:
            return time.time()
        if timestamp > 1e12:
            timestamp /= 1e3
        return timestamp
