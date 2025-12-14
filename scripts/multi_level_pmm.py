import logging
import os
import random
from decimal import Decimal
from typing import Dict, List

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class MultiLevelPMMConfig(BaseClientModel):
    script_file_name: str = os.path.basename(__file__)
    exchange: str = Field("biconomy")
    trading_pair: str = Field("MCM-USDT")
    buy_levels: str = Field(default="0.05,100.0-0.08,200.0-0.12,300.0")  # spread,usdt_amount
    sell_levels: str = Field(default="0.05,100.0-0.08,200.0-0.12,300.0")  # spread,usdt_amount
    order_refresh_time_min: int = Field(10)
    order_refresh_time_max: int = Field(20)
    price_band_threshold: Decimal = Field(default=Decimal("0.02"))  # 2% price movement required
    fill_cooldown_time: int = Field(default=120)  # Seconds to pause level after fill
    price_type: str = Field("mid")
    sell_price_floor: Decimal = Field(default=Decimal("0.20"))  # Minimum price for sell order calculations


class MultiLevelPMM(ScriptStrategyBase):
    """
    Multi-level Pure Market Making Strategy
    
    This strategy places multiple buy and sell orders at different price levels around the mid price.
    Each level can have different spreads and amounts.
    """

    create_timestamp = 0
    price_source = PriceType.MidPrice

    @classmethod
    def init_markets(cls, config: MultiLevelPMMConfig):
        cls.markets = {config.exchange: {config.trading_pair}}
        cls.price_source = PriceType.LastTrade if config.price_type == "last" else PriceType.MidPrice

    def __init__(self, connectors: Dict[str, ConnectorBase], config: MultiLevelPMMConfig):
        super().__init__(connectors)
        self.config = config
        # Parse buy and sell levels from string format
        self.buy_levels_parsed = self.parse_levels(config.buy_levels)
        self.sell_levels_parsed = self.parse_levels(config.sell_levels)
        # Track order IDs created by this bot
        self.tracked_order_ids = set()
        # Track last price for price band checking
        self.last_refresh_price = None
        # Track cooldown periods for levels after fills: {level_index: cooldown_end_timestamp}
        self.buy_level_cooldowns = {}
        self.sell_level_cooldowns = {}
    
    def parse_levels(self, levels_str: str) -> List[List[Decimal]]:
        """Parse 'spread,usdt_amount-spread,usdt_amount' format into [[spread, usdt_amount], [spread, usdt_amount], ...]"""
        levels = []
        for level_str in levels_str.split("-"):
            spread, usdt_amount = level_str.split(",")
            levels.append([Decimal(spread), Decimal(usdt_amount)])
        return levels

    def on_tick(self):
        if self.create_timestamp <= self.current_timestamp:
            current_price = self.connectors[self.config.exchange].get_price_by_type(
                self.config.trading_pair, self.price_source
            )
            
            # Skip if price is not available (empty orderbook)
            if current_price is None or current_price == Decimal("0"):
                self.logger().warning("Price not available, skipping refresh")
                random_refresh = random.randint(self.config.order_refresh_time_min, self.config.order_refresh_time_max)
                self.create_timestamp = random_refresh + self.current_timestamp
                return
            
            # Check if price has moved enough to warrant refresh
            should_refresh = True
            if self.last_refresh_price is not None:
                price_change_pct = abs(current_price - self.last_refresh_price) / self.last_refresh_price
                # Convert to Decimal for comparison
                if Decimal(str(price_change_pct)) < self.config.price_band_threshold:
                    should_refresh = False
            
            if should_refresh:
                self.cancel_all_orders()
                proposal: List[OrderCandidate] = self.create_proposal()
                proposal_adjusted: List[OrderCandidate] = self.adjust_proposal_to_budget(proposal)
                self.place_orders(proposal_adjusted)
                self.last_refresh_price = current_price
            
            # Randomize refresh time between min and max
            random_refresh = random.randint(self.config.order_refresh_time_min, self.config.order_refresh_time_max)
            self.create_timestamp = random_refresh + self.current_timestamp

    def create_proposal(self) -> List[OrderCandidate]:
        ref_price = self.connectors[self.config.exchange].get_price_by_type(self.config.trading_pair, self.price_source)
        orders = []
        current_time = self.current_timestamp
        
        # Skip if price is not available
        if ref_price is None or ref_price == Decimal("0"):
            self.logger().warning("Reference price not available, skipping order creation")
            return orders
        
        # Get last trade price for validation (Biconomy uses this for ±50% rule)
        last_price = self.connectors[self.config.exchange].get_price_by_type(self.config.trading_pair, PriceType.LastTrade)
        if last_price is None or last_price == Decimal("0"):
            self.logger().warning("Last trade price not available, using ref_price for validation")
            last_price = ref_price
        
        # Biconomy's price limits: must be within 50% to 150% of last trade price
        min_buy_price = last_price * Decimal("0.50")
        max_sell_price = last_price * Decimal("1.50")
        
        self.logger().info(f"Creating proposal: mid={ref_price}, last={last_price}, buy_floor={min_buy_price}, sell_ceiling={max_sell_price}, buy_cooldowns={self.buy_level_cooldowns}, sell_cooldowns={self.sell_level_cooldowns}")

        # Create buy orders at multiple levels (skip those on cooldown)
        for idx, level in enumerate(self.buy_levels_parsed):
            # Check if this level is on cooldown
            if idx in self.buy_level_cooldowns and self.buy_level_cooldowns[idx] > current_time:
                self.logger().info(f"Skipping buy level {idx} - on cooldown until {self.buy_level_cooldowns[idx]}")
                continue  # Skip this level - still in cooldown
            
            spread = level[0]
            usdt_amount = level[1]
            # Calculate price based on mid-price for proper spread distribution
            buy_price = ref_price * Decimal(1 - spread)
            
            # Enforce Biconomy's minimum price (50% of last trade)
            if buy_price < min_buy_price:
                self.logger().info(f"Buy level {idx}: adjusting price from {buy_price:.4f} to floor {min_buy_price:.4f} (spread {spread*100:.1f}%)")
                buy_price = min_buy_price
            
            # Convert USDT amount to base currency amount
            base_amount = usdt_amount / buy_price
            buy_order = OrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.BUY,
                amount=base_amount,
                price=buy_price
            )
            orders.append(buy_order)

        # Create sell orders at multiple levels (skip those on cooldown)
        for idx, level in enumerate(self.sell_levels_parsed):
            # Check if this level is on cooldown
            if idx in self.sell_level_cooldowns and self.sell_level_cooldowns[idx] > current_time:
                self.logger().info(f"Skipping sell level {idx} - on cooldown until {self.sell_level_cooldowns[idx]}")
                continue  # Skip this level - still in cooldown
            
            spread = level[0]
            usdt_amount = level[1]
            # Use price floor as the base for sell order pricing and amounts
            base_price_for_sells = max(ref_price, self.config.sell_price_floor)
            sell_price = base_price_for_sells * Decimal(1 + spread)
            # Convert USDT amount to base currency amount using floored price
            base_amount = usdt_amount / base_price_for_sells
            sell_order = OrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=base_amount,
                price=sell_price
            )
            orders.append(sell_order)
        
        self.logger().info(f"Created {len(orders)} order candidates")

        return orders

    def adjust_proposal_to_budget(self, proposal: List[OrderCandidate]) -> List[OrderCandidate]:
        proposal_adjusted = self.connectors[self.config.exchange].budget_checker.adjust_candidates(proposal, all_or_none=True)
        return proposal_adjusted

    def place_orders(self, proposal: List[OrderCandidate]) -> None:
        for order in proposal:
            self.place_order(connector_name=self.config.exchange, order=order)

    def place_order(self, connector_name: str, order: OrderCandidate):
        if order.order_side == TradeType.SELL:
            order_id = self.sell(connector_name=connector_name, trading_pair=order.trading_pair, amount=order.amount,
                      order_type=order.order_type, price=order.price)
        elif order.order_side == TradeType.BUY:
            order_id = self.buy(connector_name=connector_name, trading_pair=order.trading_pair, amount=order.amount,
                     order_type=order.order_type, price=order.price)
        # Track the order ID so we only cancel our own orders
        if order_id:
            self.tracked_order_ids.add(order_id)

    def cancel_all_orders(self):
        for order in self.get_active_orders(connector_name=self.config.exchange):
            # Only cancel orders that this bot created
            if order.client_order_id in self.tracked_order_ids:
                self.cancel(self.config.exchange, order.trading_pair, order.client_order_id)
                # Remove from tracking after canceling
                self.tracked_order_ids.discard(order.client_order_id)

    def did_fill_order(self, event: OrderFilledEvent):
        msg = (f"{event.trade_type.name} {round(event.amount, 2)} {event.trading_pair} {self.config.exchange} at {round(event.price, 2)}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
        
        # Find which level this fill corresponds to and put it on cooldown
        ref_price = self.connectors[self.config.exchange].get_price_by_type(self.config.trading_pair, self.price_source)
        cooldown_end = self.current_timestamp + self.config.fill_cooldown_time
        
        if event.trade_type == TradeType.BUY:
            # Find closest buy level to this fill price
            for idx, level in enumerate(self.buy_levels_parsed):
                expected_price = ref_price * Decimal(1 - level[0])
                # If fill price is within 1% of expected level price
                if abs(event.price - expected_price) / expected_price < Decimal("0.01"):
                    self.buy_level_cooldowns[idx] = cooldown_end
                    self.logger().info(f"Buy level {idx} (spread {level[0]}) on cooldown until {cooldown_end}")
                    break
        
        elif event.trade_type == TradeType.SELL:
            # Find closest sell level to this fill price
            for idx, level in enumerate(self.sell_levels_parsed):
                expected_price = ref_price * Decimal(1 + level[0])
                # If fill price is within 1% of expected level price
                if abs(event.price - expected_price) / expected_price < Decimal("0.01"):
                    self.sell_level_cooldowns[idx] = cooldown_end
                    self.logger().info(f"Sell level {idx} (spread {level[0]}) on cooldown until {cooldown_end}")
                    break
