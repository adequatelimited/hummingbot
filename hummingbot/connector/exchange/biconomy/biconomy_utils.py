from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"
USE_ETHEREUM_WALLET = False
USE_ETH_GAS_LOOKUP = False

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True,
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """Placeholder validation hook (Biconomy does not expose explicit status fields)."""
    return True


class BiconomyConfigMap(BaseConnectorConfigMap):
    connector: str = "biconomy"
    biconomy_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Biconomy API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    biconomy_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Biconomy API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

    model_config = ConfigDict(title="biconomy")


KEYS = BiconomyConfigMap.model_construct()
OTHER_DOMAINS: list[str] = []
OTHER_DOMAINS_KEYS: Dict[str, BaseConnectorConfigMap] = {}
OTHER_DOMAINS_PARAMETER: Dict[str, str] = {}
OTHER_DOMAINS_DEFAULT_FEES: Dict[str, TradeFeeSchema] = {}
OTHER_DOMAINS_EXAMPLE_PAIR: Dict[str, str] = {}
