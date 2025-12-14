from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "vip"

PUBLIC_REST_URL = "https://api.biconomy.{}/"
WS_PUBLIC_URL = "wss://bei.biconomy.com/ws"

PUBLIC_API_VERSION = "api/v1"
PRIVATE_API_VERSION = "api/v2"

# Header constants
X_SITE_ID_HEADER = "127"
X_SITE_ID_HEADER_KEY = "X-SITE-ID"
CONTENT_TYPE = "application/x-www-form-urlencoded"
WS_REQUEST_HEADERS = {
    X_SITE_ID_HEADER_KEY: X_SITE_ID_HEADER,
}

# Public REST endpoints
TICKERS_PATH_URL = "/tickers"
DEPTH_PATH_URL = "/depth"
TRADES_PATH_URL = "/trades"
KLINE_PATH_URL = "/kline"
PAIRS_PATH_URL = "/exchangeInfo"
SERVER_TIME_PATH_URL = "/time"

# Private REST endpoints
USER_BALANCES_PATH_URL = "/private/user"
LIMIT_ORDER_PATH_URL = "/private/trade/limit"
MARKET_ORDER_PATH_URL = "/private/trade/market"
CANCEL_ORDER_PATH_URL = "/private/trade/cancel"
BULK_CANCEL_PATH_URL = "/private/trade/cancel_batch"
PENDING_ORDERS_PATH_URL = "/private/order/pending"
PENDING_ORDER_DETAIL_PATH_URL = "/private/order/pending/detail"
FINISHED_ORDERS_PATH_URL = "/private/order/finished"
FINISHED_ORDER_DETAIL_PATH_URL = "/private/order/finished/detail"
ORDER_DEALS_PATH_URL = "/private/order/deals"

# Empirically the server drops the connection after ~40 seconds without a ping even though
# the public docs still claim a 180-second heartbeat. Ping every 10 seconds to stay connected.
HEARTBEAT_INTERVAL = 10
CLIENT_PING_INTERVAL = HEARTBEAT_INTERVAL

# Order / trade constants
SIDE_BUY = 2
SIDE_SELL = 1
ORDER_TYPE_LIMIT = 1
ORDER_TYPE_MARKET = 2

ORDER_EVENT_PUT = 1
ORDER_EVENT_UPDATE = 2
ORDER_EVENT_FINISH = 3

ORDER_STATE_MAP = {
    ORDER_EVENT_PUT: OrderState.OPEN,
    ORDER_EVENT_UPDATE: OrderState.OPEN,
    ORDER_EVENT_FINISH: OrderState.FILLED,
}

HBOT_ORDER_ID_PREFIX = "x-HBOTBICO"
MAX_ORDER_ID_LEN = 32

# Rate limit definitions (per docs: public 10 req/s, private 20 req/s)
SECOND = 1

PUBLIC_WEIGHT = "public"
PRIVATE_WEIGHT = "private"

RATE_LIMITS = [
    RateLimit(limit_id=PUBLIC_WEIGHT, limit=10, time_interval=SECOND),
    RateLimit(limit_id=PRIVATE_WEIGHT, limit=20, time_interval=SECOND),
    RateLimit(limit_id=TICKERS_PATH_URL, limit=10, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_WEIGHT, 1)]),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=10, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_WEIGHT, 1)]),
    RateLimit(limit_id=TRADES_PATH_URL, limit=10, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_WEIGHT, 1)]),
    RateLimit(limit_id=KLINE_PATH_URL, limit=10, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_WEIGHT, 1)]),
    RateLimit(limit_id=PAIRS_PATH_URL, limit=10, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_WEIGHT, 1)]),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=10, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_WEIGHT, 1)]),
    RateLimit(limit_id=USER_BALANCES_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
    RateLimit(limit_id=LIMIT_ORDER_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
    RateLimit(limit_id=MARKET_ORDER_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
    RateLimit(limit_id=BULK_CANCEL_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
    RateLimit(limit_id=PENDING_ORDERS_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
    RateLimit(limit_id=PENDING_ORDER_DETAIL_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
    RateLimit(limit_id=FINISHED_ORDERS_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
    RateLimit(limit_id=FINISHED_ORDER_DETAIL_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
    RateLimit(limit_id=ORDER_DEALS_PATH_URL, limit=20, time_interval=SECOND,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_WEIGHT, 1)]),
]
