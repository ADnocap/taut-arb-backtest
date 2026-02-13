"""Configuration constants and asset definitions."""

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class AssetConfig:
    name: str
    keywords: list[str]
    deribit_currency: str
    perpetual_name: str
    is_inverse: bool
    instrument_prefix: str
    threshold_min: float
    threshold_max: float


ASSETS = {
    "BTC": AssetConfig(
        name="BTC",
        keywords=["bitcoin", "btc"],
        deribit_currency="BTC",
        perpetual_name="BTC-PERPETUAL",
        is_inverse=True,
        instrument_prefix="BTC-",
        threshold_min=1000,
        threshold_max=500_000,
    ),
    "ETH": AssetConfig(
        name="ETH",
        keywords=["ethereum", "eth", "ether"],
        deribit_currency="ETH",
        perpetual_name="ETH-PERPETUAL",
        is_inverse=True,
        instrument_prefix="ETH-",
        threshold_min=100,
        threshold_max=50_000,
    ),
    "SOL": AssetConfig(
        name="SOL",
        keywords=["solana", "sol"],
        deribit_currency="USDC",
        perpetual_name="SOL_USDC-PERPETUAL",
        is_inverse=False,
        instrument_prefix="SOL_USDC-",
        threshold_min=1,
        threshold_max=5000,
    ),
    "XRP": AssetConfig(
        name="XRP",
        keywords=["ripple", "xrp"],
        deribit_currency="USDC",
        perpetual_name="XRP_USDC-PERPETUAL",
        is_inverse=False,
        instrument_prefix="XRP_USDC-",
        threshold_min=0.1,
        threshold_max=100,
    ),
}

# --- API URLs ---
CLOB_BASE_URL = "https://clob.polymarket.com"
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
GOLDSKY_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/subgraphs/"
    "orderbook-subgraph/0.0.1/gn"
)
DERIBIT_HISTORY_URL = "https://history.deribit.com/api/v2/public"
DERIBIT_MAIN_URL = "https://www.deribit.com/api/v2/public"

# --- Pagination ---
CLOB_PAGE_LIMIT = 100
GAMMA_PAGE_LIMIT = 100
DERIBIT_TRADE_COUNT = 10000
DERIBIT_MAX_PAGES_PER_DAY = 20
GOLDSKY_PAGE_SIZE = 1000

# --- Concurrency ---
POLYMARKET_SEMAPHORE = 20
DERIBIT_SEMAPHORE = 10
GOLDSKY_SEMAPHORE = 20

# --- Timing ---
REQUEST_TIMEOUT = 30
RETRY_DELAYS = [1, 2, 4, 8, 16, 32]
MAX_RETRIES = 5
PRICE_FIDELITY_MINUTES = 30
PRICE_LOOKBACK_DAYS = 7
DERIBIT_CHUNK_DAYS = 30
DVOL_RESOLUTION = "3600"

# --- Goldsky ---
GOLDSKY_EARLIEST_TIMESTAMP = 1744013119  # April 7, 2025
USDC_ASSET_ID = "0"

# --- Default date range ---
DEFAULT_COLLECTION_START = datetime(2025, 4, 7, tzinfo=timezone.utc)

# --- Exclusion topics for market filtering ---
EXCLUDE_TOPICS = [
    "volatility index", "gas price", "floor price", "dominance",
    "market cap", "nft", "token price", "total supply",
    "trading volume", "hash rate", "hashrate",
]

# --- Database ---
DB_PATH = "backtest_data.db"

# --- Asset keyword lookup (flat) ---
ASSET_KEYWORDS = {
    asset: cfg.keywords for asset, cfg in ASSETS.items()
}

# --- Barrier keywords for pre-filtering ---
BARRIER_KEYWORDS = ["reach", "dip to", "drop to", "fall to", "hit $"]
