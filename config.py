import os

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
PAYMENT_WALLET: str = os.environ.get("PAYMENT_WALLET", "")
PREMIUM_PRICE_CENTS: int = int(os.environ.get("PREMIUM_PRICE_CENTS", "1999"))
FREE_TIER_LIMIT: int = int(os.environ.get("FREE_TIER_LIMIT", "3"))
POLL_INTERVAL: int = int(os.environ.get("POLL_INTERVAL", "30"))
DEFAULT_USD_THRESHOLD: float = float(os.environ.get("DEFAULT_USD_THRESHOLD", "1000000"))
PAYMENT_TTL_SECONDS: int = int(os.environ.get("PAYMENT_TTL_SECONDS", "1800"))

LIFETIME_PREMIUM_USERS: frozenset[str] = frozenset({"artualist", "artualista"})
ADMIN_IDS: frozenset[int] = frozenset({1076673473})

REQUIRED_CHANNEL: str = "@Ledgexs"
CHANNEL_INVITE_URL: str = "https://t.me/Ledgexs"
