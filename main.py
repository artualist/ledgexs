"""
Global Whale Tracker Bot — Premium SaaS Edition
Multi-chain ERC-20 whale monitoring with SQLite persistence, smart token
search, quick-track toggles, USD value thresholds, and DEX swap detection.
"""

import os
import sys
import time
import random
import logging
import threading
import requests
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from datetime import datetime, timedelta
from typing import Any

import telebot
from telebot import types
from web3 import Web3
from web3.contract import Contract

try:
    import tweepy as _tweepy  # optional — gracefully absent if install fails
except ImportError:
    _tweepy = None  # type: ignore[assignment]

import db
import catalog as cat
import i18n
import whale_intel

# ---------------------------------------------------------------------------
# Logging — must be set up before any handler or helper references `logger`
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("whale_bot")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
PAYMENT_WALLET: str = os.environ.get("PAYMENT_WALLET", "")
PREMIUM_PRICE_CENTS: int = int(os.environ.get("PREMIUM_PRICE_CENTS", "1999"))
FREE_TIER_LIMIT: int = int(os.environ.get("FREE_TIER_LIMIT", "3"))
POLL_INTERVAL: int = int(os.environ.get("POLL_INTERVAL", "30"))
DEFAULT_USD_THRESHOLD: float = float(os.environ.get("DEFAULT_USD_THRESHOLD", "10000"))

# Usernames granted lifetime premium automatically on first /start
# (lowercase for case-insensitive match)
LIFETIME_PREMIUM_USERS: frozenset[str] = frozenset({"artualist", "artualista"})

ADMIN_IDS: frozenset[int] = frozenset({1076673473})

REQUIRED_CHANNEL: str = "@Ledgexs"
CHANNEL_INVITE_URL: str = "https://t.me/Ledgexs"

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ---------------------------------------------------------------------------
# Channel membership helpers
# ---------------------------------------------------------------------------


def check_channel_membership(user_id: int) -> bool:
    """Return True if the user is a member of REQUIRED_CHANNEL.

    Fails open (returns True) when the bot cannot reach the channel API so
    genuine members are never wrongly blocked on a transient error.
    """
    try:
        member = bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status not in ("left", "kicked", "restricted")
    except Exception as exc:
        logger.warning("channel_membership check failed for uid=%d: %s", user_id, exc)
        return True  # fail open


def _send_join_required(chat_id: int, lang: str = "en") -> None:
    """Send the 'please join our channel' gate message."""
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton(
            i18n.t("btn_join_channel", lang), url=CHANNEL_INVITE_URL
        )
    )
    markup.add(
        types.InlineKeyboardButton(
            i18n.t("btn_check_status", lang), callback_data="check_join"
        )
    )
    bot.send_message(chat_id, i18n.t("join_required", lang), reply_markup=markup)


# ---------------------------------------------------------------------------
# ADMIN PANEL COMMANDS
# ---------------------------------------------------------------------------


@bot.message_handler(commands=["admin"])
def admin_panel(message: types.Message):
    """Admin panel accessible only by users listed in ADMIN_IDS."""
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "❌ You are not authorized to use this command.")
        return

    markup = types.InlineKeyboardMarkup(row_width=2)
    btn_users = types.InlineKeyboardButton(
        "👥 User Statistics", callback_data="admin_stats"
    )
    btn_broadcast = types.InlineKeyboardButton(
        "📢 Broadcast Message", callback_data="admin_broadcast"
    )
    markup.add(btn_users, btn_broadcast)

    bot.send_message(
        message.chat.id,
        "⚙️ <b>Global Whale Tracker - Admin Panel</b>\n\nSelect an action:",
        reply_markup=markup,
    )


@bot.message_handler(commands=["users"])
def admin_users_list(message: types.Message):
    """Summarizes total users and premium status from the database."""
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "❌ You are not authorized to use this command.")
        return

    try:
        total_users = db.get_total_users_count()
        premium_users = db.get_premium_users_count()

        text = (
            "📊 <b>User Database Summary</b>\n\n"
            f"👥 <b>Total Registered Users:</b> {total_users}\n"
            f"⭐ <b>Active Premium Users:</b> {premium_users}\n"
            f"🆓 <b>Free Tier Users:</b> {total_users - premium_users}\n"
        )
        bot.send_message(message.chat.id, text)
    except Exception as e:
        logger.error(f"Error fetching user stats: {e}")
        bot.send_message(
            message.chat.id,
            "❌ An error occurred while fetching data from the database.",
        )


USDT_CA = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
USDT_DECIMALS = 6

# ── Outbound keep-alive ─────────────────────────────────────────────────────
_KEEPALIVE_INTERVAL = 180  # seconds (3 minutes)
_KEEPALIVE_URL = "https://www.google.com"


def _keepalive_loop() -> None:
    """Prevent Replit idle-shutdown by making a lightweight outbound HTTPS
    request every 3 minutes.  Uses only the stdlib — no extra dependencies.
    Errors are swallowed silently; the loop always continues.
    """
    logger.info(
        "Secure Outbound Keep-Alive thread successfully started (Ping interval: 3m)."
    )
    while True:
        time.sleep(_KEEPALIVE_INTERVAL)
        try:
            with urllib.request.urlopen(_KEEPALIVE_URL, timeout=10):
                pass  # response body intentionally discarded
        except Exception:
            pass  # network blip — retry on next tick


# ── Twitter / X client (initialised at startup if secrets are present) ─────
_twitter_client: Any = None  # tweepy.Client | None


def _init_twitter() -> None:
    """Attempt to create a Tweepy v2 client from Replit Secrets.

    Fails gracefully — missing or invalid credentials just disable X posting
    without affecting anything else.
    """
    global _twitter_client
    if _tweepy is None:
        logger.warning("tweepy not installed — X cross-posting disabled.")
        return
    api_key = os.environ.get("TWITTER_API_KEY", "")
    api_secret = os.environ.get("TWITTER_API_SECRET", "")
    access_token = os.environ.get("TWITTER_ACCESS_TOKEN", "")
    access_secret = os.environ.get("TWITTER_ACCESS_SECRET", "")
    if not all([api_key, api_secret, access_token, access_secret]):
        logger.warning(
            "One or more TWITTER_* secrets are missing — X cross-posting disabled."
        )
        return
    try:
        _twitter_client = _tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_secret,
        )
        logger.info("Twitter/X client initialised — cross-posting enabled.")
    except Exception as exc:
        logger.warning("Twitter/X client init failed: %s", exc)


# ── X tweet helpers ─────────────────────────────────────────────────────────

def _fmt_usd_short(val: float) -> str:
    """Format a USD value compactly for tweets (e.g. $2.4M, $890K)."""
    if val >= 1_000_000:
        return f"${val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"${val / 1_000:.0f}K"
    return f"${val:,.0f}"


def _build_tweet_digest(
    top_tracked: list[dict[str, Any]], top_volume: list[dict[str, Any]]
) -> str:
    """Build a ≤280-character whale digest for X (Twitter).

    Format (compact, emoji-rich, CTA at end):
        🐳 Whale Report — Jun 18, 2026

        📊 Top Tracked:
        🔷 USDT · 5 trackers
        🟡 WBNB · 3 trackers

        💰 Top 24h Volume:
        🔷 USDC — $2.4M

        📲 Track whales live 👉 t.me/LedgexsBot
    """
    now_str = datetime.utcnow().strftime("%b %d, %Y")
    CTA = "\n\n📲 Track whales live 👉 t.me/LedgexsBot"
    LIMIT = 280 - len(CTA)

    lines: list[str] = [f"🐳 Whale Report — {now_str}"]

    if top_tracked:
        lines.append("\n📊 Top Tracked:")
        for t in top_tracked[:3]:
            icon = CHAINS.get(t["chain"], {}).get("icon", "🔗")
            n = t["trackers"]
            lines.append(
                f"{icon} {t['symbol']} · {n} tracker{'s' if n != 1 else ''}"
            )

    if top_volume:
        lines.append("\n💰 Top 24h Volume:")
        for t in top_volume[:3]:
            icon = CHAINS.get(t["chain"], {}).get("icon", "🔗")
            usd = _fmt_usd_short(t["total_usd"]) if t["total_usd"] else "$0"
            lines.append(f"{icon} {t['symbol']} — {usd}")

    if not top_tracked and not top_volume:
        lines.append("\n📭 No whale activity recorded in the last 24h.")

    body = "\n".join(lines)
    # Safety: trim body if it somehow exceeds budget before adding CTA
    if len(body) > LIMIT:
        body = body[: LIMIT - 1] + "…"
    return body + CTA


def _post_tweet_digest(
    top_tracked: list[dict[str, Any]], top_volume: list[dict[str, Any]]
) -> None:
    """Cross-post the daily digest to X. Fails silently if client is absent."""
    if _twitter_client is None:
        return
    try:
        text = _build_tweet_digest(top_tracked, top_volume)
        _twitter_client.create_tweet(text=text)
        logger.info("Daily digest cross-posted to X (%d chars).", len(text))
    except Exception as exc:
        logger.warning("X cross-posting failed (non-fatal): %s", exc)


# Maps internal chain IDs to DexScreener URL path segments
DEXSCREENER_SLUGS: dict[str, str] = {
    "eth": "ethereum",
    "bsc": "bsc",
    "poly": "polygon",
    "arb": "arbitrum",
    "base": "base",
    "sol": "solana",
    "sui": "sui",
    "tron": "tron",
}
PAYMENT_SCAN_BLOCKS = 7200

# ── Multi-chain RPC config ──────────────────────────────────────────────────
CHAINS: dict[str, dict[str, str]] = {
    "eth": {
        "name": "Ethereum",
        "icon": "🔷",
        "rpc_env": "ETH_RPC_URL",   # also accepts legacy RPC_URL
        "explorer": "https://etherscan.io",
    },
    "bsc": {
        "name": "BSC",
        "icon": "🟡",
        "rpc_env": "BSC_RPC_URL",
        "explorer": "https://bscscan.com",
    },
    "poly": {
        "name": "Polygon",
        "icon": "🟣",
        "rpc_env": "POLYGON_RPC_URL",
        "explorer": "https://polygonscan.com",
    },
    "arb": {
        "name": "Arbitrum",
        "icon": "🔵",
        "rpc_env": "ARB_RPC_URL",
        "explorer": "https://arbiscan.io",
    },
    "base": {
        "name": "Base",
        "icon": "🟦",
        "rpc_env": "BASE_RPC_URL",
        "explorer": "https://basescan.org",
    },
    "sol": {
        "name": "Solana",
        "icon": "🧬",
        "rpc_env": "",   # dedicated monitor via SOL_RPC_URL
        "explorer": "https://solscan.io",
    },
    "sui": {
        "name": "Sui",
        "icon": "💧",
        "rpc_env": "",   # dedicated monitor via SUI_RPC_URL
        "explorer": "https://suiscan.xyz",
    },
    "tron": {
        "name": "Tron",
        "icon": "🔴",
        "rpc_env": "",   # dedicated monitor via TRON_RPC_URL (TronGrid REST)
        "explorer": "https://tronscan.org/#/transaction",
    },
}

# Premium RPC endpoints — loaded strictly from Replit Secrets, zero public fallbacks.
# A missing secret silently disables that chain's monitor without crashing the bot.
SOL_RPC_URL: str = os.environ.get("SOL_RPC_URL", "")
SUI_RPC_URL: str = os.environ.get("SUI_RPC_URL", "")
TRON_RPC_URL: str = os.environ.get("TRON_RPC_URL", "")

# (logging already configured above)

# ---------------------------------------------------------------------------
# Web3 instances
# ---------------------------------------------------------------------------

w3_instances: dict[str, Web3] = {}
for _cid, _cmeta in CHAINS.items():
    if not _cmeta["rpc_env"]:  # non-EVM / dedicated-monitor chain — skip Web3
        continue
    # ETH accepts both the new ETH_RPC_URL and the legacy RPC_URL secret name
    if _cid == "eth":
        _url = os.environ.get("ETH_RPC_URL", "") or os.environ.get("RPC_URL", "")
    else:
        _url = os.environ.get(_cmeta["rpc_env"], "")
    if _url:
        w3_instances[_cid] = Web3(
            Web3.HTTPProvider(_url, request_kwargs={"timeout": 15})
        )
    else:
        logger.debug("%-8s No RPC URL set (%s) — chain disabled.", _cid, _cmeta["rpc_env"])

# Solana connectivity flag — set True by _sol_monitor_loop after health-check
_sol_connected: bool = False

# Sui connectivity flag — set True by _sui_monitor_loop after health-check
_sui_connected: bool = False

# Tron connectivity flag — set True by _tron_monitor_loop after health-check
_tron_connected: bool = False

ERC20_ABI: list[dict[str, Any]] = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    },
]

# ---------------------------------------------------------------------------
# In-memory state (non-persistent)
# ---------------------------------------------------------------------------

# pending_actions: {uid -> {"action": str, ...}}
pending_actions: dict[int, dict[str, Any]] = {}
# pending_payments is now persisted in the DB via db.reserve_payment / db.get_pending_payment

state_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Price fetching with cache
# ---------------------------------------------------------------------------

_price_cache: dict[str, tuple[float, float]] = {}  # cg_id -> (price, timestamp)
_price_lock = threading.Lock()
PRICE_TTL = 300  # seconds

STABLECOINS = {
    "USDT",
    "USDC",
    "DAI",
    "BUSD",
    "FRAX",
    "LUSD",
    "TUSD",
    "USDP",
    "USDe",
    "GUSD",
}


def get_price_usd(symbol: str) -> float:
    if symbol.upper() in STABLECOINS:
        return 1.0
    cg_id = cat.COINGECKO_IDS.get(symbol)
    if not cg_id:
        return 0.0
    with _price_lock:
        cached = _price_cache.get(cg_id)
        if cached and time.time() - cached[1] < PRICE_TTL:
            return cached[0]
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd"},
            timeout=6,
        )
        price = float(r.json().get(cg_id, {}).get("usd", 0))
        with _price_lock:
            _price_cache[cg_id] = (price, time.time())
        return price
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _shorten(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}"


def _get_w3(chain_id: str) -> Web3 | None:
    return w3_instances.get(chain_id)


def _available_chains() -> list[str]:
    chains = [cid for cid, w in w3_instances.items() if w.is_connected()]
    if _sol_connected and "sol" not in chains:
        chains.append("sol")
    if _sui_connected and "sui" not in chains:
        chains.append("sui")
    if _tron_connected and "tron" not in chains:
        chains.append("tron")
    return chains


def _is_sol_address(s: str) -> bool:
    """Validate a Solana Base58 public key (32-44 chars, no 0/O/I/l)."""
    import re

    return bool(re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", s.strip()))


def _is_sui_address(s: str) -> bool:
    """Accept a raw Sui package ID (0xHEX) or full coin type (0xHEX::module::Type)."""
    import re

    return bool(
        re.match(
            r"^0x[0-9a-fA-F]+((::[a-zA-Z_][a-zA-Z0-9_]*){2})?$",
            s.strip(),
        )
    )


def _is_tron_address(s: str) -> bool:
    """Validate a Tron TRC-20 contract or wallet address (Base58, 34 chars, starts with T)."""
    import re

    return bool(re.match(r"^T[1-9A-HJ-NP-Za-km-z]{33}$", s.strip()))


def _chain_label(chain_id: str) -> str:
    c = CHAINS.get(chain_id, {})
    return f"{c.get('icon', '🔗')} {c.get('name', chain_id)}"


def _explorer(chain_id: str) -> str:
    return CHAINS.get(chain_id, {}).get("explorer", "https://etherscan.io")


def _classify_transfer(chain_id: str, sender: str, receiver: str) -> str:
    sender_lc = str(sender).lower().strip()
    receiver_lc = str(receiver).lower().strip()

    # CEX cüzdanlarını dinamik olarak küçük harfe çevirerek tara
    cex_wallets_lc = {
        str(k).lower(): v for k, v in getattr(cat, "CEX_WALLETS", {}).items()
    }

    cex_dst = cex_wallets_lc.get(receiver_lc)
    cex_src = cex_wallets_lc.get(sender_lc)

    # 1. NET CEX EŞLEŞMELERİ
    if cex_src and cex_dst:
        label = cex_dst if cex_dst == cex_src else f"{cex_src} → {cex_dst}"
        return f"🏛️ CEX CONSOLIDATION {label}"
    if cex_dst:
        return f"🏛️ CEX DEPOSIT → {cex_dst}"
    if cex_src:
        return f"🏦 CEX WITHDRAWAL ← {cex_src}"

    # Adres isminde veya etiketinde borsa kelimesi geçiyorsa (Etherscan etiketleme desteği için koruma)
    if "binance" in sender_lc or "okx" in sender_lc or "bybit" in sender_lc:
        return "🏦 CEX WITHDRAWAL"
    if "binance" in receiver_lc or "okx" in receiver_lc or "bybit" in receiver_lc:
        return "🏛️ CEX DEPOSIT"

    # 2. DEX ROUTER EŞLEŞMELERİ
    raw_routers = cat.DEX_ROUTERS.get(chain_id, set())
    routers_lc = {str(r).lower().strip() for r in raw_routers}

    if receiver_lc in routers_lc:
        return "🚨 WHALE SELL"
    if sender_lc in routers_lc:
        return "🔥 WHALE BUY"

    # 3. GELİŞMİŞ ARBİTRAJ / ÇOKLU TRANSFER TUZAĞI ÇÖZÜMÜ (Ekran görüntülerindeki sorun)
    # Eğer gönderen veya alan adres standart bir cüzdan gibi durmuyor,
    # veya işlem karmaşık bir kontrat içi havuz transferiyse:
    if "0x51c72848c68a965f66fa7a88855f9f7784502a7f" in [sender_lc, receiver_lc]:
        return "⚔️ MEV / DEX ARBITRAGE"

    # Genel havuz etkileşimleri yakalama (Eğer adres bilinen router değil ama token havuz sözleşmesiyse)
    if "pool" in receiver_lc or "pair" in receiver_lc or "router" in receiver_lc:
        return "🚨 WHALE SELL (DEX)"
    if "pool" in sender_lc or "pair" in sender_lc or "router" in sender_lc:
        return "🔥 WHALE BUY (DEX)"

    # 4. HİÇBİRİ DEĞİLSE
    return "📦 COLD WALLET TRANSFER"


# ── RPC resilience: uniform retry with exponential backoff ──────────────────


def _rpc_post(url: str, payload: dict, *, retries: int = 3, timeout: int = 10) -> dict:
    """POST a JSON-RPC payload; retries up to `retries` times (1 s → 2 s backoff)."""
    delay = 1.0
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(retries):
        try:
            r = requests.post(url, json=payload, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
    raise last_exc


def _rpc_get(
    url: str, *, params: dict | None = None, retries: int = 3, timeout: int = 10
) -> dict:
    """GET a JSON endpoint; retries up to `retries` times (1 s → 2 s backoff)."""
    delay = 1.0
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
    raise last_exc


def _fetch_sol_token_info(mint: str) -> dict[str, Any] | None:
    """Fetch SPL token metadata via DexScreener (name/symbol) + Solana RPC (decimals)."""
    name: str | None = None
    symbol: str | None = None
    decimals: int = 9  # Solana default

    # 1. DexScreener — fast public API for name & symbol (with retry)
    try:
        data = _rpc_get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
        pairs: list[dict] = data.get("pairs") or []
        for pair in pairs:
            bt = pair.get("baseToken") or {}
            if bt.get("address", "").lower() == mint.lower():
                name = bt.get("name") or name
                symbol = bt.get("symbol") or symbol
                break
    except Exception as exc:
        logger.debug("DexScreener sol %s: %s", mint, exc)

    # 2. Solana RPC — getAccountInfo (jsonParsed) for decimals (with retry)
    if SOL_RPC_URL:
        try:
            data = _rpc_post(
                SOL_RPC_URL,
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getAccountInfo",
                    "params": [mint, {"encoding": "jsonParsed"}],
                },
            )
            value = data.get("result", {}).get("value") or {}
            parsed = (value.get("data") or {}).get("parsed") or {}
            if parsed.get("type") == "mint":
                decimals = int((parsed.get("info") or {}).get("decimals", 9))
        except Exception as exc:
            logger.debug("Solana getAccountInfo %s: %s", mint, exc)

    if name is None and symbol is None:
        return None  # Unknown token — reject gracefully

    return {
        "ca": mint,
        "chain": "sol",
        "name": name or symbol or "Unknown SPL Token",
        "symbol": symbol or "???",
        "decimals": decimals,
    }


def _fetch_sui_token_info(coin_type: str) -> dict[str, Any] | None:
    """Fetch name/symbol/decimals for a Sui coin type via suix_getCoinMetadata."""
    if not SUI_RPC_URL:
        return None
    try:
        data = _rpc_post(
            SUI_RPC_URL,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_getCoinMetadata",
                "params": [coin_type],
            },
        )
        meta = data.get("result")
        if not meta:
            return None
        return {
            "ca": coin_type,
            "chain": "sui",
            "name": meta.get("name") or "Unknown",
            "symbol": meta.get("symbol") or "???",
            "decimals": int(meta.get("decimals", 9)),
        }
    except Exception as exc:
        logger.warning("fetch_sui_token_info %s: %s", coin_type, exc)
        return None


def _fetch_tron_token_info(contract: str) -> dict[str, Any] | None:
    """Fetch TRC-20 token metadata via DexScreener (name/symbol) + TronGrid (decimals)."""
    name: str | None = None
    symbol: str | None = None
    decimals: int = 6  # USDT/TRC-20 standard default

    # 1. DexScreener for name & symbol (with retry)
    try:
        data = _rpc_get(f"https://api.dexscreener.com/latest/dex/tokens/{contract}")
        for pair in data.get("pairs") or []:
            bt = pair.get("baseToken") or {}
            if bt.get("address", "").lower() == contract.lower():
                name = bt.get("name") or name
                symbol = bt.get("symbol") or symbol
                break
    except Exception as exc:
        logger.debug("DexScreener tron %s: %s", contract, exc)

    # 2. TronGrid — contract ABI for decimals (with retry)
    if TRON_RPC_URL:
        try:
            data = _rpc_get(
                f"{TRON_RPC_URL.rstrip('/')}/v1/contracts/{contract}",
                params={"visible": "true"},
            )
            # TronGrid wraps token info differently; decimals default 6 is used
            # unless the ABI explicitly exposes it (handled at scan time)
        except Exception as exc:
            logger.debug("TronGrid contract info %s: %s", contract, exc)

    if name is None and symbol is None:
        return None

    return {
        "ca": contract,
        "chain": "tron",
        "name": name or symbol or "Unknown TRC-20",
        "symbol": symbol or "???",
        "decimals": decimals,
    }


def _fetch_token_info(ca: str, chain_id: str) -> dict[str, Any] | None:
    if chain_id == "sol":
        return _fetch_sol_token_info(ca)
    if chain_id == "sui":
        return _fetch_sui_token_info(ca)
    if chain_id == "tron":
        return _fetch_tron_token_info(ca)
    w3 = _get_w3(chain_id)
    if w3 is None:
        return None
    try:
        contract: Contract = w3.eth.contract(
            address=Web3.to_checksum_address(ca), abi=ERC20_ABI
        )
        name = contract.functions.name().call()
        symbol = contract.functions.symbol().call()
        decimals = contract.functions.decimals().call()
        return {
            "ca": Web3.to_checksum_address(ca),
            "chain": chain_id,
            "name": name,
            "symbol": symbol,
            "decimals": decimals,
        }
    except Exception as exc:
        logger.warning("fetch_token_info %s/%s: %s", chain_id, ca, exc)
        return None


def _assign_unique_payment(uid: int) -> int:
    """Reserve a collision-free 6-decimal USDT amount for this user in the DB.

    Base = PREMIUM_PRICE in USDT base units.  We append a random 6-digit
    fractional part (000001–999999) so the on-chain amount is unique per user
    and never collides with any currently-active reservation.
    """
    # Re-use an existing valid reservation so the displayed amount is stable
    existing = db.get_pending_payment(uid)
    if existing is not None:
        return existing
    base = PREMIUM_PRICE_CENTS * (10 ** USDT_DECIMALS) // 100
    for _ in range(200):
        frac = random.randint(1, 999_999)
        candidate = base + frac
        if not db.is_amount_reserved(candidate):
            db.reserve_payment(uid, candidate)
            return candidate
    # Astronomically unlikely fallback — just reserve without collision check
    fallback = base + random.randint(1, 999_999)
    db.reserve_payment(uid, fallback)
    return fallback


def _usdt_display(base_units: int) -> str:
    """Format USDT base units as a 6-decimal string (e.g. '20.123456')."""
    return f"{base_units / (10**USDT_DECIMALS):.6f}"


def _verify_usdt_payment(expected_amount: int) -> bool:
    w3 = _get_w3("eth")
    if not PAYMENT_WALLET or w3 is None or not w3.is_connected():
        return False
    try:
        usdt: Contract = w3.eth.contract(
            address=Web3.to_checksum_address(USDT_CA), abi=ERC20_ABI
        )
        latest = w3.eth.block_number
        events = usdt.events.Transfer.get_logs(
            from_block=max(0, latest - PAYMENT_SCAN_BLOCKS),
            to_block=latest,
            argument_filters={"to": Web3.to_checksum_address(PAYMENT_WALLET)},
        )
        return any(e["args"]["value"] == expected_amount for e in events)
    except Exception as exc:
        logger.warning("Payment verify error: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Keyboard builders
# ---------------------------------------------------------------------------


def kb_language_select() -> types.InlineKeyboardMarkup:
    """Inline keyboard presenting all 7 supported languages."""
    kb = types.InlineKeyboardMarkup(row_width=2)
    btns = [
        types.InlineKeyboardButton(label, callback_data=f"lang_set:{code}")
        for code, label in i18n.LANGS.items()
    ]
    kb.add(*btns)
    return kb


def kb_main_menu(uid: int, lang: str = "en") -> types.InlineKeyboardMarkup:
    premium = db.is_premium(uid)
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton(
            i18n.t("btn_search", lang), callback_data="main_search"
        ),
        types.InlineKeyboardButton(
            i18n.t("btn_popular", lang), callback_data="main_popular"
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            i18n.t("btn_track", lang), callback_data="main_track"
        ),
        types.InlineKeyboardButton(
            i18n.t("btn_watchlist", lang), callback_data="main_mylist"
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            i18n.t("btn_history", lang), callback_data="main_history"
        ),
        types.InlineKeyboardButton(
            i18n.t("btn_leaderboard", lang), callback_data="main_leaderboard"
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            i18n.t("btn_digest", lang), callback_data="main_digest"
        ),
        types.InlineKeyboardButton(
            i18n.t("btn_language", lang), callback_data="main_lang"
        ),
    )
    if premium:
        kb.add(
            types.InlineKeyboardButton(
                i18n.t("btn_membership", lang), callback_data="main_membership"
            )
        )
    else:
        kb.add(
            types.InlineKeyboardButton(
                i18n.t("btn_upgrade", lang), callback_data="main_premium"
            )
        )
    return kb


def kb_digest_menu(
    current_hour: int | None, lang: str = "en"
) -> types.InlineKeyboardMarkup:
    """Time-picker keyboard for daily digest. Hours shown in UTC every 3 h."""
    kb = types.InlineKeyboardMarkup(row_width=4)
    btns = []
    for h in range(0, 24, 3):
        label = f"{'✅ ' if h == current_hour else ''}{h:02d}:00"
        btns.append(types.InlineKeyboardButton(label, callback_data=f"digest_set:{h}"))
    kb.add(*btns)
    kb.add(
        types.InlineKeyboardButton(
            i18n.t("btn_digest_disabled", lang)
            if current_hour is None
            else i18n.t("btn_digest_disable", lang),
            callback_data="digest_off",
        )
    )
    kb.add(
        types.InlineKeyboardButton(i18n.t("btn_home", lang), callback_data="main_home")
    )
    return kb


def kb_back_home(lang: str = "en") -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton(i18n.t("btn_home", lang), callback_data="main_home")
    )
    return kb


def kb_chain_select(lang: str = "en") -> types.InlineKeyboardMarkup:
    avail = _available_chains()
    kb = types.InlineKeyboardMarkup(row_width=2)
    btns = [
        types.InlineKeyboardButton(
            _chain_label(cid), callback_data=f"chain_select:{cid}"
        )
        for cid in CHAINS
        if cid in avail
    ]
    kb.add(*btns)
    kb.add(
        types.InlineKeyboardButton(i18n.t("btn_back", lang), callback_data="main_home")
    )
    return kb


def kb_qt_list(
    uid: int, tokens: list[dict[str, Any]], lang: str = "en"
) -> types.InlineKeyboardMarkup:
    """Quick-track toggle keyboard for a list of catalog tokens.

    Each token gets its own row: [➕/✅ SYMBOL] [📊 DexScreener]
    """
    kb = types.InlineKeyboardMarkup(row_width=2)
    for t in tokens:
        chain_id = t["chain"]
        ca = t["ca"]
        tracked = db.is_tracked(uid, chain_id, ca)
        qt_label = f"✅ {t['symbol']}" if tracked else f"➕ {t['symbol']}"
        slug = DEXSCREENER_SLUGS.get(chain_id, chain_id)
        kb.row(
            types.InlineKeyboardButton(qt_label, callback_data=f"qt:{chain_id}:{ca}"),
            types.InlineKeyboardButton(
                i18n.t("btn_dexscreener", lang),
                url=f"https://dexscreener.com/{slug}/{ca}",
            ),
        )
    kb.add(
        types.InlineKeyboardButton(i18n.t("btn_home", lang), callback_data="main_home")
    )
    return kb


def kb_watchlist_menu(
    tracked: list[dict[str, Any]], lang: str = "en"
) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    for t in tracked:
        chain_id = t["chain"]
        icon = CHAINS.get(chain_id, {}).get("icon", "🔗")
        paused = "⏸" if t.get("paused") else ""
        label = f"{icon} {t['name']} ({t['symbol']}) {paused}".strip()
        kb.add(
            types.InlineKeyboardButton(
                label, callback_data=f"tmenu:{chain_id}:{t['ca']}"
            )
        )
    kb.add(
        types.InlineKeyboardButton(i18n.t("btn_home", lang), callback_data="main_home")
    )
    return kb


def kb_token_menu(
    chain_ca: str, paused: bool = False, lang: str = "en"
) -> types.InlineKeyboardMarkup:
    pause_label = (
        i18n.t("btn_resume_alerts", lang)
        if paused
        else i18n.t("btn_pause_alerts", lang)
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton(
            i18n.t("btn_stats", lang), callback_data=f"tstat:{chain_ca}"
        ),
        types.InlineKeyboardButton(
            i18n.t("btn_set_threshold", lang), callback_data=f"tset:{chain_ca}"
        ),
    )
    kb.add(
        types.InlineKeyboardButton(pause_label, callback_data=f"ttoggle:{chain_ca}"),
        types.InlineKeyboardButton(
            i18n.t("btn_untrack", lang), callback_data=f"tuntrack:{chain_ca}"
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            i18n.t("btn_set_label", lang), callback_data=f"tsetlabel:{chain_ca}"
        )
    )
    kb.add(
        types.InlineKeyboardButton(
            i18n.t("btn_back_watchlist", lang), callback_data="main_mylist"
        )
    )
    return kb


def kb_chart_links(chain_id: str, ca: str, lang: str = "en") -> types.InlineKeyboardMarkup:
    """Return an inline keyboard with DexScreener (and Birdeye for Solana) URL buttons."""
    kb = types.InlineKeyboardMarkup(row_width=2)
    slug = DEXSCREENER_SLUGS.get(chain_id, chain_id)
    btns = [
        types.InlineKeyboardButton(
            i18n.t("btn_dexscreener", lang),
            url=f"https://dexscreener.com/{slug}/{ca}",
        )
    ]
    if chain_id == "sol":
        btns.append(
            types.InlineKeyboardButton(
                i18n.t("btn_birdeye", lang),
                url=f"https://birdeye.so/token/{ca}?chain=solana",
            )
        )
    kb.add(*btns)
    return kb


def kb_verify_payment(lang: str = "en") -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton(
            i18n.t("btn_verify_payment", lang), callback_data="pay_verify"
        )
    )
    kb.add(
        types.InlineKeyboardButton(i18n.t("btn_back", lang), callback_data="main_home")
    )
    return kb


# ---------------------------------------------------------------------------
# Quick-track toggle helper (used by both search & popular)
# ---------------------------------------------------------------------------


def _toggle_qt(
    uid: int, chat_id: int, chain_id: str, ca: str, call: types.CallbackQuery
) -> None:
    """Toggle tracking state and refresh the inline keyboard in-place."""
    premium = db.is_premium(uid)
    lang = db.get_user_language(uid) or "en"

    # Gate multi-chain for free users
    if chain_id != "eth" and not premium:
        bot.answer_callback_query(
            call.id,
            i18n.t("toast_multichain_locked", lang),
            show_alert=True,
        )
        return

    already = db.is_tracked(uid, chain_id, ca)

    if already:
        db.remove_from_watchlist(uid, chain_id, ca)
        bot.answer_callback_query(
            call.id, i18n.t("toast_removed", lang), show_alert=False
        )
    else:
        n = db.count_watchlist(uid)
        if not premium and n >= FREE_TIER_LIMIT:
            bot.answer_callback_query(
                call.id,
                i18n.t("toast_limit_locked", lang).format(limit=FREE_TIER_LIMIT),
                show_alert=True,
            )
            return

        # Try catalog first for instant add, else fetch from chain
        tok = cat.catalog_token(chain_id, ca)
        if tok:
            db.add_to_watchlist(
                uid, chain_id, ca, tok["name"], tok["symbol"], 18, DEFAULT_USD_THRESHOLD
            )
        else:
            info = _fetch_token_info(ca, chain_id)
            if info:
                db.add_to_watchlist(
                    uid,
                    chain_id,
                    ca,
                    info["name"],
                    info["symbol"],
                    info["decimals"],
                    DEFAULT_USD_THRESHOLD,
                )
            else:
                bot.answer_callback_query(
                    call.id, i18n.t("toast_token_info_err", lang), show_alert=True
                )
                return
        bot.answer_callback_query(
            call.id, i18n.t("toast_added", lang), show_alert=False
        )

    # Rebuild keyboard from the existing message buttons
    new_kb = _rebuild_qt_keyboard(uid, call)
    if new_kb:
        try:
            bot.edit_message_reply_markup(
                chat_id, call.message.message_id, reply_markup=new_kb
            )
        except Exception:
            pass


def _rebuild_qt_keyboard(
    uid: int, call: types.CallbackQuery
) -> types.InlineKeyboardMarkup | None:
    """Parse qt: buttons from the current message and rebuild with updated states."""
    curr = call.message.reply_markup
    if not curr:
        return None
    token_keys: list[tuple[str, str]] = []
    for row in curr.keyboard:
        for btn in row:
            cd = getattr(btn, "callback_data", "")
            if cd.startswith("qt:"):
                parts = cd.split(":", 2)
                if len(parts) == 3:
                    token_keys.append((parts[1], parts[2]))

    if not token_keys:
        return None

    kb = types.InlineKeyboardMarkup(row_width=2)
    btns = []
    for chain_id, ca in token_keys:
        tok = cat.catalog_token(chain_id, ca)
        if tok:
            label = ("✅ " if db.is_tracked(uid, chain_id, ca) else "➕ ") + tok[
                "symbol"
            ]
        else:
            label = ("✅ " if db.is_tracked(uid, chain_id, ca) else "➕ ") + ca[:6]
        btns.append(
            types.InlineKeyboardButton(label, callback_data=f"qt:{chain_id}:{ca}")
        )
    kb.add(*btns)
    kb.add(types.InlineKeyboardButton("🏠 Main Menu", callback_data="main_home"))
    return kb


# ---------------------------------------------------------------------------
# Background whale monitor
# ---------------------------------------------------------------------------


# Shared executor for concurrent EVM chain scanning (sol/sui/tron use own threads)
_CHAIN_EXECUTOR = ThreadPoolExecutor(max_workers=20, thread_name_prefix="ChainScan")


def _monitor_loop() -> None:
    logger.info("Whale monitor thread started (concurrent EVM scanner, workers=20).")
    last_seen: dict[str, int] = {}
    while True:
        time.sleep(POLL_INTERVAL)
        try:
            subscribers = db.get_all_subscribers()
        except Exception as exc:
            logger.error("DB error loading subscribers: %s", exc)
            continue
        # EVM-only keys — sol/sui/tron each have their own dedicated monitor thread
        evm_keys = [k for k in subscribers if not k.startswith(("sol:", "sui:", "tron:"))]
        if not evm_keys:
            continue
        futs: dict = {}
        for k in evm_keys:
            parts = k.split(":", 1)
            if len(parts) != 2:
                continue
            chain_id, ca = parts
            futs[_CHAIN_EXECUTOR.submit(
                _check_whale_activity, chain_id, ca, subscribers[k], last_seen
            )] = k
        for fut in as_completed(futs, timeout=max(POLL_INTERVAL - 2, 5)):
            k = futs[fut]
            try:
                fut.result()
            except Exception as exc:
                logger.error("Monitor error %s: %s", k, exc)


def _sol_check_token(mint: str, uids: set[int], last_sol_sig: dict[str, str]) -> None:
    """Poll Solana JSON-RPC for recent large SPL token transfers on `mint`."""
    if not SOL_RPC_URL:
        return
    try:
        data = _rpc_post(
            SOL_RPC_URL,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [mint, {"limit": 20}],
            },
        )
        sigs_raw: list[dict] = data.get("result") or []
    except Exception as exc:
        logger.debug("Solana getSignatures %s: %s", mint, exc)
        return

    last_sig = last_sol_sig.get(mint, "")
    new_sigs: list[str] = []
    for s in sigs_raw:
        if s.get("signature") == last_sig:
            break
        new_sigs.append(s["signature"])
    if sigs_raw:
        last_sol_sig[mint] = sigs_raw[0]["signature"]

    for sig in new_sigs:
        try:
            tx_data = _rpc_post(
                SOL_RPC_URL,
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        sig,
                        {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
                    ],
                },
            )
            tx = tx_data.get("result")
            if not tx:
                continue

            meta = tx.get("meta") or {}
            pre_ = {b["accountIndex"]: b for b in (meta.get("preTokenBalances") or [])}
            post_ = {
                b["accountIndex"]: b for b in (meta.get("postTokenBalances") or [])
            }

            for idx, pb in post_.items():
                if pb.get("mint") != mint:
                    continue
                preb = pre_.get(idx, {})
                pre_amt = float((preb.get("uiTokenAmount") or {}).get("uiAmount") or 0)
                post_amt = float((pb.get("uiTokenAmount") or {}).get("uiAmount") or 0)
                diff = abs(post_amt - pre_amt)
                if diff == 0:
                    continue

                for uid in uids:
                    entry = db.get_watchlist_entry(uid, "sol", mint)
                    if not entry or entry.get("paused"):
                        continue
                    symbol = entry.get("symbol", "SOL-TOKEN")
                    price = get_price_usd(symbol)
                    diff_usd = diff * price if price > 0 else 0
                    threshold = entry.get("threshold_usd", DEFAULT_USD_THRESHOLD)
                    if price > 0 and diff_usd < threshold:
                        continue
                    if price == 0 and diff < threshold:
                        continue

                    premium = db.is_premium(uid)
                    if premium:
                        sol_routers = cat.DEX_ROUTERS.get("sol", set())
                        accts = [
                            a.get("pubkey", "")
                            for a in (
                                tx.get("transaction", {})
                                .get("message", {})
                                .get("accountKeys")
                                or []
                            )
                        ]
                        is_dex = any(a in sol_routers for a in accts)
                        if is_dex:
                            alert_type = (
                                "🔥 WHALE BUY"
                                if post_amt > pre_amt
                                else "🚨 WHALE SELL"
                            )
                        else:
                            alert_type = "📦 COLD WALLET TRANSFER"
                    else:
                        alert_type = "🐳 Whale Transfer"

                    usd_str = f"${diff_usd:,.0f}" if price > 0 else "price unknown"
                    alert_text = (
                        f"{alert_type} — <b>{entry['name']} ({symbol})</b>  🟢 Solana\n\n"
                        f"<b>Amount:</b> {diff:,.2f} {symbol}"
                        + (f"  (~{usd_str})" if price > 0 else "")
                        + "\n"
                        f"<b>Tx:</b> <a href='https://solscan.io/tx/{sig}'>{_shorten(sig)}</a>"
                    )
                    user_rec = db.get_user(uid)
                    if user_rec:
                        _lang = db.get_user_language(uid) or "en"
                        bot.send_message(
                            user_rec["chat_id"],
                            alert_text,
                            reply_markup=kb_chart_links("sol", mint, _lang),
                            disable_web_page_preview=True,
                        )
                    db.save_alert(
                        "sol",
                        mint,
                        entry["name"],
                        symbol,
                        diff,
                        diff_usd,
                        "",
                        "",
                        sig,
                        0,
                        alert_type,
                    )
        except Exception as exc:
            logger.debug("Solana tx parse %s: %s", sig, exc)


def _sol_monitor_loop() -> None:
    """Background thread for Solana SPL token whale monitoring."""
    global _sol_connected
    logger.info("Solana monitor thread started (RPC: %s)", SOL_RPC_URL or "not configured")
    # Health-check
    if not SOL_RPC_URL:
        logger.warning("SOL_RPC_URL not set — Solana monitor disabled.")
    else:
        try:
            data = _rpc_post(
                SOL_RPC_URL,
                {"jsonrpc": "2.0", "id": 1, "method": "getHealth"},
                timeout=8,
            )
            health = data.get("result", "unknown")
            logger.info("Solana RPC health: %s", health)
            _sol_connected = True
        except Exception as exc:
            logger.warning("Solana RPC unreachable: %s", exc)

    last_sol_sig: dict[str, str] = {}
    while True:
        time.sleep(POLL_INTERVAL)
        if not _sol_connected:
            continue
        try:
            subscribers = db.get_all_subscribers()
        except Exception as exc:
            logger.error("DB error (sol monitor): %s", exc)
            continue
        for key, uids in subscribers.items():
            if not key.startswith("sol:"):
                continue
            mint = key[4:]
            try:
                _sol_check_token(mint, uids, last_sol_sig)
            except Exception as exc:
                logger.error("Sol monitor error %s: %s", key, exc)


def _sui_check_coin(
    coin_type: str, uids: set[int], last_sui_cursor: dict[str, Any]
) -> None:
    """Poll Sui JSON-RPC for recent large coin movements on coin_type."""
    if not SUI_RPC_URL:
        return
    try:
        cursor = last_sui_cursor.get(coin_type)
        resp = _rpc_post(
            SUI_RPC_URL,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_queryTransactionBlocks",
                "params": [{"filter": None}, cursor, 20, True],
            },
        )
        data = resp.get("result") or {}
        items = data.get("data") or []
        digests = [item["digest"] for item in items if item.get("digest")]
        next_cursor = data.get("nextCursor")
        if cursor is None:
            # First run — just record cursor position, don't alert on old txs
            last_sui_cursor[coin_type] = next_cursor
            return
        if next_cursor:
            last_sui_cursor[coin_type] = next_cursor
        if not digests:
            return
    except Exception as exc:
        logger.debug("Sui queryTxBlocks %s: %s", coin_type, exc)
        return

    # Batch-fetch transaction details with balance changes
    try:
        mr = _rpc_post(
            SUI_RPC_URL,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sui_multiGetTransactionBlocks",
                "params": [digests, {"showBalanceChanges": True}],
            },
            timeout=15,
        )
        txs: list[dict] = mr.get("result") or []
    except Exception as exc:
        logger.debug("Sui multiGetTx: %s", exc)
        return

    for tx in txs:
        if not tx:
            continue
        digest = tx.get("digest", "")
        balance_changes: list[dict] = tx.get("balanceChanges") or []
        # Sum absolute value of all changes for this coin type
        total_moved = 0
        for bc in balance_changes:
            if bc.get("coinType") != coin_type:
                continue
            try:
                total_moved += abs(int(bc.get("amount", "0")))
            except (ValueError, TypeError):
                pass
        if total_moved == 0:
            continue

        for uid in uids:
            try:
                entry = db.get_watchlist_entry(uid, "sui", coin_type)
                if not entry or entry.get("paused"):
                    continue
                decimals = entry.get("decimals", 9)
                symbol = entry.get("symbol", "SUI-TOKEN")
                name = entry.get("name", symbol)
                diff = total_moved / (10**decimals)
                price = get_price_usd(symbol)
                diff_usd = diff * price if price > 0 else 0
                threshold = entry.get("threshold_usd", DEFAULT_USD_THRESHOLD)
                if price > 0 and diff_usd < threshold:
                    continue
                if price == 0 and diff < threshold:
                    continue

                premium = db.is_premium(uid)
                alert_type = "🐳 Whale Transfer"
                if premium:
                    # Detect DEX swaps via MoveCall package IDs
                    prog_txs = (
                        tx.get("transaction", {})
                        .get("data", {})
                        .get("transaction", {})
                        .get("transactions")
                        or []
                    )
                    called_pkgs = {
                        (pt.get("MoveCall") or {}).get("package", "") for pt in prog_txs
                    }
                    called_pkgs.discard("")
                    sui_dex_routers = cat.DEX_ROUTERS.get("sui", set())
                    if any(p in sui_dex_routers for p in called_pkgs):
                        inflow = sum(
                            int(bc.get("amount", "0"))
                            for bc in balance_changes
                            if bc.get("coinType") == coin_type
                            and int(bc.get("amount", "0")) > 0
                        )
                        alert_type = "🔥 WHALE BUY" if inflow > 0 else "🚨 WHALE SELL"
                    else:
                        alert_type = "📦 COLD WALLET TRANSFER"

                usd_str = f"${diff_usd:,.0f}" if price > 0 else "price unknown"
                alert_text = (
                    f"{alert_type} — <b>{name} ({symbol})</b>  🌊 Sui\n\n"
                    f"<b>Amount:</b> {diff:,.2f} {symbol}"
                    + (f"  (~{usd_str})" if price > 0 else "")
                    + "\n"
                    f"<b>Tx:</b> <a href='https://suiscan.xyz/mainnet/tx/{digest}'>"
                    f"{_shorten(digest)}</a>"
                )
                user_rec = db.get_user(uid)
                if user_rec:
                    _lang = db.get_user_language(uid) or "en"
                    bot.send_message(
                        user_rec["chat_id"],
                        alert_text,
                        reply_markup=kb_chart_links("sui", coin_type, _lang),
                        disable_web_page_preview=True,
                    )
                db.save_alert(
                    "sui",
                    coin_type,
                    name,
                    symbol,
                    diff,
                    diff_usd,
                    "",
                    "",
                    digest,
                    0,
                    alert_type,
                )
            except Exception as exc:
                logger.warning("Sui alert uid %d: %s", uid, exc)


def _sui_monitor_loop() -> None:
    """Background thread for Sui coin whale monitoring."""
    global _sui_connected
    logger.info("Sui monitor thread started (RPC: %s)", SUI_RPC_URL or "not configured")
    if not SUI_RPC_URL:
        logger.warning("SUI_RPC_URL not set — Sui monitor disabled.")
    else:
        try:
            data = _rpc_post(
                SUI_RPC_URL,
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "suix_getReferenceGasPrice",
                    "params": [],
                },
                timeout=8,
            )
            gas = data.get("result", "unknown")
            logger.info("Sui RPC health: reference gas price = %s MIST", gas)
            _sui_connected = True
        except Exception as exc:
            logger.warning("Sui RPC unreachable: %s", exc)

    last_sui_cursor: dict[str, Any] = {}
    while True:
        time.sleep(POLL_INTERVAL)
        if not _sui_connected:
            continue
        try:
            subscribers = db.get_all_subscribers()
        except Exception as exc:
            logger.error("DB error (sui monitor): %s", exc)
            continue
        for key, uids in subscribers.items():
            if not key.startswith("sui:"):
                continue
            coin_type = key[4:]
            try:
                _sui_check_coin(coin_type, uids, last_sui_cursor)
            except Exception as exc:
                logger.error("Sui monitor error %s: %s", key, exc)


def _tron_check_token(
    contract: str, uids: set[int], last_tron_ts: dict[str, int]
) -> None:
    """Poll TronGrid REST API for recent TRC-20 Transfer events on `contract`."""
    if not TRON_RPC_URL:
        return
    last_ts = last_tron_ts.get(contract, 0)
    try:
        data = _rpc_get(
            f"{TRON_RPC_URL.rstrip('/')}/v1/contracts/{contract}/events",
            params={
                "event_name": "Transfer",
                "only_confirmed": "true",
                "limit": "20",
                "order_by": "block_timestamp,desc",
            },
        )
        events: list[dict] = data.get("data") or []
    except Exception as exc:
        logger.debug("TronGrid events %s: %s", contract, exc)
        return

    if not events:
        return

    latest_ts = events[0].get("block_timestamp", last_ts)
    last_tron_ts[contract] = latest_ts

    if last_ts == 0:
        return  # First run — just record cursor, don't alert on old txs

    new_events = [e for e in events if e.get("block_timestamp", 0) > last_ts]
    for event in new_events:
        result = event.get("result") or {}
        raw_value = result.get("_value") or result.get("value", "0")
        try:
            amount_raw = int(raw_value)
        except (ValueError, TypeError):
            continue

        tx_hash = event.get("transaction_id", "")

        for uid in uids:
            try:
                entry = db.get_watchlist_entry(uid, "tron", contract)
                if not entry or entry.get("paused"):
                    continue

                decimals = entry.get("decimals", 6)
                symbol = entry.get("symbol", "TRC-20")
                name = entry.get("name", symbol)
                amount = amount_raw / (10**decimals)
                price = get_price_usd(symbol)
                amount_usd = amount * price if price > 0 else 0
                threshold = entry.get("threshold_usd", DEFAULT_USD_THRESHOLD)

                if price > 0 and amount_usd < threshold:
                    continue
                if price == 0 and amount < threshold:
                    continue

                alert_type = "🐳 Whale Transfer"
                usd_str = f"${amount_usd:,.0f}" if price > 0 else "price unknown"
                tx_link = f"https://tronscan.org/#/transaction/{tx_hash}"
                alert_text = (
                    f"{alert_type} — <b>{name} ({symbol})</b>  🔴 Tron\n\n"
                    f"<b>Amount:</b> {amount:,.2f} {symbol}"
                    + (f"  (~{usd_str})" if price > 0 else "")
                    + "\n"
                    f"<b>Tx:</b> <a href='{tx_link}'>{_shorten(tx_hash)}</a>"
                )
                user_rec = db.get_user(uid)
                if user_rec:
                    _lang = db.get_user_language(uid) or "en"
                    bot.send_message(
                        user_rec["chat_id"],
                        alert_text,
                        reply_markup=kb_chart_links("tron", contract, _lang),
                        disable_web_page_preview=True,
                    )
                db.save_alert(
                    "tron", contract, name, symbol,
                    amount, amount_usd, "", "", tx_hash, 0, alert_type,
                )
            except Exception as exc:
                logger.warning("Tron alert uid %d: %s", uid, exc)


def _tron_monitor_loop() -> None:
    """Background thread for Tron TRC-20 token whale monitoring via TronGrid REST."""
    global _tron_connected
    logger.info("Tron monitor thread started (RPC: %s)", TRON_RPC_URL or "not configured")
    if not TRON_RPC_URL:
        logger.warning("TRON_RPC_URL not set — Tron monitor disabled.")
        return

    # Health-check: POST /wallet/getnowblock (native Tron HTTP API, works on
    # TronGrid, Alchemy, and any standard Tron full-node)
    try:
        data = _rpc_post(
            f"{TRON_RPC_URL.rstrip('/')}/wallet/getnowblock",
            {},
        )
        block_num = (
            data.get("block_header", {}).get("raw_data", {}).get("number", "?")
        )
        if block_num != "?":
            logger.info("Tron RPC health: latest block #%s", block_num)
            _tron_connected = True
        else:
            logger.warning("Tron RPC returned no block data — chain monitor disabled.")
    except Exception as exc:
        logger.warning("Tron RPC unreachable: %s", exc)
        return

    last_tron_ts: dict[str, int] = {}
    while True:
        time.sleep(POLL_INTERVAL)
        if not _tron_connected:
            continue
        try:
            subscribers = db.get_all_subscribers()
        except Exception as exc:
            logger.error("DB error (tron monitor): %s", exc)
            continue
        for key, uids in subscribers.items():
            if not key.startswith("tron:"):
                continue
            contract = key[5:]
            try:
                _tron_check_token(contract, uids, last_tron_ts)
            except Exception as exc:
                logger.error("Tron monitor error %s: %s", key, exc)


def _post_channel_digest() -> None:
    """Build and post the daily whale insights report to @Ledgexs, then cross-post to X."""
    top_tracked = db.get_top_tracked_tokens(5)
    top_volume = db.get_top_volume_tokens(5)
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    lines: list[str] = [
        "🐳 <b>Daily Whale Insights &amp; Top Tracked Tokens</b>",
        f"📅 {now_str}\n",
    ]

    if top_tracked:
        lines.append("📊 <b>Most Tracked Tokens</b>")
        for i, t in enumerate(top_tracked, 1):
            icon = CHAINS.get(t["chain"], {}).get("icon", "🔗")
            lines.append(
                f"{i}. {icon} <b>{t['name']} (${t['symbol']})</b>"
                f" — {t['trackers']} tracker{'s' if t['trackers'] != 1 else ''}"
            )
        lines.append("")

    if top_volume:
        lines.append("💰 <b>Top Volume (Last 24h)</b>")
        for i, t in enumerate(top_volume, 1):
            icon = CHAINS.get(t["chain"], {}).get("icon", "🔗")
            usd = f"${t['total_usd']:,.0f}" if t["total_usd"] else "$0"
            lines.append(
                f"{i}. {icon} <b>{t['token_name']} (${t['symbol']})</b> — {usd}"
            )
        lines.append("")

    if not top_tracked and not top_volume:
        lines.append("📭 No whale activity recorded in the last 24 hours.")
        lines.append("")

    lines.append(
        "🔔 Track whale wallets in real time →"
        " <a href='https://t.me/LedgexsBot'>@LedgexsBot</a>"
    )

    report = "\n".join(lines)
    try:
        bot.send_message(
            REQUIRED_CHANNEL,
            report,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        logger.info("Channel digest posted to %s", REQUIRED_CHANNEL)
    except Exception as exc:
        logger.warning("Channel digest post to %s failed: %s", REQUIRED_CHANNEL, exc)

    # Cross-post to X — wrapped in its own try/except so Telegram posting is never blocked
    _post_tweet_digest(top_tracked, top_volume)


def _channel_digest_loop() -> None:
    """Background thread — posts the channel digest at exactly 20:00 UTC daily.

    Never fires immediately on startup or restart. Always waits until the next
    20:00 UTC wall-clock moment before posting for the first time.
    """
    logger.info("Channel digest thread started (fires at 20:00 UTC daily).")
    while True:
        now = datetime.utcnow()
        # Next 20:00 UTC — if we're already past it today, schedule for tomorrow
        target = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        wait_secs = (target - now).total_seconds()
        logger.info(
            "Channel digest: next broadcast at %s UTC (in %.0f min).",
            target.strftime("%Y-%m-%d 20:00"),
            wait_secs / 60,
        )
        time.sleep(wait_secs)
        try:
            _post_channel_digest()
        except Exception as exc:
            logger.warning("Channel digest error: %s", exc)
        # Sleep 90 s after posting to avoid re-triggering if we wake fractionally early
        time.sleep(90)


def _digest_loop() -> None:
    """
    Background thread — checks every 5 minutes whether any user is due a
    daily digest at the current UTC hour and sends it if so.
    """
    logger.info("Daily digest thread started.")
    while True:
        try:
            now = datetime.utcnow()
            hour = now.hour
            today = now.strftime("%Y-%m-%d")
            for row in db.get_users_for_digest(hour, today):
                uid = row["uid"]
                chat_id = row["chat_id"]
                try:
                    _send_daily_digest(uid, chat_id)
                    db.mark_digest_sent(uid, today)
                except Exception as exc:
                    logger.warning("Digest error uid=%d: %s", uid, exc)
        except Exception as exc:
            logger.error("Digest loop error: %s", exc)
        time.sleep(300)  # 5-minute tick


def _check_whale_activity(
    chain_id: str, ca: str, uids: set[int], last_seen: dict[str, int]
) -> None:
    w3 = _get_w3(chain_id)
    if w3 is None or not w3.is_connected():
        return

    key = f"{chain_id}:{ca}"
    latest = w3.eth.block_number
    from_b = last_seen.get(key, latest - 1)
    if from_b >= latest:
        return
    last_seen[key] = latest

    contract: Contract = w3.eth.contract(
        address=Web3.to_checksum_address(ca), abi=ERC20_ABI
    )
    try:
        decimals = contract.functions.decimals().call()
        symbol = contract.functions.symbol().call()
        name = contract.functions.name().call()
    except Exception:
        decimals, symbol, name = 18, "???", "Unknown"

    try:
        events = contract.events.Transfer.get_logs(
            from_block=from_b + 1, to_block=latest
        )
    except Exception as exc:
        logger.debug("get_logs %s: %s", key, exc)
        return

    price_usd = get_price_usd(symbol)
    exp = _explorer(chain_id)
    clabel = _chain_label(chain_id)

    for event in events:
        amount = event["args"]["value"] / (10**decimals)
        amount_usd = amount * price_usd if price_usd > 0 else 0
        sender = event["args"]["from"]
        receiver = event["args"]["to"]
        tx_hash = event["transactionHash"].hex()
        block = event["blockNumber"]

        # ── Global intelligence pass (runs once per event, not per user) ─────
        # NOTE: NO intelligence alerts are ever posted to the public channel.
        # @Ledgexs is reserved for news only. All whale/CEX/accumulation alerts
        # go exclusively as DMs to subscribed users.

        if price_usd > 0:
            # DCA Accumulation — track sub-threshold buys from the sending wallet
            if whale_intel.ACCUM_MIN_USD <= amount_usd <= whale_intel.ACCUM_MAX_USD:
                whale_intel.accum_tracker.record(
                    chain_id, ca, str(sender), amount, amount_usd, symbol
                )
                trigger = whale_intel.accum_tracker.check_trigger(
                    chain_id, ca, str(sender)
                )
                if trigger:
                    accum_text = whale_intel.format_accum_alert(
                        str(sender),
                        trigger["total_amount"],
                        symbol,
                        trigger["total_usd"],
                        trigger["tx_count"],
                        clabel,
                    )
                    # DM every subscriber of this token — never post to public channel
                    for _uid in uids:
                        try:
                            _rec = db.get_user(_uid)
                            if _rec:
                                bot.send_message(
                                    _rec["chat_id"],
                                    accum_text,
                                    disable_web_page_preview=True,
                                )
                        except Exception:
                            pass

        # ── Per-user alerts (threshold-filtered, existing logic) ──────────────

        for uid in uids:
            try:
                entry = db.get_watchlist_entry(uid, chain_id, ca)
                if not entry or entry.get("paused"):
                    continue

                threshold_usd = entry.get("threshold_usd", DEFAULT_USD_THRESHOLD)
                premium = db.is_premium(uid)

                # USD threshold check; fall back to token count if no price
                if price_usd > 0:
                    if amount_usd < threshold_usd:
                        continue
                else:
                    if amount < threshold_usd:
                        continue

                # DEX / CEX classification (Premium only)
                if premium:
                    # Parametreleri garantiye alarak fonksiyona gönderiyoruz
                    alert_type = _classify_transfer(
                        str(chain_id), str(sender), str(receiver)
                    )
                else:
                    alert_type = "🐳 Whale Transfer"

                usd_str = f"${amount_usd:,.0f}" if price_usd > 0 else "price unknown"

                # Smart Money: resolve labels for sender/receiver
                sender_lbl = db.get_label_for_address(uid, sender)
                receiver_lbl = db.get_label_for_address(uid, receiver)
                sender_disp = (
                    f"<b>{sender_lbl}</b> (<code>{_shorten(sender)}</code>)"
                    if sender_lbl
                    else f"<code>{sender}</code>"
                )
                receiver_disp = (
                    f"<b>{receiver_lbl}</b> (<code>{_shorten(receiver)}</code>)"
                    if receiver_lbl
                    else f"<code>{receiver}</code>"
                )

                alert_text = (
                    f"{alert_type} — <b>{name} ({symbol})</b>\n"
                    f"<b>Chain:</b> {clabel}\n\n"
                    f"<b>Amount:</b> {amount:,.2f} {symbol}"
                    + (f"  (~{usd_str})" if price_usd > 0 else "")
                    + "\n"
                    f"<b>From:</b> {sender_disp}\n"
                    f"<b>To:</b>   {receiver_disp}\n"
                    f"<b>Tx:</b> <a href='{exp}/tx/{tx_hash}'>"
                    f"{_shorten(tx_hash)}</a>\n"
                    f"<b>Block:</b> {block}"
                )

                user_rec = db.get_user(uid)
                if user_rec:
                    _lang = db.get_user_language(uid) or "en"
                    bot.send_message(
                        user_rec["chat_id"],
                        alert_text,
                        reply_markup=kb_chart_links(chain_id, ca, _lang),
                        disable_web_page_preview=True,
                    )

                db.save_alert(
                    chain_id,
                    ca,
                    name,
                    symbol,
                    amount,
                    amount_usd,
                    sender,
                    receiver,
                    tx_hash,
                    block,
                    alert_type,
                )
            except Exception as exc:
                logger.warning("Alert uid %d: %s", uid, exc)


# ---------------------------------------------------------------------------
# Send-screen helpers
# ---------------------------------------------------------------------------


def _send_main_menu(chat_id: int, uid: int) -> None:
    lang = db.get_user_language(uid) or "en"
    premium = db.is_premium(uid)
    n = db.count_watchlist(uid)
    avail = _available_chains()
    chains_s = " · ".join(_chain_label(c) for c in CHAINS if c in avail)
    tier = (
        i18n.t("tier_premium", lang)
        if premium
        else f"{i18n.t('tier_free', lang)} ({n}/{FREE_TIER_LIMIT})"
    )

    bot.send_message(
        chat_id,
        f"{i18n.t('menu_title', lang)}\n\n"
        f"{i18n.t('menu_subtitle', lang)}\n\n"
        f"{i18n.t('menu_chains', lang)} {chains_s or i18n.t('none_configured', lang)}\n"
        f"{i18n.t('menu_tier', lang)} {tier}\n"
        f"{i18n.t('menu_threshold', lang)} ${DEFAULT_USD_THRESHOLD:,.0f} USD",
        reply_markup=kb_main_menu(uid, lang),
        disable_web_page_preview=True,
    )


def _send_watchlist(chat_id: int, uid: int) -> None:
    lang = db.get_user_language(uid) or "en"
    tracked = db.get_watchlist(uid)
    if not tracked:
        bot.send_message(
            chat_id,
            f"{i18n.t('watchlist_empty', lang)}\n\n{i18n.t('watchlist_empty_hint', lang)}",
            reply_markup=kb_back_home(lang),
        )
        return

    lines = [f"{i18n.t('watchlist_title', lang)}\n"]
    for i, t in enumerate(tracked, 1):
        thr = t.get("threshold_usd", DEFAULT_USD_THRESHOLD)
        clabel = _chain_label(t["chain"])
        status = (
            i18n.t("status_paused", lang)
            if t.get("paused")
            else i18n.t("status_active", lang)
        )
        lines.append(
            f"{i}. <b>{t['name']} ({t['symbol']})</b>  {clabel}\n"
            f"   {i18n.t('label_threshold', lang)}: ${thr:,.0f} · {status}\n"
            f"   <code>{t['ca']}</code>"
        )
    bot.send_message(
        chat_id,
        "\n\n".join(lines),
        reply_markup=kb_watchlist_menu(tracked, lang),
        disable_web_page_preview=True,
    )


def _send_history(chat_id: int, uid: int) -> None:
    lang = db.get_user_language(uid) or "en"
    alerts = db.get_recent_alerts(20)
    if not alerts:
        bot.send_message(
            chat_id,
            i18n.t("history_empty", lang),
            reply_markup=kb_back_home(lang),
        )
        return
    lines = [i18n.t("history_title", lang).format(count=len(alerts)) + "\n"]
    for i, h in enumerate(alerts, 1):
        usd_s = f"  (~${h['amount_usd']:,.0f})" if h.get("amount_usd") else ""
        exp = _explorer(h.get("chain", "eth"))
        lines.append(
            f"<b>{i}. {h['token_name']} ({h['symbol']})</b>  {h.get('alert_type', '')}\n"
            f"   💰 {h['amount_tok']:,.2f}{usd_s}\n"
            f"   🔗 <a href='{exp}/tx/{h['tx_hash']}'>{_shorten(h['tx_hash'])}</a>"
        )
    bot.send_message(
        chat_id,
        "\n\n".join(lines),
        reply_markup=kb_back_home(lang),
        disable_web_page_preview=True,
    )


def _send_leaderboard(chat_id: int, uid: int) -> None:
    lang = db.get_user_language(uid) or "en"
    top = db.get_leaderboard(5)
    if not top:
        bot.send_message(
            chat_id,
            i18n.t("leaderboard_empty", lang),
            reply_markup=kb_back_home(lang),
        )
        return
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    lines = [i18n.t("leaderboard_title", lang) + "\n"]
    for rank, h in enumerate(top):
        usd_s = f"  (~${h['amount_usd']:,.0f})" if h.get("amount_usd") else ""
        exp = _explorer(h.get("chain", "eth"))
        lines.append(
            f"{medals[rank]} <b>{h['token_name']} ({h['symbol']})</b>\n"
            f"   💰 <b>{h['amount_tok']:,.2f}{usd_s}</b>\n"
            f"   🔗 <a href='{exp}/tx/{h['tx_hash']}'>{_shorten(h['tx_hash'])}</a>"
        )
    bot.send_message(
        chat_id,
        "\n\n".join(lines),
        reply_markup=kb_back_home(lang),
        disable_web_page_preview=True,
    )


def _send_non_evm_token_stats(
    chat_id: int, chain_id: str, ca: str, uid: int, paused: bool, lang: str
) -> None:
    """Display token stats card for Solana, Sui, and Tron tokens."""
    entry = db.get_watchlist_entry(uid, chain_id, ca)
    if not entry:
        bot.send_message(
            chat_id,
            i18n.t("token_not_found", lang),
            reply_markup=kb_back_home(lang),
        )
        return

    name = entry.get("name") or "Unknown"
    symbol = entry.get("symbol") or "???"
    clabel = _chain_label(chain_id)
    chain_ca = f"{chain_id}:{ca}"
    price = get_price_usd(symbol)
    price_str = f"${price:,.4f}" if price else i18n.t("stat_na", lang)

    lines: list[str] = [
        f"📊 <b>{name} ({symbol})</b>  {clabel}",
        f"<code>{ca}</code>",
        "",
        f"<b>{i18n.t('stat_price', lang)}:</b> {price_str}",
        "",
    ]

    # Solana: fetch recent transaction count as activity indicator
    if chain_id == "sol" and SOL_RPC_URL:
        try:
            resp = _rpc_post(
                SOL_RPC_URL,
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [ca, {"limit": 20}],
                },
            )
            rpc_err = resp.get("error")
            if rpc_err:
                logger.warning("getSignaturesForAddress %s: %s", ca, rpc_err)
            else:
                sigs = resp.get("result") or []
                lines.append(
                    f"<b>{i18n.t('stat_transfers', lang)}:</b>  <b>~{len(sigs)}</b> (recent txs)"
                )
                lines.append("")
        except Exception as exc:
            logger.debug("Sol stats sigs %s: %s", ca, exc)

    # Explorer deep-links per chain
    _EXPLORER_URLS: dict[str, str] = {
        "sol": f"https://solscan.io/token/{ca}",
        "sui": f"https://suiscan.xyz/mainnet/coin/{ca}",
        "tron": f"https://tronscan.org/#/contract/{ca}",
    }
    explorer_url = _EXPLORER_URLS.get(chain_id, f"{_explorer(chain_id)}/token/{ca}")
    lines.append(f"🔗 <a href='{explorer_url}'>{i18n.t('link_explorer', lang)}</a>")

    bot.send_message(
        chat_id,
        "\n".join(lines),
        reply_markup=kb_token_menu(chain_ca, paused, lang),
        disable_web_page_preview=True,
    )


def _send_token_stats(
    chat_id: int, chain_ca: str, uid: int, paused: bool = False
) -> None:
    lang = db.get_user_language(uid) or "en"
    chain_id, ca = chain_ca.split(":", 1)

    # Non-EVM chains have dedicated stats display — never call _get_w3 for them
    if chain_id in ("sol", "sui", "tron"):
        _send_non_evm_token_stats(chat_id, chain_id, ca, uid, paused, lang)
        return

    w3 = _get_w3(chain_id)
    if w3 is None or not w3.is_connected():
        bot.send_message(
            chat_id,
            i18n.t("err_rpc_unavailable", lang),
            reply_markup=kb_back_home(lang),
        )
        return
    contract: Contract = w3.eth.contract(
        address=Web3.to_checksum_address(ca), abi=ERC20_ABI
    )
    try:
        name = contract.functions.name().call()
        symbol = contract.functions.symbol().call()
        decimals = contract.functions.decimals().call()
        supply = contract.functions.totalSupply().call() / (10**decimals)
    except Exception as exc:
        bot.send_message(
            chat_id,
            i18n.t("err_contract_read", lang).format(error=exc),
            reply_markup=kb_back_home(lang),
        )
        return

    SCAN = 300
    latest = w3.eth.block_number
    count, vol, largest = 0, 0.0, 0.0
    senders: set[str] = set()
    receivers: set[str] = set()
    try:
        for e in contract.events.Transfer.get_logs(
            from_block=max(0, latest - SCAN), to_block=latest
        ):
            amt = e["args"]["value"] / (10**decimals)
            count += 1
            vol += amt
            largest = max(largest, amt)
            senders.add(e["args"]["from"])
            receivers.add(e["args"]["to"])
        scan_ok = True
    except Exception:
        scan_ok = False

    price = get_price_usd(symbol)
    exp = _explorer(chain_id)
    clabel = _chain_label(chain_id)
    price_str = f"${price:,.4f}" if price else i18n.t("stat_na", lang)
    lines = [
        f"📊 <b>{name} ({symbol})</b>  {clabel}",
        f"<code>{ca}</code>",
        "",
        f"<b>{i18n.t('stat_total_supply', lang)}:</b> {supply:,.2f} {symbol}",
        f"<b>{i18n.t('stat_price', lang)}:</b> {price_str}",
        "",
        f"<b>{i18n.t('stat_last_1h', lang).format(blocks=SCAN)}</b>",
    ]
    if scan_ok:
        lines += [
            f"{i18n.t('stat_transfers', lang)}:    <b>{count:,}</b>",
            f"{i18n.t('stat_volume', lang)}:       <b>{vol:,.2f} {symbol}</b>",
            f"{i18n.t('stat_largest', lang)}:      <b>{largest:,.2f} {symbol}</b>",
            f"{i18n.t('stat_senders', lang)}:  <b>{len(senders):,}</b>",
            f"{i18n.t('stat_receivers', lang)}: <b>{len(receivers):,}</b>",
        ]
    else:
        lines.append(i18n.t("err_scan_failed", lang))
    lines += ["", f"🔗 <a href='{exp}/token/{ca}'>{i18n.t('link_explorer', lang)}</a>"]
    bot.send_message(
        chat_id,
        "\n".join(lines),
        reply_markup=kb_token_menu(chain_ca, paused, lang),
        disable_web_page_preview=True,
    )


def _send_premium_info(chat_id: int, uid: int) -> None:
    lang = db.get_user_language(uid) or "en"
    if db.is_premium(uid):
        _send_membership(chat_id, uid)
        return
    if not PAYMENT_WALLET:
        bot.send_message(
            chat_id,
            f"{i18n.t('premium_title', lang)}\n\n"
            f"{i18n.t('premium_features', lang)}\n\n"
            f"{i18n.t('premium_contact_admin', lang)}",
            reply_markup=kb_back_home(lang),
        )
        return
    amount_units = _assign_unique_payment(uid)
    amount_display = _usdt_display(amount_units)
    bot.send_message(
        chat_id,
        f"{i18n.t('premium_title', lang)}\n\n"
        f"{i18n.t('premium_features', lang)}\n\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"{i18n.t('premium_payment_send', lang).format(amount=amount_display)}\n"
        f"<code>{PAYMENT_WALLET}</code>\n\n"
        f"{i18n.t('premium_payment_warning', lang)}",
        reply_markup=kb_verify_payment(lang),
        disable_web_page_preview=True,
    )


def _send_digest_menu(chat_id: int, uid: int) -> None:
    lang = db.get_user_language(uid) or "en"
    tracked = db.get_watchlist(uid)
    hour = db.get_digest_setting(uid)
    count = len(tracked)
    hour_str = (
        f"{hour:02d}:00 UTC"
        if hour is not None
        else i18n.t("digest_disabled_label", lang)
    )
    bot.send_message(
        chat_id,
        f"{i18n.t('digest_title', lang)}\n\n"
        f"{i18n.t('digest_description', lang)}\n\n"
        f"{i18n.t('digest_tracked', lang).format(count=count)}\n"
        f"{i18n.t('digest_current_time', lang).format(time=hour_str)}\n\n"
        f"{i18n.t('digest_pick_hour', lang)}",
        reply_markup=kb_digest_menu(hour, lang),
    )


def _send_daily_digest(uid: int, chat_id: int) -> None:
    """Compile and send the daily whale digest to a single user."""
    lang = db.get_user_language(uid) or "en"
    alerts = db.get_user_digest_alerts(uid, limit=5)
    tracked = db.get_watchlist(uid)
    token_count = len(tracked)
    now_str = datetime.utcnow().strftime("%B %d, %Y")
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    plural_s = "s" if token_count != 1 else ""

    if not alerts:
        msg = (
            f"{i18n.t('digest_daily_title', lang).format(date=now_str)}\n\n"
            f"{i18n.t('digest_no_activity', lang).format(count=token_count, plural=plural_s)}"
        )
    else:
        a_plural = "s" if len(alerts) != 1 else ""
        lines = [f"{i18n.t('digest_daily_title', lang).format(date=now_str)}\n"]
        lines.append(
            i18n.t("digest_top_moves", lang).format(count=len(alerts), plural=a_plural)
            + "\n"
        )
        for i, a in enumerate(alerts):
            usd_s = f"  (~${a['amount_usd']:,.0f})" if a.get("amount_usd") else ""
            atype = a.get("alert_type", "🐳 Transfer")
            clabel = _chain_label(a.get("chain", "eth"))
            exp = _explorer(a.get("chain", "eth"))
            tx = a.get("tx_hash", "")
            lines.append(
                f"{medals[i]} <b>{a['token_name']} ({a['symbol']})</b>  {clabel}\n"
                f"   {atype}\n"
                f"   💰 {a['amount_tok']:,.2f} {a['symbol']}{usd_s}\n"
                f"   🔗 <a href='{exp}/tx/{tx}'>{_shorten(tx)}</a>"
            )
        lines.append(
            f"\n<i>{i18n.t('digest_monitoring', lang).format(count=token_count, plural=plural_s)}</i>"
        )
        msg = "\n\n".join(lines)

    try:
        bot.send_message(
            chat_id, msg, disable_web_page_preview=True, reply_markup=kb_back_home(lang)
        )
    except Exception as exc:
        logger.warning("Digest send failed uid=%d: %s", uid, exc)


def _send_membership(chat_id: int, uid: int) -> None:
    lang = db.get_user_language(uid) or "en"
    user = db.get_user(uid)
    if not user:
        return
    exp = user.get("premium_expiry")
    lines = [f"{i18n.t('membership_title', lang)}\n", i18n.t("membership_tier", lang)]
    if exp:
        try:
            expiry_dt = datetime.fromisoformat(exp)
            days_left = (expiry_dt - datetime.utcnow()).days
            billing_dt = expiry_dt.strftime("%B %d, %Y")
            lines += [
                i18n.t("membership_status_active", lang),
                i18n.t("membership_days_remaining", lang).format(
                    days=max(0, days_left)
                ),
                i18n.t("membership_renews", lang).format(date=billing_dt),
            ]
        except Exception:
            lines.append(i18n.t("membership_status_active", lang))
    else:
        lines.append(i18n.t("membership_status_lifetime", lang))

    lines += ["", i18n.t("membership_features", lang)]
    bot.send_message(chat_id, "\n".join(lines), reply_markup=kb_back_home(lang))


# ---------------------------------------------------------------------------
# /start and /help commands
# ---------------------------------------------------------------------------


@bot.message_handler(commands=["start", "menu"])
def handle_start(message: types.Message) -> None:
    uid = message.from_user.id
    chat_id = message.chat.id
    username = (message.from_user.username or "").lower()
    db.upsert_user(uid, chat_id)

    # Auto-grant lifetime premium to designated users
    if username in LIFETIME_PREMIUM_USERS and not db.is_premium(uid):
        db.set_premium_lifetime(uid)
        logger.info("Lifetime premium granted to @%s (uid=%d)", username, uid)

    with state_lock:
        pending_actions.pop(uid, None)

    # ── Step 1: channel gate ─────────────────────────────────────────────
    lang = db.get_user_language(uid) or "en"
    if not check_channel_membership(uid):
        _send_join_required(chat_id, lang)
        return

    # ── Step 2: first-time language selection ────────────────────────────
    stored_lang = db.get_user_language(uid)
    if stored_lang is None:
        bot.send_message(
            chat_id,
            i18n.t("lang_prompt", "en"),
            reply_markup=kb_language_select(),
        )
        return

    # ── Step 3: show main menu in their language ─────────────────────────
    _send_main_menu(chat_id, uid)


@bot.message_handler(commands=["language"])
def handle_cmd_language(message: types.Message) -> None:
    """Let users change their language preference at any time."""
    uid = message.from_user.id
    db.upsert_user(uid, message.chat.id)
    lang = db.get_user_language(uid) or "en"
    bot.send_message(
        message.chat.id,
        i18n.t("lang_prompt", lang),
        reply_markup=kb_language_select(),
    )


@bot.message_handler(commands=["search"])
def handle_cmd_search(message: types.Message) -> None:
    uid = message.from_user.id
    db.upsert_user(uid, message.chat.id)
    lang = db.get_user_language(uid) or "en"
    with state_lock:
        pending_actions[uid] = {"action": "awaiting_search"}
    bot.send_message(
        message.chat.id,
        i18n.t("search_prompt", lang),
        reply_markup=kb_back_home(lang),
    )


@bot.message_handler(commands=["popular"])
def handle_cmd_popular(message: types.Message) -> None:
    uid = message.from_user.id
    db.upsert_user(uid, message.chat.id)
    _show_popular(message.chat.id, uid)


@bot.message_handler(commands=["digest"])
def handle_cmd_digest(message: types.Message) -> None:
    uid = message.from_user.id
    db.upsert_user(uid, message.chat.id)
    _send_digest_menu(message.chat.id, uid)


# ---------------------------------------------------------------------------
# Callback router
# ---------------------------------------------------------------------------


@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call: types.CallbackQuery) -> None:
    uid = call.from_user.id
    chat_id = call.message.chat.id
    data = call.data

    db.upsert_user(uid, chat_id)
    bot.answer_callback_query(call.id)
    lang = db.get_user_language(uid) or "en"

    # ── Navigation ─────────────────────────────────────────────────────────

    if data == "main_home":
        with state_lock:
            pending_actions.pop(uid, None)
        _send_main_menu(chat_id, uid)

    elif data == "main_mylist":
        with state_lock:
            pending_actions.pop(uid, None)
        _send_watchlist(chat_id, uid)

    elif data == "main_history":
        _send_history(chat_id, uid)

    elif data == "main_leaderboard":
        _send_leaderboard(chat_id, uid)

    elif data == "main_premium":
        _send_premium_info(chat_id, uid)

    elif data == "main_digest":
        _send_digest_menu(chat_id, uid)

    elif data == "main_membership":
        _send_membership(chat_id, uid)

    # ── Language selection ──────────────────────────────────────────────────

    elif data == "main_lang":
        lang = db.get_user_language(uid) or "en"
        bot.send_message(
            chat_id,
            i18n.t("lang_prompt", lang),
            reply_markup=kb_language_select(),
        )

    elif data.startswith("lang_set:"):
        chosen = data[len("lang_set:") :]
        if chosen in i18n.LANGS:
            db.set_user_language(uid, chosen)
            bot.answer_callback_query(call.id, i18n.t("lang_set_confirm", chosen))
            _send_main_menu(chat_id, uid)
        else:
            bot.answer_callback_query(call.id, "Unknown language.", show_alert=True)

    # ── Channel join re-check ───────────────────────────────────────────────

    elif data == "check_join":
        if check_channel_membership(uid):
            stored_lang = db.get_user_language(uid)
            if stored_lang is None:
                bot.send_message(
                    chat_id,
                    i18n.t("lang_prompt", "en"),
                    reply_markup=kb_language_select(),
                )
            else:
                _send_main_menu(chat_id, uid)
        else:
            bot.answer_callback_query(
                call.id,
                i18n.t("join_required", lang).split("\n\n")[0],
                show_alert=True,
            )

    # ── Search ─────────────────────────────────────────────────────────────

    elif data == "main_search":
        with state_lock:
            pending_actions[uid] = {"action": "awaiting_search"}
        bot.send_message(
            chat_id,
            i18n.t("search_prompt", lang),
            reply_markup=kb_back_home(lang),
        )

    # ── Daily digest settings ───────────────────────────────────────────────

    elif data.startswith("digest_set:"):
        try:
            hour = int(data[len("digest_set:") :])
            if 0 <= hour <= 23:
                db.set_digest_hour(uid, hour)
                bot.send_message(
                    chat_id,
                    i18n.t("digest_set_confirm", lang).format(hour=f"{hour:02d}"),
                    reply_markup=kb_digest_menu(hour, lang),
                )
        except ValueError:
            pass

    elif data == "digest_off":
        db.set_digest_hour(uid, None)
        bot.send_message(
            chat_id,
            i18n.t("digest_off_confirm", lang),
            reply_markup=kb_back_home(lang),
        )

    # ── Popular quick-track ─────────────────────────────────────────────────

    elif data == "main_popular":
        _show_popular(chat_id, uid)

    # ── Custom track (manual CA entry) ─────────────────────────────────────

    elif data == "main_track":
        avail = _available_chains()
        if not avail:
            bot.send_message(
                chat_id, i18n.t("err_no_chains", lang), reply_markup=kb_back_home(lang)
            )
            return
        with state_lock:
            pending_actions[uid] = {"action": "awaiting_chain"}
        bot.send_message(
            chat_id,
            i18n.t("prompt_select_chain", lang),
            reply_markup=kb_chain_select(lang),
        )

    elif data.startswith("chain_select:"):
        chain_id = data[len("chain_select:") :]
        if chain_id not in CHAINS:
            return
        if chain_id != "eth" and not db.is_premium(uid):
            bot.send_message(
                chat_id,
                i18n.t("err_premium_multichain", lang),
                reply_markup=kb_back_home(lang),
            )
            return
        with state_lock:
            pending_actions[uid] = {"action": "awaiting_ca", "chain": chain_id}
        if chain_id == "sol":
            prompt_key = "prompt_enter_sol_ca"
        elif chain_id == "sui":
            prompt_key = "prompt_enter_sui_ca"
        elif chain_id == "tron":
            prompt_key = "prompt_enter_tron_ca"
        else:
            prompt_key = "prompt_enter_ca"
        bot.send_message(
            chat_id,
            i18n.t(prompt_key, lang).format(chain=_chain_label(chain_id)),
            reply_markup=kb_back_home(lang),
        )

    # ── Quick-track toggle ─────────────────────────────────────────────────

    elif data.startswith("qt:"):
        parts = data.split(":", 2)
        if len(parts) == 3:
            _toggle_qt(uid, chat_id, parts[1], parts[2], call)

    # ── Token management ───────────────────────────────────────────────────

    elif data.startswith("tmenu:"):
        _, chain_id, ca = data.split(":", 2)
        entry = db.get_watchlist_entry(uid, chain_id, ca)
        if not entry:
            bot.send_message(
                chat_id,
                i18n.t("token_not_found", lang),
                reply_markup=kb_back_home(lang),
            )
            return
        chain_ca = f"{chain_id}:{ca}"
        paused = bool(entry.get("paused"))
        thr = entry.get("threshold_usd", DEFAULT_USD_THRESHOLD)
        status = (
            i18n.t("status_paused", lang) if paused else i18n.t("status_active", lang)
        )
        clabel = _chain_label(chain_id)
        bot.send_message(
            chat_id,
            f"🔹 <b>{entry['name']} ({entry['symbol']})</b>  {clabel}\n"
            f"<code>{ca}</code>\n\n"
            + i18n.t("token_info_label", lang).format(threshold=thr, status=status),
            reply_markup=kb_token_menu(chain_ca, paused, lang),
            disable_web_page_preview=True,
        )

    elif data.startswith("tstat:"):
        chain_ca = data[len("tstat:") :]
        chain_id, ca = chain_ca.split(":", 1)
        entry = db.get_watchlist_entry(uid, chain_id, ca)
        paused = bool(entry.get("paused")) if entry else False
        bot.send_message(chat_id, i18n.t("fetching_stats", lang))
        _send_token_stats(chat_id, chain_ca, uid, paused)

    elif data.startswith("ttoggle:"):
        chain_ca = data[len("ttoggle:") :]
        chain_id, ca = chain_ca.split(":", 1)
        entry = db.get_watchlist_entry(uid, chain_id, ca)
        if not entry:
            bot.send_message(
                chat_id,
                i18n.t("token_not_found", lang),
                reply_markup=kb_back_home(lang),
            )
            return
        new_paused = not bool(entry.get("paused"))
        db.set_paused(uid, chain_id, ca, new_paused)
        bot.send_message(
            chat_id,
            i18n.t("alerts_paused", lang).format(name=entry["name"])
            if new_paused
            else i18n.t("alerts_resumed", lang).format(name=entry["name"]),
            reply_markup=kb_token_menu(chain_ca, new_paused, lang),
        )

    elif data.startswith("tuntrack:"):
        chain_ca = data[len("tuntrack:") :]
        chain_id, ca = chain_ca.split(":", 1)
        entry = db.get_watchlist_entry(uid, chain_id, ca)
        db.remove_from_watchlist(uid, chain_id, ca)
        name = entry["name"] if entry else ca[:10]
        bot.send_message(
            chat_id,
            i18n.t("untrack_confirm", lang).format(
                name=name, chain=_chain_label(chain_id)
            ),
            reply_markup=kb_back_home(lang),
        )

    elif data.startswith("tset:"):
        chain_ca = data[len("tset:") :]
        chain_id, ca = chain_ca.split(":", 1)
        entry = db.get_watchlist_entry(uid, chain_id, ca)
        if not entry:
            bot.send_message(
                chat_id,
                i18n.t("token_not_found", lang),
                reply_markup=kb_back_home(lang),
            )
            return
        with state_lock:
            pending_actions[uid] = {
                "action": "awaiting_threshold",
                "chain_ca": chain_ca,
            }
        premium = db.is_premium(uid)
        hint = (
            ""
            if premium
            else i18n.t("threshold_hint_free", lang).format(limit=DEFAULT_USD_THRESHOLD)
        )
        bot.send_message(
            chat_id,
            i18n.t("prompt_set_threshold", lang).format(
                name=entry["name"],
                symbol=entry["symbol"],
                current=entry.get("threshold_usd", DEFAULT_USD_THRESHOLD),
            )
            + hint,
            reply_markup=kb_back_home(lang),
        )

    elif data.startswith("tsetlabel:"):
        chain_ca = data[len("tsetlabel:"):]
        chain_id, ca = chain_ca.split(":", 1)
        entry = db.get_watchlist_entry(uid, chain_id, ca)
        if not entry:
            bot.send_message(
                chat_id,
                i18n.t("token_not_found", lang),
                reply_markup=kb_back_home(lang),
            )
            return
        with state_lock:
            pending_actions[uid] = {"action": "awaiting_label", "chain_ca": chain_ca}
        current_label = db.get_wallet_label(uid, chain_id, ca)
        hint = f"\n\n<i>Current label: <b>{current_label}</b></i>" if current_label else ""
        bot.send_message(
            chat_id,
            i18n.t("prompt_set_label", lang).format(
                name=entry["name"], symbol=entry["symbol"]
            ) + hint,
            reply_markup=kb_back_home(lang),
        )

    # ── Payment ────────────────────────────────────────────────────────────

    elif data == "pay_verify":
        amount_units = db.get_pending_payment(uid)
        if amount_units is None:
            bot.send_message(
                chat_id,
                i18n.t("payment_no_pending", lang),
                reply_markup=kb_back_home(lang),
            )
            return
        amount_display = _usdt_display(amount_units)
        bot.send_message(
            chat_id,
            i18n.t("payment_scanning", lang).format(amount=amount_display),
        )
        if _verify_usdt_payment(amount_units):
            db.set_premium(uid, days=30)
            db.release_payment(uid)
            bot.send_message(
                chat_id,
                i18n.t("payment_confirmed", lang),
                reply_markup=kb_main_menu(uid, lang),
            )
            logger.info("User %d upgraded to Premium.", uid)
        else:
            bot.send_message(
                chat_id,
                i18n.t("payment_not_found", lang).format(
                    amount=amount_display, wallet=PAYMENT_WALLET
                ),
                reply_markup=kb_verify_payment(lang),
                disable_web_page_preview=True,
            )


# ---------------------------------------------------------------------------
# Popular helper (used from callback and command)
# ---------------------------------------------------------------------------


def _show_popular(chat_id: int, uid: int) -> None:
    lang = db.get_user_language(uid) or "en"
    tokens = cat.get_popular_tokens()
    bot.send_message(
        chat_id,
        i18n.t("popular_title", lang),
        reply_markup=kb_qt_list(uid, tokens, lang),
    )


# ---------------------------------------------------------------------------
# Text message handler
# ---------------------------------------------------------------------------


@bot.message_handler(
    func=lambda m: m.content_type == "text" and not m.text.startswith("/")
)
def handle_text_input(message: types.Message) -> None:
    uid = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()

    db.upsert_user(uid, chat_id)
    lang = db.get_user_language(uid) or "en"

    with state_lock:
        action_info = pending_actions.get(uid)

    if action_info is None:
        bot.send_message(
            chat_id, i18n.t("use_menu", lang), reply_markup=kb_main_menu(uid, lang)
        )
        return

    action = action_info["action"]

    # ── Search query ───────────────────────────────────────────────────────

    if action == "awaiting_search":
        with state_lock:
            pending_actions.pop(uid, None)
        results = cat.search_catalog(text)
        if not results:
            bot.send_message(
                chat_id,
                i18n.t("search_no_results", lang).format(query=text),
                reply_markup=kb_back_home(lang),
            )
            return
        bot.send_message(
            chat_id,
            i18n.t("search_results", lang).format(query=text, count=len(results)),
            reply_markup=kb_qt_list(uid, results, lang),
        )

    # ── Awaiting CA ────────────────────────────────────────────────────────

    elif action == "awaiting_ca":
        chain_id = action_info.get("chain", "eth")
        with state_lock:
            pending_actions.pop(uid, None)

        if chain_id == "sol":
            if not _is_sol_address(text):
                bot.send_message(
                    chat_id,
                    i18n.t("err_invalid_address_sol", lang),
                    reply_markup=kb_main_menu(uid, lang),
                )
                return
            ca = text.strip()
        elif chain_id == "sui":
            if not _is_sui_address(text):
                bot.send_message(
                    chat_id,
                    i18n.t("err_invalid_address_sol", lang),
                    reply_markup=kb_main_menu(uid, lang),
                )
                return
            ca = text.strip()
        elif chain_id == "tron":
            if not _is_tron_address(text):
                bot.send_message(
                    chat_id,
                    i18n.t("err_invalid_address_tron", lang),
                    reply_markup=kb_main_menu(uid, lang),
                )
                return
            ca = text.strip()
        else:
            if not Web3.is_address(text):
                bot.send_message(
                    chat_id,
                    i18n.t("err_invalid_address_evm", lang),
                    reply_markup=kb_main_menu(uid, lang),
                )
                return
            ca = Web3.to_checksum_address(text)
        premium = db.is_premium(uid)

        if db.is_tracked(uid, chain_id, ca):
            bot.send_message(
                chat_id,
                i18n.t("already_tracking", lang).format(
                    ca=ca, chain=_chain_label(chain_id)
                ),
                reply_markup=kb_main_menu(uid, lang),
            )
            return

        n = db.count_watchlist(uid)
        if not premium and n >= FREE_TIER_LIMIT:
            bot.send_message(
                chat_id,
                i18n.t("limit_reached", lang).format(limit=FREE_TIER_LIMIT),
                reply_markup=kb_main_menu(uid, lang),
            )
            return

        clabel = _chain_label(chain_id)
        bot.send_message(
            chat_id, i18n.t("fetching_token_info", lang).format(chain=clabel)
        )

        info = _fetch_token_info(ca, chain_id)
        if info is None:
            bot.send_message(
                chat_id,
                i18n.t("err_invalid_token", lang).format(ca=ca, chain=clabel),
                reply_markup=kb_main_menu(uid, lang),
            )
            return

        db.add_to_watchlist(
            uid,
            chain_id,
            ca,
            info["name"],
            info["symbol"],
            info["decimals"],
            DEFAULT_USD_THRESHOLD,
        )
        bot.send_message(
            chat_id,
            i18n.t("tracking_started", lang).format(
                name=info["name"],
                symbol=info["symbol"],
                chain=clabel,
                ca=ca,
                threshold=DEFAULT_USD_THRESHOLD,
            ),
            reply_markup=kb_main_menu(uid, lang),
            disable_web_page_preview=True,
        )

    # ── Awaiting threshold ─────────────────────────────────────────────────

    elif action == "awaiting_label":
        chain_ca = action_info.get("chain_ca", "")
        chain_id, ca = chain_ca.split(":", 1) if ":" in chain_ca else ("eth", chain_ca)
        with state_lock:
            pending_actions.pop(uid, None)
        entry = db.get_watchlist_entry(uid, chain_id, ca)
        if not entry:
            bot.send_message(
                chat_id,
                i18n.t("token_not_found", lang),
                reply_markup=kb_main_menu(uid, lang),
            )
            return
        if text.strip().lower() == "clear":
            db.set_wallet_label(uid, chain_id, ca, "")
            bot.send_message(
                chat_id,
                i18n.t("label_cleared", lang).format(
                    name=entry["name"], symbol=entry["symbol"]
                ),
                reply_markup=kb_token_menu(chain_ca, bool(entry.get("paused")), lang),
            )
        else:
            label = text.strip()[:64]  # cap at 64 chars
            db.set_wallet_label(uid, chain_id, ca, label)
            bot.send_message(
                chat_id,
                i18n.t("label_saved", lang).format(
                    label=label, name=entry["name"], symbol=entry["symbol"]
                ),
                reply_markup=kb_token_menu(chain_ca, bool(entry.get("paused")), lang),
            )

    elif action == "awaiting_threshold":
        chain_ca = action_info.get("chain_ca", "")
        chain_id, ca = chain_ca.split(":", 1) if ":" in chain_ca else ("eth", chain_ca)
        with state_lock:
            pending_actions.pop(uid, None)

        try:
            new_thr = float(text.replace(",", "").replace("$", ""))
            if new_thr <= 0:
                raise ValueError
        except ValueError:
            bot.send_message(
                chat_id,
                i18n.t("err_invalid_amount", lang),
                reply_markup=kb_main_menu(uid, lang),
            )
            return

        premium = db.is_premium(uid)
        if not premium and new_thr < DEFAULT_USD_THRESHOLD:
            bot.send_message(
                chat_id,
                i18n.t("threshold_below_free", lang).format(
                    limit=DEFAULT_USD_THRESHOLD
                ),
                reply_markup=kb_main_menu(uid, lang),
            )
            return

        entry = db.get_watchlist_entry(uid, chain_id, ca)
        if not entry:
            bot.send_message(
                chat_id,
                i18n.t("token_not_found", lang),
                reply_markup=kb_main_menu(uid, lang),
            )
            return

        old_thr = entry.get("threshold_usd", DEFAULT_USD_THRESHOLD)
        db.set_threshold(uid, chain_id, ca, new_thr)
        bot.send_message(
            chat_id,
            i18n.t("threshold_updated", lang).format(
                name=entry["name"],
                symbol=entry["symbol"],
                old=old_thr,
                new=new_thr,
            ),
            reply_markup=kb_token_menu(chain_ca, bool(entry.get("paused")), lang),
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is not set.")

    db.init_db()
    logger.info("Database initialised at %s", db.DB_PATH)

    logger.info("Clearing webhook and any pending conflict sessions...")
    bot.delete_webhook(drop_pending_updates=True)

    # Register bot command menu
    try:
        bot.set_my_commands(
            [
                types.BotCommand("start", "Main Menu"),
                types.BotCommand("menu", "Main Menu"),
                types.BotCommand("search", "Search Tokens"),
                types.BotCommand("popular", "Popular Tokens"),
                types.BotCommand("digest", "Daily Digest Settings"),
                types.BotCommand("language", "Change Language / 语言 / Langue"),
            ]
        )
    except Exception as exc:
        logger.warning("Could not set bot commands: %s", exc)

    connected: list[str] = []
    for cid, w3i in w3_instances.items():
        if w3i.is_connected():
            logger.info("%-8s connected — block %d", cid, w3i.eth.block_number)
            connected.append(cid)
        else:
            logger.warning("%-8s RPC configured but unreachable.", cid)

    if not w3_instances:
        logger.warning("No RPC URLs configured.")

    if not PAYMENT_WALLET:
        logger.warning("PAYMENT_WALLET not set — premium shows contact-admin message.")

    # Initialise optional Twitter/X client (non-fatal if secrets absent)
    _init_twitter()

    # Launch catalog auto-fetch in background (non-blocking)
    catalog_thread = threading.Thread(
        target=cat.fetch_dynamic_catalog, daemon=True, name="CatalogFetch"
    )
    catalog_thread.start()

    monitor_thread = threading.Thread(target=_monitor_loop, daemon=True, name="Monitor")
    monitor_thread.start()

    sol_thread = threading.Thread(
        target=_sol_monitor_loop, daemon=True, name="SolMonitor"
    )
    sol_thread.start()

    sui_thread = threading.Thread(
        target=_sui_monitor_loop, daemon=True, name="SuiMonitor"
    )
    sui_thread.start()

    tron_thread = threading.Thread(
        target=_tron_monitor_loop, daemon=True, name="TronMonitor"
    )
    tron_thread.start()

    digest_thread = threading.Thread(target=_digest_loop, daemon=True, name="Digest")
    digest_thread.start()

    channel_digest_thread = threading.Thread(
        target=_channel_digest_loop, daemon=True, name="ChannelDigest"
    )
    channel_digest_thread.start()

    threading.Thread(
        target=_keepalive_loop, daemon=True, name="KeepAlive"
    ).start()

    # ── News Aggregator (isolated — any failure here is non-fatal) ──────────
    try:
        import news_aggregator
        news_aggregator.start_news_aggregator()
    except Exception as _news_exc:
        logger.warning("News aggregator failed to start (non-fatal): %s", _news_exc)

    logger.info("Bot is running…")
    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=5, logger_level=logging.WARNING)
        except Exception as _poll_exc:
            logger.error("Polling crashed: %s — restarting in 5 s…", _poll_exc)
            time.sleep(5)
