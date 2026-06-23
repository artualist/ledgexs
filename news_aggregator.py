"""
bot/news_aggregator.py
======================
Completely isolated News Aggregator module.

Listens to a configurable list of source Telegram channels via a Telethon
UserBot, deduplicates semantically via AI, AI-rewrites each unique post in
strict global English (@Ledgexs brand voice), then cross-posts to @Ledgexs
(Bot API, with media album support) and X/Twitter (media upload via v1.1 API,
no @Ledgexs signature).

Any failure inside this module is caught and logged — it can NEVER crash or
block the main bot thread.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import threading
import time
import random
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from functools import partial

logger = logging.getLogger("whale_bot.news")

# ── Config ────────────────────────────────────────────────────────────────────

SOURCE_CHANNELS: list[str] = [
    # CoingraphNews operates multiple channel accounts — all four must be listed
    "@lookonchainchannel",
    "@cointelegraph",
    "@bitcoinmagazinetelegram",
    "@fin_watch",
    "@unfolded",
    "@ninjanewstr",
    "@watcherguru",
    "@coinmuhendisihaber",
    "@news_crypto",
    # Only add channels here whose posts you WANT published to @Ledgexs.
]

# Normalised set for fast O(1) membership check (lowercase, no @)
# Used by the manual filter inside the event handler instead of relying on
# Telethon's chats= resolution which silently drops unresolvable channels.
_SOURCE_USERNAMES: set[str] = {ch.lstrip("@").lower() for ch in SOURCE_CHANNELS}

DEST_CHANNEL      = "@Ledgexs"
TELEGRAM_SIG = (
    "\n\n"
    "━━━━━━━━━━━━━━━\n"
    "<b>Ledgexs</b> | <a href='https://t.me/LedgexsWhale'>Whale Alert</a> | <a href='https://x.com/Ledgexs'>X</a> | <a href='https://t.me/LedgexsBot'>LX Whale Bot</a>"
)
MIN_TEXT_LEN       = 15               # skip media-only / trivially short messages
TWEET_MAX          = 25000
TWITTER_MAX_MEDIA  = 4               # Twitter hard limit
TG_MAX_MEDIA       = 10              # Telegram sendMediaGroup hard limit
DEDUP_CACHE_SIZE   = 60              # rolling window of recent summaries
DEDUP_WINDOW_HOURS = 3               # only compare against stories from the last N hours
GROUP_COLLECT_S    = 1.5             # seconds to wait for all album frames to arrive (was 1.2)
MEDIA_DIR          = Path("/tmp/news_media")
_WS_RE             = re.compile(r'\s+')

# Pre-AI fingerprint dedup — catches near-exact duplicates without an API call.
FINGERPRINT_WORDS = 25
FINGERPRINT_SIM_THRESHOLD = 0.55    # Jaccard similarity above this → duplicate
FINGERPRINT_WINDOW_S = 3 * 3600     # how long to keep fingerprints (3 hours)

# ── AI prompts ────────────────────────────────────────────────────────────────

AI_COMBINED_PROMPT = (
    "You are the senior crypto-intelligence editor for @Ledgexs. "
    "Your objective is to provide elite-level, high-signal information in STRICT GLOBAL ENGLISH.\n\n"

    "CRITICAL LANGUAGE RULE: ALL output MUST be in English. If the input is in Turkish, Arabic, or any other language, "
    "you MUST translate it to fluent, professional English immediately. NEVER output non-English text.\n\n"

    "CRITICAL RULE 1 (DEDUPLICATION):\n"
    "Output ONLY the word DUPLICATE if the INCOMING NEWS describes the EXACT SAME specific event as one of the RECENTLY PUBLISHED STORIES.\n"
    "A duplicate requires ALL THREE of the following to match:\n"
    "  a) Same specific subject/entity (same person, same company, same token)\n"
    "  b) Same specific action or decision (same verb/event)\n"
    "  c) Similar timing (within the same news cycle)\n\n"
    "EXAMPLES OF DUPLICATES (output DUPLICATE):\n"
    "  - Recent: 'Saylor hints at buying BTC'  |  Incoming: 'MicroStrategy may purchase more Bitcoin'\n"
    "  - Recent: 'SEC approves Bitcoin ETF'     |  Incoming: 'Bitcoin ETF gets SEC greenlight'\n\n"
    "EXAMPLES OF NOT DUPLICATES (DO NOT output DUPLICATE — rewrite them):\n"
    "  - Recent: 'Saylor hints at buying BTC'  |  Incoming: 'BlackRock buys $500M BTC' (different actor)\n"
    "  - Recent: 'BTC hits $100k'              |  Incoming: 'ETH breaks $4,000' (different asset)\n"
    "  - Recent: 'Saylor hints at buying BTC'  |  Incoming: 'MicroStrategy confirms purchase of 10,000 BTC' (same actor but NEW specific detail: confirmed amount)\n"
    "  - Recent: 'Fed raises rates'            |  Incoming: 'Bitcoin drops 5% after rate decision' (market reaction, different event)\n"
    "BE CAREFUL: Different sources reporting DIFFERENT ANGLES or FOLLOW-UP DETAILS of the same general topic are NOT duplicates.\n\n"

    "CRITICAL RULE 2 (SPAM FILTER): Output ONLY the word SKIP if the message contains NO actual news or factual information whatsoever — "
    "for example a pure giveaway announcement, a pure 'subscribe to our channel' call-to-action with no news, or a pure paid advertisement. "
    "DO NOT skip a message for any of the following reasons:\n"
    "  • The message starts or ends with the channel's own name or Telegram/Twitter handle.\n"
    "  • The message contains URLs or links (they will be removed in the rewrite step).\n"
    "  • The message mentions any company, project, token, protocol, exchange, government body, or public figure by name.\n"
    "  • The message is written in Turkish, Arabic, or another language (translate it instead).\n"
    "RULE: If there is at least ONE factual claim — a price, an event, a decision, a statement by a person or organisation — it is news. Rewrite it.\n\n"

    "STEP 3 — THE REWRITE (only if not DUPLICATE and not SKIP):\n"
    "1. FORMATTING: Start with exactly one of these HTML tags:\n"
    "     a) <b>🚨 JUST IN:</b> — for new, timely developments and unexpected announcements\n"
    "     b) <b>⚡ BREAKING:</b> — for major, high-impact events that shift market sentiment\n"
    "     c) <b>📊 MARKET ALERT:</b> — for price action, technical indicators, or on-chain data\n"
    "2. LENGTH: MAXIMUM 1-2 sentences summarising the news (NOT TOO LONG).\n"
    "3. AI INSIGHT: MAXIMUM ONLY 1 sentences of professional analysis. No headers or labels — write it as a direct follow-up paragraph.\n"
    "Give a SHARP, OPINIONATED read. Sound like a real analyst who's been watching markets for years — not a PR bot.\n"
    "  RULES for the take:\n"
    "  • Take a position. Be concrete. If something smells off, say so.\n"
    "  • Use pattern recognition, timing analysis, or counter-consensus reads.\n"
    "  • GOOD examples:\n"
    "      'On-chain shows accumulation started 72h before this broke — someone knew.'\n"
    "      'Suspicious timing. Miner reserves just hit a 2-year low — forced selling likely follows.'\n"
    "      'Classic headline pump. The last 4 times this pattern played out, price faded 8-15% within 72h.'\n"
    "      'The fine print matters more than the headline here — read clause 4(b) before celebrating.'\n"
    "      'They needed a catalyst to reset positioning after 6 weeks of ETF outflows. Here it is.'\n"
    "  • FORBIDDEN language: 'may', 'could potentially', 'might', 'it is worth noting', 'significant development', "
    "'this could be bullish', 'this is significant'. No hedging. No PR speak.\n\n"
    "4. DATA INTEGRITY: Keep all numbers, prices, and percentages IDENTICAL to the source.\n"
    "5. CLEANING: Remove ALL URLs and source citations.\n\n"

    "RECENTLY PUBLISHED STORIES (last {window_hours} hours only — compare against these):\n{recent_stories}\n\n"
    "INCOMING NEWS:\n{incoming_news}"
)

MARKET_INSIGHT_PROMPT = (
    "You are an analyst at @Ledgexs writing a Telegram market update. Sharp, direct, authentic.\n\n"
    "CRITICAL DATA RULE: Use EXACT numbers from the DATA PROVIDED. No rounding, no approximating. "
    "If the data is missing a key figure, output SKIP instead of inventing it.\n\n"
    "FORMAT RULES:\n"
    "1. HEADLINE: <b>📊 $SYMBOL — [KEY LEVEL OR OBSERVATION]</b> — e.g. <b>📊 $BTC — Testing $107K</b>.\n"
    "   HTML <b> tags. No ** asterisks. No vague action words ('Surges!', 'Pumps!').\n"
    "2. BODY (3 sentences max):\n"
    "   • Sentence 1: Price + 24h change + volume — exact numbers, no approximation.\n"
    "   • Sentence 2: Key levels — use the 24h high/low as immediate support/resistance.\n"
    "   • Sentence 3: Your actual read on momentum — why bullish or bearish, what confirms it.\n"
    "     Write like someone with a position: 'The bounce off $X was clean — bulls are defending.'\n"
    "     Not like a press release: 'Momentum appears to be trending positively at this time.'\n"
    "3. FORMATTING: HTML only (<b>, <i>). ZERO markdown. No bullet points, no headers.\n"
    "4. CASHTAG: Use the coin's $SYMBOL exactly ONCE — in the headline only.\n"
    "5. SPACING: Blank line (\\n\\n) between headline and body.\n"
    "6. LENGTH: Telegram-friendly — dense but readable. No padding.\n\n"
    "EXAMPLE (BTC at $107,200):\n"
    "<b>📊 $BTC — Holding $107K Support</b>\n\n"
    "BTC trades at <b>$107,200</b> (+2.4% / 24h) on $38B volume. "
    "24h range: $105,200 support / $109,500 resistance. "
    "Price reclaimed the VWAP after the morning flush — bulls are in control short-term, but $109.5K is a wall.\n\n"
    "DATA PROVIDED:\n{data}"
)


# ── Env vars ──────────────────────────────────────────────────────────────────

_BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
_API_ID      = os.environ.get("TELEGRAM_API_ID", "")
_API_HASH    = os.environ.get("TELEGRAM_API_HASH", "")
_SESSION_STR = os.environ.get("TELETHON_SESSION", "")
_AI_BASE_URL = "https://api.openai.com/v1"
_AI_API_KEY  = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY", "dummy")

# ── Optional-import guards ────────────────────────────────────────────────────

try:
    from telethon import TelegramClient, events     # type: ignore
    from telethon.sessions import StringSession     # type: ignore
    _TELETHON_OK = True
except ImportError:
    _TELETHON_OK = False
    logger.warning("news_aggregator: telethon not installed — module disabled.")

try:
    from openai import OpenAI as _OpenAI            # type: ignore
    _OPENAI_OK = True
except ImportError:
    _OPENAI_OK = False
    logger.warning("news_aggregator: openai package not installed — AI rewrite disabled.")

try:
    import tweepy as _tweepy                        # type: ignore
    _TWEEPY_OK = True
except ImportError:
    _TWEEPY_OK = False

try:
    import requests as _requests                    # type: ignore
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False

# ── Shared OpenAI client ──────────────────────────────────────────────────────

def _make_ai_client() -> Any:
    if not _OPENAI_OK or not _AI_BASE_URL:
        return None
    try:
        return _OpenAI(base_url=_AI_BASE_URL, api_key=_AI_API_KEY)
    except Exception as exc:
        logger.warning("news_aggregator: AI client init failed: %s", exc)
        return None

_ai_client: Any = _make_ai_client()

# ── Twitter clients (v2 for tweets, v1.1 for media upload) ───────────────────

def _build_twitter_clients() -> tuple[Any, Any]:
    if not _TWEEPY_OK:
        return None, None
    keys = (
        os.environ.get("TWITTER_API_KEY", ""),
        os.environ.get("TWITTER_API_SECRET", ""),
        os.environ.get("TWITTER_ACCESS_TOKEN", ""),
        os.environ.get("TWITTER_ACCESS_SECRET", ""),
    )
    if not all(keys):
        return None, None
    try:
        client_v2 = _tweepy.Client(
            consumer_key=keys[0],
            consumer_secret=keys[1],
            access_token=keys[2],
            access_token_secret=keys[3],
        )
        auth = _tweepy.OAuth1UserHandler(*keys)
        api_v1 = _tweepy.API(auth)
        return client_v2, api_v1
    except Exception as exc:
        logger.warning("news_aggregator: Twitter client init failed: %s", exc)
        return None, None

_twitter_v2: Any
_twitter_v1: Any
_twitter_v2, _twitter_v1 = _build_twitter_clients()


def _verify_twitter_credentials() -> None:
    """Check at startup that all four Twitter secrets are present and accepted.

    Uses the cheapest possible v1.1 call (verify_credentials) so we surface
    bad keys immediately in the log instead of silently failing on every post.
    Failures are non-fatal — Twitter posting is just disabled.
    """
    if _twitter_v1 is None:
        missing = [k for k in (
            "TWITTER_API_KEY", "TWITTER_API_SECRET",
            "TWITTER_ACCESS_TOKEN", "TWITTER_ACCESS_SECRET",
        ) if not os.environ.get(k)]
        if missing:
            logger.warning(
                "news_aggregator: Twitter disabled — missing secrets: %s",
                ", ".join(missing),
            )
        else:
            logger.warning("news_aggregator: Twitter disabled — tweepy unavailable.")
        return
    try:
        me = _twitter_v1.verify_credentials()
        logger.info(
            "news_aggregator: Twitter credentials OK — authenticated as @%s.",
            getattr(me, "screen_name", "?"),
        )
    except Exception as exc:
        logger.warning(
            "news_aggregator: Twitter credentials INVALID (%s) — "
            "check TWITTER_API_KEY / TWITTER_API_SECRET / TWITTER_ACCESS_TOKEN / TWITTER_ACCESS_SECRET.",
            exc,
        )


_verify_twitter_credentials()

# ── Deduplication cache ───────────────────────────────────────────────────────
# Each entry is (unix_timestamp, summary_text) so we can discard entries
# older than DEDUP_WINDOW_HOURS when building the AI prompt context.

_dedup_cache: deque[tuple[float, str]] = deque(maxlen=DEDUP_CACHE_SIZE)
_dedup_lock  = threading.Lock()

# Pre-AI fingerprint cache: (unix_timestamp, frozenset_of_words).
# Catches near-exact duplicates (Jaccard ≥ FINGERPRINT_SIM_THRESHOLD)
# without spending an API call.
_fingerprint_cache: deque[tuple[float, frozenset]] = deque(maxlen=200)

# Serialises AI dedup calls so concurrent messages never race past an empty cache.
# Must be acquired BEFORE the AI call and released AFTER _cache_add() so the
# second message always sees the first message's result in the cache.
_dedup_processing_lock: asyncio.Lock | None = None


def _get_dedup_lock() -> asyncio.Lock:
    global _dedup_processing_lock
    if _dedup_processing_lock is None:
        _dedup_processing_lock = asyncio.Lock()
    return _dedup_processing_lock


def _make_fingerprint(text: str) -> frozenset:
    """Normalise text and return a frozenset of its first FINGERPRINT_WORDS words.

    Used for fast Jaccard-based pre-dedup before the AI call.
    Words shorter than 3 chars (stopwords, articles) are excluded.
    """
    words = re.sub(r"[^a-z0-9\s]", "", text.lower()).split()
    meaningful = [w for w in words if len(w) >= 3][:FINGERPRINT_WORDS]
    return frozenset(meaningful)


def _is_fingerprint_duplicate(fp: frozenset) -> bool:
    """Return True if fp is too similar to any recently cached fingerprint."""
    if not fp:
        return False
    now = time.time()
    with _dedup_lock:
        for ts, cached_fp in _fingerprint_cache:
            if now - ts > FINGERPRINT_WINDOW_S:
                continue
            if not cached_fp:
                continue
            intersection = len(fp & cached_fp)
            union = len(fp | cached_fp)
            if union > 0 and intersection / union >= FINGERPRINT_SIM_THRESHOLD:
                return True
    return False


def _cache_add(summary: str, raw_fingerprint: frozenset | None = None) -> None:
    """Add a summary to the dedup cache with the current timestamp.

    Also adds the raw fingerprint to the fingerprint cache if provided.
    """
    now = time.time()
    with _dedup_lock:
        _dedup_cache.append((now, summary[:500]))
        if raw_fingerprint:
            _fingerprint_cache.append((now, raw_fingerprint))


# ── AI helpers (sync — always call via run_in_executor from async context) ────

def _ai_dedup_and_rewrite(raw_text: str) -> str | None:
    """Synchronous — must be called via run_in_executor to avoid blocking the event loop."""
    global _ai_client
    if _ai_client is None:
        _ai_client = _make_ai_client()
        if _ai_client is None:
            raise RuntimeError("AI client unavailable")

    cutoff = time.time() - DEDUP_WINDOW_HOURS * 3600
    with _dedup_lock:
        # Only include stories published within the dedup window (last 3 hours).
        # Comparing against 10-hour-old cache entries causes false positives when
        # a related but genuinely new story arrives later in the day.
        recent = [(ts, s) for ts, s in _dedup_cache if ts >= cutoff]

    recent_stories = (
        "\n---\n".join(f"{i+1}. {s}" for i, (ts, s) in enumerate(recent))
        if recent else "No recent stories."
    )

    formatted_prompt = AI_COMBINED_PROMPT.format(
        window_hours=DEDUP_WINDOW_HOURS,
        recent_stories=recent_stories,
        incoming_news=raw_text,
    )

    resp = _ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a professional crypto news editor. "
                    "YOUR ONLY OUTPUT LANGUAGE IS ENGLISH. "
                    "If input is Turkish, translate to English. "
                    "Never output in any other language."
                ),
            },
            {"role": "user", "content": formatted_prompt},
        ],
        temperature=0,
        max_tokens=300,
    )
    result = (resp.choices[0].message.content or "").strip()

    if not result:
        raise RuntimeError("AI returned empty string")

    # FIX: Use regex instead of exact equality so that trailing punctuation,
    # mixed case, or extra whitespace variations ("DUPLICATE.", "Duplicate\n",
    # "SKIP." etc.) are all caught correctly.  Previously "DUPLICATE." slipped
    # through the == check and got posted verbatim as a caption on images.
    _control_word = result.strip().rstrip('.,!? \n').upper()

    if _control_word == "DUPLICATE":
        logger.info("news_aggregator: duplicate detected — skipped.")
        return None

    if _control_word == "SKIP":
        logger.info("news_aggregator: AI marked as spam/skip — discarded.")
        return None

    # Safety guard: a legitimate rewrite always starts with one of the three
    # HTML bold tags defined in the prompt ("<b>🚨", "<b>⚡", "<b>📊").
    # If the result is suspiciously short AND doesn't start with "<b>", it is
    # almost certainly a hallucinated control word variant or garbage output.
    # In that case discard silently rather than posting junk to the channel.
    if not result.startswith("<b>") and len(result) < 40:
        logger.warning(
            "news_aggregator: AI output looks invalid (no <b> tag, %d chars) — discarded: %r",
            len(result), result[:60],
        )
        return None

    logger.info("news_aggregator: AI rewrite OK  %d → %d chars.", len(raw_text), len(result))
    return result


def _sync_gpt_analysis(prompt: str) -> str:
    """Synchronous GPT call — must be called via run_in_executor from async context."""
    global _ai_client
    if _ai_client is None:
        _ai_client = _make_ai_client()
        if _ai_client is None:
            return "AI service unavailable."
    resp = _ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
    )
    return (resp.choices[0].message.content or "").strip()


async def _call_gpt_for_analysis(prompt: str) -> str:
    """Async wrapper — offloads the blocking OpenAI HTTP call to a thread."""
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, _sync_gpt_analysis, prompt)
    except Exception as exc:
        logger.warning("news_aggregator: GPT analysis error: %s", exc)
        return "AI service unavailable."


# ── Market analysis (periodic) ────────────────────────────────────────────────
#
# Data sources (no API key needed, verified to work from Railway/AWS US):
#   1. Bybit v5     — api.bybit.com         — no geo-block, high rate limits
#   2. CoinPaprika  — api.coinpaprika.com   — free tier, global access, market caps
#
# Binance is US-IP geo-blocked on Railway. CoinCap has DNS issues on Railway.
# CoinGecko free tier is rate-limited (30 req/min → 429s). All three avoided.
#
# Bybit USDT spot pairs (symbol → pair string)
_BYBIT_PAIRS: dict[str, str] = {
    "BTC": "BTCUSDT",  "ETH": "ETHUSDT",  "SOL": "SOLUSDT",
    "BNB": "BNBUSDT",  "XRP": "XRPUSDT",  "ADA": "ADAUSDT",
    "DOGE": "DOGEUSDT","AVAX": "AVAXUSDT", "LINK": "LINKUSDT",
    "DOT": "DOTUSDT",  "MATIC": "MATICUSDT","UNI": "UNIUSDT",
    "ATOM": "ATOMUSDT", "LTC": "LTCUSDT",  "TRX": "TRXUSDT",
    "NEAR": "NEARUSDT","FTM": "FTMUSDT",   "ALGO": "ALGOUSDT",
    "SUI": "SUIUSDT",  "APT": "APTUSDT",   "OP": "OPUSDT",
    "ARB": "ARBUSDT",  "INJ": "INJUSDT",   "TIA": "TIAUSDT",
    "SEI": "SEIUSDT",  "WIF": "WIFUSDT",   "BONK": "BONKUSDT",
    "PEPE": "PEPEUSDT","TON": "TONUSDT",   "NOT": "NOTUSDT",
    "FLOKI": "FLOKIUSDT","HYPE": "HYPEUSDT","PENGU": "PENGUUSDT",
    "EIGEN": "EIGENUSDT","ENA": "ENAUSDT", "PENDLE": "PENDLEUSDT",
}

# CoinPaprika coin IDs (symbol → id) for market cap + fallback prices
_COINPAPRIKA_IDS: dict[str, str] = {
    "BTC": "btc-bitcoin",      "ETH": "eth-ethereum",    "SOL": "sol-solana",
    "BNB": "bnb-binance-coin", "XRP": "xrp-xrp",        "ADA": "ada-cardano",
    "DOGE": "doge-dogecoin",   "AVAX": "avax-avalanche", "LINK": "link-chainlink",
    "DOT": "dot-polkadot",     "MATIC": "matic-polygon", "UNI": "uni-uniswap",
    "ATOM": "atom-cosmos",     "LTC": "ltc-litecoin",    "TRX": "trx-tron",
    "NEAR": "near-near-protocol","ALGO": "algo-algorand","SUI": "sui-sui",
    "APT": "apt-aptos",        "OP": "op-optimism",      "ARB": "arb-arbitrum",
    "INJ": "inj-injective",    "TON": "ton-toncoin",     "PEPE": "pepe-pepe",
    "SEI": "sei-sei-network",  "WIF": "wif-dogwifhat",
}


def _bybit_ticker(pair: str) -> dict:
    """Fetch a single Bybit spot ticker. Returns the ticker dict or {}."""
    resp = _requests.get(
        "https://api.bybit.com/v5/market/tickers",
        params={"category": "spot", "symbol": pair},
        timeout=10,
    )
    resp.raise_for_status()
    items = resp.json().get("result", {}).get("list", [])
    return items[0] if items else {}


async def _get_trending_coins() -> list[str]:
    """Fetch top-volatility coins from CoinPaprika top-100.

    Returns the 3 coins with the largest absolute 24h price change —
    most active coins for market analysis posts.
    """
    def _fetch() -> list[str]:
        resp = _requests.get(
            "https://api.coinpaprika.com/v1/tickers",
            params={"limit": 100},
            timeout=12,
        )
        resp.raise_for_status()
        assets = resp.json()
        active = sorted(
            [a for a in assets if a.get("quotes", {}).get("USD", {}).get("percent_change_24h") is not None],
            key=lambda a: abs(float(a["quotes"]["USD"].get("percent_change_24h", 0) or 0)),
            reverse=True,
        )
        return [a["symbol"].upper() for a in active[:3]]

    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _fetch)
    except Exception as exc:
        logger.warning("Trend fetch (CoinPaprika) failed: %s", exc)
        return ["BTC", "ETH", "SOL"]


async def _get_market_data(coin: str) -> str | None:
    """Fetch price/volume/change: Bybit primary → CoinPaprika fallback.

    Both work from Railway (AWS US). Binance is US-IP geo-blocked; CoinGecko
    rate-limits aggressively; CoinCap has DNS issues on Railway — all avoided.
    """
    sym = coin.upper()

    def _fetch_bybit() -> str | None:
        pair = _BYBIT_PAIRS.get(sym)
        if not pair:
            return None
        t = _bybit_ticker(pair)
        if not t:
            return None
        price  = float(t.get("lastPrice", 0) or 0)
        # price24hPcnt is a decimal ratio ("0.0215" = 2.15%)
        change = float(t.get("price24hPcnt", 0) or 0) * 100
        vol    = float(t.get("turnover24h", 0) or 0)   # USDT volume
        high   = float(t.get("highPrice24h", 0) or 0)
        low    = float(t.get("lowPrice24h", 0) or 0)
        return (
            f"Asset: ${sym} | Price: ${price:,.4f} | 24h Change: {change:+.2f}% | "
            f"24h Volume: ${vol:,.0f} | 24h High: ${high:,.4f} | 24h Low: ${low:,.4f}"
        )

    def _fetch_coinpaprika() -> str | None:
        cid  = _COINPAPRIKA_IDS.get(sym)
        if not cid:
            return None
        resp = _requests.get(
            f"https://api.coinpaprika.com/v1/tickers/{cid}",
            timeout=12,
        )
        resp.raise_for_status()
        q      = resp.json().get("quotes", {}).get("USD", {})
        price  = float(q.get("price", 0) or 0)
        change = float(q.get("percent_change_24h", 0) or 0)
        vol    = float(q.get("volume_24h", 0) or 0)
        cap    = float(q.get("market_cap", 0) or 0)
        return (
            f"Asset: ${sym} | Price: ${price:,.4f} | 24h Change: {change:+.2f}% | "
            f"24h Volume: ${vol:,.0f} | Market Cap: ${cap:,.0f}"
        )

    loop = asyncio.get_running_loop()

    # Primary: Bybit
    try:
        result = await loop.run_in_executor(None, _fetch_bybit)
        if result:
            return result
    except Exception as exc:
        logger.debug("Market data: Bybit failed for %s: %s", sym, exc)

    # Fallback: CoinPaprika
    try:
        result = await loop.run_in_executor(None, _fetch_coinpaprika)
        if result:
            return result
    except Exception as exc:
        logger.warning("Market data: CoinPaprika failed for %s: %s", sym, exc)

    return None   # caller skips posting when None


async def _periodic_market_analysis(tg_client: Any) -> None:
    """
    Every 4 hours, picks the #1 trending coin from CoinGecko top-3, generates
    an AI market insight, and posts to Telegram AND Twitter (text-only).

    BUG FIX: sleep was at the TOP of the loop, causing the first post to be
    delayed 4 hours after startup.  It is now at the BOTTOM so the first
    analysis runs ~30 s after the bot connects.
    """
    # Short initial delay so the bot fully connects before the first post.
    await asyncio.sleep(30)
    while True:
        try:
            trending_coins = await _get_trending_coins()
            if not trending_coins:
                logger.warning("periodic_market_analysis: no trending coins returned — retrying in 15 min.")
                await asyncio.sleep(900)
                continue

            # Try each trending coin in order until one returns valid market data.
            # If CoinGecko returns 404 for a coin (delisted / ID mismatch),
            # fall through to the next rather than retrying the same coin endlessly.
            # Final fallback: BTC → ETH → SOL (always available).
            FALLBACK_COINS = ["BTC", "ETH", "SOL"]
            candidates = trending_coins + [c for c in FALLBACK_COINS if c not in trending_coins]

            coin        = None
            market_data = None
            for candidate in candidates:
                data = await _get_market_data(candidate)
                if data is not None:
                    coin        = candidate
                    market_data = data
                    break
                logger.warning(
                    "periodic_market_analysis: $%s returned no data — trying next candidate.", candidate
                )

            if market_data is None:
                logger.warning(
                    "periodic_market_analysis: all candidates failed — skipping this round."
                )
                await asyncio.sleep(900)
                continue

            logger.info("periodic_market_analysis: posting analysis for $%s", coin)
            prompt      = MARKET_INSIGHT_PROMPT.format(data=market_data)
            raw_text    = await _call_gpt_for_analysis(prompt)

            # Strip any stray ** markdown the AI may have produced
            analysis_text = _ASTERISK_RE.sub("", raw_text).strip()

            # If AI says SKIP (no usable data), don't post
            if analysis_text.strip().upper() == "SKIP":
                logger.warning(
                    "periodic_market_analysis: AI returned SKIP for $%s "
                    "— not posting.", coin
                )
                await asyncio.sleep(900)
                continue

            loop = asyncio.get_running_loop()

            # ── Telegram ─────────────────────────────────────────────────────
            await loop.run_in_executor(None, _post_to_telegram, analysis_text, [])

            # Market analysis posts to Telegram only — not cross-posted to X.

            logger.info("periodic_market_analysis: done — sleeping 4 h.")

        except Exception as e:
            logger.warning("periodic_market_analysis error: %s", e)

        # Sleep AFTER posting so the first run happens immediately at startup.
        await asyncio.sleep(14400)   # 4-hour cadence


# ── Text helpers ──────────────────────────────────────────────────────────────

_HTML_TAG_RE     = re.compile(r"<[^>]+>")
_MULTI_SPACES_RE = re.compile(r"[ \t]{2,}")
_MULTI_NL_RE     = re.compile(r"\n{3,}")


def _strip_html(text: str) -> str:
    clean = _HTML_TAG_RE.sub("", text)
    clean = _MULTI_SPACES_RE.sub(" ", clean)
    clean = _MULTI_NL_RE.sub("\n\n", clean)
    return clean.strip()


def _remove_hashtags(text: str) -> str:
    text = re.sub(r'#\w+', '', text)
    return text.strip()


def _clean_text(text: str) -> str:
    text = re.sub(r'https?://\S+|www\.\S+', ' ', text)
    text = re.sub(r'\[.*?\]\([^)]*\)?', ' ', text)
    text = re.sub(r'(?<!\d)@\w+', ' ', text)
    text = _WS_RE.sub(" ", text).strip()
    return text


def _fallback_rewrite(raw_text: str) -> str:
    """AI hatası durumunda haberi kaybetmemek için zorunlu temizlik."""
    clean = _clean_text(raw_text)
    return f"<b>MARKET ALERT:</b> {clean[:250]} (Translated to English automatically)"


# ── Fallback cleaning patterns ────────────────────────────────────────────────

_URL_RE       = re.compile(r"https?://\S+|www\.\S+")
_MENTION_RE   = re.compile(r"@\w+")
_MD_LINK_RE   = re.compile(r"\[.*?\]\([^)]*\)?")
_ASTERISK_RE  = re.compile(r"\*{1,4}")
_PAREN_RE     = re.compile(
    r"\(\s*(?:Twitter|X|Bloomberg|Reuters|WSJ|FT|CNBC|Forbes|BBC)\s*/?\w*\s*\)",
    re.IGNORECASE,
)
_SOURCE_RE    = re.compile(
    r"\b(cointelegraph|coindesk|watcherguru|watcher\s*guru|ninjanews|ninja\s*news|"
    r"unfolded|fin_?watch|bitcoinmagazine|bitcoin\s*magazine|decrypt|theblock|"
    r"blockworks|cryptoslate|cryptopotato)\b",
    re.IGNORECASE,
)
_SENTENCE_SEP = re.compile(r"(?<=[.!?])\s+")

# ── Media helpers ─────────────────────────────────────────────────────────────

def _ensure_media_dir() -> None:
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)


def _cleanup_media_dir() -> None:
    try:
        if MEDIA_DIR.exists():
            shutil.rmtree(str(MEDIA_DIR))
            MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.debug("news_aggregator: media cleanup error: %s", exc)

# ── Telegram poster (sync — call via run_in_executor) ─────────────────────────

def _post_to_telegram(tg_text: str, media_paths: list[str]) -> None:
    if not _BOT_TOKEN or not _REQUESTS_OK:
        return

    base    = f"https://api.telegram.org/bot{_BOT_TOKEN}"
    caption = f"{tg_text}{TELEGRAM_SIG}"

    try:
        if not media_paths:
            resp = _requests.post(
                f"{base}/sendMessage",
                json={
                    "chat_id": DEST_CHANNEL,
                    "text": caption,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                    "disable_notification": True,
                },
                timeout=15,
            )

        elif len(media_paths) == 1:
            file_size = os.path.getsize(media_paths[0])
            if file_size > 10_485_760:
                logger.warning(
                    "news_aggregator: Single photo too large (%d bytes), sending text only.",
                    file_size,
                )
                resp = _requests.post(
                    f"{base}/sendMessage",
                    json={
                        "chat_id": DEST_CHANNEL,
                        "text": caption,
                        "parse_mode": "HTML",
                        "disable_notification": True,
                        "disable_web_page_preview": True,
                    },
                    timeout=15,
                )
            else:
                with open(media_paths[0], "rb") as fh:
                    resp = _requests.post(
                        f"{base}/sendPhoto",
                        data={
                            "chat_id": DEST_CHANNEL,
                            "caption": caption,
                            "disable_notification": True,
                            "parse_mode": "HTML",
                        },
                        files={"photo": fh},
                        timeout=30,
                    )

        else:
            paths      = media_paths[:TG_MAX_MEDIA]
            total_size = sum(os.path.getsize(p) for p in paths)

            if total_size > 50_000_000:
                logger.warning(
                    "news_aggregator: Album too large (%d bytes), sending text only.",
                    total_size,
                )
                resp = _requests.post(
                    f"{base}/sendMessage",
                    json={
                        "chat_id": DEST_CHANNEL,
                        "text": caption,
                        "parse_mode": "HTML",
                        "disable_notification": True,
                        "disable_web_page_preview": True,
                    },
                    timeout=15,
                )
            else:
                media_json: list[dict] = []
                files: dict[str, Any]  = {}
                for i, p in enumerate(paths):
                    key        = f"photo{i}"
                    files[key] = open(p, "rb")
                    item: dict[str, Any] = {"type": "photo", "media": f"attach://{key}"}
                    if i == 0:
                        item["caption"]    = caption
                        item["parse_mode"] = "HTML"
                    media_json.append(item)

                resp = _requests.post(
                    f"{base}/sendMediaGroup",
                    data={
                        "chat_id": DEST_CHANNEL,
                        "media": json.dumps(media_json),
                        "disable_notification": True,
                    },
                    files=files,
                    timeout=45,
                )
                for fh in files.values():
                    fh.close()

        if resp.ok:
            logger.info(
                "news_aggregator: Posted to %s (%d media).", DEST_CHANNEL, len(media_paths)
            )
        else:
            logger.warning(
                "news_aggregator: Telegram post failed %s: %s",
                resp.status_code,
                resp.text[:300],
            )
            if media_paths:
                _requests.post(
                    f"{base}/sendMessage",
                    json={
                        "chat_id": DEST_CHANNEL,
                        "text": caption,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                        "disable_notification": True,
                    },
                    timeout=15,
                )
    except Exception as exc:
        logger.warning("news_aggregator: Telegram post error: %s", exc)


# ── Twitter / X poster (sync — call via run_in_executor) ─────────────────────

def _post_to_twitter(rewritten_text: str, media_paths: list[str]) -> None:
    """Post news to X/Twitter — news summary only, AI commentary stripped (Premium account)."""
    if _twitter_v2 is None:
        return

    # AI rewrite format: first paragraph = news summary, second = AI commentary.
    # Twitter gets only the first paragraph — no AI opinion.
    plain = _strip_html(rewritten_text)
    plain = plain.split("\n\n")[0].strip()

    # ── Media upload ─────────────────────────────────────────────────────────
    media_ids: list[int] = []
    if media_paths and _twitter_v1 is not None:
        for path in media_paths[:TWITTER_MAX_MEDIA]:
            try:
                media = _twitter_v1.media_upload(filename=path)
                media_ids.append(media.media_id)
                logger.debug("news_aggregator: Twitter media uploaded: %s", media.media_id)
            except Exception as exc:
                logger.warning("news_aggregator: Twitter media upload failed: %s", exc)

    # ── Post tweet ───────────────────────────────────────────────────────────
    try:
        kwargs: dict[str, Any] = {"text": plain, "user_auth": True}
        if media_ids:
            kwargs["media_ids"] = media_ids
        _twitter_v2.create_tweet(**kwargs)
        logger.info("news_aggregator: X post OK (%d chars, media=%d).", len(plain), len(media_ids))
    except Exception as exc:
        logger.warning("news_aggregator: X post failed: %s", exc)


# ── Core news handler ─────────────────────────────────────────────────────────

async def _handle_news(
    raw_text: str,
    messages: list[Any],
    tg_client: Any,
    source_username: str,
) -> None:
    # 0. Temel filtreleme
    if len(raw_text) < 20:
        logger.info("Ignored short/invalid news from %s", source_username)
        return

    loop = asyncio.get_running_loop()

    # --- PRE-DEDUP: fast Jaccard fingerprint check (no AI cost) ---
    # Build a normalised word-set from the raw text and compare it against
    # recently cached fingerprints.  If the similarity is above the threshold,
    # we know the message is near-identical to something we already processed
    # within the last FINGERPRINT_WINDOW_S seconds — skip immediately.
    # This is the FIRST line of defence; AI dedup is the second.
    raw_fp = _make_fingerprint(raw_text)
    if _is_fingerprint_duplicate(raw_fp):
        logger.info(
            "news_aggregator: fingerprint duplicate from @%s — skipped before AI.",
            source_username,
        )
        return

    # 1. AI Rewrite ve Duplicate Kontrolü — SERIALIZED via _dedup_processing_lock.
    #
    # WHY: _handle_news runs concurrently for every incoming message.  Without a
    # lock, three messages about the same event can all reach run_in_executor at
    # the same instant, read an identical (empty) cache snapshot, all pass the
    # duplicate check, and all get posted.  The lock forces sequential processing
    # so each message sees the updated cache from the previous one.
    #
    # _cache_add() is called INSIDE the lock (immediately after AI approval) so
    # the next waiter finds the result in the cache before it even starts its AI
    # call.  Posting to Telegram / Twitter happens OUTSIDE the lock to avoid
    # holding it during slow HTTP calls.
    rewritten: str | None = None
    async with _get_dedup_lock():
        # Re-check fingerprint inside the lock as well — another coroutine may have
        # processed the same story while we were waiting to acquire the lock.
        if _is_fingerprint_duplicate(raw_fp):
            logger.info(
                "news_aggregator: fingerprint duplicate (post-lock) from @%s — skipped.",
                source_username,
            )
            return

        try:
            rewritten = await loop.run_in_executor(None, _ai_dedup_and_rewrite, raw_text)
            if rewritten is None:  # Duplicate or SKIP detected by AI
                return
        except Exception as exc:
            logger.warning("news_aggregator: AI failed (%s) — using fallback.", exc)
            rewritten = _fallback_rewrite(raw_text)
            if not rewritten:
                return

        # 2. SKIP / stray control-word guard (second line of defence)
        _cw = rewritten.strip().rstrip('.,!? \n').upper()
        if _cw in ("SKIP", "DUPLICATE"):
            logger.info("Skipping control-word output from %s: %r", source_username, rewritten[:30])
            return

        # Add to cache NOW — while still holding the lock — so the next
        # concurrent message always sees this result before it calls the AI.
        # Also store the raw fingerprint so future near-exact messages are caught
        # by the fast fingerprint check without an API call.
        _cache_add(_strip_html(rewritten), raw_fingerprint=raw_fp)

    # 3. Medyayı indir (lock dışında — yavaş I/O, dedup'u etkilemez)
    media_paths: list[str] = []
    _ensure_media_dir()
    for msg in messages:
        if msg.media is None:
            continue
        try:
            path = await tg_client.download_media(msg, file=str(MEDIA_DIR) + "/")
            if path:
                media_paths.append(path)
        except Exception as exc:
            logger.debug("news_aggregator: media download failed: %s", exc)

    # 4. Paylaşım (lock dışında)
    try:
        clean_tg_text = _remove_hashtags(rewritten)

        # Blocking HTTP calls offloaded to thread pool so event loop stays free
        await loop.run_in_executor(None, _post_to_telegram, clean_tg_text, media_paths)

        # Mirror to Twitter: same text + image as Telegram, no extra processing.
        await loop.run_in_executor(None, _post_to_twitter, rewritten, media_paths)

    finally:
        _cleanup_media_dir()


# ── Telethon async client ─────────────────────────────────────────────────────

async def _run_news_client() -> None:
    if not _TELETHON_OK or not all([_API_ID, _API_HASH, _SESSION_STR]):
        return

    client = TelegramClient(
        StringSession(_SESSION_STR),
        int(_API_ID),
        _API_HASH,
        connection_retries=None,
        retry_delay=5,
        auto_reconnect=True,
    )

    pending_groups: dict[int, list[Any]]     = {}
    pending_tasks:  dict[int, asyncio.Task]  = {}

    # ── Channel resolution ────────────────────────────────────────────────────
    # Resolve all SOURCE_CHANNELS to their numeric Telegram entity IDs at startup.
    # This is the DEFINITIVE fix for @coingraphnews and similar channels that are
    # silently missed when we rely on chat.username alone:
    #
    # Problem 1: chat.username is None for channels/megagroups that have no public
    #   username (some CoingraphNews sub-accounts fall into this category).
    # Problem 2: Telethon returns None for chat.username when the entity has not
    #   been previously cached by the session — even for channels that DO have a
    #   username.
    # Problem 3: If the Telegram account behind the session has NOT joined a channel
    #   it will receive zero updates from it, regardless of any filter. We therefore
    #   attempt to join each unresolved channel and log clearly if that fails.
    #
    # The resolved numeric IDs are stored in `_source_entity_ids`.
    # The event handler accepts a message if:
    #   abs(event.chat_id) in _source_entity_ids        ← primary (always works)
    #   OR chat.username.lower() in _SOURCE_USERNAMES   ← fallback for new channels
    #
    # Both username AND id are accepted so that newly added channels work before
    # the bot is restarted.
    _source_entity_ids: set[int] = set()

    async def _resolve_source_channels() -> None:
        """Resolve every SOURCE_CHANNEL to its numeric entity ID, joining if needed.

        WHY auto-join: Telethon only delivers updates for channels the session
        account has physically joined.  If the account is not a member of a
        channel it will never receive messages from it regardless of filters.
        We call JoinChannelRequest for each channel that wasn't already joined.
        UserAlreadyParticipantError is silently ignored (already a member).
        """
        from telethon.tl.functions.channels import JoinChannelRequest   # type: ignore
        from telethon.errors import UserAlreadyParticipantError          # type: ignore

        for ch in SOURCE_CHANNELS:
            try:
                entity = await client.get_entity(ch)
                eid = getattr(entity, "id", None)
                if eid is not None:
                    _source_entity_ids.add(abs(int(eid)))
                    logger.info("news_aggregator: resolved %-40s → entity_id=%d", ch, abs(int(eid)))
                else:
                    logger.warning("news_aggregator: could not get id for %s", ch)
                    continue

                # Auto-join so the session receives updates from this channel.
                try:
                    await client(JoinChannelRequest(entity))
                    logger.info("news_aggregator: joined %s", ch)
                except UserAlreadyParticipantError:
                    pass   # already a member — no action needed
                except Exception as join_exc:
                    logger.warning("news_aggregator: could not join %s: %s", ch, join_exc)

            except Exception as exc:
                logger.warning(
                    "news_aggregator: cannot resolve %s (%s) — "
                    "will still attempt username-based matching.",
                    ch, exc,
                )

        logger.info(
            "news_aggregator: %d/%d source channels resolved by entity ID.",
            len(_source_entity_ids), len(SOURCE_CHANNELS),
        )

    async def _process_group(grouped_id: int, username: str) -> None:
        await asyncio.sleep(GROUP_COLLECT_S)
        msgs = pending_groups.pop(grouped_id, [])
        pending_tasks.pop(grouped_id, None)
        if not msgs:
            return
        raw = next((m.text for m in msgs if m.text), "").strip()
        try:
            await _handle_news(raw, msgs, client, username)
        except Exception as exc:
            logger.warning("news_aggregator: group handler error: %s", exc)

    # Listen to ALL new messages — do NOT pass chats= to the decorator.
    # Telethon resolves the chats= list at registration time; if any channel
    # fails to resolve (network hiccup, not yet in session cache) it is silently
    # dropped and its events NEVER fire.  Manual filtering below is safer.
    @client.on(events.NewMessage())
    async def _on_new_message(event: events.NewMessage.Event) -> None:
        try:
            # ── Primary filter: numeric entity ID ────────────────────────────
            # abs() because Telethon returns negative chat_ids for channels.
            event_chat_id = abs(event.chat_id or 0)

            # ── DIAGNOSTIC: log messages from recognised source channels only.
            # Non-source senders (own bot posts, DMs, unrelated groups) are
            # silently skipped here — they generate no log line at all.
            # Change logger.debug → logger.info temporarily if you need to
            # audit which channels Telethon is actually seeing.
            try:
                _diag_chat = await event.get_chat()
                _diag_uname = (getattr(_diag_chat, "username", None) or "").lower()
            except Exception:
                _diag_uname = ""
            _in_ids   = event_chat_id in _source_entity_ids
            _in_names = _diag_uname in _SOURCE_USERNAMES
            if _in_ids or _in_names:  # only log genuine source-channel hits
                logger.debug(
                    "TG-IN  id=%-14d  username=%-35s  id_match=%s  name_match=%s",
                    event_chat_id, _diag_uname or "(none)", _in_ids, _in_names,
                )

            # ── Filter: accept if ID or username matches a source channel ─────
            # _diag_uname is already fetched above (diagnostic block).
            username: str = _diag_uname or f"id:{event_chat_id}"
            if event_chat_id not in _source_entity_ids:
                if _diag_uname not in _SOURCE_USERNAMES:
                    return  # not one of our sources — discard

            msg        = event.message
            raw        = (msg.text or "").strip()
            grouped_id = getattr(msg, "grouped_id", None)

            if grouped_id is not None:
                if grouped_id not in pending_groups:
                    pending_groups[grouped_id] = []
                pending_groups[grouped_id].append(msg)
                existing = pending_tasks.get(grouped_id)
                if existing and not existing.done():
                    existing.cancel()
                pending_tasks[grouped_id] = asyncio.create_task(
                    _process_group(grouped_id, username)
                )
            else:
                await _handle_news(raw, [msg], client, username)

        except Exception as exc:
            logger.warning("news_aggregator: event handler error: %s", exc)

    try:
        await client.start()
        logger.info("news_aggregator: Telethon UserBot connected — resolving source channels…")
        # Resolve channels AFTER connect so session cache is populated.
        await _resolve_source_channels()
        asyncio.create_task(_periodic_market_analysis(client))
        logger.info("news_aggregator: all systems running. Listening for news…")
        await client.run_until_disconnected()
    finally:
        for task in pending_tasks.values():
            task.cancel()
        await client.disconnect()


# ── Thread entry-point ────────────────────────────────────────────────────────

def _news_thread_target() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while True:
        try:
            loop.run_until_complete(_run_news_client())
        except Exception as exc:
            logger.warning(
                "news_aggregator: event loop crashed (%s) — restarting in 60 s.", exc
            )
            time.sleep(60)


def start_news_aggregator() -> threading.Thread:
    t = threading.Thread(target=_news_thread_target, daemon=True, name="NewsAggregator")
    t.start()
    logger.info("News aggregator thread started.")
