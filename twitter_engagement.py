"""
twitter_engagement.py
=====================
@Ledgexs Twitter Engagement Engine — completely Telegram-independent.

Runs as an isolated daemon thread alongside the main bot.
Any exception inside is caught and logged — this module can NEVER crash main.py.

Features
--------
1. FEAR & GREED  — daily at UTC 15:00; alternative.me real data + GPT commentary
2. TOP MOVERS    — daily at 09:00 UTC; CoinPaprika top-3 gain/loss narrative

All features degrade gracefully to no-op if API keys are missing.

Env vars used (same as news_aggregator):
  TWITTER_API_KEY, TWITTER_API_SECRET
  TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET
  AI_INTEGRATIONS_OPENAI_API_KEY   (or OPENAI_API_KEY as fallback)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("whale_bot.twitter_engagement")

# ── Timing constants ───────────────────────────────────────────────────────────
FEAR_GREED_UTC_HOUR  = 15    # Fear & Greed tweet hour (UTC)
TOP_MOVERS_HOUR_UTC  = 9     # daily Top Movers post at 09:00 UTC
HTTP_TIMEOUT         = 15    # seconds for external API requests

# ── State file — persists last-replied tweet IDs across Railway restarts ───────
# IMPORTANT: must be on the /data volume (persistent), NOT /tmp (cleared on restart).
_STATE_FILE = "/data/twitter_engagement_state.json"

# ── OpenAI / AI base URL ───────────────────────────────────────────────────────
_AI_API_KEY = (
    os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
    or os.environ.get("OPENAI_API_KEY")
    or "dummy"
)
_AI_BASE_URL = "https://api.openai.com/v1"

# ── GPT prompts ────────────────────────────────────────────────────────────────

_FEAR_GREED_PROMPT_TMPL = (
    "You are an analyst at @Ledgexs who has seen 3 crypto cycles and isn't impressed by either panic or euphoria.\n"
    "Fear & Greed Index: {value}/100 — \"{label}\"\n"
    "7-day readings (oldest→newest): {trend}\n\n"
    "Write ONE tweet. Rules:\n"
    "- Lead with the number and trend direction — cold, factual.\n"
    "- Then give your actual read: what does this level historically lead to? Be specific and honest.\n"
    "  Don't hedge with 'may' or 'could'. If the historical record is mixed, say it's mixed and why.\n"
    "  Example: 'Every sub-20 reading in 2022-2023 was a buying opportunity within 3 weeks. 2018 was different — 6 months of pain followed.'\n"
    "- If sentiment is shifting, call the direction. If it's not confirmed, say what would confirm it.\n"
    "- Sound like a human who has real money on the line — not a disclaimer machine.\n"
    "- MAX 240 characters. Tight prose. 1-2 emojis only.\n"
    "- FORBIDDEN: hashtags, cashtags ($SYMBOL — write BTC not $BTC), 'NFA', 'DYOR', 'to the moon'.\n"
    "Output ONLY the tweet text."
)

_TOP_MOVERS_PROMPT_TMPL = (
    "You are an analyst at @Ledgexs. The data header already shows the names and numbers.\n"
    "24H market data:\n"
    "TOP GAINERS:\n{gainers}\n\n"
    "TOP LOSERS:\n{losers}\n\n"
    "Write ONE insight — NOT a recap of what's already in the header. Rules:\n"
    "- What's the structural story behind this divergence? Sector rotation, macro trigger, narrative flip, leverage cleanup? Name it.\n"
    "- If a gain smells like distribution (thin volume, no real catalyst, post-parabolic), say so directly.\n"
    "- If a dump has a clear cause, say the cause. If it's a liquidity cascade, name that too.\n"
    "- Write like someone who's seen this pattern before and has an opinion — not a neutral observer.\n"
    "- MAX 180 characters. One clean take. 1 emoji max.\n"
    "- FORBIDDEN: hashtags, cashtags ($SYMBOL — write BTC not $BTC), 'NFA'.\n"
    "Output ONLY the insight text."
)

# ── Client initialisation ──────────────────────────────────────────────────────

def _build_clients() -> tuple[Any, Any, Any]:
    """Returns (_twitter_v1, _twitter_v2, _openai_client). Any may be None."""
    tw_v1 = None
    tw_v2 = None
    ai    = None

    try:
        import tweepy  # type: ignore
        keys = (
            os.environ.get("TWITTER_API_KEY", ""),
            os.environ.get("TWITTER_API_SECRET", ""),
            os.environ.get("TWITTER_ACCESS_TOKEN", ""),
            os.environ.get("TWITTER_ACCESS_SECRET", ""),
        )
        if all(keys):
            auth  = tweepy.OAuth1UserHandler(*keys)
            tw_v1 = tweepy.API(auth)
            tw_v2 = tweepy.Client(
                bearer_token=os.environ.get("TWITTER_BEARER_TOKEN", ""),
                consumer_key=keys[0],
                consumer_secret=keys[1],
                access_token=keys[2],
                access_token_secret=keys[3],
                wait_on_rate_limit=False,
            )
            logger.info("twitter_engagement: Twitter clients initialised.")
        else:
            logger.warning("twitter_engagement: Twitter keys incomplete — Twitter disabled.")
    except ImportError:
        logger.warning("twitter_engagement: tweepy not installed.")
    except Exception as exc:
        logger.warning("twitter_engagement: Twitter client init failed: %s", exc)

    try:
        from openai import OpenAI  # type: ignore
        ai = OpenAI(api_key=_AI_API_KEY, base_url=_AI_BASE_URL)
        logger.info("twitter_engagement: OpenAI client initialised.")
    except ImportError:
        logger.warning("twitter_engagement: openai package not installed.")
    except Exception as exc:
        logger.warning("twitter_engagement: OpenAI init failed: %s", exc)

    return tw_v1, tw_v2, ai


_twitter_v1: Any
_twitter_v2: Any
_openai_client: Any
_twitter_v1, _twitter_v2, _openai_client = _build_clients()

# ── Persistent state ───────────────────────────────────────────────────────────

def _load_state() -> dict:
    try:
        with open(_STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    try:
        with open(_STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as exc:
        logger.warning("twitter_engagement: could not save state: %s", exc)


# In-memory state; hydrated from disk at startup.
_state: dict = _load_state()
_state.setdefault("fear_greed_last_date", "")
_state.setdefault("top_movers_last_date", "")

# ── Shared helpers ─────────────────────────────────────────────────────────────

def _gpt(prompt: str, max_tokens: int = 200, temperature: float = 0.75) -> str:
    """Synchronous GPT-4o-mini call. Returns empty string on failure."""
    if _openai_client is None:
        return ""
    try:
        resp = _openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.warning("twitter_engagement: GPT call failed: %s", exc)
        return ""


def _post_tweet(text: str) -> str | None:
    """Post a single tweet via v2. Returns tweet ID on success, None on failure."""
    if _twitter_v2 is None:
        return None
    try:
        resp = _twitter_v2.create_tweet(text=text[:280], user_auth=True)
        tid  = resp.data.get("id") if resp and resp.data else None
        if tid:
            logger.info("twitter_engagement: tweeted [%s…]", text[:60])
        return tid
    except Exception as exc:
        logger.warning("twitter_engagement: post_tweet failed: %s", exc)
        return None


def _safe_get(url: str, params: dict | None = None) -> dict | list | None:
    """GET request with timeout; returns parsed JSON or None."""
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.warning("twitter_engagement: HTTP GET %s failed: %s", url, exc)
        return None


# ── Feature 1: Fear & Greed Index ─────────────────────────────────────────────

async def _fear_and_greed_loop() -> None:
    """Once daily at UTC 15:00: fetch Fear & Greed Index, generate AI commentary, tweet."""
    await asyncio.sleep(120)  # let the bot fully start first
    logger.info("twitter_engagement: fear_and_greed_loop started.")

    while True:
        now_utc    = datetime.now(timezone.utc)
        today_str  = now_utc.strftime("%Y-%m-%d")
        last_date  = _state.get("fear_greed_last_date", "")

        if now_utc.hour >= FEAR_GREED_UTC_HOUR and last_date != today_str:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _post_fear_greed)
        else:
            mins_left = max(0, (FEAR_GREED_UTC_HOUR - now_utc.hour) * 60 - now_utc.minute)
            logger.debug(
                "twitter_engagement: fear_greed — posted today: %s, next window in ~%d min.",
                last_date == today_str, mins_left,
            )

        await asyncio.sleep(900)  # check every 15 min


def _post_fear_greed() -> None:
    """Synchronous: fetch Alternative.me F&G data and tweet."""
    data = _safe_get("https://api.alternative.me/fng/", params={"limit": 7})
    if not data or "data" not in data:
        logger.warning("twitter_engagement: F&G API returned no data.")
        return

    entries = data["data"]
    today   = entries[0]
    value   = int(today.get("value", 0))
    label   = today.get("value_classification", "Unknown")

    # Build 7-day trend (oldest → newest)
    trend_parts = [str(int(e.get("value", 0))) for e in reversed(entries[1:])]
    trend = " → ".join(trend_parts) + f" → {value}"

    prompt = _FEAR_GREED_PROMPT_TMPL.format(value=value, label=label, trend=trend)
    # GPT is instructed to keep under 220 chars; add header on top
    body = _gpt(prompt, max_tokens=160)
    if not body:
        return

    # Emoji header based on zone
    if value <= 25:
        icon = "😱"
    elif value <= 45:
        icon = "😰"
    elif value <= 55:
        icon = "😐"
    elif value <= 75:
        icon = "😏"
    else:
        icon = "🤑"

    full_tweet = f"{icon} Fear & Greed: {value}/100 — {label}\n\n{body}"
    # Safety truncate without "..." — GPT should have kept it short already
    full_tweet = full_tweet[:280]

    tid = _post_tweet(full_tweet)
    if tid:
        _state["fear_greed_last_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        _save_state(_state)


# ── Feature 3: Daily Top Movers ───────────────────────────────────────────────

async def _top_movers_loop() -> None:
    """At 09:00 UTC daily: fetch CoinGecko top gainers + losers, tweet."""
    await asyncio.sleep(300)  # 5 min initial delay
    logger.info("twitter_engagement: top_movers_loop started.")

    while True:
        now_utc  = datetime.now(timezone.utc)
        today_str = now_utc.strftime("%Y-%m-%d")
        already_posted = _state.get("top_movers_last_date", "") == today_str

        if not already_posted and now_utc.hour >= TOP_MOVERS_HOUR_UTC:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _post_top_movers, today_str)

        await asyncio.sleep(1800)  # check every 30 min


def _post_top_movers(today_str: str) -> None:
    """Fetch top gainers/losers from CoinPaprika top-100 (no API key, works from Railway)."""
    # CoinPaprika /v1/tickers returns top-100 market-cap ranked coins with 24h change
    data = _safe_get("https://api.coinpaprika.com/v1/tickers", params={"limit": 100})
    if not data or not isinstance(data, list):
        logger.warning("twitter_engagement: CoinPaprika top movers returned no data.")
        return

    stablecoins = {"usdt", "usdc", "busd", "dai", "tusd", "frax", "usdp", "usdd", "gusd", "fdusd"}

    def _change(a: dict) -> float:
        try:
            return float((a.get("quotes") or {}).get("USD", {}).get("percent_change_24h") or 0)
        except (TypeError, ValueError):
            return 0.0

    def _price(a: dict) -> float:
        try:
            return float((a.get("quotes") or {}).get("USD", {}).get("price") or 0)
        except (TypeError, ValueError):
            return 0.0

    tradeable = [a for a in data if a.get("symbol", "").lower() not in stablecoins]
    sorted_by_change = sorted(tradeable, key=_change)
    losers  = sorted_by_change[:3]
    gainers = sorted_by_change[-3:][::-1]

    def _fmt(coins: list) -> str:
        lines = []
        for c in coins:
            name   = c.get("symbol", "?").upper()
            change = _change(c)
            price  = _price(c)
            lines.append(f"  {name}: {change:+.1f}% @ ${price:,.4g}")
        return "\n".join(lines)

    def _short_fmt(coins: list) -> str:
        return "  ".join(f"{c.get('symbol','?').upper()} {_change(c):+.1f}%" for c in coins)

    gainers_str   = _fmt(gainers)
    losers_str    = _fmt(losers)
    gainers_short = _short_fmt(gainers)
    losers_short  = _short_fmt(losers)

    prompt = _TOP_MOVERS_PROMPT_TMPL.format(gainers=gainers_str, losers=losers_str)
    insight = _gpt(prompt, max_tokens=120)
    if not insight:
        return

    # Structured tweet: emoji header + data + AI insight
    # GPT prompt caps insight at 180 chars; total with header stays under 280
    header = (
        f"📊 24H MOVERS\n"
        f"📈 {gainers_short}\n"
        f"📉 {losers_short}\n\n"
    )
    full = (header + insight)[:280]

    tid = _post_tweet(full)
    if tid:
        _state["top_movers_last_date"] = today_str
        _save_state(_state)


# ── Main async runner ──────────────────────────────────────────────────────────

async def _run_engagement() -> None:
    """Launches Fear & Greed and Top Movers loops as concurrent asyncio tasks."""
    logger.info("twitter_engagement: starting engagement tasks…")
    tasks = [
        asyncio.create_task(_fear_and_greed_loop(), name="fear_and_greed"),
        asyncio.create_task(_top_movers_loop(),      name="top_movers"),
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


# ── Thread entry-point (mirrors news_aggregator pattern) ──────────────────────

def _engagement_thread_target() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(_run_engagement())
        except Exception as exc:
            logger.warning(
                "twitter_engagement: event loop crashed (%s) — restarting in 60 s.", exc
            )
            time.sleep(60)


def start_twitter_engagement() -> threading.Thread:
    """Start the Twitter engagement engine as a daemon thread. Call from main.py."""
    t = threading.Thread(
        target=_engagement_thread_target,
        daemon=True,
        name="TwitterEngagement",
    )
    t.start()
    logger.info("Twitter engagement thread started.")
    return t
