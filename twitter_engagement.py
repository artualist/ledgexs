"""
twitter_engagement.py
=====================
@Ledgexs Twitter Engagement Engine — completely Telegram-independent.

Runs as an isolated daemon thread alongside the main bot.
Any exception inside is caught and logged — this module can NEVER crash main.py.

Features
--------
1. REPLY MONITOR      — polls TARGET_ACCOUNTS every 15 min; AI reply via v1.1
2. FEAR & GREED       — every 6 h; alternative.me real data + GPT commentary
3. TOP MOVERS         — daily at 09:00 UTC; CoinGecko top-3 gain/loss narrative
4. ON-CHAIN DETECTIVE — every 8 h; blockchain.info BTC + CoinGecko ETH metrics
5. THREAD STORYTELLING — every 12 h; 4-tweet GPT market narrative thread

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
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("whale_bot.twitter_engagement")

# ── Target accounts — Twitter usernames (no @) ────────────────────────────────
# The reply monitor will track these accounts and reply to their fresh tweets.
TARGET_ACCOUNTS: list[str] = [
    "elonmusk",
    "cz_binance",
    "arthur_hayes",
    "saylor",
    "VitalikButerin",
    "WatcherGuru",
    "DocumentingBTC",
    "APompliano",
    "RaoulGMI",
    "CoinDesk",
    "Cointelegraph",
    "BitcoinMagazine",
    "CryptoSlate",
    "lookonchain",
    "nansen_ai",
    "TheBlock__",
]

# ── Timing constants ───────────────────────────────────────────────────────────
REPLY_POLL_INTERVAL_S   = 60 * 60          # poll every 60 min; 1 reply per cycle
REPLY_COOLDOWN_H        = 6                # min hours between replies to same account
FEAR_GREED_INTERVAL_S   = 6 * 3600        # Fear & Greed tweet cadence
TOP_MOVERS_HOUR_UTC     = 9               # daily Top Movers post at 09:00 UTC
ONCHAIN_INTERVAL_S      = 8 * 3600        # On-Chain Detective cadence
THREAD_INTERVAL_S       = 12 * 3600       # Thread Storytelling cadence
HTTP_TIMEOUT            = 15              # seconds for external API requests

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

_REPLY_PROMPT_TMPL = (
    "You are a senior analyst at @Ledgexs, a crypto intelligence account.\n"
    "@{username} just posted:\n\n\"{tweet_text}\"\n\n"
    "Write ONE reply. Hard rules:\n"
    "- HARD LIMIT: 200 characters. Count every character. Exceed this → output discarded.\n"
    "- Must include at least one SPECIFIC data point, price level, or historical precedent.\n"
    "  If the claim can't be backed by something concrete, challenge the premise instead.\n"
    "- Objective: if the post is bullish, weigh the bearish case. If bearish, weigh the bull case.\n"
    "- Tone: senior analyst — not a fan, not a troll, not a cheerleader.\n"
    "- FORBIDDEN: 'Great point', 'Exactly', 'This', generic agreement, hype language.\n"
    "- FORBIDDEN: hashtags, @mentions, cashtags ($SYMBOL — write BTC not $BTC).\n"
    "- Write as if talking to a professional — dense, direct, no padding.\n"
    "Output ONLY the reply text."
)

_FEAR_GREED_PROMPT_TMPL = (
    "You are a senior market strategist at @Ledgexs.\n"
    "Fear & Greed Index: {value}/100 — \"{label}\"\n"
    "7-day readings (oldest→newest): {trend}\n\n"
    "Write ONE analytical tweet. Rules:\n"
    "- Open with the raw number and direction — state the fact, not the emotion.\n"
    "- What does this reading historically precede? Give BOTH the bullish AND bearish historical outcome.\n"
    "  Example: 'Sub-20 readings preceded 3 of the last 5 cycle bottoms — but also 2 extended bleeds.'\n"
    "- If trend is declining, say declining. If conditions for reversal aren't confirmed, say so.\n"
    "- No hype, no 'to the moon', no 'buy the fear'. Calibrated, probabilistic language.\n"
    "- MAX 240 characters. Dense prose. 1-2 emojis only where they sharpen meaning.\n"
    "- FORBIDDEN: hashtags, cashtags ($SYMBOL — write BTC not $BTC).\n"
    "Output ONLY the tweet text."
)

_TOP_MOVERS_PROMPT_TMPL = (
    "You are a quantitative analyst at @Ledgexs.\n"
    "24H market data:\n"
    "TOP GAINERS:\n{gainers}\n\n"
    "TOP LOSERS:\n{losers}\n\n"
    "Write ONE sharp analytical insight (NOT a recap — the data header already lists the names). Rules:\n"
    "- Identify the structural reason behind the divergence: sector rotation? macro catalyst? "
    "  leverage unwind? narrative shift? Be specific.\n"
    "- Flag if any gain looks unsustainable (thin volume, no catalyst, post-pump pattern).\n"
    "- Quantify where possible: 'X% gain on Y% below-avg volume = distribution risk.'\n"
    "- Objective: don't celebrate gains or dramatise losses. Analyse.\n"
    "- MAX 180 characters. Analyst voice. 1 emoji max.\n"
    "- FORBIDDEN: hashtags, cashtags ($SYMBOL — write BTC not $BTC).\n"
    "Output ONLY the insight text."
)

_ONCHAIN_PROMPT_TMPL = (
    "You are a senior on-chain analyst at @Ledgexs. You interpret raw blockchain data.\n"
    "Live on-chain readings:\n\n"
    "BITCOIN:\n{btc_data}\n\n"
    "ETHEREUM:\n{eth_data}\n\n"
    "Write ONE on-chain brief. Rules:\n"
    "- Lead with the single most anomalous or high-signal data point across both chains.\n"
    "- Interpret BEHAVIOUR, not price: are large holders accumulating, distributing, or neutral?\n"
    "  What does the mempool/fee/congestion data imply about urgency?\n"
    "- If BTC and ETH diverge meaningfully, call it out — divergence is often the signal.\n"
    "- If data is ambiguous or inconclusive, say so. Never force a bullish or bearish conclusion.\n"
    "- Precision over drama. Write like a quant brief, not a news headline.\n"
    "- MAX 240 characters. 1-2 emojis (🔍 🐋 📊 ⚡ only).\n"
    "- FORBIDDEN: hashtags, cashtags ($SYMBOL — write BTC not $BTC).\n"
    "Output ONLY the tweet text."
)

_THREAD_PROMPT_TMPL = (
    "You are a senior macro and crypto research analyst at @Ledgexs.\n"
    "Live market context:\n{market_context}\n\n"
    "Current trending news:\n{news_context}\n\n"
    "Pick the SINGLE MOST SIGNIFICANT macro or geopolitical event from the news above "
    "that is currently relevant to financial markets and crypto. "
    "DO NOT write about individual coin price action or technical levels. "
    "Focus on real-world events: central bank decisions, geopolitics, regulation, major economic data, "
    "institutional moves, or structural market shifts.\n\n"
    "Write a 6-tweet research thread structured exactly as follows:\n\n"
    "Tweet 1 — HOOK (MAX 200 chars): "
    "BOLD TITLE IN CAPS + emojis describing the event. "
    "Then 1-2 sentences explaining why it matters RIGHT NOW for markets. "
    "End with '🧵 Thread'. Make it impossible to scroll past.\n\n"
    "Tweet 2 — WHAT HAPPENED (MAX 240 chars): "
    "Specific facts and numbers about the event. No vague statements.\n\n"
    "Tweet 3 — CRYPTO/MARKET IMPACT (MAX 240 chars): "
    "How does this directly affect crypto and traditional financial markets right now?\n\n"
    "Tweet 4 — HISTORICAL PRECEDENT (MAX 240 chars): "
    "What happened last time something similar occurred? Specific example with outcome.\n\n"
    "Tweet 5 — RISKS & SCENARIOS (MAX 240 chars): "
    "Two or three key risks or scenarios to watch. What would confirm or deny the thesis?\n\n"
    "Tweet 6 — CTA (MAX 240 chars): "
    "Friendly self-promo directing readers to Ledgexs channels for more intelligence like this:\n"
    "• Telegram news: t.me/ledgexsofficial\n"
    "• Whale alerts: t.me/LedgexsWhale\n"
    "• Whale bot: @LX_Whale_Bot on Telegram\n"
    "• Follow @Ledgexs on X\n"
    "Keep it genuine and concise — not spammy.\n\n"
    "Tone: senior analyst writing for institutional-grade traders. Sharp, engaging, not dry.\n"
    "FORBIDDEN in all tweets: hashtags, cashtags ($SYMBOL — write BTC not $BTC), "
    "'NFA', 'DYOR', 'to the moon', hype language.\n"
    "REQUIRED: at least one specific number or data point per tweet (tweets 1-5).\n\n"
    "Output ONLY a valid JSON array of exactly 6 strings:\n"
    "[\"tweet1\", \"tweet2\", \"tweet3\", \"tweet4\", \"tweet5\", \"tweet6\"]"
)

# Fallback topics used only when live news fetch returns no usable data.
# Topics are deliberately macro/geopolitical — not coin-specific — to maximise engagement.
_THREAD_TOPICS_FALLBACK = [
    "How central bank rate decisions ripple through crypto markets",
    "Why geopolitical tensions drive capital into Bitcoin as a reserve asset",
    "The relationship between US dollar strength and global crypto risk appetite",
    "How US-China trade tensions reshape the global crypto mining landscape",
    "The IMF, World Bank, and crypto: an uneasy coexistence with trillion-dollar implications",
    "Energy policy, oil prices, and the hidden link to Bitcoin's mining economics",
    "How sovereign debt crises historically trigger crypto adoption cycles",
    "The real reason institutional money moves in and out of crypto during market stress",
    "Global inflation regimes and why crypto behaves differently in each one",
    "How banking sector crises create structural demand for self-custodied assets",
    "Why emerging market currency collapses consistently drive crypto on-ramp volume",
    "The EU's MiCA framework: what it actually changes for global crypto markets",
    "CBDC rollouts worldwide: threat or catalyst for decentralised crypto adoption?",
    "How global sanctions and financial exclusion accelerate crypto infrastructure growth",
    "The strategic Bitcoin reserve debate: what it means if governments hold BTC",
    "Why crypto market cycles are increasingly correlated with traditional risk-on/off regimes",
]

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

# Cache username → numeric Twitter user ID (avoids repeated lookups per session)
_reply_user_id_cache: dict[str, str] = {}

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
# Schema: {
#   "replied": {"username": {"tweet_id": str, "ts": float}},
#   "fear_greed_last_ts": float,
#   "top_movers_last_date": str,    # "YYYY-MM-DD"
#   "onchain_last_ts": float,
#   "thread_last_ts": float,
# }
_state: dict = _load_state()
_state.setdefault("replied", {})
_state.setdefault("fear_greed_last_ts", 0.0)
_state.setdefault("top_movers_last_date", "")
_state.setdefault("onchain_last_ts", 0.0)
_state.setdefault("thread_last_ts", 0.0)

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


def _post_thread(tweets: list[str]) -> bool:
    """Post a list of tweet texts as a connected thread. Returns True on success."""
    if _twitter_v2 is None or not tweets:
        return False
    prev_id: str | None = None
    success = 0
    for i, text in enumerate(tweets):
        try:
            kwargs: dict[str, Any] = {"text": text[:280], "user_auth": True}
            if prev_id:
                kwargs["in_reply_to_tweet_id"] = prev_id
            resp = _twitter_v2.create_tweet(**kwargs)
            prev_id = resp.data.get("id") if resp and resp.data else None
            if prev_id:
                success += 1
            time.sleep(2)   # brief gap between tweets
        except Exception as exc:
            logger.warning("twitter_engagement: thread tweet %d failed: %s", i + 1, exc)
            break
    logger.info("twitter_engagement: posted thread %d/%d tweets.", success, len(tweets))
    return success == len(tweets)


def _reply_v1(tweet_id: str, reply_text: str, username: str) -> bool:
    """Post a reply to tweet_id via v1.1 API. Returns True on success."""
    if _twitter_v1 is None:
        return False
    try:
        _twitter_v1.update_status(
            status=reply_text[:280],
            in_reply_to_status_id=tweet_id,
            auto_populate_reply_metadata=True,
        )
        logger.info(
            "twitter_engagement: replied to @%s tweet %s — %r",
            username, tweet_id, reply_text[:60],
        )
        return True
    except Exception as exc:
        logger.warning(
            "twitter_engagement: v1.1 reply to @%s tweet %s failed: %s",
            username, tweet_id, exc,
        )
        return False


def _safe_get(url: str, params: dict | None = None) -> dict | list | None:
    """GET request with timeout; returns parsed JSON or None."""
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.warning("twitter_engagement: HTTP GET %s failed: %s", url, exc)
        return None


# ── Feature 1: Reply Monitor ────────────────────────────────────────────────────

async def _reply_monitor() -> None:
    """
    Every 60 minutes: ONE reply posted across TARGET_ACCOUNTS.

    Logic per cycle:
      1. Shuffle accounts for variety.
      2. Collect eligible accounts (not in 6h cooldown).
      3. Priority: pick the first eligible account that has a tweet from the last 60 min.
      4. Fallback: if none tweeted recently, pick the first eligible account
         and reply to their latest tweet (any age, skip if already replied).
      5. Post exactly 1 reply and stop. Never skip a cycle silently.

    Requirements: Twitter app must have READ permission (not Write-only).
    """
    await asyncio.sleep(300)  # 5 min startup grace — let other systems settle
    logger.info("twitter_engagement: reply_monitor started — %d accounts, 60-min cycle.", len(TARGET_ACCOUNTS))

    while True:
        if _twitter_v2 is not None:
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(None, _do_reply_round)
            except Exception as exc:
                logger.warning("twitter_engagement: reply_monitor cycle error: %s", exc)

        logger.info("twitter_engagement: reply_monitor cycle done — sleeping 60 min.")
        await asyncio.sleep(REPLY_POLL_INTERVAL_S)


def _resolve_user_id(username: str) -> str | None:
    """Return cached numeric user ID for username, or None on failure."""
    if username in _reply_user_id_cache:
        return _reply_user_id_cache[username]
    try:
        resp = _twitter_v2.get_user(username=username, user_auth=True)
        if resp and resp.data:
            uid = str(resp.data.id)
            _reply_user_id_cache[username] = uid
            return uid
        logger.warning("twitter_engagement: could not resolve @%s (empty response)", username)
    except Exception as exc:
        err = str(exc)
        if "453" in err or "403" in err:
            logger.warning(
                "twitter_engagement: READ DENIED resolving @%s — set READ permission at "
                "developer.twitter.com → Apps → [app] → User auth settings.", username
            )
        else:
            logger.warning("twitter_engagement: could not resolve @%s: %s", username, exc)
    return None


def _fetch_latest_tweet(user_id: str, username: str):
    """Fetch up to 10 recent original tweets via v2. Returns list or None."""
    try:
        resp = _twitter_v2.get_users_tweets(
            id=user_id,
            max_results=10,
            exclude=["retweets", "replies"],
            tweet_fields=["created_at", "text"],
            user_auth=True,
        )
        return resp.data if (resp and resp.data) else []
    except Exception as exc:
        err = str(exc)
        if "453" in err or "403" in err:
            logger.warning(
                "twitter_engagement: READ DENIED fetching @%s timeline — check app permissions.",
                username,
            )
        else:
            logger.warning("twitter_engagement: get_users_tweets @%s failed: %s", username, exc)
        return None


def _do_reply_round() -> None:
    """
    Synchronous: one full reply cycle.
    Picks ONE eligible account, finds a tweet to reply to, posts the reply.
    """
    import random

    now = time.time()
    cooldown_s = REPLY_COOLDOWN_H * 3600
    recent_window_s = 3600  # "recent" = posted within the last 60 min

    # ── Build candidate list (shuffle for variety) ──────────────────────────
    candidates = list(TARGET_ACCOUNTS)
    random.shuffle(candidates)

    eligible: list[str] = [
        u for u in candidates
        if now - _state["replied"].get(u, {}).get("ts", 0.0) >= cooldown_s
    ]

    if not eligible:
        logger.info(
            "twitter_engagement: all %d accounts in %dh cooldown — skipping cycle.",
            len(TARGET_ACCOUNTS), REPLY_COOLDOWN_H,
        )
        return

    # ── Pass 1: look for a tweet posted within the last 60 min ──────────────
    for username in eligible:
        uid = _resolve_user_id(username)
        if not uid:
            continue
        tweets = _fetch_latest_tweet(uid, username)
        if tweets is None:
            continue   # API error for this account — try next
        for tw in tweets:
            created_at = getattr(tw, "created_at", None)
            if created_at and (now - created_at.timestamp()) <= recent_window_s:
                tid = str(tw.id)
                if tid == _state["replied"].get(username, {}).get("tweet_id", ""):
                    continue  # already replied
                if _attempt_reply(username, tw):
                    return    # done for this cycle

    # ── Pass 2 (fallback): reply to latest tweet from any eligible account ───
    logger.info(
        "twitter_engagement: no recent tweets found — falling back to latest available tweet."
    )
    for username in eligible:
        uid = _resolve_user_id(username)
        if not uid:
            continue
        tweets = _fetch_latest_tweet(uid, username)
        if not tweets:
            continue
        for tw in tweets:
            tid = str(tw.id)
            if tid == _state["replied"].get(username, {}).get("tweet_id", ""):
                continue  # already replied to this one
            if _attempt_reply(username, tw):
                return

    logger.info("twitter_engagement: no replyable tweet found across all eligible accounts this cycle.")


def _attempt_reply(username: str, tw: Any) -> bool:
    """
    Generate AI reply and post it. Returns True on success.
    """
    tweet_text = getattr(tw, "text", "") or ""
    clean = re.sub(r"https?://\S+", "", tweet_text).strip()
    clean = re.sub(r"@\S+", "", clean).strip()

    if len(clean) < 15:
        logger.debug("twitter_engagement: @%s tweet too short to reply — skipping.", username)
        return False

    prompt = _REPLY_PROMPT_TMPL.format(username=username, tweet_text=clean[:400])
    reply_text = _gpt(prompt, max_tokens=100, temperature=0.8)

    if not reply_text or len(reply_text) < 10:
        logger.warning("twitter_engagement: GPT returned empty reply for @%s.", username)
        return False

    reply_text = re.sub(r"@\S+", "", reply_text).strip()[:275]

    success = _reply_v1(str(tw.id), reply_text, username)
    if success:
        _state["replied"][username] = {"tweet_id": str(tw.id), "ts": time.time()}
        _save_state(_state)
    return success


# ── Feature 2: Fear & Greed Index ─────────────────────────────────────────────

async def _fear_and_greed_loop() -> None:
    """Every 6 hours: fetch Fear & Greed Index, generate AI commentary, tweet."""
    await asyncio.sleep(120)  # let the bot fully start first
    logger.info("twitter_engagement: fear_and_greed_loop started.")

    while True:
        last_ts: float = _state.get("fear_greed_last_ts", 0.0)
        if time.time() - last_ts >= FEAR_GREED_INTERVAL_S:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _post_fear_greed)
        else:
            remaining = (last_ts + FEAR_GREED_INTERVAL_S - time.time()) / 60
            logger.debug("twitter_engagement: fear_greed sleeping %.0f min.", remaining)

        await asyncio.sleep(1800)  # check every 30 min; post when due


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
        _state["fear_greed_last_ts"] = time.time()
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
    """Synchronous: fetch top gainers/losers from CoinGecko and tweet."""
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    data = _safe_get("https://api.coingecko.com/api/v3/coins/markets", params=params)
    if not data or not isinstance(data, list):
        logger.warning("twitter_engagement: CoinGecko top movers returned no data.")
        return

    # Filter out stablecoins
    stablecoins = {"usdt", "usdc", "busd", "dai", "tusd", "frax", "usdp", "usdd", "gusd"}
    tradeable   = [c for c in data if c.get("symbol", "").lower() not in stablecoins]

    sorted_by_change = sorted(
        tradeable,
        key=lambda c: c.get("price_change_percentage_24h") or 0,
    )
    losers  = sorted_by_change[:3]    # most negative
    gainers = sorted_by_change[-3:][::-1]  # most positive

    def _fmt(coins: list, emoji: str) -> str:
        lines = []
        for c in coins:
            name   = c.get("symbol", "?").upper()
            change = c.get("price_change_percentage_24h") or 0
            price  = c.get("current_price") or 0
            lines.append(f"  {name}: {change:+.1f}% @ ${price:,.4g}")
        return "\n".join(lines)

    # Build compact per-coin label: SYM +X.X%
    def _short_fmt(coins: list) -> str:
        parts = []
        for c in coins:
            sym    = c.get("symbol", "?").upper()
            change = c.get("price_change_percentage_24h") or 0
            parts.append(f"{sym} {change:+.1f}%")
        return "  ".join(parts)

    gainers_str = _fmt(gainers, "📈")
    losers_str  = _fmt(losers,  "📉")
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


# ── Feature 4: On-Chain Detective ─────────────────────────────────────────────

async def _onchain_detective_loop() -> None:
    """Every 8 hours: fetch real BTC + ETH on-chain data and tweet an analysis."""
    await asyncio.sleep(600)  # 10 min initial delay
    logger.info("twitter_engagement: onchain_detective_loop started.")

    while True:
        last_ts = _state.get("onchain_last_ts", 0.0)
        if time.time() - last_ts >= ONCHAIN_INTERVAL_S:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _post_onchain)
        await asyncio.sleep(1800)


def _fetch_btc_onchain() -> str:
    """
    Fetch BTC on-chain data from mempool.space (free, reliable, no API key).
    blockchain.info stats endpoint has inconsistent field names and unit issues —
    mempool.space is the authoritative source for Bitcoin network data.
    """
    lines: list[str] = []

    # Mempool fee rates (priority / standard / economy sat/vB)
    fees = _safe_get("https://mempool.space/api/v1/fees/recommended")
    if fees and isinstance(fees, dict):
        fastest  = fees.get("fastestFee", "?")
        half_hr  = fees.get("halfHourFee", "?")
        economy  = fees.get("economyFee", "?")
        lines.append(f"Mempool fees — Fast: {fastest} sat/vB | Std: {half_hr} | Economy: {economy}")

    # Latest block height
    height = _safe_get("https://mempool.space/api/blocks/tip/height")
    if isinstance(height, int):
        lines.append(f"Latest block: #{height:,}")

    # Network hashrate (3-day rolling average from mempool.space)
    hashrate_data = _safe_get("https://mempool.space/api/v1/mining/hashrate/3d")
    if hashrate_data and isinstance(hashrate_data, dict):
        hr_list = hashrate_data.get("hashrates", [])
        if hr_list:
            latest_hr = hr_list[-1].get("avgHashrate", 0)
            if latest_hr:
                eh = latest_hr / 1e18
                lines.append(f"Hash rate (3d avg): {eh:.1f} EH/s")

    # Mempool size
    mempool_info = _safe_get("https://mempool.space/api/mempool")
    if mempool_info and isinstance(mempool_info, dict):
        count = mempool_info.get("count", 0)
        vsize = mempool_info.get("vsize", 0)
        lines.append(f"Mempool: {count:,} unconfirmed txs ({vsize / 1e6:.1f} MB)")

    # BTC price + volume from CoinGecko as complement
    btc = _safe_get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true", "include_24hr_vol": "true"},
    )
    if btc and "bitcoin" in btc:
        bd      = btc["bitcoin"]
        price   = bd.get("usd", 0)
        change  = bd.get("usd_24h_change", 0) or 0
        vol     = bd.get("usd_24h_vol", 0) or 0
        lines.append(f"BTC price: ${price:,.0f} ({change:+.2f}% 24h) | Volume: ${vol / 1e9:.2f}B")

    return "\n".join(lines) if lines else "BTC on-chain data temporarily unavailable."


def _fetch_eth_onchain() -> str:
    """Fetch ETH metrics from CoinGecko (free, no API key)."""
    data = _safe_get(
        "https://api.coingecko.com/api/v3/coins/ethereum",
        params={"localization": "false", "tickers": "false", "community_data": "false", "developer_data": "false"},
    )
    if not data:
        return "ETH on-chain data temporarily unavailable."

    mkt  = data.get("market_data", {})
    price            = mkt.get("current_price", {}).get("usd", 0)
    vol_24h          = mkt.get("total_volume", {}).get("usd", 0)
    market_cap       = mkt.get("market_cap", {}).get("usd", 0)
    price_change_24h = mkt.get("price_change_percentage_24h", 0)
    price_change_7d  = mkt.get("price_change_percentage_7d", 0)

    # Staking / supply from blockchain data
    blockchain_data = data.get("block_time_in_minutes", "?")
    circulating     = mkt.get("circulating_supply", 0)

    lines = [
        f"Price: ${price:,.2f} ({price_change_24h:+.2f}% 24h, {price_change_7d:+.2f}% 7d)",
        f"Volume (24h): ${vol_24h / 1e9:.2f}B",
        f"Market cap: ${market_cap / 1e9:.1f}B",
        f"Circulating supply: {circulating / 1e6:.2f}M ETH",
        f"Avg block time: {blockchain_data} min",
    ]
    return "\n".join(l for l in lines if l)


def _post_onchain() -> None:
    """Synchronous: build on-chain snapshot and tweet detective analysis."""
    btc_data = _fetch_btc_onchain()
    eth_data = _fetch_eth_onchain()

    prompt = _ONCHAIN_PROMPT_TMPL.format(btc_data=btc_data, eth_data=eth_data)
    tweet_text = _gpt(prompt, max_tokens=160)
    if not tweet_text:
        return

    full = ("🔍 " + tweet_text)[:280]
    tid = _post_tweet(full)
    if tid:
        _state["onchain_last_ts"] = time.time()
        _save_state(_state)


# ── Feature 5: Thread Storytelling ────────────────────────────────────────────

def _fetch_trending_news() -> str:
    """Fetch top trending crypto/macro news headlines from CryptoPanic free API.

    Returns a formatted string of headlines for use in the thread prompt,
    or a curated fallback topic string if the API is unavailable.
    """
    try:
        data = _safe_get(
            "https://cryptopanic.com/api/free/v1/posts/",
            params={"public": "true", "filter": "hot", "kind": "news"},
        )
        if data and isinstance(data.get("results"), list):
            headlines = [
                r.get("title", "")
                for r in data["results"][:8]
                if r.get("title")
            ]
            if headlines:
                return "TOP TRENDING NEWS (use the most impactful macro/geopolitical one):\n" + "\n".join(
                    f"• {h}" for h in headlines
                )
    except Exception as exc:
        logger.debug("twitter_engagement: news fetch failed: %s", exc)

    # Fallback: provide a macro topic context for GPT to work with
    import random as _rnd
    fallback = _rnd.choice(_THREAD_TOPICS_FALLBACK)
    return f"No live news available. Write about this macro topic instead:\n• {fallback}"


async def _thread_storytelling_loop() -> None:
    """Every 12 hours: post a 6-tweet analytical thread on a major macro/geopolitical event."""
    await asyncio.sleep(900)  # 15 min initial delay
    logger.info("twitter_engagement: thread_storytelling_loop started.")

    while True:
        last_ts = _state.get("thread_last_ts", 0.0)
        if time.time() - last_ts >= THREAD_INTERVAL_S:
            loop = asyncio.get_running_loop()

            # Fetch live news headlines + market context in parallel via executor
            market_context, news_context = await asyncio.gather(
                loop.run_in_executor(None, _build_market_context),
                loop.run_in_executor(None, _fetch_trending_news),
            )
            await loop.run_in_executor(None, _post_thread_now, news_context, market_context)

        await asyncio.sleep(1800)


def _build_market_context() -> str:
    """Build a short real-data market snapshot to ground GPT thread generation."""
    lines: list[str] = []

    # BTC + ETH prices from CoinGecko
    markets = _safe_get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum",
            "price_change_percentage": "24h,7d",
        },
    )
    if markets and isinstance(markets, list):
        for c in markets:
            sym   = c.get("symbol", "").upper()
            price = c.get("current_price", 0)
            c24   = c.get("price_change_percentage_24h", 0) or 0
            c7d   = c.get("price_change_percentage_7d", 0) or 0
            lines.append(f"${sym}: ${price:,.2f} ({c24:+.2f}% 24h, {c7d:+.2f}% 7d)")

    # Fear & Greed
    fng = _safe_get("https://api.alternative.me/fng/", params={"limit": 1})
    if fng and "data" in fng:
        v = fng["data"][0].get("value", "?")
        l = fng["data"][0].get("value_classification", "")
        lines.append(f"Fear & Greed: {v}/100 ({l})")

    # Global market cap from CoinGecko
    global_data = _safe_get("https://api.coingecko.com/api/v3/global")
    if global_data and "data" in global_data:
        gd        = global_data["data"]
        total_mkt = gd.get("total_market_cap", {}).get("usd", 0)
        btc_dom   = gd.get("market_cap_percentage", {}).get("btc", 0)
        lines.append(f"Total market cap: ${total_mkt / 1e12:.2f}T")
        lines.append(f"BTC dominance: {btc_dom:.1f}%")

    return "\n".join(lines) if lines else "Market data temporarily unavailable."


def _post_thread_now(news_context: str, market_context: str) -> None:
    """Synchronous: generate 6-tweet thread via GPT and post."""
    prompt = _THREAD_PROMPT_TMPL.format(news_context=news_context, market_context=market_context)
    raw = _gpt(prompt, max_tokens=900, temperature=0.8)
    if not raw:
        return

    # Parse JSON array from GPT response
    try:
        # GPT sometimes wraps the JSON in markdown code blocks — strip them
        clean = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("```").strip()
        tweets: list[str] = json.loads(clean)
        if not isinstance(tweets, list) or len(tweets) < 6:
            raise ValueError(f"expected 6 tweets, got {len(tweets) if isinstance(tweets, list) else type(tweets)}")
    except Exception as exc:
        logger.warning("twitter_engagement: thread JSON parse failed: %s — raw: %r", exc, raw[:200])
        return

    logger.info("twitter_engagement: posting 6-tweet thread — context: %r", news_context[:80])
    success = _post_thread(tweets)
    if success:
        _state["thread_last_ts"] = time.time()
        _save_state(_state)


# ── Main async runner ──────────────────────────────────────────────────────────

async def _run_engagement() -> None:
    """Launches all engagement feature coroutines as concurrent asyncio tasks."""
    logger.info("twitter_engagement: starting all engagement tasks…")
    tasks = [
        asyncio.create_task(_reply_monitor(),            name="reply_monitor"),
        asyncio.create_task(_fear_and_greed_loop(),      name="fear_and_greed"),
        asyncio.create_task(_top_movers_loop(),          name="top_movers"),
        asyncio.create_task(_onchain_detective_loop(),   name="onchain_detective"),
        asyncio.create_task(_thread_storytelling_loop(), name="thread_storytelling"),
    ]
    # Run forever; individual task exceptions are caught inside each coroutine.
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
