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
REPLY_POLL_INTERVAL_S   = 15 * 60          # how often to poll all accounts (15 min)
REPLY_COOLDOWN_H        = 3                # minimum hours between replies to same account
FEAR_GREED_INTERVAL_S   = 6 * 3600        # Fear & Greed tweet cadence
TOP_MOVERS_HOUR_UTC     = 9               # daily Top Movers post at 09:00 UTC
ONCHAIN_INTERVAL_S      = 8 * 3600        # On-Chain Detective cadence
THREAD_INTERVAL_S       = 12 * 3600       # Thread Storytelling cadence
HTTP_TIMEOUT            = 15              # seconds for external API requests

# ── State file — persists last-replied tweet IDs across Railway restarts ───────
_STATE_FILE = "/tmp/twitter_engagement_state.json"

# ── OpenAI / AI base URL ───────────────────────────────────────────────────────
_AI_API_KEY = (
    os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
    or os.environ.get("OPENAI_API_KEY")
    or "dummy"
)
_AI_BASE_URL = "https://api.openai.com/v1"

# ── GPT prompts ────────────────────────────────────────────────────────────────

_REPLY_PROMPT_TMPL = (
    "You are @Ledgexs — a professional crypto intelligence brand on Twitter.\n"
    "The following tweet was just posted by @{username}:\n\n"
    "\"{tweet_text}\"\n\n"
    "Write a SHORT reply (1-2 sentences, MAX 220 characters) that adds a sharp, "
    "insightful crypto perspective relevant to what they said. Sound natural and "
    "confident. No hashtags. No emojis. No @mentions. No 'Great point!' filler. "
    "Output ONLY the reply text."
)

_FEAR_GREED_PROMPT_TMPL = (
    "You are @Ledgexs — a professional crypto intelligence brand.\n"
    "Today's Crypto Fear & Greed Index: {value}/100 — '{label}'.\n"
    "7-day trend: {trend}\n\n"
    "Write a punchy, insightful tweet (MAX 240 characters) about what this reading "
    "signals for the market. Add a contrarian or forward-looking take. "
    "No hashtags. No emojis. Factual and authoritative tone. "
    "Output ONLY the tweet text."
)

_TOP_MOVERS_PROMPT_TMPL = (
    "You are @Ledgexs — a professional crypto intelligence brand.\n"
    "Today's top performers and worst performers over the last 24 hours:\n\n"
    "TOP GAINERS:\n{gainers}\n\n"
    "TOP LOSERS:\n{losers}\n\n"
    "Write a concise, analytical tweet (MAX 260 characters) that highlights the "
    "most interesting insight from this data — why are these moving? What does it "
    "signal? Be specific and factual. No hashtags. Minimal emojis. "
    "Output ONLY the tweet text."
)

_ONCHAIN_PROMPT_TMPL = (
    "You are @Ledgexs — a professional crypto intelligence brand.\n"
    "Here is real on-chain data captured right now:\n\n"
    "BITCOIN NETWORK:\n{btc_data}\n\n"
    "ETHEREUM METRICS:\n{eth_data}\n\n"
    "Write a compelling 'on-chain detective' tweet (MAX 260 characters) that "
    "reveals what these on-chain signals tell us about market positioning. "
    "Be specific — mention real numbers. Authoritative, investigative tone. "
    "No hashtags. Output ONLY the tweet text."
)

_THREAD_PROMPT_TMPL = (
    "You are @Ledgexs — a professional crypto intelligence brand on Twitter.\n"
    "Current market context:\n{market_context}\n\n"
    "Write a Twitter THREAD of exactly 4 tweets on this topic: '{topic}'.\n"
    "Rules:\n"
    "- Tweet 1: A hook/question that stops the scroll (under 200 chars). End with '🧵'.\n"
    "- Tweets 2-3: Substantive insights with REAL data and analysis.\n"
    "- Tweet 4: A bold conclusion or prediction.\n"
    "- Each tweet MAX 260 characters.\n"
    "- No hashtags. Minimal emojis only where impactful.\n"
    "- Sound like a professional analyst, not a hype account.\n"
    "Output ONLY a JSON array of 4 strings, nothing else. Example:\n"
    "[\"tweet1 text\", \"tweet2 text\", \"tweet3 text\", \"tweet4 text\"]"
)

_THREAD_TOPICS = [
    "Why Bitcoin dominance rising or falling matters for altcoin season",
    "What on-chain whale accumulation signals about the next 30 days",
    "The real reason crypto market cycles are compressing",
    "Why stablecoin supply growth is a leading indicator for bull runs",
    "What the Fear & Greed Index actually predicts — and where it fails",
    "The hidden correlation between macro interest rates and crypto prices",
    "Why Ethereum gas fees spiking is actually bullish signal",
    "What top CEX inflows/outflows tell us about retail vs institutional behaviour",
    "The difference between a dead-cat bounce and a true trend reversal",
    "Why DeFi TVL is the most overlooked crypto health metric",
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
    Every 15 minutes, polls each account in TARGET_ACCOUNTS using the v1.1
    user_timeline endpoint (900 req/15min user auth — plenty of headroom).
    For each account with a new tweet we haven't replied to, generate an AI
    reply and post it via v1.1 update_status.

    WHY v1.1: Twitter v2 create_tweet with in_reply_to_tweet_id returns 403
    ("not engaged") for accounts we haven't interacted with. v1.1 update_status
    does NOT have this restriction for public tweets.
    """
    # Stagger the first poll so all accounts aren't hit simultaneously.
    await asyncio.sleep(60)
    logger.info("twitter_engagement: reply_monitor started — %d accounts.", len(TARGET_ACCOUNTS))

    while True:
        if _twitter_v1 is None:
            await asyncio.sleep(REPLY_POLL_INTERVAL_S)
            continue

        loop = asyncio.get_running_loop()
        for username in TARGET_ACCOUNTS:
            try:
                await loop.run_in_executor(None, _check_and_reply_one, username)
            except Exception as exc:
                logger.warning("twitter_engagement: reply_monitor error for @%s: %s", username, exc)
            # 3-5 second gap between accounts to avoid burst
            await asyncio.sleep(4)

        logger.info("twitter_engagement: reply_monitor round complete — sleeping %d min.", REPLY_POLL_INTERVAL_S // 60)
        await asyncio.sleep(REPLY_POLL_INTERVAL_S)


def _check_and_reply_one(username: str) -> None:
    """Synchronous: check @username for a fresh tweet and reply if appropriate."""
    replied_data = _state["replied"].get(username, {})
    last_ts: float = replied_data.get("ts", 0.0)

    # Cooldown: don't reply to the same account more than once per REPLY_COOLDOWN_H hours.
    if time.time() - last_ts < REPLY_COOLDOWN_H * 3600:
        logger.debug(
            "twitter_engagement: @%s cooldown active (last %.0fm ago) — skipping.",
            username, (time.time() - last_ts) / 60,
        )
        return

    # Fetch recent tweets via v1.1 (excludes retweets, excludes pure replies)
    try:
        tweets = _twitter_v1.user_timeline(
            screen_name=username,
            count=10,
            include_rts=False,
            exclude_replies=True,
            tweet_mode="extended",
        )
    except Exception as exc:
        logger.warning("twitter_engagement: user_timeline @%s failed: %s", username, exc)
        return

    if not tweets:
        return

    last_replied_id = replied_data.get("tweet_id", "")
    target_tweet = None
    for tw in tweets:
        tid = str(tw.id)
        if tid == last_replied_id:
            break   # already replied to this and everything after it
        # Skip tweets older than 30 minutes (don't pile on to stale content)
        age_min = (time.time() - tw.created_at.timestamp()) / 60
        if age_min > 30:
            continue
        target_tweet = tw
        break   # most recent eligible tweet

    if target_tweet is None:
        return

    tweet_text = getattr(target_tweet, "full_text", None) or getattr(target_tweet, "text", "")
    # Strip URLs and @mentions from tweet text for cleaner GPT prompt
    clean_text = re.sub(r"https?://\S+", "", tweet_text).strip()
    clean_text = re.sub(r"@\S+", "", clean_text).strip()
    if len(clean_text) < 15:
        return  # too little content to reply meaningfully

    prompt = _REPLY_PROMPT_TMPL.format(username=username, tweet_text=clean_text[:400])
    reply_text = _gpt(prompt, max_tokens=80, temperature=0.8)

    if not reply_text or len(reply_text) < 10:
        return

    # Remove any @mentions GPT might have snuck in (we don't want to @ anyone else)
    reply_text = re.sub(r"@\S+", "", reply_text).strip()

    success = _reply_v1(str(target_tweet.id), reply_text, username)
    if success:
        _state["replied"][username] = {"tweet_id": str(target_tweet.id), "ts": time.time()}
        _save_state(_state)


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
    value   = today.get("value", "?")
    label   = today.get("value_classification", "Unknown")

    # Build 7-day trend string
    trend_parts = []
    for e in reversed(entries[1:]):   # oldest to newest
        v = int(e.get("value", 0))
        trend_parts.append(f"{v}")
    trend = " → ".join(trend_parts) + f" → {value}"

    prompt = _FEAR_GREED_PROMPT_TMPL.format(value=value, label=label, trend=trend)
    tweet_text = _gpt(prompt, max_tokens=120)

    if not tweet_text:
        return

    # Prepend the actual index value so it's immediately visible
    full_tweet = f"Fear & Greed Index: {value}/100 — {label}\n\n{tweet_text}"
    if len(full_tweet) > 280:
        full_tweet = full_tweet[:277] + "..."

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
            lines.append(f"  ${name}: {change:+.1f}% @ ${price:,.4g}")
        return "\n".join(lines)

    gainers_str = _fmt(gainers, "📈")
    losers_str  = _fmt(losers,  "📉")

    prompt = _TOP_MOVERS_PROMPT_TMPL.format(gainers=gainers_str, losers=losers_str)
    tweet_text = _gpt(prompt, max_tokens=150)
    if not tweet_text:
        return

    # Build header
    gainer_names = " / ".join(f"${c.get('symbol','').upper()}" for c in gainers)
    loser_names  = " / ".join(f"${c.get('symbol','').upper()}" for c in losers)
    header = f"24H MOVERS — {today_str}\n📈 {gainer_names}\n📉 {loser_names}\n\n"
    full   = header + tweet_text
    if len(full) > 280:
        full = full[:277] + "..."

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
    """Fetch real BTC network stats from blockchain.info (free, no API key)."""
    stats = _safe_get("https://blockchain.info/stats?format=json")
    mempool = _safe_get("https://api.blockchain.info/mempool/fees")

    lines: list[str] = []
    if stats:
        hash_rate    = stats.get("hash_rate", 0)
        n_tx         = stats.get("n_tx", 0)
        total_fees   = stats.get("total_fees_btc", 0)
        btc_sent     = stats.get("total_btc_sent", 0)
        miners_rev   = stats.get("miners_revenue_btc", 0)
        lines += [
            f"Hash rate: {hash_rate / 1e18:.2f} EH/s" if hash_rate else "",
            f"Transactions today: {n_tx:,}",
            f"Total fees (24h): {total_fees:.4f} BTC",
            f"BTC sent (24h): {btc_sent / 1e8:,.0f} BTC",
            f"Miner revenue (24h): {miners_rev / 1e8:.2f} BTC",
        ]
    if mempool and isinstance(mempool, dict):
        priority = mempool.get("priority", "?")
        lines.append(f"Mempool priority fee: {priority} sat/byte")

    return "\n".join(l for l in lines if l) or "BTC on-chain data temporarily unavailable."


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

    prefix = "🔍 ON-CHAIN SIGNAL\n\n"
    full   = prefix + tweet_text
    if len(full) > 280:
        full = full[:277] + "..."

    tid = _post_tweet(full)
    if tid:
        _state["onchain_last_ts"] = time.time()
        _save_state(_state)


# ── Feature 5: Thread Storytelling ────────────────────────────────────────────

async def _thread_storytelling_loop() -> None:
    """Every 12 hours: post a 4-tweet analytical thread on a crypto market topic."""
    await asyncio.sleep(900)  # 15 min initial delay
    logger.info("twitter_engagement: thread_storytelling_loop started.")

    _topic_index = 0

    while True:
        last_ts = _state.get("thread_last_ts", 0.0)
        if time.time() - last_ts >= THREAD_INTERVAL_S:
            loop = asyncio.get_running_loop()

            # Rotate through topics deterministically
            topic_idx = _topic_index % len(_THREAD_TOPICS)
            topic = _THREAD_TOPICS[topic_idx]
            _topic_index += 1

            # Build current market context to ground the thread in real data
            market_context = await loop.run_in_executor(None, _build_market_context)
            await loop.run_in_executor(None, _post_thread_now, topic, market_context)

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


def _post_thread_now(topic: str, market_context: str) -> None:
    """Synchronous: generate thread via GPT and post."""
    prompt = _THREAD_PROMPT_TMPL.format(topic=topic, market_context=market_context)
    raw = _gpt(prompt, max_tokens=600, temperature=0.8)
    if not raw:
        return

    # Parse JSON array from GPT response
    try:
        # GPT sometimes wraps the JSON in markdown code blocks — strip them
        clean = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("```").strip()
        tweets: list[str] = json.loads(clean)
        if not isinstance(tweets, list) or len(tweets) < 2:
            raise ValueError("not a list")
    except Exception as exc:
        logger.warning("twitter_engagement: thread JSON parse failed: %s — raw: %r", exc, raw[:200])
        return

    logger.info("twitter_engagement: posting thread on topic: %r", topic[:60])
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
