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
from pathlib import Path
from typing import Any
from functools import partial

logger = logging.getLogger("whale_bot.news")

# ── Config ────────────────────────────────────────────────────────────────────

SOURCE_CHANNELS: list[str] = [
    "@coingraphnews",
    "@cointelegraph",
    "@bitcoinmagazinetelegram",
    "@fin_watch",
    "@yoyodexhaber",
    "@unfolded",
    "@ninjanewstr",
    "@watcherguru",
    "@coinmuhendisihaber",
    "@news_crypto",
    "@jrkripto",
]

# Normalised set for fast O(1) membership check (lowercase, no @)
# Used by the manual filter inside the event handler instead of relying on
# Telethon's chats= resolution which silently drops unresolvable channels.
_SOURCE_USERNAMES: set[str] = {ch.lstrip("@").lower() for ch in SOURCE_CHANNELS}

DEST_CHANNEL      = "@Ledgexs"
TELEGRAM_SIG = (
    "\n\n"
    "━━━━━━━━━━━━━━━\n"
    "<b>Ledgexs</b> | <a href='https://t.me/Ledgexs'>News</a> | <a href='https://x.com/Ledgexs'>X</a> | <a href='https://t.me/LedgexsBot'>LX Whale Bot</a>"
)
MIN_TEXT_LEN      = 15               # skip media-only / trivially short messages
TWEET_MAX         = 25000
TWITTER_MAX_MEDIA = 4                # Twitter hard limit
TG_MAX_MEDIA      = 10               # Telegram sendMediaGroup hard limit
DEDUP_CACHE_SIZE  = 60               # rolling window of recent summaries
GROUP_COLLECT_S   = 1.2              # seconds to wait for all album frames to arrive
MEDIA_DIR         = Path("/tmp/news_media")
_WS_RE            = re.compile(r'\s+')

# ── AI prompts ────────────────────────────────────────────────────────────────

AI_COMBINED_PROMPT = (
    "You are the senior crypto-intelligence editor for @Ledgexs. "
    "Your objective is to provide elite-level, high-signal information in STRICT GLOBAL ENGLISH.\n\n"
    
    "CRITICAL LANGUAGE RULE: ALL output MUST be in English. If the input is in Turkish, Arabic, or any other language, "
    "you MUST translate it to fluent, professional English immediately. NEVER output non-English text.\n\n"
    
    "CRITICAL RULE 1 (DEDUPLICATION): If the INCOMING NEWS reports the same event, geopolitical incident, or market movement as any of the RECENTLY PUBLISHED STORIES, output ONLY: DUPLICATE. "
    "BE STRICT: If the core event is the same (e.g., Strait of Hormuz closure), it is a duplicate, even if the wording or source is different.\n\n"
    "CRITICAL RULE 2 (SPAM FILTER): Output ONLY the word SKIP if the message contains NO actual news or factual information whatsoever — "
    "for example a pure giveaway announcement, a pure 'subscribe to our channel' call-to-action with no news, or a pure paid advertisement. "
    "DO NOT skip a message for any of the following reasons — these are all normal and expected in crypto news feeds:\n"
    "  • The message starts or ends with the channel's own name or Telegram/Twitter handle.\n"
    "  • The message contains URLs or links (they will be removed in the rewrite step).\n"
    "  • The message mentions any company, project, token, protocol, exchange, government body, or public figure by name.\n"
    "  • The message is written in Turkish, Arabic, or another language (translate it instead).\n"
    "RULE: If there is at least ONE factual claim — a price, an event, a decision, a statement by a person or organisation — it is news. Rewrite it.\n\n"
    
    "STEP 3 — THE REWRITE (STRICT FORMATTING):\n"
    "1. FORMATTING & TAGGING (DECISION LOGIC):\n"
    "   - You MUST analyze the news content and pick ONE of these tags to start your post:\n"
    "     a) <b>🚨 JUST IN:</b> (Use for new, timely developments and unexpected announcements.)\n"
    "     b) <b>⚡ BREAKING:</b> (Use for major, high-impact events that shift market sentiment.)\n"
    "     c) <b>📊 MARKET ALERT:</b> (Use for price action, technical indicators, or on-chain data alerts.)\n"
    "2. LENGTH: MAXIMUM 1-2 SENTENCES in English.\n"
    "3. AI INSIGHT (MAXIMUM 1-2 SENTENCES): Provide a short, professional analysis on how this impacts the market or the asset. DO NOT use any headers, labels, or titles for this section. Simply provide the analysis as a direct follow-up paragraph.\n"
    "4. DATA INTEGRITY: Keep all numbers, prices, and percentages IDENTICAL to the source.\n"
    "5. CLEANING: Remove ALL URLs and redundant source citations.\n"
    
    "RECENTLY PUBLISHED STORIES:\n{recent_stories}\n\n"
    "INCOMING NEWS:\n{incoming_news}"
)

MARKET_INSIGHT_PROMPT = (
    "You are a professional crypto analyst for @Ledgexs. Write an ultra-short market insight post.\n\n"
    "STRICT FORMAT RULES:\n"
    "1. HEADLINE: One punchy line using <b>🚀 $SYMBOL SHORT_ACTION!</b> — use HTML <b> tags, NEVER ** asterisks.\n"
    "2. BODY: EXACTLY 2 SHORT sentences max. One explains what is happening; one explains why it matters.\n"
    "3. FORMATTING: HTML only (<b>, <i>). ZERO markdown (no **, no __, no #). No bullet points, no headers.\n"
    "4. LANGUAGE: Professional English. Be concise — readers skim on mobile.\n\n"
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

# ── Deduplication cache ───────────────────────────────────────────────────────

_dedup_cache: deque[str] = deque(maxlen=DEDUP_CACHE_SIZE)
_dedup_lock  = threading.Lock()


def _cache_add(summary: str) -> None:
    with _dedup_lock:
        _dedup_cache.append(summary[:500])


# ── AI helpers (sync — always call via run_in_executor from async context) ────

def _ai_dedup_and_rewrite(raw_text: str) -> str | None:
    """Synchronous — must be called via run_in_executor to avoid blocking the event loop."""
    global _ai_client
    if _ai_client is None:
        _ai_client = _make_ai_client()
        if _ai_client is None:
            raise RuntimeError("AI client unavailable")

    with _dedup_lock:
        cache_snapshot = list(_dedup_cache)
        recent_stories = (
            "\n---\n".join(f"{i+1}. {s}" for i, s in enumerate(cache_snapshot))
            if cache_snapshot else "No recent stories."
        )

    formatted_prompt = AI_COMBINED_PROMPT.format(
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

# CoinGecko slug map — used by _get_market_data to resolve symbol → API ID
_CG_IDS: dict[str, str] = {
    "BTC": "bitcoin",        "ETH": "ethereum",       "SOL": "solana",
    "BNB": "binancecoin",    "XRP": "ripple",          "ADA": "cardano",
    "DOGE": "dogecoin",      "AVAX": "avalanche-2",    "LINK": "chainlink",
    "DOT": "polkadot",       "MATIC": "matic-network", "UNI": "uniswap",
    "ATOM": "cosmos",        "LTC": "litecoin",        "TRX": "tron",
    "NEAR": "near",          "FTM": "fantom",           "ALGO": "algorand",
    "SUI": "sui",            "APT": "aptos",            "OP": "optimism",
    "ARB": "arbitrum",       "INJ": "injective-protocol", "TIA": "celestia",
    "SEI": "sei-network",    "PENGU": "pudgy-penguins", "HYPE": "hyperliquid",
    "WIF": "dogwifcoin",     "BONK": "bonk",            "PEPE": "pepe",
    "TON": "the-open-network", "NOT": "notcoin",        "FLOKI": "floki",
}


async def _get_trending_coins() -> list[str]:
    """Fetch top trending coins from CoinGecko (no API key required)."""
    def _fetch() -> list[str]:
        resp = _requests.get(
            "https://api.coingecko.com/api/v3/search/trending", timeout=12
        )
        resp.raise_for_status()
        data = resp.json()
        return [coin["item"]["symbol"].upper() for coin in data["coins"][:3]]

    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _fetch)
    except Exception as e:
        logger.warning(f"Trend fetch failed: {e}")
        return ["BTC", "ETH", "SOL"]


async def _get_market_data(coin: str) -> str:
    """Fetch price/volume/change for a coin via CoinGecko (replaces geo-blocked Binance API)."""
    def _fetch() -> str:
        cg_id = _CG_IDS.get(coin.upper(), coin.lower())
        resp  = _requests.get(
            f"https://api.coingecko.com/api/v3/coins/{cg_id}",
            params={
                "localization": "false", "tickers": "false",
                "community_data": "false", "developer_data": "false",
            },
            timeout=12,
        )
        resp.raise_for_status()
        data   = resp.json()
        market = data.get("market_data", {})
        price  = market.get("current_price",             {}).get("usd", 0) or 0
        change = market.get("price_change_percentage_24h", 0) or 0
        vol    = market.get("total_volume",              {}).get("usd", 0) or 0
        cap    = market.get("market_cap",                {}).get("usd", 0) or 0
        high   = market.get("high_24h",                  {}).get("usd", 0) or 0
        low    = market.get("low_24h",                   {}).get("usd", 0) or 0
        return (
            f"Asset: ${coin} | Price: ${price:,.4f} | 24h Change: {change:+.2f}% | "
            f"24h Volume: ${vol:,.0f} | Market Cap: ${cap:,.0f} | "
            f"24h High: ${high:,.4f} | 24h Low: ${low:,.4f}"
        )

    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _fetch)
    except Exception as e:
        logger.warning(f"Market data fetch failed for {coin}: {e}")
        return f"${coin}: current price data temporarily unavailable."


async def _periodic_market_analysis(tg_client: Any) -> None:
    """
    Every 4 hours:
      1. Fetch the top-3 trending coins from CoinGecko.
      2. Pick only the #1 trending coin (not all three — avoids flooding).
      3. Generate a short AI market insight (HTML, max 2 sentences).
      4. Post to Telegram (@Ledgexs) AND X/Twitter.
    """
    while True:
        try:
            await asyncio.sleep(14400)   # 4-hour cadence
            trending_coins = await _get_trending_coins()
            if not trending_coins:
                continue

            coin        = trending_coins[0]    # top-1 trending coin only
            market_data = await _get_market_data(coin)
            prompt      = MARKET_INSIGHT_PROMPT.format(data=market_data)
            raw_text    = await _call_gpt_for_analysis(prompt)

            # Strip any stray ** markdown the AI may have produced
            analysis_text = _ASTERISK_RE.sub("", raw_text).strip()

            loop = asyncio.get_running_loop()

            # ── Telegram ─────────────────────────────────────────────────────
            await loop.run_in_executor(None, _post_to_telegram, analysis_text, [])

            # ── X / Twitter ──────────────────────────────────────────────────
            await loop.run_in_executor(None, _post_to_twitter, analysis_text, [])

        except Exception as e:
            logger.warning(f"Periodic analysis error: {e}")
            await asyncio.sleep(60)


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
    if _twitter_v2 is None:
        return

    plain = _strip_html(rewritten_text)
    tweet = (plain[: TWEET_MAX - 1] + "…") if len(plain) > TWEET_MAX else plain

    media_ids: list[int] = []
    if media_paths and _twitter_v1 is not None:
        for path in media_paths[:TWITTER_MAX_MEDIA]:
            try:
                media = _twitter_v1.media_upload(filename=path)
                media_ids.append(media.media_id)
                logger.debug("news_aggregator: Twitter media uploaded: %s", media.media_id)
            except Exception as exc:
                logger.warning("news_aggregator: Twitter media upload failed: %s", exc)

    try:
        if media_ids:
            _twitter_v2.create_tweet(text=tweet, media_ids=media_ids, user_auth=True)
        else:
            _twitter_v2.create_tweet(text=tweet, user_auth=True)
        logger.info(
            "news_aggregator: Cross-posted to X (%d chars, %d media).",
            len(tweet),
            len(media_ids),
        )
    except Exception as exc:
        logger.warning("news_aggregator: X post failed: %s", exc)


# ── Twitter Auto Comment Helper ───────────────────────────────────────────────

# Telegram kanal username'i (key) → karşılık gelen Twitter/X @username (value).
# Artık numeric ID saklamıyoruz; ID'ler runtime'da get_user() ile çekilir ve
# tekrar kullanım için bellekte önbelleğe alınır.
TWITTER_TARGET_MAPPING: dict[str, str] = {
    "cointelegraph":           "Cointelegraph",
    "watcherguru":             "WatcherGuru",
    "bitcoinmagazinetelegram": "BitcoinMagazine",
    "coinbureau":              "CoinBureau",
    "lookonchainchannel":      "lookonchain",
    "bulltheory":              "bulltheoryio",
    "cryptoquant_official":    "cryptoquant_com",
    "ninjanewstr":             "ninjanewsx",
}

# Runtime username → numeric ID cache (avoids a lookup API call on every news item)
_twitter_id_cache: dict[str, str] = {}


def _sync_twitter_auto_comment(tg_username: str, news_context: str, reply_text: str) -> None:
    """
    Synchronous Twitter auto-comment — resolves @username to numeric ID on
    first use (cached in _twitter_id_cache), then replies to the most recent
    standalone (root) tweet of that account.

    Why username instead of hardcoded ID:
      Numeric IDs never change but are error-prone to maintain manually.
      Using get_user(username=...) lets us store readable names and have the
      API always return the authoritative ID.

    Why filter for root tweets only (in_reply_to_user_id is None):
      get_users_tweets includes the account's own replies to other users.
      Replying inside a foreign conversation triggers 403 even when the target
      account has "Everyone can reply" — the restriction comes from the root
      tweet's owner, not the target.
    """
    tw_username = TWITTER_TARGET_MAPPING[tg_username.lower()]

    # ── Step 1: resolve username → numeric ID (use cache after first lookup) ──
    if tw_username not in _twitter_id_cache:
        user_resp = _twitter_v2.get_user(username=tw_username, user_auth=True)
        if user_resp.data is None:
            logger.warning(
                "news_aggregator: Twitter user @%s not found — skipping auto-comment.",
                tw_username,
            )
            return
        _twitter_id_cache[tw_username] = str(user_resp.data.id)
        logger.debug("news_aggregator: resolved @%s → %s", tw_username, _twitter_id_cache[tw_username])

    twitter_id = _twitter_id_cache[tw_username]

    # ── Step 2: fetch recent tweets and pick the latest root (standalone) one ──
    tweets = _twitter_v2.get_users_tweets(
        id=twitter_id,
        max_results=10,
        tweet_fields=["conversation_id", "in_reply_to_user_id"],
        user_auth=True,
    )

    if not tweets.data:
        logger.info("news_aggregator: no tweets found for @%s — skipping auto-comment.", tw_username)
        return

    target_tweet = None
    for tweet in tweets.data:
        if getattr(tweet, "in_reply_to_user_id", None) is None:
            target_tweet = tweet
            break

    if target_tweet is None:
        logger.info(
            "news_aggregator: all recent tweets from @%s are replies — skipping auto-comment.",
            tw_username,
        )
        return

    # ── Step 3: post the reply ────────────────────────────────────────────────
    _twitter_v2.create_tweet(
        text=reply_text,
        in_reply_to_tweet_id=str(target_tweet.id),
        user_auth=True,
    )
    logger.info(
        "news_aggregator: Auto-commented on @%s tweet (id=%s) — Success!",
        tw_username, target_tweet.id,
    )


async def _twitter_auto_comment(tg_username: str, news_context: str) -> None:
    if _twitter_v2 is None or tg_username.lower() not in TWITTER_TARGET_MAPPING:
        return

    try:
        prompt = (
            f"News Content: {news_context[:300]}\n"
            f"Latest Tweet from {tg_username}: (see context)\n"
            "CRITICAL: If the News Content and the Latest Tweet are completely unrelated, "
            "DO NOT reply with specific details from the news. Instead, provide a generic "
            "professional market insight relevant to the market current sentiment. "
            "Tone: @Ledgexs brand voice. No hashtags, no emojis."
        )
        reply_text = await _call_gpt_for_analysis(prompt)

        # Randomised delay to appear natural
        await asyncio.sleep(random.randint(60, 180))

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            _sync_twitter_auto_comment,
            tg_username,
            news_context,
            reply_text,
        )
    except Exception as e:
        logger.warning(f"Twitter auto-comment failed: {e}")


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

    # 1. AI Rewrite ve Duplicate Kontrolü (run_in_executor — blocks for 1-5s)
    try:
        rewritten = await loop.run_in_executor(None, _ai_dedup_and_rewrite, raw_text)
        if rewritten is None:  # Duplicate
            return
    except Exception as exc:
        logger.warning("news_aggregator: AI failed (%s) — using fallback.", exc)
        rewritten = _fallback_rewrite(raw_text)
        if not rewritten:
            return

    # 2. SKIP / stray control-word guard (second line of defence)
    # _ai_dedup_and_rewrite already catches DUPLICATE and SKIP, but if the
    # fallback path or any other edge case produces a control word, stop here.
    _cw = rewritten.strip().rstrip('.,!? \n').upper()
    if _cw in ("SKIP", "DUPLICATE"):
        logger.info("Skipping control-word output from %s: %r", source_username, rewritten[:30])
        return

    # 3. Medyayı indir
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

    # 4. Paylaşım ve Cache Güncelleme
    try:
        clean_tg_text = _remove_hashtags(rewritten)

        # Blocking HTTP calls offloaded to thread pool so event loop stays free
        await loop.run_in_executor(None, _post_to_telegram, clean_tg_text, media_paths)

        if media_paths:
            await loop.run_in_executor(None, _post_to_twitter, rewritten, media_paths)
        else:
            logger.info("news_aggregator: Text-only news sent to Telegram.")

        await _twitter_auto_comment(source_username, rewritten)

        _cache_add(_strip_html(rewritten))

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

    # FIX: Do NOT pass chats=SOURCE_CHANNELS to the decorator.
    # Telethon resolves those usernames at registration time. If any channel
    # (e.g. @watcherguru, @coingraphnews) fails to resolve — because the
    # session hasn't seen it before or a transient network error occurs —
    # that channel is silently dropped and its events never fire.
    # Instead we listen to ALL new messages and filter manually below.
    @client.on(events.NewMessage())
    async def _on_new_message(event: events.NewMessage.Event) -> None:
        try:
            chat     = await event.get_chat()
            username = (getattr(chat, 'username', None) or "").lower()

            # Manual filter: only process messages from our source channels
            if username not in _SOURCE_USERNAMES:
                return

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
        asyncio.create_task(_periodic_market_analysis(client))
        await client.start()
        logger.info("news_aggregator: Telethon UserBot connected.")
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
