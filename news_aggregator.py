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
import ccxt
from collections import deque
from pathlib import Path
from typing import Any
from functools import partial

logger = logging.getLogger("whale_bot.news")

async def _get_trending_coins() -> list[str]:
    try:
        # CoinGecko'nun public API'sinden trendleri çeker
        resp = _requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=10)
        data = resp.json()
        # İlk 3 trend olan coinin sembolünü al
        return [coin['item']['symbol'].upper() for coin in data['coins'][:3]]
    except Exception as e:
        logger.warning(f"Trend fetch failed: {e}")
        return ["BTC", "ETH", "SOL"] # API patlarsa yedek liste

async def _get_market_data(coin: str) -> str:
    try:
        # Binance üzerinden veriyi çek
        exchange = ccxt.binance()
        ticker = exchange.fetch_ticker(f"{coin}/USDT")
        ohlcv = exchange.fetch_ohlcv(f"{coin}/USDT", timeframe='4h', limit=5)
        
        # Yapay zekanın anlayacağı şekilde veriyi metne döküyoruz
        price = ticker['last']
        change = ticker['percentage']
        volume = ticker['baseVolume']
        
        data_summary = (
            f"Asset: {coin}/USDT | Price: {price} | 24h Change: {change}% | "
            f"24h Volume: {volume} | "
            f"Recent Trend: Last 5 candles (4h) closing prices: {[c[4] for c in ohlcv]}"
        )
        return data_summary
    except Exception as e:
        logger.warning(f"Market data fetch failed for {coin}: {e}")
        return f"Market data for {coin} is currently unavailable due to API error."

async def _call_gpt_for_analysis(prompt: str) -> str:
    global _ai_client
    if _ai_client is None:
        _ai_client = _make_ai_client() # Hata varsa tekrar dene
        if _ai_client is None: return "AI service unavailable."

    resp = _ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )
    return (resp.choices[0].message.content or "").strip()

def _remove_hashtags(text: str) -> str:
    text = re.sub(r'#\w+', '', text)
    text = re.sub(r'\$[a-zA-Z_]+\b', '', text)
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
    # Eğer metin hala çok kısaysa veya saçmaysa haber atlanmasın ama etiketlensin
    return f"<b>MARKET ALERT:</b> {clean[:250]} (Translated to English automatically)"

# ── Config ────────────────────────────────────────────────────────────────────

SOURCE_CHANNELS: list[str] = [
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
_WS_RE          = re.compile(r'\s+')

# ── AI prompts ────────────────────────────────────────────────────────────────

AI_COMBINED_PROMPT = (
    "You are the senior crypto-intelligence editor for @Ledgexs. "
    "Your objective is to provide elite-level, high-signal information in STRICT GLOBAL ENGLISH.\n\n"
    
    "CRITICAL LANGUAGE RULE: ALL output MUST be in English. If the input is in Turkish, Arabic, or any other language, "
    "you MUST translate it to fluent, professional English immediately. NEVER output non-English text.\n\n"
    
    "CRITICAL RULE 1 (DEDUPLICATION): If the INCOMING NEWS covers the same core event as the RECENTLY PUBLISHED STORIES, output ONLY: DUPLICATE\n"
    "CRITICAL RULE 2 (SPAM FILTER): If the input is primarily selling a product, a paid promotion, a referral link, "
    
    "STEP 3 — THE REWRITE (STRICT FORMATTING):\n"
    "1. FORMATTING & TAGGING (DECISION LOGIC):\n"
    "   - You MUST analyze the news content and pick ONE of these tags to start your post:\n"
    "     a) <b>🚨 JUST IN:</b> (Use for new, timely developments and unexpected announcements.)\n"
    "     b) <b>⚡ BREAKING:</b> (Use for major, high-impact events that shift market sentiment.)\n"
    "     c) <b>📊 MARKET ALERT:</b> (Use for price action, technical indicators, or on-chain data alerts.)\n"
    "2. LENGTH: MAXIMUM 2-3 SENTENCES in English.\n"
    "3. AI INSIGHT (MAXIMUM 2-3 SENTENCES): Provide a short, professional analysis on how this impacts the market or the asset.\n"
    "4. DATA INTEGRITY: Keep all numbers, prices, and percentages IDENTICAL to the source.\n"
    "5. CLEANING: Remove ALL URLs and redundant source citations.\n"
    
    "RECENTLY PUBLISHED STORIES:\n"
    "{recent_stories}\n\n"
    "INCOMING NEWS:\n"
    "{incoming_news}"
)

MARKET_INSIGHT_PROMPT = (
    "You are a professional crypto analyst for @Ledgexs. Create a market insight post for a trending token.\n\n"
    "FORMAT RULES:\n"
    "1. TITLE: Start with a catchy, bold headline in ALL CAPS (e.g., 🚀 $MYX EXPLOSIVE ON-CHAIN ACTIVITY!)\n"
    "2. THE EVENT (Max 3 sentences): Explain the specific trigger (huge wallet move, price pump/dump, or protocol update).\n"
    "3. ANALYSIS (Max 3 sentences): Provide a sharp, professional insight on potential next moves.\n"
    "4. LANGUAGE: Professional English.\n\n"
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


def _ai_dedup_and_rewrite(raw_text: str) -> str | None:
    if _ai_client is None:
        raise RuntimeError("AI client unavailable")

    with _dedup_lock:
        cache_snapshot = list(_dedup_cache)
        recent_stories = "\n---\n".join(f"{i+1}. {s}" for i, s in enumerate(cache_snapshot)) if cache_snapshot else "No recent stories."

    formatted_prompt = AI_COMBINED_PROMPT.format(
        recent_stories=recent_stories,
        incoming_news=raw_text
    )

    resp = _ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a professional crypto news editor. YOUR ONLY OUTPUT LANGUAGE IS ENGLISH. If input is Turkish, translate to English. Never output in any other language."},
            {"role": "user",   "content": formatted_prompt}, 
        ],
        temperature=0,
        max_tokens=300,
    )
    result = (resp.choices[0].message.content or "").strip()

    if not result:
        raise RuntimeError("AI returned empty string")

    if result.upper() == "DUPLICATE":
        logger.info("news_aggregator: duplicate detected — skipped.")
        return None          

    logger.info("news_aggregator: AI rewrite OK  %d → %d chars.", len(raw_text), len(result))
    return result

# ── Text helpers ──────────────────────────────────────────────────────────────

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACES_RE = re.compile(r"[ \t]{2,}")
_MULTI_NL_RE = re.compile(r"\n{3,}")

def _strip_html(text: str) -> str:
    clean = _HTML_TAG_RE.sub("", text)
    clean = _MULTI_SPACES_RE.sub(" ", clean)
    clean = _MULTI_NL_RE.sub("\n\n", clean)
    return clean.strip()

# ── Fallback cleaning patterns ────────────────────────────────────────────────

_URL_RE      = re.compile(r"https?://\S+|www\.\S+")
_MENTION_RE  = re.compile(r"@\w+")
_MD_LINK_RE  = re.compile(r"\[.*?\]\([^)]*\)?")
_ASTERISK_RE = re.compile(r"\*{1,4}")
_PAREN_RE    = re.compile(r"\(\s*(?:Twitter|X|Bloomberg|Reuters|WSJ|FT|CNBC|Forbes|BBC)\s*/?\w*\s*\)", re.IGNORECASE)
_SOURCE_RE   = re.compile(r"\b(cointelegraph|coindesk|watcherguru|watcher\s*guru|ninjanews|ninja\s*news|unfolded|fin_?watch|bitcoinmagazine|bitcoin\s*magazine|decrypt|theblock|blockworks|cryptoslate|cryptopotato)\b", re.IGNORECASE)
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

# ── Telegram poster ───────────────────────────────────────────────────────────

def _post_to_telegram(tg_text: str, media_paths: list[str]) -> None:
    if not _BOT_TOKEN or not _REQUESTS_OK:
        return

    base = f"https://api.telegram.org/bot{_BOT_TOKEN}"
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
            if file_size > 10485760: 
                logger.warning("news_aggregator: Single photo too large (%d bytes), sending text only.", file_size)
                resp = _requests.post(f"{base}/sendMessage", json={"chat_id": DEST_CHANNEL, "text": caption, "parse_mode": "HTML", "disable_notification": True, "disable_web_page_preview": True}, timeout=15)
            else:
                with open(media_paths[0], "rb") as fh:
                    resp = _requests.post(f"{base}/sendPhoto", data={"chat_id": DEST_CHANNEL, "caption": caption, "disable_notification": True, "parse_mode": "HTML"}, files={"photo": fh}, timeout=30)

        else:
            paths = media_paths[:TG_MAX_MEDIA]
            total_size = sum(os.path.getsize(p) for p in paths)
            
            if total_size > 50000000: 
                logger.warning("news_aggregator: Album too large (%d bytes), sending text only.", total_size)
                resp = _requests.post(f"{base}/sendMessage", json={"chat_id": DEST_CHANNEL, "text": caption, "parse_mode": "HTML", "disable_notification": True, "disable_web_page_preview": True}, timeout=15)
            else:
                media_json: list[dict] = []
                files: dict[str, Any] = {}
                for i, p in enumerate(paths):
                    key = f"photo{i}"
                    files[key] = open(p, "rb")
                    item: dict[str, Any] = {"type": "photo", "media": f"attach://{key}"}
                    if i == 0:
                        item["caption"]    = caption
                        item["parse_mode"] = "HTML"
                    media_json.append(item)

                resp = _requests.post(f"{base}/sendMediaGroup", data={"chat_id": DEST_CHANNEL, "media": json.dumps(media_json), "disable_notification": True}, files=files, timeout=45)
                for fh in files.values(): fh.close()

        if resp.ok:
            logger.info("news_aggregator: Posted to %s (%d media).", DEST_CHANNEL, len(media_paths))
        else:
            logger.warning("news_aggregator: Telegram post failed %s: %s", resp.status_code, resp.text[:300])
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

# ── Twitter / X poster ────────────────────────────────────────────────────────

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
            _twitter_v2.create_tweet(text=tweet, media_ids=media_ids)
        else:
            _twitter_v2.create_tweet(text=tweet)
        logger.info("news_aggregator: Cross-posted to X (%d chars, %d media).", len(tweet), len(media_ids))
    except Exception as exc:
        logger.warning("news_aggregator: X post failed: %s", exc)

# ── Core news handler ─────────────────────────────────────────────────────────

async def _handle_news(raw_text: str, messages: list[Any], tg_client: Any) -> None:
    if len(raw_text.strip()) < MIN_TEXT_LEN:
        return

    rewritten: str | None
    try:
        rewritten = _ai_dedup_and_rewrite(raw_text)
        if rewritten is None:
            return
        if len(rewritten.strip()) < 5:
            raise RuntimeError(f"AI output too short: {rewritten!r}")
    except Exception as exc:
        logger.warning("news_aggregator: AI call failed (%s) — using fallback.", exc)
        rewritten = _fallback_rewrite(raw_text)
        if not rewritten:
            logger.warning("news_aggregator: fallback also empty — dropping message.")
            return

    _cache_add(_strip_html(rewritten))

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

    # ── KRİTİK DEĞİŞİKLİK BURADA ──────────────────────────────────────────────
    try:
        clean_tg_text = _remove_hashtags(rewritten)
        # Telegram'a her halükarda (medyalı veya medyasız) gönderiyoruz
        _post_to_telegram(clean_tg_text, media_paths)
        
        # Twitter'a YALNIZCA listede indirilen bir medya varsa gönderiyoruz
        if media_paths:
            _post_to_twitter(rewritten, media_paths)
        else:
            logger.info("news_aggregator: Text-only news detected. Sent to Telegram, skipped for X.")
            
    finally:
        _cleanup_media_dir()

async def _periodic_market_analysis(tg_client):
    loop = asyncio.get_running_loop()
    while True:
        try:
            await asyncio.sleep(14400) 
            trending_coins = await _get_trending_coins()
            
            for coin in trending_coins:
                market_data = await loop.run_in_executor(None, partial(_get_market_data_sync, coin))
                prompt = MARKET_INSIGHT_PROMPT.format(data=market_data)
                analysis_text = await _call_gpt_for_analysis(prompt)
    
                final_message = f"{analysis_text}{TELEGRAM_SIG}"
                await tg_client.send_message('@Ledgexs', final_message, parse_mode='html')
    
                await asyncio.sleep(5)

def _get_market_data_sync(coin: str) -> str:
    # Bu senkron bir fonksiyon, run_in_executor bunu rahatça çalıştırır
    try:
        exchange = ccxt.binance()
        ticker = exchange.fetch_ticker(f"{coin}/USDT")
        ohlcv = exchange.fetch_ohlcv(f"{coin}/USDT", timeframe='4h', limit=5)
        price = ticker['last']
        change = ticker['percentage']
        volume = ticker['baseVolume']
        return f"Asset: {coin}/USDT | Price: {price} | 24h Change: {change}% | Volume: {volume} | Recent Trend: {[c[4] for c in ohlcv]}"
    except Exception as e:
        return f"Market data for {coin} unavailable: {e}"

# ── Telethon async client ─────────────────────────────────────────────────────

async def _run_news_client() -> None:
    if not _TELETHON_OK:
        return

    if not all([_API_ID, _API_HASH, _SESSION_STR]):
        logger.warning(
            "news_aggregator: TELEGRAM_API_ID / TELEGRAM_API_HASH / TELETHON_SESSION "
            "not configured — news aggregator disabled.\n"
            "  1. Get API credentials at https://my.telegram.org\n"
            "  2. Run  python bot/gen_session.py  to generate TELETHON_SESSION\n"
            "  3. Add all three values to Replit Secrets and restart."
        )
        return

    client = TelegramClient(
        StringSession(_SESSION_STR), 
        int(_API_ID), 
        _API_HASH,
        connection_retries=None,  # Bağlantı koparsa sonsuza kadar yeniden denesin
        retry_delay=5,            # Denemeler arası 5 saniye beklesin
        auto_reconnect=True       # Ağ gelince otomatik yeniden bağlansın
    )

    asyncio.create_task(_periodic_market_analysis(client))

    pending_groups: dict[int, list[Any]] = {}
    pending_tasks:  dict[int, asyncio.Task] = {}  # type: ignore[type-arg]

    async def _process_group(grouped_id: int) -> None:
        await asyncio.sleep(GROUP_COLLECT_S)
        msgs  = pending_groups.pop(grouped_id, [])
        pending_tasks.pop(grouped_id, None)
        if not msgs:
            return
        raw = next((m.text for m in msgs if m.text), "").strip()
        try:
            await _handle_news(raw, msgs, client)
        except Exception as exc:
            logger.warning("news_aggregator: group handler error: %s", exc)

    @client.on(events.NewMessage(chats=SOURCE_CHANNELS))
    async def _on_new_message(event: events.NewMessage.Event) -> None:
        msg = event.message
        raw = (msg.text or "").strip()
        grouped_id: int | None = getattr(msg, "grouped_id", None)

        channel_name = event.chat.username if event.chat and hasattr(event.chat, 'username') else "unknown"

        if channel_name == "cointelegraph":
            if not raw.upper().startswith("JUST IN:"):
                return

        if channel_name == "bitcoinmagazinetelegram":
            raw = re.sub(r'^Bitcoin Magazine\s*\(Twitter/X\)\s*', '', raw, flags=re.IGNORECASE).strip()

        try:
            if grouped_id is not None:
                if grouped_id not in pending_groups:
                    pending_groups[grouped_id] = []
                pending_groups[grouped_id].append(msg)

                existing = pending_tasks.get(grouped_id)
                if existing and not existing.done():
                    existing.cancel()
                pending_tasks[grouped_id] = asyncio.create_task(_process_group(grouped_id))
            else:
                await _handle_news(raw, [msg], client)

        except Exception as exc:
            logger.warning("news_aggregator: event handler error (non-fatal): %s", exc)

    try:
        await client.start()
        logger.info("news_aggregator: Warming up entity cache by fetching dialogs...")
        await client.get_dialogs()
        logger.info("news_aggregator: Telethon UserBot connected. Listening to: %s", SOURCE_CHANNELS)
        await client.run_until_disconnected()
    except Exception as exc:
        logger.warning("news_aggregator: Telethon client error: %s", exc)
    finally:
        for task in pending_tasks.values():
            task.cancel()
        try:
            await client.disconnect()
        except Exception:
            pass

# ── Thread entry-point ────────────────────────────────────────────────────────

def _news_thread_target() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(_run_news_client())
        except Exception as exc:
            logger.warning("news_aggregator: event loop crashed (%s) — restarting in 60 s.", exc)
        time.sleep(60)

def start_news_aggregator() -> threading.Thread:
    t = threading.Thread(target=_news_thread_target, daemon=True, name="NewsAggregator")
    t.start()
    logger.info("News aggregator thread started.")
    return t
