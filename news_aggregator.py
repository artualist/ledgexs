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
from collections import deque
from pathlib import Path
from typing import Any

logger = logging.getLogger("whale_bot.news")

def _remove_hashtags(text: str) -> str:
    # # ve $ ile başlayan, boşlukla biten ifadeleri temizler
    return re.sub(r'(#|\$)\w+', '', text).strip()

# ── Config ────────────────────────────────────────────────────────────────────

SOURCE_CHANNELS: list[str] = [
    "@cointelegraph",
    "@fin_watch",
    "@bitcoinmagazinetelegram",
    "@yoyodexhaber",
    "@unfolded",
    "@ninjanewstr",
    "@WatcherGuru",
    "@coinmuhendisihaber",
    "@news_crypto",
    "@coinbureau",
    "@jrkripto",
]
DEST_CHANNEL      = "@Ledgexs"
TELEGRAM_SIG      = "\n\n@Ledgexs"   # appended only to Telegram posts
MIN_TEXT_LEN      = 30               # skip media-only / trivially short messages
TWEET_MAX         = 25000
TWITTER_MAX_MEDIA = 4                # Twitter hard limit
TG_MAX_MEDIA      = 10               # Telegram sendMediaGroup hard limit
DEDUP_CACHE_SIZE  = 30               # rolling window of recent summaries
GROUP_COLLECT_S   = 1.2              # seconds to wait for all album frames to arrive
MEDIA_DIR         = Path("/tmp/news_media")

# ── AI prompts ────────────────────────────────────────────────────────────────

AI_COMBINED_PROMPT = (
    "You are the official English content writer for @Ledgexs, a global crypto intelligence channel.\n\n"
    "CRITICAL RULE: REJECT ADVERTISEMENTS AND SPONSORED CONTENT.\n"
    "CRITICAL RULE: NEVER ALTER NUMERICAL DATA. Price values like '67,000' or '1.5 million' must remain exactly as they appear in the source. Do not remove digits, commas, or decimal points from numbers."
    "If the incoming news is an advertisement, a promotional article, a 'sponsored by', "
    "a partner post, or a crypto marketing announcement, reply ONLY with the word: DUPLICATE\n\n"
    "STEP 1 — DEDUPLICATION:\n"
    "If RECENTLY PUBLISHED STORIES are listed below, check whether the INCOMING NEWS covers "
    "(e.g., 'Cointelegraph reports', 'WatcherGuru says'), treat it as a promotional/source-branded post "
    "and reply ONLY with: DUPLICATE\n\n"
    "STEP 2 — REWRITE:\n"
    "Rewrite the INCOMING NEWS following these STRICT rules:\n\n"
    "1. LANGUAGE: STRICTLY GLOBAL ENGLISH. Translate any foreign language to fluent, professional English.\n\n"
    "2. CLEANING:\n"
    "   • Strip ALL URLs (http://...), markdown links, source names (cointelegraph, WatcherGuru, etc.), and platform names (Twitter/X, Bloomberg).\n"
    "   • Keep professional news markers like 'JUST IN', 'BREAKING', or 'ALERT' only if they are directly related to the news delivery. "
    "     Do not add *extra* artificial emojis or sirens. Keep the news style clean but urgent.\n"
    "   • If the original text naturally starts with strong words like BREAKING or NEW, you may keep their English translation, but DO NOT force them.\n\n"
    "3. FORMAT:\n"
    "   • STRUCTURE: Reflect the complexity of the news. If the content is detailed, use 2 clear paragraphs. "
    "     If it is a brief update, use a single concise paragraph.\n"
    "   • PRESERVATION: Do not artificially truncate or cram long information into a single line.\n"
    "   • Append 3-4 highly relevant cashtags/hashtags at the very end of the last paragraph. Separate them with a space."
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
        _dedup_cache.append(summary[:200])


def _ai_dedup_and_rewrite(raw_text: str) -> str | None:
    if _ai_client is None:
        raise RuntimeError("AI client unavailable")

    with _dedup_lock:
        cache_snapshot = list(_dedup_cache)

    if cache_snapshot:
        cache_text = "\n---\n".join(f"{i+1}. {s}" for i, s in enumerate(cache_snapshot))
        user_msg = (
            f"RECENTLY PUBLISHED STORIES:\n{cache_text}\n\n"
            f"INCOMING NEWS:\n{raw_text}"
        )
    else:
        user_msg = f"INCOMING NEWS:\n{raw_text}"

    resp = _ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": AI_COMBINED_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        max_tokens=420,
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
_MULTI_WS_RE = re.compile(r"\s{2,}")

def _strip_html(text: str) -> str:
    clean = _HTML_TAG_RE.sub("", text)
    return _MULTI_WS_RE.sub(" ", clean).strip()

# ── Fallback cleaning patterns ────────────────────────────────────────────────

_URL_RE      = re.compile(r"https?://\S+|www\.\S+")
_MENTION_RE  = re.compile(r"@\w+")
_MD_LINK_RE  = re.compile(r"\[.*?\]\([^)]*\)?")
_ASTERISK_RE = re.compile(r"\*{1,4}")
_PAREN_RE    = re.compile(r"\(\s*(?:Twitter|X|Bloomberg|Reuters|WSJ|FT|CNBC|Forbes|BBC)\s*/?\w*\s*\)", re.IGNORECASE)
_SOURCE_RE   = re.compile(r"\b(cointelegraph|coindesk|watcherguru|watcher\s*guru|ninjanews|ninja\s*news|unfolded|fin_?watch|bitcoinmagazine|bitcoin\s*magazine|decrypt|theblock|blockworks|cryptoslate|cryptopotato)\b", re.IGNORECASE)
_SENTENCE_SEP = re.compile(r"(?<=[.!?])\s+")


def _clean_text(text: str) -> str:
    text = _MD_LINK_RE.sub(" ", text)
    text = _URL_RE.sub(" ", text)
    
    # Mention'ları sadece "kelime sınırı" olan yerlerden sil (sayısal veriye dokunma)
    text = re.sub(r'(?<!\d)@\w+', ' ', text) 
    
    # Diğer temizlikler
    text = _ASTERISK_RE.sub(" ", text)
    text = _PAREN_RE.sub(" ", text)
    
    # SOURCE_RE'yi kullanırken kelime sınırlarına dikkat et
    text = _SOURCE_RE.sub(" ", text)
    
    # Sayısal karakterleri (0-9), nokta (.) ve virgül (,) içeren blokları 
    # temizlemeden sadece boşlukları düzeltiyoruz:
    text = _MULTI_WS_RE.sub(" ", text).strip()
    return text


def _fallback_rewrite(raw_text: str) -> str:
    text = _clean_text(raw_text)
    sentences = [s.strip() for s in _SENTENCE_SEP.split(text) if s.strip()]
    body = " ".join(sentences[:2]) if sentences else text[:300].strip()

    if not body:
        return ""

    logger.info("news_aggregator: using regex fallback rewrite (%d chars).", len(body))
    return body

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
    caption = tg_text + TELEGRAM_SIG

    try:
        if not media_paths:
            resp = _requests.post(
                f"{base}/sendMessage",
                json={
                    "chat_id": DEST_CHANNEL,
                    "text": caption,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )

        elif len(media_paths) == 1:
            file_size = os.path.getsize(media_paths[0])
            if file_size > 10485760: # 10MB sınırı
                logger.warning("news_aggregator: Single photo too large (%d bytes), sending text only.", file_size)
                resp = _requests.post(f"{base}/sendMessage", json={"chat_id": DEST_CHANNEL, "text": caption, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=15)
            else:
                with open(media_paths[0], "rb") as fh:
                    resp = _requests.post(f"{base}/sendPhoto", data={"chat_id": DEST_CHANNEL, "caption": caption, "parse_mode": "HTML"}, files={"photo": fh}, timeout=30)

        else:
            # Albüm (MediaGroup) durumu
            paths = media_paths[:TG_MAX_MEDIA]
            # Önce toplam boyuta bakalım, çok büyükse albümü komple iptal edip sadece metin atalım
            total_size = sum(os.path.getsize(p) for p in paths)
            
            if total_size > 50000000: # 50MB toplam sınır
                logger.warning("news_aggregator: Album too large (%d bytes), sending text only.", total_size)
                resp = _requests.post(f"{base}/sendMessage", json={"chat_id": DEST_CHANNEL, "text": caption, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=15)
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

                resp = _requests.post(f"{base}/sendMediaGroup", data={"chat_id": DEST_CHANNEL, "media": json.dumps(media_json)}, files=files, timeout=45)
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

    try:
        # Telegram için hashtag'leri ayıklıyoruz:
        clean_tg_text = _remove_hashtags(rewritten)
        _post_to_telegram(clean_tg_text, media_paths)
        
        # Twitter'da kalsın istediğin için orijinali gönderiyoruz:
        _post_to_twitter(rewritten, media_paths) 
    finally:
        _cleanup_media_dir()

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

    client = TelegramClient(StringSession(_SESSION_STR), int(_API_ID), _API_HASH)

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
