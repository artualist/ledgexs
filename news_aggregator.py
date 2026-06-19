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

Required Replit Secrets
-----------------------
  TELEGRAM_API_ID       – Telegram API app ID   (https://my.telegram.org)
  TELEGRAM_API_HASH     – Telegram API app hash
  TELETHON_SESSION      – StringSession string (run bot/gen_session.py once)
  AI_INTEGRATIONS_OPENAI_BASE_URL  – auto-set by Replit AI integration
  AI_INTEGRATIONS_OPENAI_API_KEY   – auto-set by Replit AI integration

Optional (silently disabled if absent)
---------------------------------------
  BOT_TOKEN             – posts to @Ledgexs via Telegram Bot API
  TWITTER_API_KEY, TWITTER_API_SECRET,
  TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET
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

# ── Config ────────────────────────────────────────────────────────────────────

SOURCE_CHANNELS: list[str] = [
    "@cointelegraph",
    "@fin_watch",
    "@bitcoinmagazinetelegram",
    "@yoyodexhaber",
    "@unfolded",
    "@ninjanewstr",
    "@WatcherGuru",
]
DEST_CHANNEL      = "@Ledgexs"
TELEGRAM_SIG      = "\n\n@Ledgexs"   # appended only to Telegram posts
MIN_TEXT_LEN      = 30               # skip media-only / trivially short messages
TWEET_MAX         = 280
TWITTER_MAX_MEDIA = 4                # Twitter hard limit
TG_MAX_MEDIA      = 10               # Telegram sendMediaGroup hard limit
DEDUP_CACHE_SIZE  = 30               # rolling window of recent summaries
GROUP_COLLECT_S   = 1.2              # seconds to wait for all album frames to arrive
MEDIA_DIR         = Path("/tmp/news_media")

# ── AI prompts ────────────────────────────────────────────────────────────────

AI_COMBINED_PROMPT = (
    "You are the official content writer for @Ledgexs, a global crypto intelligence channel.\n\n"
    "STEP 1 — DEDUPLICATION (only if a cache is provided):\n"
    "If RECENTLY PUBLISHED STORIES are listed below, check whether the INCOMING NEWS covers "
    "the EXACT same technical or market event (same token, same price move, same announcement, "
    "same hack). If it is a duplicate, reply with ONLY the single word: DUPLICATE\n\n"
    "STEP 2 — REWRITE (if unique or no cache):\n"
    "Rewrite the INCOMING NEWS as a ready-to-publish post following these STRICT rules:\n\n"
    "1. OUTPUT LANGUAGE: Always English. Translate Turkish or any other language into clean, "
    "fluent global English.\n\n"
    "2. AGGRESSIVE TEXT CLEANING (CRITICAL — apply before everything else):\n"
    "   • Strip ALL website URLs and hyperlinks (http://, https://, www.).\n"
    "   • Strip ALL markdown link syntax — including patterns like [****](, [text](url), etc.\n"
    "   • Strip ALL placeholder asterisks and empty bold markers (e.g. ****, **, *).\n"
    "   • Strip ALL source channel usernames and brand names "
    "(cointelegraph, coindesk, ninjanews, unfolded, WatcherGuru, fin_watch, "
    "bitcoinmagazine, decrypt, theblock, blockworks, cryptoslate, etc.).\n"
    "   • Remove parenthetical platform labels such as (Twitter/X), (Bloomberg), (Reuters), etc.\n"
    "   • Collapse any double/triple spaces left behind into a single space.\n\n"
    "3. DYNAMIC HEADER (choose ONE — all on the SAME LINE as the news body, no newline after):\n"
    "   • Scan the cleaned text for a strong news prefix: "
    "BREAKING, NEW, ALERT, UPDATE, URGENT, SHOCKING, DEVELOPING, EXCLUSIVE, OFFICIAL.\n"
    "   • If one is found: use it as the header in HTML bold with the siren emoji. "
    "Example: 🚨 <b>BREAKING:</b> El Salvador purchases more Bitcoin.\n"
    "   • If NONE is found: default to: 🚨 <b>JUST IN:</b> [news body]\n"
    "   • The emoji, the bold header tag, and the news body must all be on ONE single line — "
    "no line break after the header.\n\n"
    "4. BODY: Exactly 1–2 sentences of clean, rewritten news continuing directly after the "
    "header on the same line. High-impact, professional, no hashtags.\n\n"
    "5. FORMAT: Plain text with ONLY the one HTML bold tag around the header word. "
    "No asterisks, no underscores, no markdown, no hashtags, no signature.\n\n"
    "6. OUTPUT: The finished post and nothing else — no preamble, no commentary, no labels."
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
    """Return (tweepy.Client v2, tweepy.API v1.1) or (None, None)."""
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
    """Single AI call that deduplicates AND rewrites in one shot.

    Returns:
      None      — the story is a duplicate (skip it)
      str       — the finished, ready-to-publish rewritten post
    Raises RuntimeError if the AI client is unavailable (caller uses fallback).
    """
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
        model="gpt-5-mini",
        messages=[
            {"role": "system", "content": AI_COMBINED_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        max_completion_tokens=420,
    )
    result = (resp.choices[0].message.content or "").strip()

    if not result:
        raise RuntimeError("AI returned empty string")

    if result.upper() == "DUPLICATE":
        logger.info("news_aggregator: duplicate detected — skipped.")
        return None          # signal: skip this story

    logger.info("news_aggregator: AI rewrite OK  %d → %d chars.", len(raw_text), len(result))
    return result

# ── Text helpers ──────────────────────────────────────────────────────────────

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_WS_RE = re.compile(r"\s{2,}")


def _strip_html(text: str) -> str:
    """Strip HTML tags and collapse whitespace — safe for Twitter plain text."""
    clean = _HTML_TAG_RE.sub("", text)
    return _MULTI_WS_RE.sub(" ", clean).strip()


# ── Fallback cleaning patterns ────────────────────────────────────────────────

_URL_RE      = re.compile(r"https?://\S+|www\.\S+")
_MENTION_RE  = re.compile(r"@\w+")
# Malformed markdown link blocks: [anything]( with optional trailing )
_MD_LINK_RE  = re.compile(r"\[.*?\]\([^)]*\)?")
# Bare asterisk clusters left after stripping markdown
_ASTERISK_RE = re.compile(r"\*{1,4}")
# Parenthetical platform labels: (Twitter/X), (Bloomberg), (Reuters), …
_PAREN_RE    = re.compile(
    r"\(\s*(?:Twitter|X|Bloomberg|Reuters|WSJ|FT|CNBC|Forbes|BBC)\s*/?\w*\s*\)",
    re.IGNORECASE,
)
_SOURCE_RE = re.compile(
    r"\b(cointelegraph|coindesk|watcherguru|watcher\s*guru|ninjanews|ninja\s*news|"
    r"unfolded|fin_?watch|bitcoinmagazine|bitcoin\s*magazine|decrypt|theblock|"
    r"blockworks|cryptoslate|cryptopotato)\b",
    re.IGNORECASE,
)
_SENTENCE_SEP = re.compile(r"(?<=[.!?])\s+")

# Strip ONLY "JUST IN" pre-headers (the generic default) added by source channels.
# e.g. "📰 JUST IN:" or "🔴 JUST IN:" — strong keywords (BREAKING, ALERT…) are preserved.
_EXISTING_HEADER_RE = re.compile(
    r"^[\U0001F000-\U0001FFFF\u2600-\u26FF\u2700-\u27BF"
    r"\U0001F300-\U0001F9FF\U0001FA00-\U0001FAFF]*"
    r"\s*JUST\s*IN\s*:?\s*",
    re.IGNORECASE,
)

# Detect strong prefixes AFTER cleaning — includes optional leading emoji
# so "🚨 BREAKING:" and bare "ALERT:" are both matched.
_STRONG_PREFIX_RE = re.compile(
    r"^[\U0001F000-\U0001FFFF\u2600-\u26FF\u2700-\u27BF"
    r"\U0001F300-\U0001F9FF\U0001FA00-\U0001FAFF]*"
    r"\s*(BREAKING|NEW|ALERT|UPDATE|URGENT|SHOCKING|DEVELOPING|EXCLUSIVE|OFFICIAL)"
    r"\s*:?\s*",
    re.IGNORECASE,
)


def _clean_text(text: str) -> str:
    """Apply all stripping rules to raw source text, returning clean plain text.

    Order matters: strip structured garbage first, then symbols, then whitespace.
    """
    text = _EXISTING_HEADER_RE.sub("", text)   # remove pre-existing channel headers
    text = _MD_LINK_RE.sub(" ", text)           # [****]( … ) markdown links
    text = _URL_RE.sub(" ", text)               # bare URLs
    text = _MENTION_RE.sub(" ", text)           # @usernames
    text = _ASTERISK_RE.sub(" ", text)          # leftover * clusters
    text = _PAREN_RE.sub(" ", text)             # (Twitter/X) etc.
    text = _SOURCE_RE.sub(" ", text)            # source brand names
    text = _MULTI_WS_RE.sub(" ", text).strip()
    return text


def _pick_header(clean_text: str) -> tuple[str, str]:
    """Return (header_html, remaining_body).

    Promotes BREAKING/NEW/ALERT/… found at the start to a bold HTML header.
    Defaults to JUST IN if no strong prefix is present.
    """
    m = _STRONG_PREFIX_RE.match(clean_text)
    if m:
        keyword = m.group(1).upper()
        body    = clean_text[m.end():].strip()
        return f"🚨 <b>{keyword}:</b>", body
    
    # EĞER HABER ZATEN "JUST IN" İÇERİYORSA BİR DAHA EKLEME
    if clean_text.upper().startswith("JUST IN"):
        # "JUST IN" kısmını temizle, sadece gövdeyi al, emojiyi ekle
        body = clean_text[7:].strip(" :") 
        return "🚨 <b>JUST IN:</b>", body

    # İçermiyorsa normal şekilde ekle
    return "🚨 <b>JUST IN:</b>", clean_text


def _fallback_rewrite(raw_text: str) -> str:
    """Regex-based fallback when AI returns empty/None.

    Applies aggressive cleaning, picks a dynamic header, and takes the first
    2 sentences. Never returns an empty string if the input had usable content.
    """
    text = _clean_text(raw_text)
    header, body = _pick_header(text)

    sentences = [s.strip() for s in _SENTENCE_SEP.split(body) if s.strip()]
    body = " ".join(sentences[:2]) if sentences else body[:300].strip()

    if not body:
        return ""

    logger.info("news_aggregator: using regex fallback rewrite (%d chars).", len(body))
    return f"{header} {body}"

# ── Media helpers ─────────────────────────────────────────────────────────────

def _ensure_media_dir() -> None:
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)


def _cleanup_media_dir() -> None:
    """Delete all files inside MEDIA_DIR (not the dir itself)."""
    try:
        if MEDIA_DIR.exists():
            shutil.rmtree(str(MEDIA_DIR))
            MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.debug("news_aggregator: media cleanup error: %s", exc)

# ── Telegram poster ───────────────────────────────────────────────────────────

def _post_to_telegram(tg_text: str, media_paths: list[str]) -> None:
    """Send AI-rewritten post + optional media to DEST_CHANNEL via Bot API."""
    if not _BOT_TOKEN or not _REQUESTS_OK:
        return

    base = f"https://api.telegram.org/bot{_BOT_TOKEN}"
    caption = tg_text + TELEGRAM_SIG   # signature only on Telegram

    try:
        if not media_paths:
            # ── Text-only ─────────────────────────────────────────────────────
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
            # ── Single photo ──────────────────────────────────────────────────
            with open(media_paths[0], "rb") as fh:
                resp = _requests.post(
                    f"{base}/sendPhoto",
                    data={
                        "chat_id": DEST_CHANNEL,
                        "caption": caption,
                        "parse_mode": "HTML",
                    },
                    files={"photo": fh},
                    timeout=30,
                )

        else:
            # ── Media album (sendMediaGroup) ──────────────────────────────────
            paths = media_paths[:TG_MAX_MEDIA]
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

            resp = _requests.post(
                f"{base}/sendMediaGroup",
                data={
                    "chat_id": DEST_CHANNEL,
                    "media": json.dumps(media_json),
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
                resp.status_code, resp.text[:300],
            )
            # Fallback: try text-only
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
    """Upload media (if any) and post to X. No @Ledgexs signature."""
    if _twitter_v2 is None:
        return

    # Plain text for Twitter — strip HTML, no signature
    plain = _strip_html(rewritten_text)
    tweet = (plain[: TWEET_MAX - 1] + "…") if len(plain) > TWEET_MAX else plain

    # Upload media via v1.1 API (max 4 images)
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
        logger.info(
            "news_aggregator: Cross-posted to X (%d chars, %d media).",
            len(tweet), len(media_ids),
        )
    except Exception as exc:
        logger.warning("news_aggregator: X post failed: %s", exc)

# ── Core news handler (async — runs inside the Telethon event loop) ───────────

async def _handle_news(
    raw_text: str,
    messages: list[Any],
    tg_client: Any,
) -> None:
    """Full pipeline for one news item (single message or grouped album)."""
    if len(raw_text.strip()) < MIN_TEXT_LEN:
        return

    # 1. Combined dedup + AI rewrite (single call)
    rewritten: str | None
    try:
        rewritten = _ai_dedup_and_rewrite(raw_text)
        if rewritten is None:
            return   # confirmed duplicate — stop here
        if len(rewritten.strip()) < 5:
            raise RuntimeError(f"AI output too short: {rewritten!r}")
    except Exception as exc:
        # AI unavailable or returned garbage — use regex fallback
        logger.warning("news_aggregator: AI call failed (%s) — using fallback.", exc)
        rewritten = _fallback_rewrite(raw_text)
        if not rewritten:
            logger.warning("news_aggregator: fallback also empty — dropping message.")
            return

    # 2. Add plain-text summary to dedup cache
    _cache_add(_strip_html(rewritten))

    # 4. Download media from all messages in the group
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

    # 5. Publish
    try:
        _post_to_telegram(rewritten, media_paths)
        _post_to_twitter(rewritten, media_paths)
    finally:
        # 6. Always clean up temp files
        _cleanup_media_dir()

# ── Telethon async client ─────────────────────────────────────────────────────

async def _run_news_client() -> None:
    """Async core: connect the Telethon UserBot and listen for new messages."""
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
    )

    # pending_groups[grouped_id] = list of messages collected so far
    pending_groups: dict[int, list[Any]] = {}
    # pending_tasks[grouped_id] = asyncio.Task scheduled to process the group
    pending_tasks:  dict[int, asyncio.Task] = {}  # type: ignore[type-arg]

    async def _process_group(grouped_id: int) -> None:
        """Called after GROUP_COLLECT_S seconds — process the completed album."""
        await asyncio.sleep(GROUP_COLLECT_S)
        msgs  = pending_groups.pop(grouped_id, [])
        pending_tasks.pop(grouped_id, None)
        if not msgs:
            return
        # Use the text from whichever message in the group has it
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
                # ── Album: accumulate and debounce ────────────────────────────
                if grouped_id not in pending_groups:
                    pending_groups[grouped_id] = []
                pending_groups[grouped_id].append(msg)

                # Cancel the previous timer and start fresh
                existing = pending_tasks.get(grouped_id)
                if existing and not existing.done():
                    existing.cancel()
                pending_tasks[grouped_id] = asyncio.create_task(
                    _process_group(grouped_id)
                )
            else:
                # ── Single message ────────────────────────────────────────────
                await _handle_news(raw, [msg], client)

        except Exception as exc:
            logger.warning("news_aggregator: event handler error (non-fatal): %s", exc)

    try:
        await client.start()
        logger.info(
            "news_aggregator: Telethon UserBot connected. Listening to: %s",
            SOURCE_CHANNELS,
        )
        await client.run_until_disconnected()
    except Exception as exc:
        logger.warning("news_aggregator: Telethon client error: %s", exc)
    finally:
        # Cancel any pending group tasks on disconnect
        for task in pending_tasks.values():
            task.cancel()
        try:
            await client.disconnect()
        except Exception:
            pass

# ── Thread entry-point ────────────────────────────────────────────────────────

def _news_thread_target() -> None:
    """Runs in a daemon thread. Creates its own asyncio loop, auto-restarts on crash."""
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
    """Launch the news aggregator in a background daemon thread.

    Completely safe to call from main.py — any exception inside the module
    is isolated and will never crash or block the main bot thread.
    """
    t = threading.Thread(
        target=_news_thread_target, daemon=True, name="NewsAggregator"
    )
    t.start()
    logger.info("News aggregator thread started.")
    return t
