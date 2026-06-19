import sqlite3
import os
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

DB_PATH = "/data/whale.db"
_lock = threading.Lock()  # Lock'u en başa aldık

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db() -> None:
    # 1. Klasör yapısını oluştur
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # 2. Yazma yetkisini kontrol et
    if not os.access(os.path.dirname(DB_PATH), os.W_OK):
        print(f"HATA: {os.path.dirname(DB_PATH)} klasörüne yazma yetkin yok!")
        return

    # 3. Veritabanını başlat
    with _lock, _conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                uid INTEGER PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                premium INTEGER DEFAULT 0,
                premium_expiry TEXT,
                digest_hour INTEGER DEFAULT NULL,
                last_digest_date TEXT DEFAULT NULL,
                language TEXT DEFAULT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );
            /* ... diğer CREATE TABLE komutlarını buraya aynen ekle ... */
        """)

def init_db() -> None:
    with _lock, _conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                uid              INTEGER PRIMARY KEY,
                chat_id          INTEGER NOT NULL,
                premium          INTEGER DEFAULT 0,
                premium_expiry   TEXT,
                digest_hour      INTEGER DEFAULT NULL,
                last_digest_date TEXT    DEFAULT NULL,
                language         TEXT    DEFAULT NULL,
                created_at       TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS watchlist (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                uid           INTEGER NOT NULL,
                chain         TEXT    NOT NULL DEFAULT 'eth',
                ca            TEXT    NOT NULL,
                name          TEXT    NOT NULL,
                symbol        TEXT    NOT NULL,
                decimals      INTEGER DEFAULT 18,
                threshold_usd REAL    DEFAULT 10000.0,
                paused        INTEGER DEFAULT 0,
                added_at      TEXT    DEFAULT (datetime('now')),
                UNIQUE(uid, chain, ca)
            );

            CREATE TABLE IF NOT EXISTS pending_payments (
                uid          INTEGER PRIMARY KEY,
                amount_units INTEGER NOT NULL UNIQUE,
                created_at   INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                chain       TEXT,
                ca          TEXT,
                token_name  TEXT,
                symbol      TEXT,
                amount_tok  REAL,
                amount_usd  REAL,
                sender      TEXT,
                receiver    TEXT,
                tx_hash     TEXT,
                block       INTEGER,
                alert_type  TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            );
        """)

        # ── ESKİ VERİLERİ KURTARMA SORGULARI ─────────────────────────────────
        try:
            c.execute(
                "UPDATE watchlist SET ca = LOWER(TRIM(ca)), chain = LOWER(TRIM(chain));"
            )
            c.execute(
                "UPDATE alerts SET ca = LOWER(TRIM(ca)), chain = LOWER(TRIM(chain)), sender = LOWER(TRIM(sender)), receiver = LOWER(TRIM(receiver));"
            )
        except Exception:
            pass
        # ─────────────────────────────────────────────────────────────────────

        # Safe migrations for existing databases
        for col_sql in [
            "ALTER TABLE users ADD COLUMN digest_hour      INTEGER DEFAULT NULL",
            "ALTER TABLE users ADD COLUMN last_digest_date TEXT    DEFAULT NULL",
            "ALTER TABLE users ADD COLUMN language         TEXT    DEFAULT 'en'",
            "ALTER TABLE watchlist ADD COLUMN wallet_label TEXT    DEFAULT NULL",
        ]:
            try:
                c.execute(col_sql)
            except Exception:
                pass  # Column already exists


# ── Pending payments ────────────────────────────────────────────────────────


def cleanup_expired_payments() -> None:
    """Delete payment reservations older than PAYMENT_TTL_SECONDS."""
    cutoff = int(time.time()) - PAYMENT_TTL_SECONDS
    with _lock, _conn() as c:
        c.execute("DELETE FROM pending_payments WHERE created_at < ?", (cutoff,))


def is_amount_reserved(amount_units: int) -> bool:
    """Return True if amount_units is already reserved by an active (non-expired) user."""
    cutoff = int(time.time()) - PAYMENT_TTL_SECONDS
    with _conn() as c:
        row = c.execute(
            "SELECT 1 FROM pending_payments WHERE amount_units=? AND created_at >= ?",
            (amount_units, cutoff),
        ).fetchone()
    return row is not None


def reserve_payment(uid: int, amount_units: int) -> None:
    """Reserve amount_units for uid, replacing any previous reservation for that user."""
    cleanup_expired_payments()
    with _lock, _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO pending_payments(uid, amount_units, created_at) VALUES(?,?,?)",
            (uid, amount_units, int(time.time())),
        )


def get_pending_payment(uid: int) -> int | None:
    """Return the still-valid reserved amount_units for uid, or None if expired/absent."""
    cutoff = int(time.time()) - PAYMENT_TTL_SECONDS
    with _conn() as c:
        row = c.execute(
            "SELECT amount_units FROM pending_payments WHERE uid=? AND created_at >= ?",
            (uid, cutoff),
        ).fetchone()
    return row["amount_units"] if row else None


def release_payment(uid: int) -> None:
    """Remove any payment reservation for uid (called on success or explicit cancel)."""
    with _lock, _conn() as c:
        c.execute("DELETE FROM pending_payments WHERE uid=?", (uid,))


# ── Users ───────────────────────────────────────────────────────────────────


def upsert_user(uid: int, chat_id: int) -> None:
    with _lock, _conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO users(uid, chat_id) VALUES(?,?)",
            (uid, chat_id),
        )
        c.execute("UPDATE users SET chat_id=? WHERE uid=?", (chat_id, uid))


def get_user(uid: int) -> dict[str, Any] | None:
    with _conn() as c:
        row = c.execute("SELECT * FROM users WHERE uid=?", (uid,)).fetchone()
    return dict(row) if row else None


def is_premium(uid: int) -> bool:
    user = get_user(uid)
    if not user or not user["premium"]:
        return False
    exp = user.get("premium_expiry")
    if exp:
        try:
            now_utc = datetime.now(timezone.utc)
            exp_dt = datetime.fromisoformat(exp)
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            return exp_dt > now_utc
        except Exception:
            return False
    return True


def set_premium(uid: int, days: int = 30) -> None:
    expiry = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    with _lock, _conn() as c:
        c.execute(
            "UPDATE users SET premium=1, premium_expiry=? WHERE uid=?",
            (expiry, uid),
        )


def set_premium_lifetime(uid: int) -> None:
    """Grant lifetime premium — premium=1, no expiry date ever."""
    with _lock, _conn() as c:
        c.execute(
            "UPDATE users SET premium=1, premium_expiry=NULL WHERE uid=?",
            (uid,),
        )


# ── Language preference ───────────────────────────────────────────────────────


def get_user_language(uid: int) -> str | None:
    """Return the stored language code for the user, or None if not yet set."""
    with _conn() as c:
        row = c.execute(
            "SELECT language FROM users WHERE uid=?", (uid,)
        ).fetchone()
    if row is None:
        return None
    lang = row["language"]
    # Treat the sentinel default 'en' as set; return None only when truly missing
    return lang if lang else None


def set_user_language(uid: int, lang: str) -> None:
    """Persist the user's chosen language code (e.g. 'en', 'zh', 'tr')."""
    with _lock, _conn() as c:
        c.execute("UPDATE users SET language=? WHERE uid=?", (lang, uid))


# ── Daily Digest ─────────────────────────────────────────────────────────────


def set_digest_hour(uid: int, hour: int | None) -> None:
    """Set the UTC hour (0–23) for the user's daily digest, or None to disable."""
    with _lock, _conn() as c:
        c.execute(
            "UPDATE users SET digest_hour=?, last_digest_date=NULL WHERE uid=?",
            (hour, uid),
        )


def get_digest_setting(uid: int) -> int | None:
    """Return the user's configured digest_hour, or None if disabled."""
    with _conn() as c:
        row = c.execute("SELECT digest_hour FROM users WHERE uid=?", (uid,)).fetchone()
    return row["digest_hour"] if row else None


def get_users_for_digest(hour: int, today: str) -> list[dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            """SELECT uid, chat_id FROM users
               WHERE digest_hour=?
                 AND (last_digest_date IS NULL OR last_digest_date != ?)""",
            (hour, today),
        ).fetchall()
    return [dict(r) for r in rows]


def mark_digest_sent(uid: int, today: str) -> None:
    with _lock, _conn() as c:
        c.execute(
            "UPDATE users SET last_digest_date=? WHERE uid=?",
            (today, uid),
        )


def get_user_digest_alerts(uid: int, limit: int = 5) -> list[dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            """SELECT a.* FROM alerts a
               INNER JOIN watchlist w
                 ON a.chain = w.chain AND a.ca = w.ca AND w.uid = ?
               WHERE a.created_at > datetime('now', '-24 hours')
               ORDER BY a.amount_usd DESC
               LIMIT ?""",
            (uid, limit),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Watchlist ────────────────────────────────────────────────────────────────


def get_watchlist(uid: int) -> list[dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM watchlist WHERE uid=? ORDER BY added_at", (uid,)
        ).fetchall()
    return [dict(r) for r in rows]


def count_watchlist(uid: int) -> int:
    with _conn() as c:
        return c.execute(
            "SELECT COUNT(*) FROM watchlist WHERE uid=?", (uid,)
        ).fetchone()[0]


def add_to_watchlist(
    uid: int,
    chain: str,
    ca: str,
    name: str,
    symbol: str,
    decimals: int,
    threshold_usd: float = 10000.0,
) -> bool:
    try:
        with _lock, _conn() as c:
            c.execute(
                """INSERT OR IGNORE INTO watchlist
                   (uid, chain, ca, name, symbol, decimals, threshold_usd)
                   VALUES(?,?,?,?,?,?,?)""",
                (
                    uid,
                    chain.lower().strip(),
                    ca.lower().strip(),
                    name,
                    symbol,
                    decimals,
                    threshold_usd,
                ),
            )
        return True
    except Exception:
        return False


def remove_from_watchlist(uid: int, chain: str, ca: str) -> None:
    with _lock, _conn() as c:
        c.execute(
            "DELETE FROM watchlist WHERE uid=? AND chain=? AND ca=?",
            (uid, chain.lower().strip(), ca.lower().strip()),
        )


def is_tracked(uid: int, chain: str, ca: str) -> bool:
    with _conn() as c:
        row = c.execute(
            "SELECT 1 FROM watchlist WHERE uid=? AND chain=? AND ca=?",
            (uid, chain.lower().strip(), ca.lower().strip()),
        ).fetchone()
    return row is not None


def set_paused(uid: int, chain: str, ca: str, paused: bool) -> None:
    with _lock, _conn() as c:
        c.execute(
            "UPDATE watchlist SET paused=? WHERE uid=? AND chain=? AND ca=?",
            (1 if paused else 0, uid, chain.lower().strip(), ca.lower().strip()),
        )


def set_wallet_label(uid: int, chain: str, ca: str, label: str) -> None:
    """Set a human-readable alias for a tracked address (e.g. 'Smart Money 1')."""
    with _lock, _conn() as c:
        c.execute(
            "UPDATE watchlist SET wallet_label=? WHERE uid=? AND chain=? AND ca=?",
            (label.strip() or None, uid, chain.lower().strip(), ca.lower().strip()),
        )


def get_wallet_label(uid: int, chain: str, ca: str) -> str | None:
    """Return the wallet label for a tracked address, or None if unset."""
    with _conn() as c:
        row = c.execute(
            "SELECT wallet_label FROM watchlist WHERE uid=? AND chain=? AND ca=?",
            (uid, chain.lower().strip(), ca.lower().strip()),
        ).fetchone()
    return row["wallet_label"] if row and row["wallet_label"] else None


def get_label_for_address(uid: int, address: str) -> str | None:
    """Return wallet_label if *address* matches any tracked CA for this user."""
    addr = address.lower().strip()
    with _conn() as c:
        row = c.execute(
            "SELECT wallet_label FROM watchlist "
            "WHERE uid=? AND LOWER(ca)=? AND wallet_label IS NOT NULL",
            (uid, addr),
        ).fetchone()
    return row["wallet_label"] if row else None


def get_top_tracked_tokens(limit: int = 5) -> list[dict[str, Any]]:
    """Return tokens with the most individual trackers across all users."""
    with _conn() as c:
        rows = c.execute(
            """SELECT chain, ca, name, symbol, COUNT(uid) AS trackers
               FROM watchlist
               GROUP BY chain, ca
               ORDER BY trackers DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_top_volume_tokens(limit: int = 5) -> list[dict[str, Any]]:
    """Return tokens with the highest total USD volume in the last 24 hours."""
    with _conn() as c:
        rows = c.execute(
            """SELECT chain, ca, token_name, symbol, SUM(amount_usd) AS total_usd
               FROM alerts
               WHERE created_at > datetime('now', '-24 hours')
               GROUP BY chain, ca
               ORDER BY total_usd DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def set_threshold(uid: int, chain: str, ca: str, threshold_usd: float) -> None:
    with _lock, _conn() as c:
        c.execute(
            "UPDATE watchlist SET threshold_usd=? WHERE uid=? AND chain=? AND ca=?",
            (threshold_usd, uid, chain.lower().strip(), ca.lower().strip()),
        )


def get_watchlist_entry(uid: int, chain: str, ca: str) -> dict[str, Any] | None:
    with _conn() as c:
        row = c.execute(
            "SELECT * FROM watchlist WHERE uid=? AND chain=? AND ca=?",
            (uid, chain.lower().strip(), ca.lower().strip()),
        ).fetchone()
    return dict(row) if row else None


def get_all_subscribers() -> dict[str, set[int]]:
    result: dict[str, set[int]] = {}
    with _conn() as c:
        rows = c.execute(
            "SELECT uid, chain, ca FROM watchlist WHERE paused=0"
        ).fetchall()
    for row in rows:
        chain_lc = str(row["chain"]).lower().strip()
        ca_lc = str(row["ca"]).lower().strip()
        key = f"{chain_lc}:{ca_lc}"
        result.setdefault(key, set()).add(row["uid"])
    return result


def save_alert(
    chain: str,
    ca: str,
    token_name: str,
    symbol: str,
    amount_tok: float,
    amount_usd: float,
    sender: str,
    receiver: str,
    tx_hash: str,
    block: int,
    alert_type: str,
) -> None:
    with _lock, _conn() as c:
        c.execute(
            """INSERT INTO alerts
               (chain,ca,token_name,symbol,amount_tok,amount_usd,
                sender,receiver,tx_hash,block,alert_type)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                chain.lower().strip(),
                ca.lower().strip(),
                token_name,
                symbol,
                amount_tok,
                amount_usd,
                sender.lower().strip(),
                receiver.lower().strip(),
                tx_hash,
                block,
                alert_type,
            ),
        )


def get_recent_alerts(limit: int = 20) -> list[dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_leaderboard(limit: int = 5) -> list[dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM alerts ORDER BY amount_usd DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


# ── ADMIN PANEL PANELİ İÇİN YENİ EKLENEN SORGULAR ─────────────────────────────


def get_bot_stats() -> dict[str, int]:
    """Botun genel üye ve takip istatistiklerini özetler."""
    stats = {}
    with _conn() as c:
        stats["total_users"] = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        stats["premium_users"] = c.execute(
            "SELECT COUNT(*) FROM users WHERE premium=1"
        ).fetchone()[0]
        stats["total_tracked_tokens"] = c.execute(
            "SELECT COUNT(DISTINCT ca) FROM watchlist"
        ).fetchone()[0]
        stats["total_alerts_sent"] = c.execute(
            "SELECT COUNT(*) FROM alerts"
        ).fetchone()[0]
    return stats


def get_total_users_count() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) FROM users").fetchone()[0]


def get_premium_users_count() -> int:
    """Count users with active premium (lifetime or not yet expired)."""
    with _conn() as c:
        rows = c.execute(
            "SELECT premium_expiry FROM users WHERE premium=1"
        ).fetchall()
    now = datetime.utcnow()
    count = 0
    for row in rows:
        exp = row["premium_expiry"]
        if exp is None:                          # lifetime
            count += 1
        else:
            try:
                if datetime.fromisoformat(exp) > now:
                    count += 1
            except Exception:
                pass
    return count


def get_all_users_list() -> list[dict[str, Any]]:
    """Kayıtlı tüm kullanıcıları detaylarıyla listeler."""
    with _conn() as c:
        rows = c.execute(
            "SELECT uid, chat_id, premium, premium_expiry, created_at FROM users ORDER BY created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]
