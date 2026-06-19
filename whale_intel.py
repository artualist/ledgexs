"""
bot/whale_intel.py
==================
Advanced on-chain intelligence module.

Provides two independently testable capabilities that are imported and used
inside `_check_whale_activity()` in main.py:

1. CEX Inflow / Outflow Formatting
   - `parse_cex_direction(alert_type)` — extracts (direction, cex_name) from the
     label already produced by `_classify_transfer()`.
   - `format_cex_alert(...)` — returns a ready-to-send Telegram HTML string for
     WALLET→CEX inflows (potential sell-off) and CEX→WALLET outflows (accumulation).

2. DCA / Smart Accumulation Tracker
   - Thread-safe rolling 12-hour window per (chain, token, wallet) tuple.
   - Records sub-threshold transfers ($500–$10k each) and fires one alert when
     cumulative USD ≥ $20,000 across ≥ 5 transactions.
   - `accum_tracker` is a module-level singleton imported by main.py.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger("whale_bot.intel")

# ── Accumulation Tracker constants ────────────────────────────────────────────
ACCUM_WINDOW_S   = 12 * 3600   # 12-hour rolling window
ACCUM_USD_THRESH = 20_000.0    # cumulative USD threshold to fire an alert
ACCUM_TX_THRESH  = 5           # minimum number of separate transactions
ACCUM_MIN_USD    = 500.0       # per-tx minimum (noise filter)
ACCUM_MAX_USD    = 10_000.0    # per-tx ceiling  (above this = normal whale alert)


# ── Address shortener (mirrors _shorten in main.py — no circular import) ─────
def _short(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}" if len(addr) > 10 else addr


# ─────────────────────────────────────────────────────────────────────────────
# Part 1 — CEX Flow Detection
# ─────────────────────────────────────────────────────────────────────────────

def parse_cex_direction(alert_type: str) -> tuple[str, str] | None:
    """Parse an alert_type string from _classify_transfer() into (direction, cex_name).

    Returns:
        ("INFLOW",  cex_name)  — wallet → CEX (potential sell-off)
        ("OUTFLOW", cex_name)  — CEX → wallet (withdrawal / accumulation)
        None                   — not a CEX transfer
    """
    if "CEX DEPOSIT" in alert_type:
        # e.g. "🏛️ CEX DEPOSIT → Binance"
        parts = alert_type.split("→", 1)
        cex_name = parts[-1].strip() if len(parts) > 1 else "Unknown CEX"
        return "INFLOW", cex_name

    if "CEX WITHDRAWAL" in alert_type:
        # e.g. "🏦 CEX WITHDRAWAL ← OKX"
        parts = alert_type.split("←", 1)
        cex_name = parts[-1].strip() if len(parts) > 1 else "Unknown CEX"
        return "OUTFLOW", cex_name

    if "CEX CONSOLIDATION" in alert_type:
        parts = alert_type.split("CEX CONSOLIDATION", 1)
        cex_name = parts[-1].strip() if len(parts) > 1 else "CEX"
        return "INFLOW", cex_name

    return None


def format_cex_alert(
    direction: str,
    cex_name: str,
    amount: float,
    symbol: str,
    amount_usd: float,
    tx_hash: str,
    explorer: str,
    chain_label: str,
) -> str:
    """Return a formatted CEX inflow/outflow alert for Telegram (HTML).

    All content on one line after the bold header; @LedgexsBot footer appended.
    """
    usd_str  = f"~${amount_usd:,.0f}"
    tx_link  = f"<a href='{explorer}/tx/{tx_hash}'>{_short(tx_hash)}</a>"

    if direction == "INFLOW":
        header = "🚨 <b>WALLET-TO-CEX INFLOW (Potential Sell-Off):</b>"
        body   = (
            f"Whale moved <b>{amount:,.2f} ${symbol}</b> ({usd_str}) "
            f"to <b>{cex_name}</b>."
        )
    else:  # OUTFLOW
        header = "🐋 <b>CEX-TO-WALLET OUTFLOW (Accumulation):</b>"
        body   = (
            f"Whale withdrew <b>{amount:,.2f} ${symbol}</b> ({usd_str}) "
            f"from <b>{cex_name}</b> into a private wallet."
        )

    return (
        f"{header} {body}\n"
        f"<b>Chain:</b> {chain_label}  |  <b>Tx:</b> {tx_link}\n\n"
        f"@LedgexsBot"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Part 2 — DCA / Smart Accumulation Tracker
# ─────────────────────────────────────────────────────────────────────────────

def format_accum_alert(
    wallet: str,
    total_amount: float,
    symbol: str,
    total_usd: float,
    tx_count: int,
    chain_label: str,
) -> str:
    """Return a formatted smart-accumulation alert for Telegram (HTML)."""
    return (
        f"👀 <b>SMART ACCUMULATION DETECTED:</b> Wallet "
        f"<code>{_short(wallet)}</code> has quietly accumulated "
        f"<b>{total_amount:,.4f} ${symbol}</b> "
        f"(~${total_usd:,.0f}) on <b>{chain_label}</b> "
        f"over the last 12 hours via {tx_count} smaller transactions.\n\n"
        f"@LedgexsBot"
    )


class AccumulationTracker:
    """Thread-safe 12-hour rolling-window DCA accumulation tracker.

    Key: "{chain}:{ca}:{wallet_lowercase}"
    Entry fields:
        total_usd    — cumulative USD value of recorded transfers
        total_amount — cumulative token amount
        tx_count     — number of distinct transactions recorded
        first_ts     — Unix timestamp of the first transfer in the window
        symbol       — token ticker (for formatting)
        alerted      — True once an alert has been fired (suppresses repeats)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, Any]] = {}

    # ── private ───────────────────────────────────────────────────────────────

    @staticmethod
    def _key(chain: str, ca: str, wallet: str) -> str:
        return f"{chain}:{ca}:{wallet.lower()}"

    def _expired(self, entry: dict[str, Any]) -> bool:
        return time.time() - entry["first_ts"] > ACCUM_WINDOW_S

    # ── public API ────────────────────────────────────────────────────────────

    def record(
        self,
        chain: str,
        ca: str,
        wallet: str,
        amount: float,
        amount_usd: float,
        symbol: str,
    ) -> None:
        """Record one sub-threshold transfer.

        Silently ignores transfers below ACCUM_MIN_USD or above ACCUM_MAX_USD.
        Resets the window if the previous window has expired or was already alerted.
        """
        if not (ACCUM_MIN_USD <= amount_usd <= ACCUM_MAX_USD):
            return

        k   = self._key(chain, ca, wallet)
        now = time.time()

        with self._lock:
            entry = self._data.get(k)
            # Only reset when the 12-hour window itself has expired.
            # An alerted entry stays locked until the window expires so no
            # second alert can fire within the same 12-hour period.
            if entry and self._expired(entry):
                entry = None

            if entry is None:
                self._data[k] = {
                    "total_usd":    amount_usd,
                    "total_amount": amount,
                    "tx_count":     1,
                    "first_ts":     now,
                    "symbol":       symbol,
                    "alerted":      False,
                }
            else:
                entry["total_usd"]    += amount_usd
                entry["total_amount"] += amount
                entry["tx_count"]     += 1

    def check_trigger(
        self, chain: str, ca: str, wallet: str
    ) -> dict[str, Any] | None:
        """Return a copy of the accumulated data if both thresholds are met.

        Marks the entry as alerted immediately so the same accumulation is
        never re-fired. Returns None if thresholds are not met or already fired.
        """
        k = self._key(chain, ca, wallet)
        with self._lock:
            entry = self._data.get(k)
            if not entry or entry["alerted"] or self._expired(entry):
                return None
            if (
                entry["total_usd"]  >= ACCUM_USD_THRESH
                and entry["tx_count"] >= ACCUM_TX_THRESH
            ):
                entry["alerted"] = True
                return dict(entry)
        return None

    def purge_stale(self) -> None:
        """Remove window-expired entries. Call occasionally to keep memory lean."""
        with self._lock:
            stale = [k for k, v in self._data.items() if self._expired(v)]
            for k in stale:
                del self._data[k]
        if stale:
            logger.debug("AccumulationTracker: purged %d stale entries.", len(stale))


# ── Module-level singleton imported by main.py ───────────────────────────────
accum_tracker = AccumulationTracker()
