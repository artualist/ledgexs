"""
whale_intel.py
==============
Advanced on-chain intelligence for @LedgexsBot

1. Smart Accumulation Tracker
   - Thread-safe 24-hour rolling window per (chain, token, wallet).
   - Only records BUY-direction transfers ≥$5 K per tx.
   - Fires ONE alert when cumulative USD ≥$100 K across ≥3 transactions.
   - Alert: full wallet address + explorer link, latest tx link, no footer.

2. Coordinated Accumulation Detector
   - Fires a HIGH PRIORITY signal when ≥2 distinct wallets accumulate
     the same token within 4 hours with ≥$500 K combined volume.

3. Wallet Reputation
   - Tracks wallets that have previously triggered an accumulation alert.
   - Marks repeat accumulators as "Smart Money" for a stronger signal.

4. CEX Flow Formatter
   - Formats CEX deposit / withdrawal alerts.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger("whale_bot.intel")

# ── Accumulation tracker constants ────────────────────────────────────────────
ACCUM_WINDOW_S    = 24 * 3600   # 24-hour rolling window (was 12 h)
ACCUM_USD_THRESH  = 100_000.0   # cumulative USD to fire alert (was $20 K)
ACCUM_TX_THRESH   = 3           # minimum distinct transactions (was 5)
ACCUM_MIN_USD     = 5_000.0     # per-tx minimum — noise filter (was $500)
ACCUM_MAX_USD     = 95_000.0    # per-tx ceiling (above = direct whale alert)

# ── Coordinated accumulation constants ────────────────────────────────────────
COORD_WINDOW_S    = 4 * 3600    # 4-hour window
COORD_USD_THRESH  = 500_000.0   # $500 K combined volume to signal
COORD_WALLET_MIN  = 2           # at least 2 distinct wallets

# ── Wallet reputation ─────────────────────────────────────────────────────────
_smart_money_wallets: set[str] = set()   # wallet.lower() → previously alerted
_sm_lock = threading.Lock()


def _register_smart_money(wallet: str) -> None:
    with _sm_lock:
        _smart_money_wallets.add(wallet.lower())


def is_smart_money(wallet: str) -> bool:
    with _sm_lock:
        return wallet.lower() in _smart_money_wallets


# ── Helpers ───────────────────────────────────────────────────────────────────

def _short(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}" if len(addr) > 10 else addr


def _wallet_link(wallet: str, explorer: str) -> str:
    """Full wallet address as a clickable HTML link."""
    return f"<a href='{explorer}/address/{wallet}'><code>{wallet}</code></a>"


def _tx_link(tx_hash: str, explorer: str) -> str:
    return f"<a href='{explorer}/tx/{tx_hash}'>{_short(tx_hash)}</a>"


def _elapsed(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


# ─────────────────────────────────────────────────────────────────────────────
# 1 — CEX Flow Detection
# ─────────────────────────────────────────────────────────────────────────────

def parse_cex_direction(alert_type: str) -> tuple[str, str] | None:
    """Parse alert_type from _classify_transfer() → (direction, cex_name).

    Returns:
        ("INFLOW",  cex_name)  — wallet → CEX (potential sell-off)
        ("OUTFLOW", cex_name)  — CEX → wallet (withdrawal / accumulation)
        None                   — not a CEX transfer
    """
    if "CEX DEPOSIT" in alert_type:
        parts = alert_type.split("→", 1)
        cex_name = parts[-1].strip() if len(parts) > 1 else "Unknown CEX"
        return "INFLOW", cex_name
    if "CEX WITHDRAWAL" in alert_type:
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
    wallet: str = "",
) -> str:
    """Formatted CEX inflow/outflow alert for Telegram (HTML)."""
    usd_str   = f"~${amount_usd:,.0f}"
    tx_lnk    = _tx_link(tx_hash, explorer)
    w_display = _wallet_link(wallet, explorer) if wallet else ""

    if direction == "INFLOW":
        header = "🚨 <b>WALLET → CEX (Potential Sell-Off)</b>"
        body   = (
            f"Whale moved <b>{amount:,.4f} {symbol}</b> ({usd_str}) "
            f"to <b>{cex_name}</b>."
        )
    else:
        header = "🐋 <b>CEX → WALLET (Accumulation)</b>"
        body   = (
            f"Whale withdrew <b>{amount:,.4f} {symbol}</b> ({usd_str}) "
            f"from <b>{cex_name}</b> into a private wallet."
        )

    parts = [f"{header}\n{body}", f"<b>Chain:</b> {chain_label}  |  <b>Tx:</b> {tx_lnk}"]
    if w_display:
        parts.append(f"<b>Wallet:</b> {w_display}")
    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# 2 — Smart Accumulation Tracker
# ─────────────────────────────────────────────────────────────────────────────

def format_accum_alert(
    wallet: str,
    total_amount: float,
    symbol: str,
    total_usd: float,
    tx_count: int,
    chain_label: str,
    explorer: str = "https://etherscan.io",
    latest_tx: str = "",
    first_ts: float = 0.0,
    price_usd: float = 0.0,
) -> str:
    """Full-detail accumulation alert — no @LedgexsBot footer."""
    avg_usd   = total_usd / max(tx_count, 1)
    price_str = f"  (@ ~${price_usd:,.4f}/{symbol})" if price_usd > 0 else ""
    elapsed   = _elapsed(time.time() - first_ts) if first_ts else "24h"
    sm_tag    = " 🧠 <b>Smart Money</b>" if is_smart_money(wallet) else ""

    lines = [
        f"👀 <b>SMART ACCUMULATION DETECTED</b>{sm_tag}",
        f"<b>{symbol}</b> | {chain_label}",
        "",
        f"<b>Wallet:</b> {_wallet_link(wallet, explorer)}",
        f"<b>Accumulated:</b> {total_amount:,.4f} {symbol} (~${total_usd:,.0f}){price_str}",
        f"<b>Transactions:</b> {tx_count} buys  |  avg ~${avg_usd:,.0f}/tx",
        f"<b>Time window:</b> last {elapsed}",
    ]
    if latest_tx:
        lines.append(f"<b>Latest tx:</b> {_tx_link(latest_tx, explorer)}")

    return "\n".join(lines)


class AccumulationTracker:
    """Thread-safe 24-hour rolling-window DCA accumulation tracker.

    Key: "{chain}:{ca}:{wallet_lowercase}"
    Entry fields:
        total_usd    — cumulative USD value
        total_amount — cumulative token amount
        tx_count     — distinct transactions recorded
        first_ts     — Unix timestamp of first transfer in window
        latest_tx    — tx hash of the most recent recorded transfer
        symbol       — token ticker
        alerted      — True once an alert has been fired (suppresses repeats)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _key(chain: str, ca: str, wallet: str) -> str:
        return f"{chain}:{ca}:{wallet.lower()}"

    def _expired(self, entry: dict[str, Any]) -> bool:
        return time.time() - entry["first_ts"] > ACCUM_WINDOW_S

    def record(
        self,
        chain: str,
        ca: str,
        wallet: str,
        amount: float,
        amount_usd: float,
        symbol: str,
        tx_hash: str = "",
    ) -> None:
        """Record one BUY-direction transfer.

        Caller is responsible for only passing BUY events and for ensuring
        amount_usd is within [ACCUM_MIN_USD, ACCUM_MAX_USD].
        Resets window if the previous 24-hour period expired or was already alerted.
        """
        if not (ACCUM_MIN_USD <= amount_usd <= ACCUM_MAX_USD):
            return

        k   = self._key(chain, ca, wallet)
        now = time.time()

        with self._lock:
            entry = self._data.get(k)
            if entry and self._expired(entry):
                entry = None

            if entry is None:
                self._data[k] = {
                    "total_usd":    amount_usd,
                    "total_amount": amount,
                    "tx_count":     1,
                    "first_ts":     now,
                    "latest_tx":    tx_hash,
                    "symbol":       symbol,
                    "alerted":      False,
                }
            else:
                entry["total_usd"]    += amount_usd
                entry["total_amount"] += amount
                entry["tx_count"]     += 1
                entry["latest_tx"]     = tx_hash or entry["latest_tx"]

    def check_trigger(
        self, chain: str, ca: str, wallet: str
    ) -> dict[str, Any] | None:
        """Return accumulated data dict if both thresholds are met, else None.

        Marks entry as alerted immediately — no repeat fires within same window.
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
        with self._lock:
            stale = [k for k, v in self._data.items() if self._expired(v)]
            for k in stale:
                del self._data[k]
        if stale:
            logger.debug("AccumulationTracker: purged %d stale entries.", len(stale))


# ─────────────────────────────────────────────────────────────────────────────
# 3 — Coordinated Accumulation Detector
# ─────────────────────────────────────────────────────────────────────────────

def format_coord_alert(
    symbol: str,
    chain_label: str,
    wallet_count: int,
    total_usd: float,
    elapsed_s: float,
) -> str:
    """HIGH PRIORITY coordinated buy alert."""
    return (
        f"🚨 <b>HIGH PRIORITY — COORDINATED ACCUMULATION</b>\n"
        f"<b>{symbol}</b> | {chain_label}\n\n"
        f"<b>{wallet_count}</b> distinct whale wallets have accumulated "
        f"<b>~${total_usd:,.0f}</b> worth of {symbol} "
        f"over the last {_elapsed(elapsed_s)}.\n\n"
        f"⚡ This is a strong coordinated buy signal."
    )


class CoordAccumulationTracker:
    """Detects coordinated accumulation: ≥2 wallets, ≥$500 K, within 4 hours."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # key: "{chain}:{ca}" → {"wallets": {wallet → total_usd}, "first_ts": float, "alerted": bool}
        self._data: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _key(chain: str, ca: str) -> str:
        return f"{chain}:{ca}"

    def _expired(self, entry: dict) -> bool:
        return time.time() - entry["first_ts"] > COORD_WINDOW_S

    def record(self, chain: str, ca: str, wallet: str, amount_usd: float) -> None:
        k   = self._key(chain, ca)
        now = time.time()
        wl  = wallet.lower()
        with self._lock:
            entry = self._data.get(k)
            if entry and (self._expired(entry) or entry.get("alerted")):
                entry = None
            if entry is None:
                self._data[k] = {
                    "wallets":  {wl: amount_usd},
                    "first_ts": now,
                    "alerted":  False,
                }
            else:
                entry["wallets"][wl] = entry["wallets"].get(wl, 0.0) + amount_usd

    def check_signal(self, chain: str, ca: str) -> dict[str, Any] | None:
        """Return signal dict if coordinated buy thresholds are met, else None."""
        k = self._key(chain, ca)
        with self._lock:
            entry = self._data.get(k)
            if not entry or entry["alerted"] or self._expired(entry):
                return None
            total_usd    = sum(entry["wallets"].values())
            wallet_count = len(entry["wallets"])
            if wallet_count >= COORD_WALLET_MIN and total_usd >= COORD_USD_THRESH:
                entry["alerted"] = True
                return {
                    "wallet_count": wallet_count,
                    "total_usd":    total_usd,
                    "elapsed_s":    time.time() - entry["first_ts"],
                }
        return None


# ── Module-level singletons imported by main.py ───────────────────────────────
accum_tracker = AccumulationTracker()
coord_tracker = CoordAccumulationTracker()
