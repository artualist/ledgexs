"""
whale_tracker.py
====================
On-chain whale transfer monitor — powered by the Ledgexs Bot's existing
RPC infrastructure (Web3/EVM, Solana JSON-RPC, Tron TronGrid REST).

No third-party data vendor or ccxt dependency.  Uses the same env vars,
the same ERC-20 Transfer event polling pattern, and the same catalog helpers
(CEX_WALLETS, DEX_ROUTERS) that main.py's monitor loops use — all inside its
own daemon threads so main.py's thread structure is never touched.

Architecture
------------
• _evm_scan_loop()  — polls ERC-20 Transfer events on ETH + BSC via web3.py
• _sol_scan_loop()  — polls Solana signatures via JSON-RPC (SOL_RPC_URL)
• _tron_scan_loop() — polls TRC-20 Transfer events via TronGrid REST (TRON_RPC_URL)
• _ad_whale_loop()  — bot-invite promo to whale channel every 6 h
• _ad_news_loop()   — whale-channel invite to news channel every 12 h

filter_whale_tx(amount_usd, symbol) → bool  (callable from anywhere)
notify_transfer(...)  → optional event-bus entry point for main.py loops
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger("whale_bot.whale_tracker")

# ── Optional imports (graceful degradation) ───────────────────────────────────

try:
    from web3 import Web3                        # type: ignore
    from web3.contract import Contract           # type: ignore
    _WEB3_OK = True
except ImportError:
    _WEB3_OK = False
    logger.warning("whale_tracker: web3 not installed — EVM scanning disabled.")

try:
    import requests as _requests                 # type: ignore
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False
    logger.warning("whale_tracker: requests not installed — posting disabled.")

try:
    import catalog as cat                        # type: ignore
    _CAT_OK = True
except ImportError:
    cat = None                                   # type: ignore
    _CAT_OK = False

# ── Env vars ──────────────────────────────────────────────────────────────────

_BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
_ETH_RPC     = os.environ.get("ETH_RPC_URL", "") or os.environ.get("RPC_URL", "")
_BSC_RPC     = os.environ.get("BSC_RPC_URL", "")
_SOL_RPC     = os.environ.get("SOL_RPC_URL", "")
_TRON_RPC    = os.environ.get("TRON_RPC_URL", "")

# ── Channels & signature ──────────────────────────────────────────────────────

WHALE_CHANNEL = "@LedgexsWhale"
NEWS_CHANNEL  = "@Ledgexs"

WHALE_SIG = (
    "\n\n━━━━━━━━━━━━━━━\n"
    "<b>Ledgexs</b> | "
    "<a href='https://t.me/Ledgexs'>News</a> | "
    "<a href='https://x.com/Ledgexs'>X</a> | "
    "<a href='https://t.me/LedgexsBot'>LX Whale Bot</a>"
)

NEWS_SIG = (
    "\n\n━━━━━━━━━━━━━━━\n"
    "<b>Ledgexs</b> | "
    "<a href='https://t.me/LedgexsWhaleAlert'>Whale Alerts</a> | "
    "<a href='https://x.com/Ledgexs'>X</a> | "
    "<a href='https://t.me/LedgexsBot'>LX Whale Bot</a>"
)

# ── USD thresholds (per symbol) ───────────────────────────────────────────────

THRESHOLDS: dict[str, float] = {
    # Tier 1 — $100 M
    "WBTC":  100_000_000,
    "WETH":  100_000_000,
    "ETH":   100_000_000,
    "BTC":   100_000_000,
    "USDT":  100_000_000,
    "USDC":  100_000_000,
    "FDUSD": 100_000_000,
    # Tier 2 — $10 M
    "SOL":    10_000_000,
    "WSOL":   10_000_000,
    "BNB":    10_000_000,
    "WBNB":   10_000_000,
    # Tier 3 — $5 M
    "XRP":     5_000_000,
    "ADA":     5_000_000,
    "XLM":     5_000_000,
    "DOGE":    5_000_000,
    "ZEC":     5_000_000,
    "XMR":     5_000_000,
    "LINK":    5_000_000,
    "HYPE":    5_000_000,
}

# ── EVM token configs ─────────────────────────────────────────────────────────
# Only tokens with widely-used ERC-20 / BEP-20 representations are listed.
# XMR / ZEC / HYPE have no major wrapped versions on these chains.

EVM_TOKENS: dict[str, list[dict[str, Any]]] = {
    "eth": [
        {"symbol": "WBTC",  "ca": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", "decimals": 8},
        {"symbol": "WETH",  "ca": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "decimals": 18},
        {"symbol": "USDT",  "ca": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "decimals": 6},
        {"symbol": "USDC",  "ca": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "decimals": 6},
        {"symbol": "LINK",  "ca": "0x514910771AF9Ca656af840dff83E8264EcF986CA", "decimals": 18},
    ],
    "bsc": [
        {"symbol": "WBNB",  "ca": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", "decimals": 18},
        {"symbol": "USDT",  "ca": "0x55d398326f99059fF775485246999027B3197955", "decimals": 18},
        {"symbol": "XRP",   "ca": "0x1D2F0da169ceB9fC7B3144628dB156f3F6c60dBe", "decimals": 18},
        {"symbol": "ADA",   "ca": "0x3EE2200Efb3400fAbB9AacF31297cBdD1d435D47", "decimals": 18},
        {"symbol": "DOGE",  "ca": "0xbA2aE424d960c26247Dd6c32edC70B295c744C43", "decimals": 8},
        {"symbol": "XLM",   "ca": "0x43C934A845205F0b514417d757d7235B8f53f1B9", "decimals": 18},
        {"symbol": "LINK",  "ca": "0xF8A0BF9cF54Bb92F17374d9e9A321E6a111a51bD", "decimals": 18},
    ],
}

# Explorer base URLs for tx links
_EXPLORERS: dict[str, str] = {
    "eth":  "https://etherscan.io/tx",
    "bsc":  "https://bscscan.com/tx",
}

_CHAIN_LABELS: dict[str, str] = {
    "eth":  "🔷 Ethereum",
    "bsc":  "🟡 BSC",
    "sol":  "🧬 Solana",
    "tron": "🔴 Tron",
}

# Solana SPL tokens to watch
SOL_TOKENS: list[dict[str, Any]] = [
    {"symbol": "WSOL",  "mint": "So11111111111111111111111111111111111111112",  "decimals": 9},
    {"symbol": "USDC",  "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "decimals": 6},
    {"symbol": "USDT",  "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  "decimals": 6},
]

# Tron TRC-20 token to watch (USDT is the dominant large-transfer token on Tron)
TRON_TOKENS: list[dict[str, Any]] = [
    {"symbol": "USDT",  "ca": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "decimals": 6},
    {"symbol": "USDC",  "ca": "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8", "decimals": 6},
]

# ── Minimal ERC-20 Transfer ABI ───────────────────────────────────────────────

_TRANSFER_ABI: list[dict] = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True,  "name": "from",  "type": "address"},
            {"indexed": True,  "name": "to",    "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    }
]

# ── Alert cooldown (per symbol, avoids spam bursts) ───────────────────────────

_ALERT_COOLDOWN_S  = 120   # seconds
_last_alert: dict[str, float] = {}
_alert_lock = threading.Lock()

def _is_cooled(key: str) -> bool:
    with _alert_lock:
        if time.time() - _last_alert.get(key, 0) < _ALERT_COOLDOWN_S:
            return False
        _last_alert[key] = time.time()
        return True

# ── Standalone price cache (no main.py import needed) ─────────────────────────

_COINGECKO_IDS: dict[str, str] = {
    "WBTC": "wrapped-bitcoin", "BTC": "bitcoin",
    "WETH": "ethereum",        "ETH": "ethereum",
    "USDT": "tether",          "USDC": "usd-coin",
    "FDUSD": "first-digital-usd",
    "BNB":  "binancecoin",     "WBNB": "binancecoin",
    "SOL":  "solana",          "WSOL": "solana",
    "XRP":  "ripple",          "ADA":  "cardano",
    "DOGE": "dogecoin",        "XLM":  "stellar",
    "LINK": "chainlink",       "ZEC":  "zcash",
    "XMR":  "monero",          "HYPE": "hyperliquid",
}
_STABLE  = {"USDT", "USDC", "BUSD", "FDUSD", "DAI", "TUSD", "USDP"}
_pcache: dict[str, tuple[float, float]] = {}
_pcache_lock = threading.Lock()
_PRICE_TTL = 300

def _get_price(symbol: str) -> float:
    sym = symbol.upper()
    if sym in _STABLE:
        return 1.0
    cg_id = _COINGECKO_IDS.get(sym)
    if not cg_id or not _REQUESTS_OK:
        return 0.0
    with _pcache_lock:
        entry = _pcache.get(cg_id)
        if entry and time.time() - entry[1] < _PRICE_TTL:
            return entry[0]
    try:
        resp = _requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd"},
            timeout=8,
        )
        price = float(resp.json().get(cg_id, {}).get("usd", 0))
        with _pcache_lock:
            _pcache[cg_id] = (price, time.time())
        return price
    except Exception:
        return 0.0

# ── Whale filter ──────────────────────────────────────────────────────────────

def filter_whale_tx(amount_usd: float, symbol: str) -> bool:
    """Return True if the USD notional meets this symbol's whale threshold."""
    threshold = THRESHOLDS.get(symbol.upper(), THRESHOLDS.get("LINK", 5_000_000))
    return amount_usd >= threshold

# ── Wallet identity lookup ────────────────────────────────────────────────────

def _wallet_label(address: str) -> str:
    """Return a human label for the address if it is a known CEX wallet."""
    if not _CAT_OK or cat is None:
        return _shorten(address)
    try:
        cex_map = {k.lower(): v for k, v in (getattr(cat, "CEX_WALLETS", {}) or {}).items()}
        label = cex_map.get(address.lower())
        return f"<b>{label}</b>" if label else _shorten(address)
    except Exception:
        return _shorten(address)

def _classify_tx(chain_id: str, sender: str, receiver: str) -> str:
    """Classify transfer type using CEX_WALLETS and DEX_ROUTERS from catalog."""
    if not _CAT_OK or cat is None:
        return "📦 TRANSFER"
    try:
        cex = {k.lower(): v for k, v in (getattr(cat, "CEX_WALLETS", {}) or {}).items()}
        routers = {str(r).lower() for r in (getattr(cat, "DEX_ROUTERS", {}).get(chain_id, set()) or set())}
    except Exception:
        return "📦 TRANSFER"

    s, r = sender.lower(), receiver.lower()
    cex_src, cex_dst = cex.get(s), cex.get(r)

    if cex_src and cex_dst:
        return f"🏛️ CEX INTERNAL ({cex_src} → {cex_dst})"
    if cex_dst:
        return f"🏛️ CEX DEPOSIT → {cex_dst}"
    if cex_src:
        return f"🏦 CEX WITHDRAWAL ← {cex_src}"
    if r in routers:
        return "🚨 WHALE SELL"
    if s in routers:
        return "🔥 WHALE BUY"
    return "📦 COLD WALLET TRANSFER"

# ── Helpers ───────────────────────────────────────────────────────────────────

def _shorten(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}" if len(addr) > 10 else addr

def _fmt_usd(v: float) -> str:
    if v >= 1_000_000_000:
        return f"${v / 1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v / 1_000:.1f}K"
    return f"${v:,.0f}"

def _fmt_amount(amount: float, symbol: str) -> str:
    if amount >= 1_000:
        return f"{amount:,.2f} {symbol}"
    if amount >= 1:
        return f"{amount:,.4f} {symbol}"
    return f"{amount:.8f} {symbol}"

def _send_telegram(text: str) -> None:
    if not _BOT_TOKEN or not _REQUESTS_OK:
        return
    try:
        resp = _requests.post(
            f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": WHALE_CHANNEL,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "disable_notification": False,
            },
            timeout=15,
        )
        if not resp.ok:
            logger.warning(
                "whale_tracker: Telegram post failed %s: %s",
                resp.status_code, resp.text[:200],
            )
    except Exception as exc:
        logger.warning("whale_tracker: Telegram send error: %s", exc)

def _send_telegram_to(chat_id: str, text: str) -> None:
    if not _BOT_TOKEN or not _REQUESTS_OK:
        return
    try:
        resp = _requests.post(
            f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "disable_notification": True,
            },
            timeout=15,
        )
        if not resp.ok:
            logger.warning(
                "whale_tracker: Telegram post to %s failed %s",
                chat_id, resp.status_code,
            )
    except Exception as exc:
        logger.warning("whale_tracker: Telegram send error (%s): %s", chat_id, exc)

# ── Alert builder ─────────────────────────────────────────────────────────────

def _build_alert(
    *,
    tx_type: str,
    network: str,
    symbol: str,
    amount: float,
    amount_usd: float,
    sender: str,
    receiver: str,
    tx_hash: str,
    tx_url: str,
) -> str:
    return (
        f"<b>🐋 WHALE ALERT: {tx_type} on {network}</b>\n\n"
        f"📊 Token: <b>{symbol}</b>\n"
        f"📦 Amount: <b>{_fmt_amount(amount, symbol)}</b>\n"
        f"💰 Value: <b>{_fmt_usd(amount_usd)}</b>\n"
        f"📤 From: {_wallet_label(sender)}\n"
        f"📥 To: {_wallet_label(receiver)}\n"
        f"🔗 Tx: <a href='{tx_url}'>{_shorten(tx_hash)}</a>"
        f"{WHALE_SIG}"
    )

# ── Public event-bus entry point ──────────────────────────────────────────────
# main.py's monitor loops can call this directly for any transfer they detect,
# so whale_tracker gets those events too — without needing its own RPC call.

def notify_transfer(
    chain_id: str,
    symbol: str,
    amount: float,
    amount_usd: float,
    sender: str,
    receiver: str,
    tx_hash: str,
) -> None:
    """
    Optional event-bus entry point.

    main.py's _check_whale_activity, _sol_check_token, etc. can call this at
    the end of each detected transfer.  whale_tracker will apply THRESHOLDS
    and fire an alert to @LedgexsWhale if the transfer qualifies.

    This path is additive — it runs alongside whale_tracker's own scan loops.
    """
    if not filter_whale_tx(amount_usd, symbol):
        return
    cooldown_key = f"{chain_id}:{symbol}:{tx_hash}"
    if not _is_cooled(cooldown_key):
        return

    explorers = {
        "eth":  "https://etherscan.io/tx",
        "bsc":  "https://bscscan.com/tx",
        "sol":  "https://solscan.io/tx",
        "tron": "https://tronscan.org/#/transaction",
    }
    tx_url  = f"{explorers.get(chain_id, 'https://etherscan.io/tx')}/{tx_hash}"
    network = _CHAIN_LABELS.get(chain_id, chain_id.upper())
    tx_type = _classify_tx(chain_id, sender, receiver)

    text = _build_alert(
        tx_type=tx_type, network=network, symbol=symbol,
        amount=amount, amount_usd=amount_usd,
        sender=sender, receiver=receiver,
        tx_hash=tx_hash, tx_url=tx_url,
    )
    _send_telegram(text)
    logger.info(
        "whale_tracker: 🐋 [notify] %s %s %s on %s — %s",
        tx_type, _fmt_usd(amount_usd), symbol, chain_id, _shorten(tx_hash),
    )

# ── EVM scan loop ─────────────────────────────────────────────────────────────

def _evm_scan_loop() -> None:
    """
    Polls ERC-20 Transfer events for whale tokens on ETH and BSC.

    Uses the exact same block-range approach as main.py's _check_whale_activity:
    - Connects via Web3 HTTPProvider using the same env vars (ETH_RPC_URL, BSC_RPC_URL)
    - Tracks last-seen block per token to avoid re-processing
    - Applies filter_whale_tx() before alerting
    - Enriches with CEX/DEX classification from catalog
    """
    if not _WEB3_OK:
        logger.warning("whale_tracker: EVM scan disabled (web3 not installed).")
        return

    rpc_map = {"eth": _ETH_RPC, "bsc": _BSC_RPC}
    w3_map: dict[str, Any] = {}
    for chain_id, url in rpc_map.items():
        if not url:
            logger.info("whale_tracker: %s RPC not configured — chain skipped.", chain_id)
            continue
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 15}))
            if w3.is_connected():
                w3_map[chain_id] = w3
                logger.info("whale_tracker: EVM connected to %s (block %d).", chain_id, w3.eth.block_number)
            else:
                logger.warning("whale_tracker: %s RPC unreachable.", chain_id)
        except Exception as exc:
            logger.warning("whale_tracker: %s Web3 init failed: %s", chain_id, exc)

    if not w3_map:
        logger.warning("whale_tracker: No EVM chains reachable — EVM scan disabled.")
        return

    last_seen: dict[str, int] = {}  # "chain:ca" → last scanned block

    while True:
        for chain_id, w3 in list(w3_map.items()):
            if not w3.is_connected():
                continue
            tokens = EVM_TOKENS.get(chain_id, [])
            for tok in tokens:
                ca      = tok["ca"]
                symbol  = tok["symbol"]
                dec     = tok["decimals"]
                key     = f"{chain_id}:{ca}"
                exp_url = _EXPLORERS.get(chain_id, "https://etherscan.io/tx")
                network = _CHAIN_LABELS.get(chain_id, chain_id.upper())

                try:
                    latest = w3.eth.block_number
                    from_b = last_seen.get(key, latest - 1)
                    if from_b >= latest:
                        continue
                    last_seen[key] = latest

                    contract: Contract = w3.eth.contract(
                        address=Web3.to_checksum_address(ca),
                        abi=_TRANSFER_ABI,
                    )
                    events = contract.events.Transfer.get_logs(
                        from_block=from_b + 1, to_block=latest
                    )
                except Exception as exc:
                    logger.debug("whale_tracker: get_logs %s: %s", key, exc)
                    continue

                price = _get_price(symbol)

                for event in events:
                    try:
                        amount     = event["args"]["value"] / (10 ** dec)
                        amount_usd = amount * price if price > 0 else 0
                        sender     = event["args"]["from"]
                        receiver   = event["args"]["to"]
                        tx_hash    = event["transactionHash"].hex()

                        if not filter_whale_tx(amount_usd, symbol):
                            continue

                        cooldown_key = f"{chain_id}:{symbol}:{tx_hash}"
                        if not _is_cooled(cooldown_key):
                            continue

                        tx_type = _classify_tx(chain_id, sender, receiver)
                        tx_url  = f"{exp_url}/{tx_hash}"

                        text = _build_alert(
                            tx_type=tx_type, network=network, symbol=symbol,
                            amount=amount, amount_usd=amount_usd,
                            sender=sender, receiver=receiver,
                            tx_hash=tx_hash, tx_url=tx_url,
                        )
                        _send_telegram(text)
                        logger.info(
                            "whale_tracker: 🐋 EVM %s %s %s on %s",
                            tx_type, _fmt_usd(amount_usd), symbol, chain_id,
                        )
                    except Exception as exc:
                        logger.debug("whale_tracker: EVM event parse error: %s", exc)

        time.sleep(12)  # one sweep every 12 s across all EVM chains


# ── Solana scan loop ──────────────────────────────────────────────────────────

def _rpc_post_sol(payload: dict) -> dict:
    resp = _requests.post(_SOL_RPC, json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()

def _sol_scan_loop() -> None:
    """
    Polls Solana JSON-RPC for large SPL token transfers.

    Mirrors the approach used in main.py's _sol_check_token:
    - getSignaturesForAddress (limit 20, since last known signature)
    - getTransaction (jsonParsed) to extract preTokenBalances / postTokenBalances
    - Applies filter_whale_tx() using SOL/USDC/USDT prices
    """
    if not _SOL_RPC or not _REQUESTS_OK:
        logger.info("whale_tracker: SOL_RPC_URL not set — Solana scan disabled.")
        return

    # Health check
    try:
        data = _rpc_post_sol({"jsonrpc": "2.0", "id": 1, "method": "getHealth"})
        logger.info("whale_tracker: Solana RPC health: %s", data.get("result", "?"))
    except Exception as exc:
        logger.warning("whale_tracker: Solana RPC unreachable: %s", exc)
        return

    last_sig: dict[str, str] = {}  # mint → last seen signature

    while True:
        for tok in SOL_TOKENS:
            mint   = tok["mint"]
            symbol = tok["symbol"]
            dec    = tok["decimals"]

            try:
                data = _rpc_post_sol({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [mint, {"limit": 20}],
                })
                sigs_raw: list[dict] = data.get("result") or []
            except Exception as exc:
                logger.debug("whale_tracker: Solana getSignatures %s: %s", mint, exc)
                continue

            prev_last = last_sig.get(mint, "")
            new_sigs: list[str] = []
            for s in sigs_raw:
                if s.get("signature") == prev_last:
                    break
                new_sigs.append(s["signature"])
            if sigs_raw:
                last_sig[mint] = sigs_raw[0]["signature"]
            if not new_sigs or not prev_last:
                # First run — just record position
                continue

            price = _get_price(symbol)

            for sig in new_sigs:
                try:
                    tx_data = _rpc_post_sol({
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getTransaction",
                        "params": [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
                    })
                    tx = tx_data.get("result")
                    if not tx:
                        continue

                    meta  = tx.get("meta") or {}
                    pre_  = {b["accountIndex"]: b for b in (meta.get("preTokenBalances") or [])}
                    post_ = {b["accountIndex"]: b for b in (meta.get("postTokenBalances") or [])}

                    for idx, pb in post_.items():
                        if pb.get("mint") != mint:
                            continue
                        preb     = pre_.get(idx, {})
                        pre_amt  = float((preb.get("uiTokenAmount") or {}).get("uiAmount") or 0)
                        post_amt = float((pb.get("uiTokenAmount") or {}).get("uiAmount") or 0)
                        diff     = abs(post_amt - pre_amt)
                        if diff == 0:
                            continue

                        amount_usd = diff * price if price > 0 else 0
                        if not filter_whale_tx(amount_usd, symbol):
                            continue

                        cooldown_key = f"sol:{symbol}:{sig}"
                        if not _is_cooled(cooldown_key):
                            continue

                        # Extract sender/receiver account keys
                        accts = [
                            a.get("pubkey", "")
                            for a in (tx.get("transaction", {}).get("message", {}).get("accountKeys") or [])
                        ]
                        sender   = accts[0] if len(accts) > 0 else "unknown"
                        receiver = accts[1] if len(accts) > 1 else "unknown"

                        # DEX detection via known Solana router addresses
                        try:
                            sol_routers = cat.DEX_ROUTERS.get("sol", set()) if _CAT_OK and cat else set()
                            is_dex = any(a in sol_routers for a in accts)
                            tx_type = ("🔥 WHALE BUY" if post_amt > pre_amt else "🚨 WHALE SELL") if is_dex else "📦 COLD WALLET TRANSFER"
                        except Exception:
                            tx_type = "📦 TRANSFER"

                        text = _build_alert(
                            tx_type=tx_type, network="🧬 Solana", symbol=symbol,
                            amount=diff, amount_usd=amount_usd,
                            sender=sender, receiver=receiver,
                            tx_hash=sig, tx_url=f"https://solscan.io/tx/{sig}",
                        )
                        _send_telegram(text)
                        logger.info(
                            "whale_tracker: 🐋 SOL %s %s %s",
                            tx_type, _fmt_usd(amount_usd), symbol,
                        )
                except Exception as exc:
                    logger.debug("whale_tracker: Solana tx parse %s: %s", sig, exc)

        time.sleep(15)


# ── Tron scan loop ────────────────────────────────────────────────────────────

def _tron_scan_loop() -> None:
    """
    Polls TronGrid REST API for large TRC-20 Transfer events.

    Mirrors main.py's _tron_check_token approach exactly:
    - GET /v1/contracts/{contract}/events?event_name=Transfer
    - Tracks last block_timestamp to avoid re-processing
    - Applies filter_whale_tx() for USD threshold
    """
    if not _TRON_RPC or not _REQUESTS_OK:
        logger.info("whale_tracker: TRON_RPC_URL not set — Tron scan disabled.")
        return

    # Health check
    try:
        resp = _requests.post(
            f"{_TRON_RPC.rstrip('/')}/wallet/getnowblock", json={}, timeout=8
        )
        block_num = resp.json().get("block_header", {}).get("raw_data", {}).get("number", "?")
        logger.info("whale_tracker: Tron RPC health: block #%s", block_num)
    except Exception as exc:
        logger.warning("whale_tracker: Tron RPC unreachable: %s", exc)
        return

    last_ts: dict[str, int] = {}  # contract → last seen block_timestamp

    while True:
        for tok in TRON_TOKENS:
            ca     = tok["ca"]
            symbol = tok["symbol"]
            dec    = tok["decimals"]

            try:
                resp = _requests.get(
                    f"{_TRON_RPC.rstrip('/')}/v1/contracts/{ca}/events",
                    params={
                        "event_name": "Transfer",
                        "only_confirmed": "true",
                        "limit": "20",
                        "order_by": "block_timestamp,desc",
                    },
                    timeout=15,
                )
                events: list[dict] = resp.json().get("data") or []
            except Exception as exc:
                logger.debug("whale_tracker: TronGrid events %s: %s", ca, exc)
                continue

            if not events:
                continue

            latest_ts = events[0].get("block_timestamp", 0)
            prev_ts   = last_ts.get(ca, 0)
            last_ts[ca] = latest_ts

            if prev_ts == 0:
                continue  # First run — record cursor only

            price = _get_price(symbol)
            new_events = [e for e in events if e.get("block_timestamp", 0) > prev_ts]

            for event in new_events:
                try:
                    result    = event.get("result") or {}
                    raw_value = result.get("_value") or result.get("value", "0")
                    amount    = int(raw_value) / (10 ** dec)
                    amount_usd = amount * price if price > 0 else 0
                    tx_hash   = event.get("transaction_id", "")

                    if not filter_whale_tx(amount_usd, symbol):
                        continue

                    cooldown_key = f"tron:{symbol}:{tx_hash}"
                    if not _is_cooled(cooldown_key):
                        continue

                    sender   = result.get("_from", result.get("from", "unknown"))
                    receiver = result.get("_to",   result.get("to",   "unknown"))
                    tx_url   = f"https://tronscan.org/#/transaction/{tx_hash}"

                    text = _build_alert(
                        tx_type="📦 TRC-20 TRANSFER", network="🔴 Tron", symbol=symbol,
                        amount=amount, amount_usd=amount_usd,
                        sender=sender, receiver=receiver,
                        tx_hash=tx_hash, tx_url=tx_url,
                    )
                    _send_telegram(text)
                    logger.info(
                        "whale_tracker: 🐋 TRON USDT transfer %s", _fmt_usd(amount_usd)
                    )
                except Exception as exc:
                    logger.debug("whale_tracker: Tron event parse error: %s", exc)

        time.sleep(20)


# ── Promotional ad loops ──────────────────────────────────────────────────────

_AD_WHALE = (
    "🐋 <b>Whale Alerts — Free to Use.</b>\n\n"
    "Every $100M+ BTC/ETH and $5M+ altcoin on-chain move lands here "
    "the moment it hits the blockchain — no delay, no noise.\n\n"
    "📲 Share this channel with fellow traders.\n"
    "👉 Track live with @LedgexsBot — free, no sign-up."
)

_AD_NEWS = (
    "🔔 <b>Track $100M+ whale transactions in real time.</b>\n\n"
    "Our on-chain monitor catches the biggest BTC, ETH, SOL and altcoin "
    "movements the instant they confirm on-chain.\n\n"
    "🆓 Free to use — no subscription needed.\n"
    "📲 Whale Alerts 👉 @LedgexsWhale\n"
    "🤖 Bot 👉 @LedgexsBot"
)

def _ad_whale_loop() -> None:
    time.sleep(1_800)   # first fire 30 min after startup
    while True:
        try:
            _send_telegram(_AD_WHALE + WHALE_SIG)
            logger.info("whale_tracker: Whale-channel ad sent.")
        except Exception as exc:
            logger.warning("whale_tracker: Whale ad error: %s", exc)
        time.sleep(6 * 3600)

def _ad_news_loop() -> None:
    time.sleep(3_600)   # first fire 1 h after startup
    while True:
        try:
            _send_telegram_to(NEWS_CHANNEL, _AD_NEWS + NEWS_SIG)
            logger.info("whale_tracker: News-channel ad sent.")
        except Exception as exc:
            logger.warning("whale_tracker: News ad error: %s", exc)
        time.sleep(12 * 3600)

# ── Crash-safe wrappers ───────────────────────────────────────────────────────

def _safe_loop(fn, name: str) -> None:
    while True:
        try:
            fn()
        except Exception as exc:
            logger.warning("whale_tracker: %s crashed (%s) — restarting in 60 s.", name, exc)
            time.sleep(60)

# ── Public API ────────────────────────────────────────────────────────────────

def start_whale_tracker(bot: Any = None) -> None:
    """
    Start all whale tracker daemon threads.

    Parameters
    ----------
    bot:
        Accepted for signature compatibility — not used internally.
        Posting is done via direct Bot API calls using BOT_TOKEN from env.
    """
    if not _BOT_TOKEN:
        logger.warning("whale_tracker: BOT_TOKEN not set — disabled.")
        return

    threads = [
        (lambda: _safe_loop(_evm_scan_loop,   "EVM"),   "WhaleEVM"),
        (lambda: _safe_loop(_sol_scan_loop,   "SOL"),   "WhaleSOL"),
        (lambda: _safe_loop(_tron_scan_loop,  "TRON"),  "WhaleTRON"),
        (_ad_whale_loop,                                 "WhaleAdLoop"),
        (_ad_news_loop,                                  "WhaleNewsAdLoop"),
    ]
    for target, name in threads:
        threading.Thread(target=target, daemon=True, name=name).start()

    logger.info(
        "whale_tracker: started — EVM(%s), SOL(%s), TRON(%s) | "
        "alerts → %s | whale-ad 6h | news-ad 12h",
        "✓" if (_ETH_RPC or _BSC_RPC) else "✗",
        "✓" if _SOL_RPC else "✗",
        "✓" if _TRON_RPC else "✗",
        WHALE_CHANNEL,
    )
