"""
Token catalog, DEX addresses, and CoinGecko ID mapping.
On startup, call fetch_dynamic_catalog() in a background thread to auto-populate
from the CoinGecko top-500 by market cap.
"""
import threading
import logging
from typing import Any

logger = logging.getLogger("whale_bot")
_catalog_lock = threading.Lock()

# Each entry: name, symbol, chain ("eth"|"bsc"), ca (checksum address)
TOKEN_CATALOG: list[dict[str, Any]] = [
    # ── Ethereum mainnet ──────────────────────────────────────────────────────
    {"name": "Wrapped Ether",          "symbol": "WETH",   "chain": "eth", "ca": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"},
    {"name": "Tether USD",             "symbol": "USDT",   "chain": "eth", "ca": "0xdAC17F958D2ee523a2206206994597C13D831ec7"},
    {"name": "USD Coin",               "symbol": "USDC",   "chain": "eth", "ca": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"},
    {"name": "Dai Stablecoin",         "symbol": "DAI",    "chain": "eth", "ca": "0x6B175474E89094C44Da98b954EedeAC495271d0F"},
    {"name": "Wrapped BTC",            "symbol": "WBTC",   "chain": "eth", "ca": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"},
    {"name": "Chainlink",              "symbol": "LINK",   "chain": "eth", "ca": "0x514910771AF9Ca656af840dff83E8264EcF986CA"},
    {"name": "Uniswap",                "symbol": "UNI",    "chain": "eth", "ca": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984"},
    {"name": "Aave",                   "symbol": "AAVE",   "chain": "eth", "ca": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9"},
    {"name": "Curve DAO Token",        "symbol": "CRV",    "chain": "eth", "ca": "0xD533a949740bb3306d119CC777fa900bA034cd52"},
    {"name": "Maker",                  "symbol": "MKR",    "chain": "eth", "ca": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2"},
    {"name": "Compound",               "symbol": "COMP",   "chain": "eth", "ca": "0xc00e94Cb662C3520282E6f5717214004A7f26888"},
    {"name": "Synthetix",              "symbol": "SNX",    "chain": "eth", "ca": "0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F"},
    {"name": "yearn.finance",          "symbol": "YFI",    "chain": "eth", "ca": "0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e"},
    {"name": "SushiSwap",              "symbol": "SUSHI",  "chain": "eth", "ca": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2"},
    {"name": "Balancer",               "symbol": "BAL",    "chain": "eth", "ca": "0xba100000625a3754423978a60c9317c58a424e3D"},
    {"name": "1inch",                  "symbol": "1INCH",  "chain": "eth", "ca": "0x111111111117dC0aa78b770fA6A738034120C302"},
    {"name": "Lido DAO",               "symbol": "LDO",    "chain": "eth", "ca": "0x5A98FcBEA516Cf06857215779Fd812CA3beF1b32"},
    {"name": "Rocket Pool",            "symbol": "RPL",    "chain": "eth", "ca": "0xD33526068D116cE69F19A9ee46F0bd304F21A51f"},
    {"name": "Frax Share",             "symbol": "FXS",    "chain": "eth", "ca": "0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0"},
    {"name": "Frax",                   "symbol": "FRAX",   "chain": "eth", "ca": "0x853d955aCEf822Db058eb8505911ED77F175b99e"},
    {"name": "Liquity USD",            "symbol": "LUSD",   "chain": "eth", "ca": "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0"},
    {"name": "ApeCoin",                "symbol": "APE",    "chain": "eth", "ca": "0x4d224452801ACEd8B2F0aebE155379bb5D594381"},
    {"name": "Shiba Inu",              "symbol": "SHIB",   "chain": "eth", "ca": "0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE"},
    {"name": "Pepe",                   "symbol": "PEPE",   "chain": "eth", "ca": "0x6982508145454Ce325dDbE47a25d4ec3d2311933"},
    {"name": "Floki",                  "symbol": "FLOKI",  "chain": "eth", "ca": "0xcf0C122c6b73ff809C693DB761e7BaeBe62b6a2E"},
    {"name": "Decentraland",           "symbol": "MANA",   "chain": "eth", "ca": "0x0F5D2fB29fb7d3CFeE444a200298f468908cC942"},
    {"name": "The Sandbox",            "symbol": "SAND",   "chain": "eth", "ca": "0x3845badAde8e6dFF049820680d1F14bD3903a5d0"},
    {"name": "Axie Infinity",          "symbol": "AXS",    "chain": "eth", "ca": "0xBB0E17EF65F82Ab018d8EDd776e8DD940327B28b"},
    {"name": "Enjin Coin",             "symbol": "ENJ",    "chain": "eth", "ca": "0xF629cBd94d3791C9250152BD8dfBDF380E2a3B9c"},
    {"name": "Immutable X",            "symbol": "IMX",    "chain": "eth", "ca": "0xF57e7e7C23978C3cAEC3C3548E3D615c346e79fF"},
    {"name": "Gala",                   "symbol": "GALA",   "chain": "eth", "ca": "0x15D4c048F83bd7e37d49eA4C83a07267Ec4203dA"},
    {"name": "Render Token",           "symbol": "RNDR",   "chain": "eth", "ca": "0x6De037ef9aD2725EB40118Bb1702EBb27e4Aeb24"},
    {"name": "The Graph",              "symbol": "GRT",    "chain": "eth", "ca": "0xc944E90C64B2c07662A292be6244BDf05Cda44a7"},
    {"name": "Fetch.ai",               "symbol": "FET",    "chain": "eth", "ca": "0xaea46A60368A7bD060eec7DF8CBa43b7EF41Ad85"},
    {"name": "Ocean Protocol",         "symbol": "OCEAN",  "chain": "eth", "ca": "0x967da4048cD07aB37855c090aAF366e4ce1b9F48"},
    {"name": "SingularityNET",         "symbol": "AGIX",   "chain": "eth", "ca": "0x5B7533812759B45C2B44C19e320ba2CD2681b542"},
    {"name": "Basic Attention Token",  "symbol": "BAT",    "chain": "eth", "ca": "0x0D8775F648430679A709E98d2b0Cb6250d2887EF"},
    {"name": "Livepeer",               "symbol": "LPT",    "chain": "eth", "ca": "0x58b6A8A3302369DAEc383334672404Ee733aB239"},
    {"name": "Numeraire",              "symbol": "NMR",    "chain": "eth", "ca": "0x1776e1F26f98b1A5dF9cD347953a26dd3Cb46671"},
    {"name": "Kyber Network Crystal",  "symbol": "KNC",    "chain": "eth", "ca": "0xdeFA4e8a7bcBA345F687a2f1456F5Edd9CE97202"},
    {"name": "Chiliz",                 "symbol": "CHZ",    "chain": "eth", "ca": "0x3506424F91fD33084466F402d5D97f05F8e3b4AF"},
    {"name": "Ankr",                   "symbol": "ANKR",   "chain": "eth", "ca": "0x8290333ceF9e6D528dD5618Fb97a76f268f3EDD4"},
    {"name": "dYdX",                   "symbol": "DYDX",   "chain": "eth", "ca": "0x92D6C1e31e14520e676a687F0a93788B716BEff5"},
    {"name": "Injective",              "symbol": "INJ",    "chain": "eth", "ca": "0xe28b3B32B6c345A34Ff64674606124Dd5Aceca30"},
    {"name": "Celer Network",          "symbol": "CELR",   "chain": "eth", "ca": "0x4F9254C83EB525f9FCf346490bbb3ed28a81C667"},
    {"name": "SKALE",                  "symbol": "SKL",    "chain": "eth", "ca": "0x00c83aeCC790e8a4453e5dD3B0B4b3680501a7A7"},
    {"name": "Storj",                  "symbol": "STORJ",  "chain": "eth", "ca": "0xB64ef51C888972c908CFacf59B47C1AfBC0Ab8aC"},
    {"name": "ENS",                    "symbol": "ENS",    "chain": "eth", "ca": "0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72"},
    {"name": "Staked Ether (Lido)",    "symbol": "stETH",  "chain": "eth", "ca": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"},
    {"name": "Rocket Pool ETH",        "symbol": "rETH",   "chain": "eth", "ca": "0xae78736Cd615f374D3085123A210448E74Fc6393"},
    {"name": "Convex Finance",         "symbol": "CVX",    "chain": "eth", "ca": "0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B"},
    {"name": "Blur",                   "symbol": "BLUR",   "chain": "eth", "ca": "0x5283D291DBCF85356A21bA090E6db59121208b44"},
    {"name": "Arbitrum",               "symbol": "ARB",    "chain": "eth", "ca": "0xB50721BCf8d664c30412Cfbc6cf7a15145234ad1"},
    {"name": "Worldcoin",              "symbol": "WLD",    "chain": "eth", "ca": "0x163f8C2467924be0ae7B5347228CABF260318753"},
    {"name": "Pendle",                 "symbol": "PENDLE", "chain": "eth", "ca": "0x808507121B80c02388fAd14726482e061B8da827"},
    {"name": "Ethena",                 "symbol": "ENA",    "chain": "eth", "ca": "0x57e114B691Db790C35207b2e685D4A43181e6061"},
    {"name": "USDe",                   "symbol": "USDe",   "chain": "eth", "ca": "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"},
    {"name": "Velodrome Finance",      "symbol": "VELO",   "chain": "eth", "ca": "0x3c8B650257cFb5f272f799F5e2b4e65093a11a05"},
    # ── BSC ─────────────────────────────────────────────────────────────────
    {"name": "Wrapped BNB",            "symbol": "WBNB",   "chain": "bsc", "ca": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"},
    {"name": "Tether USD (BSC)",       "symbol": "USDT",   "chain": "bsc", "ca": "0x55d398326f99059fF775485246999027B3197955"},
    {"name": "USD Coin (BSC)",         "symbol": "USDC",   "chain": "bsc", "ca": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"},
    {"name": "Dai Stablecoin (BSC)",   "symbol": "DAI",    "chain": "bsc", "ca": "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3"},
    {"name": "PancakeSwap",            "symbol": "CAKE",   "chain": "bsc", "ca": "0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82"},
    {"name": "Venus",                  "symbol": "XVS",    "chain": "bsc", "ca": "0xcF6BB5389c92Bdda8a3747Ddb454cB7a64626C63"},
    {"name": "Alpaca Finance",         "symbol": "ALPACA", "chain": "bsc", "ca": "0x8F0528cE5eF7B51152A59745bEfDD91D97091d2F"},
    {"name": "DODO (BSC)",             "symbol": "DODO",   "chain": "bsc", "ca": "0x67ee3Cb086F8a16f34beE3ca72FAD36F7Db929e2"},
    {"name": "Chainlink (BSC)",        "symbol": "LINK",   "chain": "bsc", "ca": "0xF8A0BF9cF54Bb92F17374d9e9A321E6a111a51bD"},
    {"name": "Uniswap (BSC)",          "symbol": "UNI",    "chain": "bsc", "ca": "0xBf5140A22578168FD562DCcF235E5D43A02ce9B1"},
    {"name": "Shiba Inu (BSC)",        "symbol": "SHIB",   "chain": "bsc", "ca": "0x2859e4544C4bB03966803b044A93563Bd2D0DD4D"},
    {"name": "Injective (BSC)",        "symbol": "INJ",    "chain": "bsc", "ca": "0xa2B726B1145A4773F68593CF171187d8EBe4d495"},
    {"name": "Ankr (BSC)",             "symbol": "ANKR",   "chain": "bsc", "ca": "0xf307910A4c7bbc79691fD374879B36359b674Ab1"},
]

# Symbols in the "🔥 Popular" quick-track menu (ETH chain)
TOP_POPULAR_SYMBOLS = [
    "WETH", "WBTC", "USDT", "USDC", "DAI",
    "LINK", "UNI", "AAVE", "SHIB", "PEPE",
    "APE", "ENS", "LDO", "ARB", "MANA",
    "SAND", "AXS", "GRT", "FET", "RNDR",
    "INJ", "DYDX", "SNX", "CRV", "MKR",
]

# Build quick lookup helpers
_catalog_by_key: dict[str, dict[str, Any]] = {
    f"{t['chain']}:{t['ca'].lower()}": t for t in TOKEN_CATALOG
}

def search_catalog(query: str) -> list[dict[str, Any]]:
    """Case-insensitive search by name or symbol, max 20 results."""
    q = query.strip().lower()
    if not q:
        return []
    results = [
        t for t in TOKEN_CATALOG
        if q in t["symbol"].lower() or q in t["name"].lower()
    ]
    return results[:20]


def get_popular_tokens() -> list[dict[str, Any]]:
    """Return the TOP_POPULAR_SYMBOLS entries from the ETH catalog."""
    symbol_map = {
        t["symbol"]: t for t in TOKEN_CATALOG if t["chain"] == "eth"
    }
    return [symbol_map[s] for s in TOP_POPULAR_SYMBOLS if s in symbol_map]


def catalog_token(chain: str, ca: str) -> dict[str, Any] | None:
    return _catalog_by_key.get(f"{chain}:{ca.lower()}")


def fetch_dynamic_catalog() -> None:
    """
    Auto-populate TOKEN_CATALOG and COINGECKO_IDS from the CoinGecko free API.
    Fetches top 500 coins by market cap (2 pages × 250) then cross-references
    /coins/list?include_platform=true for Ethereum and BSC contract addresses.
    Safe to call in a background thread — uses _catalog_lock.
    """
    import requests as _req

    CG_BASE = "https://api.coingecko.com/api/v3"
    HEADERS  = {"Accept": "application/json"}

    # ── Step 1: top-500 by market cap ────────────────────────────────────────
    top500: dict[str, dict] = {}
    for page in (1, 2):
        try:
            r = _req.get(
                f"{CG_BASE}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order":       "market_cap_desc",
                    "per_page":    250,
                    "page":        page,
                    "sparkline":   "false",
                },
                headers=HEADERS,
                timeout=20,
            )
            r.raise_for_status()
            for coin in r.json():
                cid = coin.get("id")
                if cid:
                    top500[cid] = {
                        "name":   coin.get("name", ""),
                        "symbol": (coin.get("symbol") or "").upper(),
                        "rank":   coin.get("market_cap_rank") or 999,
                    }
        except Exception as exc:
            logger.warning("CoinGecko markets page %d failed: %s", page, exc)

    if not top500:
        logger.warning("Dynamic catalog: could not fetch market data; skipping.")
        return

    # ── Step 2: all coins with platform (contract) addresses ─────────────────
    try:
        r = _req.get(
            f"{CG_BASE}/coins/list",
            params={"include_platform": "true"},
            headers=HEADERS,
            timeout=40,
        )
        r.raise_for_status()
        all_coins: list[dict] = r.json()
    except Exception as exc:
        logger.warning("CoinGecko coins/list failed: %s", exc)
        return

    # ── Step 3: merge and update globals ─────────────────────────────────────
    def _valid_addr(addr: object) -> bool:
        return (
            isinstance(addr, str)
            and addr.startswith("0x")
            and len(addr) == 42
        )

    new_entries: list[dict] = []
    new_cg_ids:  dict[str, str] = {}

    for coin in all_coins:
        cid = coin.get("id")
        if cid not in top500:
            continue
        info      = top500[cid]
        symbol    = info["symbol"]
        platforms = coin.get("platforms") or {}
        eth_ca    = platforms.get("ethereum", "")
        bsc_ca    = platforms.get("binance-smart-chain", "")

        if symbol:
            new_cg_ids[symbol] = cid

        if _valid_addr(eth_ca):
            new_entries.append({
                "name":   info["name"],
                "symbol": symbol,
                "chain":  "eth",
                "ca":     eth_ca,
            })
        if _valid_addr(bsc_ca):
            new_entries.append({
                "name":   f"{info['name']} (BSC)",
                "symbol": symbol,
                "chain":  "bsc",
                "ca":     bsc_ca,
            })

    with _catalog_lock:
        existing_keys: set[str] = {
            f"{t['chain']}:{t['ca'].lower()}" for t in TOKEN_CATALOG
        }
        added = 0
        for entry in new_entries:
            key = f"{entry['chain']}:{entry['ca'].lower()}"
            if key not in existing_keys:
                TOKEN_CATALOG.append(entry)
                existing_keys.add(key)
                added += 1

        COINGECKO_IDS.update(
            {k: v for k, v in new_cg_ids.items() if k not in COINGECKO_IDS}
        )

        _catalog_by_key.update(
            {f"{t['chain']}:{t['ca'].lower()}": t for t in TOKEN_CATALOG}
        )

    logger.info(
        "Dynamic catalog: +%d tokens from CoinGecko top-500 (total %d)",
        added, len(TOKEN_CATALOG),
    )


# Known DEX router / aggregator addresses (lowercase) per chain.
# If sender or receiver matches → classify as BUY or SELL.
DEX_ROUTERS: dict[str, set[str]] = {
    "eth": {
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2
        "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3
        "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap Universal
        "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",  # Uniswap Universal2
        "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",  # SushiSwap
        "0x1111111254eeb25477b68fb85ed929f73a960582",  # 1inch V5
        "0x1111111254fb6c44bac0bed2854e76f90643097d",  # 1inch V4
        "0xdef1c0ded9bec7f1a1670819833240f027b25eff",  # 0x Exchange Proxy
        "0x74de5d4fcbf63e00296fd95d33236b9794016631",  # 0x V3
    },
    "bsc": {
        "0x10ed43c718714eb63d5aa57b78b54704e256024e",  # PancakeSwap V2
        "0x13f4ea83d0bd40e75c8222255bc855a974568dd4",  # PancakeSwap V3
        "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506",  # SushiSwap BSC
        "0x1111111254eeb25477b68fb85ed929f73a960582",  # 1inch V5
    },
}
DEX_ROUTERS["poly"] = DEX_ROUTERS["eth"]
DEX_ROUTERS["arb"]  = DEX_ROUTERS["eth"]
DEX_ROUTERS["sol"]  = {
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",   # Jupiter Aggregator v6
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP",  # Orca Whirlpool
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM v4
    "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv",  # Raydium CLMM
}


# ── Known CEX hot-wallet addresses ───────────────────────────────────────────
# Maps lowercase EVM address → exchange name.
# Used by _classify_transfer() to label inflows/outflows as CEX deposits/withdrawals.
CEX_WALLETS: dict[str, str] = {
    # Binance
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": "Binance",
    "0xd551234ae421e3bcba99a0da6d736074f22192ff": "Binance",
    "0x564286362092d8e7936f0549571a803b203aaced": "Binance",
    "0x0681d8db095565fe8a346fa0277bffd65d1b22a6": "Binance",
    "0xfe9e8709d3215310075d67e3ed32a380ccf451c8": "Binance",
    "0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67": "Binance",
    "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8": "Binance",
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance",
    "0x001866ae5b3de6caa5a51543fd9fb64f524f5478": "Binance",
    "0x85b931a32a0725be14285b66f1a22178c672d69b": "Binance",
    "0x708396f17127c42383e3b9014072679b2f60b82f": "Binance",
    "0xe0f0cfde7ee664943906f17f7f14342e76a5cec": "Binance",
    # Binance BSC reserves
    "0x8894e0a0c962cb723c1976a4421c95949be2d4e3": "Binance",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "Binance",
    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX",
    "0xa7efae728d2936e78bda97dc267687568dd593f3": "OKX",
    "0x98ec059dc3adfbdd63429454aeb0c990fba4a128": "OKX",
    "0x236f9f97e0e62388479bf9e1b0b4c6c8b8d7e89e": "OKX",
    # Coinbase
    "0xa090e606e30bd747d4e6245a1517ebe430f0057e": "Coinbase",
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "Coinbase",
    "0x503828976d22510aad0201ac7ec88293211d23da": "Coinbase",
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "Coinbase",
    "0x881d40237659c251811cec9c364ef91234567890": "Coinbase",
    # Kraken
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "Kraken",
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "Kraken",
    "0xe853c56864a2ebe4576a807d26fdc4a0ada51919": "Kraken",
    "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": "Kraken",
    # Bitfinex
    "0x1151314c646ce4e0efd76d1af4760ae66a9fe30f": "Bitfinex",
    "0x742d35cc6634c0532925a3b844bc454e4438f44e": "Bitfinex",
    # Huobi / HTX
    "0xab5c66752a9e8167967685f1450532fb96d5d24f": "HTX",
    "0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b": "HTX",
    "0xfdb16996831753d5331ff813c29a93c76834a0ad": "HTX",
    "0xe93381fb4c4f14bda253907b18fad305d799241a": "HTX",
    # KuCoin
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "KuCoin",
    "0xa1d8d972560c2f8144af871db508f0b0b10a3fbf": "KuCoin",
    # Gate.io
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "Gate.io",
    "0x7793cd85c11a924478d358d49b05b37e91b5810f": "Gate.io",
    # Bybit
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": "Bybit",
    "0x2e6b049f62dd0f27e7bf5abc8c5817e3d3da26f3": "Bybit",
    # Crypto.com
    "0x6262998ced04146fa42253a5c0af90ca02dfd2a3": "Crypto.com",
    "0x46340b20830761efd32832a74d7169b29feb9758": "Crypto.com",
    # Gemini
    "0xd24400ae8bfebb18ca49be86258a3c749cf46853": "Gemini",
    "0x07ee55aa48bb72dcc6e9d78256648910de513eca": "Gemini",
    # MEXC
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": "MEXC",
    # Bithumb
    "0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208": "Bithumb",
}


# CoinGecko coin ID map for live price fetching
COINGECKO_IDS: dict[str, str] = {
    "WETH": "ethereum",      "ETH": "ethereum",
    "WBTC": "wrapped-bitcoin",
    "BNB": "binancecoin",    "WBNB": "binancecoin",
    "LINK": "chainlink",     "UNI": "uniswap",
    "AAVE": "aave",          "CRV": "curve-dao-token",
    "MKR": "maker",          "COMP": "compound-governance-token",
    "SNX": "synthetix-network-token", "YFI": "yearn-finance",
    "SUSHI": "sushi",        "BAL": "balancer",
    "1INCH": "1inch",        "LDO": "lido-dao",
    "RPL": "rocket-pool",    "APE": "apecoin",
    "SHIB": "shiba-inu",     "PEPE": "pepe",
    "MANA": "decentraland",  "SAND": "the-sandbox",
    "AXS": "axie-infinity",  "ENJ": "enjincoin",
    "IMX": "immutable-x",    "GALA": "gala",
    "RNDR": "render-token",  "GRT": "the-graph",
    "FET": "fetch-ai",       "OCEAN": "ocean-protocol",
    "AGIX": "singularitynet","BAT": "basic-attention-token",
    "LPT": "livepeer",       "NMR": "numeraire",
    "KNC": "kyber-network-crystal", "CHZ": "chiliz",
    "ANKR": "ankr",          "DYDX": "dydx",
    "INJ": "injective-protocol", "CELR": "celer-network",
    "SKL": "skale",          "STORJ": "storj",
    "ENS": "ethereum-name-service", "stETH": "staked-ether",
    "rETH": "rocket-pool-eth", "CVX": "convex-finance",
    "BLUR": "blur",          "ARB": "arbitrum",
    "WLD": "worldcoin-wld",  "PENDLE": "pendle",
    "ENA": "ethena",         "CAKE": "pancakeswap-token",
    "XVS": "venus",          "FXS": "frax-share",
    "FLOKI": "floki",
}
