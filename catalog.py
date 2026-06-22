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
    # ── Liquid Staking / Re-staking (ETH) ────────────────────────────────────
    {"name": "Coinbase Staked ETH",    "symbol": "cbETH",  "chain": "eth", "ca": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704"},
    {"name": "ether.fi Staked ETH",    "symbol": "weETH",  "chain": "eth", "ca": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee"},
    {"name": "EigenLayer",             "symbol": "EIGEN",  "chain": "eth", "ca": "0xec53bF9167f50cDEB3Ae105f56099aaaB9061F83"},
    {"name": "Kelp DAO Restaked ETH",  "symbol": "rsETH",  "chain": "eth", "ca": "0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7"},
    # ── RWA / High-mcap DeFi ─────────────────────────────────────────────────
    {"name": "Ondo Finance",           "symbol": "ONDO",   "chain": "eth", "ca": "0xfAbA6f8e4a5E8Ab82F62fe7C39859FA577269BE3"},
    {"name": "Maple Finance",          "symbol": "MPL",    "chain": "eth", "ca": "0x33349B282065b0284d756F0577FB39c158F935e6"},
    {"name": "Morpho",                 "symbol": "MORPHO", "chain": "eth", "ca": "0x58D97B57BB95320F9a05dC918Aef65434969c2B2"},
    {"name": "Etherfi",                "symbol": "ETHFI",  "chain": "eth", "ca": "0xFe0c30065B384F05761f15d0CC899D4F9F9Cc0eB"},
    {"name": "Renzo Restaked ETH",     "symbol": "ezETH",  "chain": "eth", "ca": "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110"},
    {"name": "Polygon (POL)",          "symbol": "POL",    "chain": "eth", "ca": "0x455e53CBB86018Ac2B8092FdCd39d8444aFFC3F6"},
    {"name": "Optimism",               "symbol": "OP",     "chain": "eth", "ca": "0x4200000000000000000000000000000000000042"},
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

# Symbols in the "🔥 Popular" quick-track menu (ETH chain).
# Sorted by June 2026 market cap relevance; excludes dead metaverse/play-to-earn.
TOP_POPULAR_SYMBOLS = [
    # ── Market cap top ERC-20 / wrapped ─────────────────────────────────────
    "WBTC",  "WETH",  "USDT",  "USDC",  "DAI",
    # ── DeFi blue-chips ──────────────────────────────────────────────────────
    "LINK",  "UNI",   "AAVE",  "MKR",   "CRV",
    # ── L2 / governance ──────────────────────────────────────────────────────
    "ARB",   "LDO",   "ENA",   "PENDLE","ENS",
    # ── AI / infra ───────────────────────────────────────────────────────────
    "RNDR",  "FET",   "INJ",   "GRT",   "WLD",
    # ── Meme / high-volume ───────────────────────────────────────────────────
    "SHIB",  "PEPE",  "FLOKI", "DYDX",  "EIGEN",
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
# ---------------------------------------------------------------------------
# ETH canonical set — all other EVM chains inherit this + chain-specific extras.
# Every address is lowercase; _classify_transfer() lowercases inputs before lookup.
# Sources: official deployments, Etherscan verified contracts, protocol docs.
# ---------------------------------------------------------------------------
_ETH_ROUTERS: set[str] = {

    # ── Uniswap ──────────────────────────────────────────────────────────────
    "0xf164fc0ec4e93095b804a4795bbe1e041497b92a",  # Uniswap V1 (legacy)
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2 Router02
    "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3 SwapRouter
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3 SwapRouter02
    "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b",  # Uniswap Universal Router v1.2
    "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",  # Uniswap Universal Router v1.3
    "0x000000000004444c5dc75cb358380d2e3de08a90",  # Uniswap V4 PoolManager (mainnet)
    "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",  # Uniswap V2 Factory
    "0x1f98431c8ad98523631ae4a59f267346ea31f984",  # Uniswap V3 Factory
    "0x000000000022d473030f116ddee9f6b43ac78ba3",  # Uniswap Permit2 (universal)

    # ── SushiSwap ────────────────────────────────────────────────────────────
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",  # SushiSwap V2 Router
    "0x2c9c47e7d254e493001d0b483c3df31b78711d5b",  # SushiSwap V3 SwapRouter
    "0x827179dd56d07a7eea32e3873493835da2866976",  # SushiSwap RouteProcessor3
    "0x0a6e511fe0127428cbab1ab11e27ea12f8c5dba5",  # SushiSwap RouteProcessor4
    "0x5550d13389bb70f45fcdce0c8f680ebe873f7a44",  # SushiSwap RouteProcessor5

    # ── Curve Finance ────────────────────────────────────────────────────────
    "0x99a58482bd75cbab83b27ec03ca68ff489b5788f",  # Curve Router V2 (general)
    "0xf0d4c12a5768d806021f80a262b4d39d26c58b8d",  # Curve StableSwap Factory NG
    "0xb9fc157394af804a3578134a6585c0dc9cc990d4",  # Curve StableSwap Factory V1
    "0xf18056bbd320e96a48e3fbf8bc061322531aac99",  # Curve CryptoSwap Factory
    "0xdc24316b9ae028f1497c275eb9192a3ea0f67022",  # Curve stETH Pool (Lido/ETH)
    "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7",  # Curve 3pool

    # ── Balancer ─────────────────────────────────────────────────────────────
    "0xba12222222228d8ba445958a75a0704d566bf2c8",  # Balancer V2 Vault
    "0xe39b5e3b6d74016b2f6a9673d9d7d16c8a7b2a1",  # Balancer V3 Router (tentative)

    # ── 1inch ────────────────────────────────────────────────────────────────
    "0x11111112542d85b3ef69ae05771c2dccff4faa26",  # 1inch V3 AggregationRouter
    "0x1111111254fb6c44bac0bed2854e76f90643097d",  # 1inch V4 AggregationRouter
    "0x1111111254eeb25477b68fb85ed929f73a960582",  # 1inch V5 AggregationRouter
    "0x111111125421ca6dc452d289314280a0f8842a65",  # 1inch V6 AggregationRouter
    "0x1111111254760f7ab3d16433cf5ca31bfa6f6af2",  # 1inch Limit Order Protocol V3

    # ── 0x Protocol ──────────────────────────────────────────────────────────
    "0xdef1c0ded9bec7f1a1670819833240f027b25eff",  # 0x Exchange Proxy V4
    "0x74de5d4fcbf63e00296fd95d33236b9794016631",  # 0x V3 Exchange
    "0x22f9dcf4647084d6c31b2765f6910cd85c178c18",  # 0x Exchange Proxy V3 (alt)

    # ── Paraswap ─────────────────────────────────────────────────────────────
    "0x1bd435f3c054b6e901b7b108a0ab7617c808677b",  # Paraswap V4 Augustus
    "0xdef171fe48cf0115b1d80b88dc8eab59176fee57",  # Paraswap V5 Augustus
    "0x216b4b4ba9f3e719726886d34a177484278bfcae",  # Paraswap V5 TokenTransferProxy
    "0x6a000f20005980200259b80c5102003040001068",  # Paraswap V6.2 Augustus Swapper

    # ── CoW Protocol (Coincidence of Wants) ──────────────────────────────────
    "0x9008d19f58aabd9ed0d60971565aa8510560ab41",  # CoW Protocol GPv2Settlement
    "0xc92e8bdf79f0507f65a392b0ab4667716bfe0110",  # CoW Protocol GPv2VaultRelayer

    # ── Kyber Network ────────────────────────────────────────────────────────
    "0x9aab3f75489902f3a48495025729a0af77d4b11e",  # KyberSwap Classic Router
    "0x6131b5fae19ea4f9d964eac0408e4408b66337b5",  # KyberSwap Elastic Router
    "0x617dee16b86534a5d792a4d7a62fb491b544111e",  # KyberSwap Meta Aggregation V2

    # ── OKX DEX ──────────────────────────────────────────────────────────────
    "0xf332761c673b59b21ff6dfa8adda1dd2dc0d7ddf",  # OKX Dex Aggregator ETH

    # ── Odos ─────────────────────────────────────────────────────────────────
    "0xcf5540fffcdc3d510b18bfca6d2b9987b0772559",  # Odos Router V1
    "0x76f4eed9fe41262669d0cf8001d97be44aa1a0b7",  # Odos Router V2

    # ── OpenOcean ────────────────────────────────────────────────────────────
    "0x6352a56caadc4f1e25cd6c75970fa768a3304e64",  # OpenOcean V2 Exchange

    # ── Li.Fi ────────────────────────────────────────────────────────────────
    "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae",  # Li.Fi Diamond (cross-chain)

    # ── DODO ─────────────────────────────────────────────────────────────────
    "0x533da777aedce766ceae696bf90f8541a4ba80eb",  # DODO V1 Proxy
    "0xa356867fdcea8e71aeaf87805808803806231fdc",  # DODO V2 Proxy02
    "0xa2398842f37465f89540430bdc00219fa9e4d28a",  # DODO V2 RouteHelper

    # ── Bancor ───────────────────────────────────────────────────────────────
    "0x2f9ec37d6ccfff1cab21733bdadede11c823ccb0",  # Bancor V2 ContractRegistry
    "0xeef417e1d5cc832e619ae18d2f140de2999dd4fb",  # Bancor V3 Router

    # ── Transit Swap ─────────────────────────────────────────────────────────
    "0x00000047bb99ea4d791bb749d970de71ee0b1a34",  # Transit Swap V5

    # ── Hashflow ─────────────────────────────────────────────────────────────
    "0xf6a78083ca3e2a662d6dd1703c939c8ace2e268d",  # Hashflow Pool V3

    # ── WOOFi ────────────────────────────────────────────────────────────────
    "0x4c45575137773352e16e0f323f6d5a24c3e4c5e7",  # WooRouterV2 ETH

    # ── Maverick Protocol ────────────────────────────────────────────────────
    "0xeb0ef09f03bcbabb9e6e36d7e2f4e46ba60ea8a8",  # Maverick Router
    "0x32aa6f5bc847d2e2e2c94b7adc4c93a8b97c5284",  # Maverick Router V2

    # ── Bebop ────────────────────────────────────────────────────────────────
    "0xbebebeb035351f58602e0c1c8b59ecbff5d5f47b",  # Bebop JamSettlement

    # ── XY Finance ───────────────────────────────────────────────────────────
    "0xd50b7abcc25d6e98e7f3c6ca16fa4571ed0e2d57",  # XY Finance XY Router

    # ── Rubic ────────────────────────────────────────────────────────────────
    "0x3335733c454805df6a77f825f266e136fb4a3333",  # Rubic Multi Chain Router

    # ── Pendle Finance ───────────────────────────────────────────────────────
    "0x00000000005bbb0ef59571e58418f9a4357b68a0",  # Pendle Router V3

    # ── GMX (on Arbitrum, forwarded) ─────────────────────────────────────────
    # (added in arb-specific set below)

    # ── Camelot (forwarded to arb) ───────────────────────────────────────────
    # (added in arb-specific set below)

    # ── MEV / Flashbots known routers ────────────────────────────────────────
    "0x51c72848c68a965f66fa7a88855f9f7784502a7f",  # MEV Bot / DEX Arbitrage
    "0x00000000003b3cc22af3ae1eac0440bcee416b40",  # MEV Bot (known)
    "0x98c3d3183c4b8a650614ad179a1a98be0a8d6b8e",  # MEV Bot (known)
}

DEX_ROUTERS: dict[str, set[str]] = {

    # ── Ethereum Mainnet ─────────────────────────────────────────────────────
    "eth": _ETH_ROUTERS,

    # ── BNB Smart Chain ──────────────────────────────────────────────────────
    "bsc": {
        # PancakeSwap
        "0x05ff2b0db69458a0750badebc4f9e13add608c7f",  # PancakeSwap V1 (legacy)
        "0x10ed43c718714eb63d5aa57b78b54704e256024e",  # PancakeSwap V2
        "0x13f4ea83d0bd40e75c8222255bc855a974568dd4",  # PancakeSwap V3
        "0x9a489505a00ce272eaa5466b72226b16c3a07935",  # PancakeSwap Universal
        "0x678aa4bf4e210cf2166753e054d5b7c31cc7fa86",  # PancakeSwap SmartRouter V3
        # Native BSC DEXes
        "0x3a6d8ca21d1cf76f653cf26d04e7e28e75dc9f76",  # BiSwap V2
        "0xcf0febd3f17cef5b47b0cd257acf6025c5bff3b7",  # ApeSwap
        "0x7dae51bd3e3376b8c7c4900e9107f12be3af1ba8",  # MDEX BSC Router
        "0x325e343f1de602396e256b67efd1f61c3a6b38bd",  # BabySwap Router
        "0x20a304a7d126758dfe6b243d0fc515f83bef4ee5",  # Thena V1 Router (ve33)
        "0xd4ae6eca985340dd434d05d6ea3b72b1b4c09f4f",  # Thena V2 Router (FUSION)
        "0x4c45575137773352e16e0f323f6d5a24c3e4c5e7",  # WooRouterV2 BSC
        "0x8f8dd7db1bda5ed3da8c9daf3bfa471c12d58486",  # DODO BSC Proxy
        # Cross-chain aggregators (same address on BSC)
        "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506",  # SushiSwap BSC
        "0x1111111254eeb25477b68fb85ed929f73a960582",  # 1inch V5
        "0x111111125421ca6dc452d289314280a0f8842a65",  # 1inch V6
        "0xf332761c673b59b21ff6dfa8adda1dd2dc0d7ddf",  # OKX Dex BSC
        "0x6352a56caadc4f1e25cd6c75970fa768a3304e64",  # OpenOcean V2 BSC
        "0xcf5540fffcdc3d510b18bfca6d2b9987b0772559",  # Odos V1 BSC
        "0x76f4eed9fe41262669d0cf8001d97be44aa1a0b7",  # Odos V2 BSC
        "0xdef171fe48cf0115b1d80b88dc8eab59176fee57",  # Paraswap V5 BSC
        "0x6a000f20005980200259b80c5102003040001068",  # Paraswap V6 BSC
        "0x9008d19f58aabd9ed0d60971565aa8510560ab41",  # CoW Protocol BSC
        "0x00000047bb99ea4d791bb749d970de71ee0b1a34",  # Transit Swap V5
        "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae",  # Li.Fi Diamond
    },
}

# Polygon — inherits ETH routers + QuickSwap
DEX_ROUTERS["poly"] = _ETH_ROUTERS | {
    "0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff",  # QuickSwap V2 Router
    "0xf5b509bb0909a69b1c207e495f687a596c168e12",  # QuickSwap V3 / Algebra Router
    "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506",  # SushiSwap Polygon
    "0x4c45575137773352e16e0f323f6d5a24c3e4c5e7",  # WooRouterV2 Polygon
}

# Arbitrum — inherits ETH routers + Camelot + GMX
DEX_ROUTERS["arb"] = _ETH_ROUTERS | {
    "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506",  # SushiSwap Arbitrum
    "0xc873fecbd354f5a56e00e710b90ef4201db2448d",  # Camelot V2 Router
    "0x1f721e2e82f6676fce4ea07a5958cf098d339e18",  # Camelot V3 (Algebra)
    "0x182bd7b1e8e8c5a2b551e19f0c57e10b8a3a8d42",  # GMX V2 Router (Arb)
    "0xb87a436b93ffe9d75c5cfa7bacfff96430b09868",  # GMX V1 Router (Arb)
    "0x4c45575137773352e16e0f323f6d5a24c3e4c5e7",  # WooRouterV2 Arb
    "0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24",  # Uniswap V2 (Arb deployment)
    "0x1fC894cFEB5ca53c007B7855fde12e8d88A6e87b",  # Ramses Exchange Router
}

# Base — inherits ETH routers + Aerodrome + BaseSwap
DEX_ROUTERS["base"] = _ETH_ROUTERS | {
    "0xcf77a3ba9a5ca399b7c97c74d54e5b1beb874e43",  # Aerodrome Router V1
    "0x2626664c2603336e57b271c5c0b26f421741e481",  # Uniswap V3 SwapRouter02 (Base)
    "0x327df1e6de05895d2ab08513aadd9313fe505d86",  # BaseSwap Router
    "0x4c45575137773352e16e0f323f6d5a24c3e4c5e7",  # WooRouterV2 Base
    "0x6bfbcf4a0c4f0d1a5b5e7a9a3b6b2b0d7f4e3c1a",  # Horizon DEX (Base)
}

# Solana — program IDs (base58 public keys, not EVM addresses)
DEX_ROUTERS["sol"] = {
    # Jupiter Aggregator
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",   # Jupiter V6
    "juoLaVFJGfWJiudfmYB5jGUJTNo8hcFfUEQTmGEQMjA",   # Jupiter V4
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",   # Jupiter V3
    # Orca
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP",  # Orca Whirlpool V1
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzM3Mhe8bWTe",   # Orca Whirlpool V2
    "DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1",  # Orca Swap V2
    # Raydium
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM v4
    "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv",  # Raydium CLMM
    "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h",  # Raydium AMM v3
    "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS",   # Raydium Router
    # Meteora
    "LBUZKhRxPF3XUpBCjp4YzTKgLe4fen33oFqACaa1NA7",   # Meteora DLMM
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EqoizsW",   # Meteora Pools
    # Lifinity
    "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c",  # Lifinity Swap V2
    # Phoenix DEX
    "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY",   # Phoenix DEX
    # Aldrin
    "AMM55ShdkoioYAbecci3GHFpBDFPsQcHVnWGFxhCvFk2",  # Aldrin AMM V2
    # Saber (stableswap)
    "SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ",   # Saber StableSwap
}


# ── Known CEX hot-wallet / cold-wallet / custody addresses ───────────────────
# Maps lowercase EVM address → exchange name.
# Used by _classify_transfer() to label inflows/outflows as CEX deposits/withdrawals.
# Sources: Etherscan labels, Arkham Intelligence, Nansen, on-chain verification.
CEX_WALLETS: dict[str, str] = {

    # ── Binance ───────────────────────────────────────────────────────────────
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": "Binance",   # hot wallet 1
    "0xd551234ae421e3bcba99a0da6d736074f22192ff": "Binance",   # hot wallet 2
    "0x564286362092d8e7936f0549571a803b203aaced": "Binance",   # hot wallet 3
    "0x0681d8db095565fe8a346fa0277bffd65d1b22a6": "Binance",   # hot wallet 4
    "0xfe9e8709d3215310075d67e3ed32a380ccf451c8": "Binance",   # hot wallet 5
    "0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67": "Binance",   # hot wallet 6
    "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8": "Binance",   # cold wallet (largest)
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance",   # cold wallet 2
    "0x001866ae5b3de6caa5a51543fd9fb64f524f5478": "Binance",   # cold wallet 3
    "0x85b931a32a0725be14285b66f1a22178c672d69b": "Binance",   # cold wallet 4
    "0x708396f17127c42383e3b9014072679b2f60b82f": "Binance",   # cold wallet 5
    "0xe0f0cfde7ee664943906f17f7f14342e76a5cec": "Binance",   # cold wallet 6
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance",   # hot wallet 7
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance",   # hot wallet 8
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "Binance",   # hot wallet 9
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": "Binance",   # hot wallet 10
    "0x9696f59e4d72e237be84ffd425dcad154bf96976": "Binance",   # hot wallet 11
    "0x515b72ed8a97f42c568d6a143232775018f133c8": "Binance",   # hot wallet 12
    "0x1fbe2acee135d991592f167ac371f3dd893a508b": "Binance",   # hot wallet 13
    "0x4b16c5de96eb2117bbe5fd171e4d203624b014aa": "Binance",   # hot wallet 14
    "0xa344c7ada83113b3b56941f6e85bf2eb425949f3": "Binance",   # deposit relayer
    "0x7b2f052a372951d02798853e39ee56391c9f753d": "Binance",   # ETH reserve
    "0xc365c3315cf926351ccaf13fa7d19c8c4058c8e1": "Binance",   # reserve 2
    # Binance BSC / multi-chain reserves
    "0x8894e0a0c962cb723c1976a4421c95949be2d4e3": "Binance",   # BSC hot
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "Binance",   # BSC reserve
    "0xe2fc31f816a9b3aa1cab157dc79da99493e5f3fb": "Binance",   # BSC hot 2
    "0x29bdfbf7d27462a2d1c806d7ae10a6d3b7d0f89a": "Binance",  # BSC deposit

    # ── Coinbase ─────────────────────────────────────────────────────────────
    "0xa090e606e30bd747d4e6245a1517ebe430f0057e": "Coinbase",  # hot wallet 1
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "Coinbase",  # hot wallet 2
    "0x503828976d22510aad0201ac7ec88293211d23da": "Coinbase",  # hot wallet 3
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "Coinbase",  # hot wallet 4
    "0x881d40237659c251811cec9c364ef91234567890": "Coinbase",  # deposit
    "0x02466e547bfdab679fc49e96bbfc62b9747d997c": "Coinbase",  # hot wallet 5
    "0x25eaf624347d9fc4b3bd07d06c55d3a91a23adca": "Coinbase",  # hot wallet 6
    "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": "Coinbase",  # cold wallet
    "0x77696bb39917c91a0c3d2f8c61b5cc96d373b77f": "Coinbase",  # Coinbase 2
    "0x7c195d981abfdc3ddecd2ca0fed0958430488e34": "Coinbase",  # deposit 2
    "0x61755baea4f0cb1d2432b2c0188b6c8b3e0b5b53": "Coinbase Custody",
    "0x9eef87f4c08d8934cb2a3309df4dec5635338115": "Coinbase Custody",
    "0xbe686b0b5571de1bda31c0e5b14d6d3f9b9a7b1f": "Coinbase Prime",
    "0xb739d0895772dbb71a89a3754a160269068f0d45": "Coinbase Prime",

    # ── OKX / OKCoin ─────────────────────────────────────────────────────────
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX",       # hot wallet 1
    "0xa7efae728d2936e78bda97dc267687568dd593f3": "OKX",       # hot wallet 2
    "0x98ec059dc3adfbdd63429454aeb0c990fba4a128": "OKX",       # hot wallet 3
    "0x236f9f97e0e62388479bf9e1b0b4c6c8b8d7e89e": "OKX",      # cold wallet
    "0x461249076b88189f8ac9418de28b365859e46bfd": "OKX",       # hot wallet 4
    "0x8d12a197cb00d4747a1fe03395095ce2a5cc6819": "OKX",       # ERC-20 reserve
    "0x4cf4cf46f9f40ebb7d0a6e5d6b1e0f3a93d7fa60": "OKX",      # hot wallet 5
    "0xeb2629a2734e272bcc07bda959863f316f4bd4cf": "OKX",       # cold 2

    # ── Kraken ───────────────────────────────────────────────────────────────
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "Kraken",    # hot wallet 1
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "Kraken",    # hot wallet 2
    "0xe853c56864a2ebe4576a807d26fdc4a0ada51919": "Kraken",    # hot wallet 3
    "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": "Kraken",    # hot wallet 4
    "0xae2d4617c862309a3d75a0ffb358c7a5009c673f": "Kraken",    # cold wallet
    "0xda9dfa130df4de4673b89022ee50ff26f6ea73cf": "Kraken",    # hot wallet 5
    "0x8d6f396d210d385033b348bcae9e4f9ea4e045bd": "Kraken",    # hot wallet 6
    "0x53d284357ec70ce289d6d64134dfac8e511c8a3d": "Kraken",    # cold wallet 2
    "0xb9e0bc7c92c3b3a37863bccd39e1e6f2f9b2d849": "Kraken",    # reserve

    # ── Bybit ────────────────────────────────────────────────────────────────
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": "Bybit",     # hot wallet 1
    "0x2e6b049f62dd0f27e7bf5abc8c5817e3d3da26f3": "Bybit",     # hot wallet 2
    "0x1db3439a222c519ab44bb1144fc28167b4fa6ee6": "Bybit",     # cold wallet
    "0x6f489f7c60b10eb46429c43a5d1c32e9f4db04cb": "Bybit",     # hot wallet 3
    "0xd6c63a6654e0b3e22a37e1d7daf1dc7a0f0b6a1e": "Bybit",     # reserve
    "0x010eeafa4e7d4f8e43bd89daabd8d8dee18d3e7b": "Bybit",     # hot wallet 4
    "0xa9782bc3a7d5e88c27d7b5b3d11fa1af9f58fd08": "Bybit",     # cold 2

    # ── Bitfinex ─────────────────────────────────────────────────────────────
    "0x1151314c646ce4e0efd76d1af4760ae66a9fe30f": "Bitfinex",  # hot wallet 1
    "0x742d35cc6634c0532925a3b844bc454e4438f44e": "Bitfinex",  # hot wallet 2
    "0x876eabf441b2ee5b5b0554fd502a8e0600950cfa": "Bitfinex",  # cold wallet
    "0x477573f212a7bdd5f7c12889bd1ad0aa44fb82aa": "Bitfinex",  # cold wallet 2
    "0xd850942ef8811f2a866692a623011bde52a462c1": "Bitfinex",  # EOS multisig
    "0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98": "Bitfinex",  # reserve

    # ── HTX / Huobi ──────────────────────────────────────────────────────────
    "0xab5c66752a9e8167967685f1450532fb96d5d24f": "HTX",       # hot wallet 1
    "0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b": "HTX",       # hot wallet 2
    "0xfdb16996831753d5331ff813c29a93c76834a0ad": "HTX",       # hot wallet 3
    "0xe93381fb4c4f14bda253907b18fad305d799241a": "HTX",       # hot wallet 4
    "0x18709e89bd403f470088abdacebe86cc60dda12e": "HTX",       # hot wallet 5
    "0xfa4b5be3f5d7aad5a4a48a550ae43a35eb7b70a0": "HTX",       # cold wallet
    "0xa8660c8ffd6d578f657b72c0c811284aef0b735e": "HTX",       # cold wallet 2
    "0xdc76cd25977e0a5ae17155770273ad58648900d3": "HTX",       # reserve
    "0x5c985e89dde482efe97ea9f1950ad149eb73829b": "HTX",       # reserve 2

    # ── KuCoin ───────────────────────────────────────────────────────────────
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "KuCoin",    # hot wallet 1
    "0xa1d8d972560c2f8144af871db508f0b0b10a3fbf": "KuCoin",    # hot wallet 2
    "0xd6216fc19db775df9774a6e33526131da7d19a2c": "KuCoin",    # hot wallet 3
    "0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91": "KuCoin",    # cold wallet
    "0x689c56aef474df92d44a1b70850f808488f9769c": "KuCoin",    # reserve

    # ── Gate.io ──────────────────────────────────────────────────────────────
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "Gate.io",   # hot wallet 1
    "0x7793cd85c11a924478d358d49b05b37e91b5810f": "Gate.io",   # hot wallet 2
    "0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c": "Gate.io",   # hot wallet 3
    "0xd793281182a0e3e023116004778f45c29fc14f19": "Gate.io",   # cold wallet

    # ── Crypto.com ───────────────────────────────────────────────────────────
    "0x6262998ced04146fa42253a5c0af90ca02dfd2a3": "Crypto.com",
    "0x46340b20830761efd32832a74d7169b29feb9758": "Crypto.com",
    "0x72a53cdbbcc1b9efa39c834a540550e23463aacb": "Crypto.com",
    "0x7758e507850da48cd47df1fb5f875c23e3340c50": "Crypto.com",  # cold

    # ── Gemini ───────────────────────────────────────────────────────────────
    "0xd24400ae8bfebb18ca49be86258a3c749cf46853": "Gemini",    # hot wallet 1
    "0x07ee55aa48bb72dcc6e9d78256648910de513eca": "Gemini",    # hot wallet 2
    "0x5f65f7b609678448494de4c87521cdf6cef1e932": "Gemini",    # cold wallet
    "0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea": "Gemini",    # reserve

    # ── MEXC ─────────────────────────────────────────────────────────────────
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": "MEXC",     # hot wallet 1
    "0xa38913f1b4f4a3e285d49cfb7f3e98e09be6da02": "MEXC",     # hot wallet 2
    "0x4b3c57ba503abd4a07d9f9d0191b4df9e4e89b4e": "MEXC",     # reserve

    # ── Bithumb ──────────────────────────────────────────────────────────────
    "0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208": "Bithumb",   # hot wallet 1
    "0x4e44c8abc3bd35f87cf2c2ac5a36d2b17eb5f73f": "Bithumb",   # hot wallet 2

    # ── Upbit ────────────────────────────────────────────────────────────────
    "0x1a29b8b3af0ce8d42ebb4db62e9cffa2e4d61ded": "Upbit",    # hot wallet 1
    "0x4b7a6de82b99f7501ceeeedd3e6bf00265abc019": "Upbit",     # hot wallet 2
    "0xdc76cd25977e0a5ae17155770273ad58648900d3": "Upbit",     # cold wallet

    # ── Bitstamp ─────────────────────────────────────────────────────────────
    "0x00bdb5699745f5b860228c8f939abf1b9ae374ed": "Bitstamp",  # hot wallet 1
    "0x1e21fc91cb41a7d9f9c7f88a09fb4f3a2139c78b": "Bitstamp", # hot wallet 2
    "0x4bab51f285e3a9e06917a6f3fe21ab99b7a3f26a": "Bitstamp",  # reserve

    # ── Bittrex ──────────────────────────────────────────────────────────────
    "0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98": "Bittrex",  # hot wallet 1
    "0xe94b04a0fed112f3664e45adb2b8915693dd5ff3": "Bittrex",  # hot wallet 2

    # ── Poloniex ─────────────────────────────────────────────────────────────
    "0x32be343b94f860124dc4fee278fdcbd38c102d88": "Poloniex",  # hot wallet 1
    "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef": "Poloniex",  # cold wallet

    # ── Bitget ───────────────────────────────────────────────────────────────
    "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23": "Bitget",    # hot wallet 1
    "0x31e086a2c20cd9e4ae1dbd0c9d0f37847d68e4c2": "Bitget",    # hot wallet 2
    "0x5f7789f27bce72b5e3e4e8a7d1af7a02fffdaf00": "Bitget",    # cold wallet

    # ── Nexo ─────────────────────────────────────────────────────────────────
    "0x01ec5e7e03e2835bb2d1ae8d2edded298780129c": "Nexo",      # hot wallet 1
    "0xa14c7f5ba77765e56ed61ddbb22d9c43741b1f8b": "Nexo",      # cold wallet

    # ── Crypto.com (DeFi Wallet / separate custody) ───────────────────────────
    "0x021594030c44c8b31e35d5eede50b97e6c72b73d": "Crypto.com DeFi",

    # ── Robinhood Crypto ─────────────────────────────────────────────────────
    "0x22006c53c81264d5143e54fe7652ad8a3e61e86f": "Robinhood",  # custodial

    # ── eToro ────────────────────────────────────────────────────────────────
    "0x12a0e25e62c1dbd32e505446062b26aecb65f028": "eToro",

    # ── Phemex ───────────────────────────────────────────────────────────────
    "0x26a78d5b6d7a7aceedd1e6ee3229b372a624d8b7": "Phemex",

    # ── BingX ────────────────────────────────────────────────────────────────
    "0x51c35770f4f7c782d658d2f3d37c2a0ddd6b9f1a": "BingX",

    # ── Hotbit ───────────────────────────────────────────────────────────────
    "0x4a6f04c8c55c25b8e57f0abd55e3ca3f0ba6022a": "Hotbit",

    # ── Coincheck ────────────────────────────────────────────────────────────
    "0x2b70b4c1a3a5c31e65b0b3ade6e3a6b4b6e6b4b6": "Coincheck",

    # ── CoinEx ───────────────────────────────────────────────────────────────
    "0x3052f3a296e3a7e5bffac7c7c67e9d39ef0ba29f": "CoinEx",

    # ── Deribit ──────────────────────────────────────────────────────────────
    "0x77ad3a15b78101883af36ad4a875a6f5a978d80f": "Deribit",

    # ── Hyperliquid (on-chain CEX/perp) ──────────────────────────────────────
    "0x2df1c51e09aecf9cacb7bc98cb1742757f163df7": "Hyperliquid Bridge",
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
    # ── Gaming / Metaverse (kept for search; removed from popular list) ──────
    "MANA": "decentraland",  "SAND": "the-sandbox",
    "AXS": "axie-infinity",  "ENJ": "enjincoin",
    "IMX": "immutable-x",    "GALA": "gala",
    # ── AI / Infra ───────────────────────────────────────────────────────────
    "RNDR": "render-token",  "GRT": "the-graph",
    "FET": "fetch-ai",       "OCEAN": "ocean-protocol",
    "AGIX": "singularitynet","BAT": "basic-attention-token",
    "LPT": "livepeer",       "NMR": "numeraire",
    "KNC": "kyber-network-crystal", "CHZ": "chiliz",
    "ANKR": "ankr",          "DYDX": "dydx",
    "INJ": "injective-protocol", "CELR": "celer-network",
    "SKL": "skale",          "STORJ": "storj",
    # ── DeFi / Staking ───────────────────────────────────────────────────────
    "ENS": "ethereum-name-service", "stETH": "staked-ether",
    "rETH": "rocket-pool-eth",      "cbETH": "coinbase-wrapped-staked-eth",
    "weETH": "wrapped-eeth",        "rsETH": "kelp-dao-restaked-eth",
    "ezETH": "renzo-restaked-eth",  "CVX": "convex-finance",
    "BLUR": "blur",          "ARB": "arbitrum",
    "WLD": "worldcoin-wld",  "PENDLE": "pendle",
    "ENA": "ethena",         "USDe": "ethena-usde",
    "EIGEN": "eigenlayer",   "ETHFI": "ether-fi",
    "ONDO": "ondo-finance",  "MORPHO": "morpho",
    "POL": "polygon-ecosystem-token", "OP": "optimism",
    "CAKE": "pancakeswap-token",
    "XVS": "venus",          "FXS": "frax-share",
    "FLOKI": "floki",
}
