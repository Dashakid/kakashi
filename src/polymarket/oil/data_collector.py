"""
Oil market data collector — aggregates 4 sources into a unified context object.

Sources:
  1. GDELT geopolitical headlines (free, no key)
  2. EIA crude inventory weekly data (free API key from eia.gov)
  3. OPEC/sanctions RSS feeds (Reuters + FT)
  4. WTI price momentum via yfinance
"""

import json
import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp
import feedparser
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY", "")

OIL_KEYWORDS = {"oil", "crude", "opec", "sanctions", "barrel", "wti", "brent", "petroleum"}

RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://www.investing.com/rss/news_14.rss",  # Commodities
    "https://oilprice.com/rss/main",               # OilPrice.com
]


# ---------------------------------------------------------------------------
# Source 1 — GDELT headlines
# ---------------------------------------------------------------------------

async def fetch_gdelt_headlines(session: aiohttp.ClientSession) -> list[dict]:
    url = (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        "?query=oil+crude+OPEC+sanctions"
        "&mode=artlist&maxrecords=10&format=json"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; OilTrader/1.0; research)"
    }
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 429:
                print("[GDELT] Rate limited (429), skipping.")
                return []
            if resp.status != 200:
                print(f"[GDELT] HTTP {resp.status}")
                return []
            data = await resp.json(content_type=None)
            articles = data.get("articles", [])
            return [
                {
                    "title": a.get("title", ""),
                    "url": a.get("url", ""),
                    "seendate": a.get("seendate", ""),
                    "sourcecountry": a.get("sourcecountry", ""),
                }
                for a in articles
            ]
    except Exception as e:
        print(f"[GDELT] Error: {e}")
        return []


# ---------------------------------------------------------------------------
# Source 2 — EIA crude inventory (weekly)
# ---------------------------------------------------------------------------

async def fetch_eia_inventory(session: aiohttp.ClientSession) -> Optional[int]:
    """Returns the most recent weekly inventory change in barrels.
    Negative = draw (bullish), Positive = build (bearish).
    """
    if not EIA_API_KEY:
        print("[EIA] Warning: EIA_API_KEY not set, skipping.")
        return None

    url = (
        "https://api.eia.gov/v2/petroleum/sum/sndw/data/"
        f"?api_key={EIA_API_KEY}"
        "&frequency=weekly"
        "&data[0]=value"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
        "&length=4"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                print(f"[EIA] HTTP {resp.status}")
                return None
            data = await resp.json(content_type=None)
            rows = data.get("response", {}).get("data", [])
            if not rows:
                return None
            # Most recent weekly value
            latest = rows[0].get("value")
            return int(latest * 1000) if latest is not None else None  # convert Mbbl → bbl
    except Exception as e:
        print(f"[EIA] Error: {e}")
        return None


# ---------------------------------------------------------------------------
# Source 3 — RSS feeds (OPEC / sanctions)
# ---------------------------------------------------------------------------

def _is_recent(entry, hours: int = 24) -> bool:
    """Return True if the RSS entry was published within the last `hours` hours."""
    try:
        import time as _time
        published = entry.get("published_parsed") or entry.get("updated_parsed")
        if not published:
            return True  # keep if date unknown
        entry_dt = datetime.fromtimestamp(_time.mktime(published), tz=timezone.utc)
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
        return entry_dt >= cutoff
    except Exception:
        return True


def fetch_rss_headlines() -> list[dict]:
    headlines = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                title = entry.get("title", "").lower()
                summary = entry.get("summary", "").lower()
                text = title + " " + summary
                if any(kw in text for kw in OIL_KEYWORDS) and _is_recent(entry):
                    headlines.append({
                        "title": entry.get("title", ""),
                        "link": entry.get("link", ""),
                        "published": entry.get("published", ""),
                        "source": feed_url,
                    })
        except Exception as e:
            print(f"[RSS] Error fetching {feed_url}: {e}")
    return headlines


# ---------------------------------------------------------------------------
# Source 4 — WTI price momentum via yfinance
# ---------------------------------------------------------------------------

def fetch_wti_momentum() -> dict:
    try:
        wti = yf.Ticker("CL=F")
        hist = wti.history(period="5d")
        if hist.empty:
            return {}
        current_price = float(hist["Close"].iloc[-1])
        open_price = float(hist["Close"].iloc[0])
        change_pct = round((current_price - open_price) / open_price * 100, 2)
        return {
            "wti_price": round(current_price, 2),
            "wti_5d_change_pct": change_pct,
            "wti_5d_high": round(float(hist["High"].max()), 2),
            "wti_5d_low": round(float(hist["Low"].min()), 2),
        }
    except Exception as e:
        print(f"[WTI] Error: {e}")
        return {}


# ---------------------------------------------------------------------------
# Unified collector
# ---------------------------------------------------------------------------

async def collect_oil_context() -> dict:
    """Collect all sources and return a unified context dict."""
    async with aiohttp.ClientSession() as session:
        gdelt_task = asyncio.create_task(fetch_gdelt_headlines(session))
        eia_task = asyncio.create_task(fetch_eia_inventory(session))

        # RSS and yfinance are sync — run in executor threads
        loop = asyncio.get_running_loop()
        rss_task = loop.run_in_executor(None, fetch_rss_headlines)
        wti_task = loop.run_in_executor(None, fetch_wti_momentum)

        gdelt_headlines, eia_inventory, rss_headlines, wti_data = await asyncio.gather(
            gdelt_task, eia_task, rss_task, wti_task
        )

    context = {
        "timestamp": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "gdelt_headlines": gdelt_headlines,
        "eia_inventory_change_barrels": eia_inventory,
        "rss_headlines": rss_headlines,
        **wti_data,
    }
    return context


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    result = asyncio.run(collect_oil_context())
    print(json.dumps(result, indent=2))
