"""
scanner/sources/data_fetcher.py
Koordiniert alle Datenquellen mit Rate-Limiting, Caching und Fallback.

QUELLEN:
    Tradier (Vollzugriff)   → Option Chain, IV-Historie, Greeks, Flow
    yfinance (kostenlos)    → Kurse, Energy-Breite, RSI-Basis
    FRED API (kostenlos)    → Makro, Strom-Nachfrage-Proxy
    EIA API (kostenlos)     → Energie-Nachfrage (Shulman IGE-Proxy)
    Finnhub (Free Tier)     → Fundamentals, CapEx, Insider
    RSS-Feeds               → News mit Credibility-Scoring
    SEC EDGAR               → 13F-Erkennung

OUTPUT:
    data/signals/market_regime.json
    data/signals/options_data.json
    data/signals/energy_data.json
    data/signals/rss_signals.json
    data/signals/fundamentals.json
    data/signals/data_quality.json
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
import requests_cache
import feedparser
import yfinance as yf
import finnhub
import pandas as pd
import numpy as np
from tenacity import (
    retry, stop_after_attempt,
    wait_exponential, retry_if_exception_type,
)

from ..utils.config import Config
from ..utils.rate_limiter import rate_limiter
from ..utils.ticker_mapper import TickerMapper
from .tradier_client import TradierClient

logger = logging.getLogger(__name__)

# requests-cache Setup
Config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
requests_cache.install_cache(
    str(Config.CACHE_DIR / "sa_scanner_cache"),
    expire_after=3600,
)


class DataFetcher:

    def __init__(self, state_manager=None):
        self.state   = state_manager
        self.mapper  = TickerMapper()
        self.tradier = TradierClient()
        self.fh      = finnhub.Client(api_key=Config.FINNHUB_API_KEY or "")
        self.quality = {}   # Datenqualitäts-Tracking

    # ── ENERGY BREADTH (yfinance) ────────────────────────────────

    def get_energy_breadth(self) -> dict:
        logger.info("Fetching energy breadth via yfinance")
        above_200d = 0
        total      = 0
        details    = {}

        for ticker in Config.ENERGY_TICKERS:
            try:
                rate_limiter.wait("yfinance")
                data = yf.Ticker(ticker).history(period="1y", auto_adjust=True)
                if len(data) >= 200:
                    current = data["Close"].iloc[-1]
                    ma200   = data["Close"].rolling(200).mean().iloc[-1]
                    above   = current > ma200
                    details[ticker] = {
                        "current": round(float(current), 2),
                        "ma200":   round(float(ma200), 2),
                        "above":   above,
                    }
                    if above:
                        above_200d += 1
                    total += 1
            except Exception as e:
                logger.warning(f"yfinance {ticker}: {e}")

        breadth = above_200d / total if total > 0 else 0.5
        self.quality["energy_breadth"] = "OK" if total > 5 else "PARTIAL"

        return {
            "energy_breadth":     round(breadth, 3),
            "above_200d":         above_200d,
            "total_checked":      total,
            "details":            details,
            "fetched_at":         datetime.utcnow().isoformat(),
        }

    def get_rsi(self, ticker: str, period: int = 14) -> float:
        try:
            rate_limiter.wait("yfinance")
            data  = yf.Ticker(ticker).history(period="3mo")["Close"]
            if len(data) < period + 1:
                return 50.0
            delta = data.diff()
            gain  = delta.clip(lower=0).rolling(period).mean()
            loss  = (-delta.clip(upper=0)).rolling(period).mean()
            rs    = gain / loss
            rsi   = 100 - (100 / (1 + rs))
            return round(float(rsi.iloc[-1]), 1)
        except Exception as e:
            logger.warning(f"RSI {ticker}: {e}")
            return 50.0

    # ── EIA ENERGIE-DATEN ────────────────────────────────────────

    def get_eia_electricity_growth(self) -> dict:
        logger.info("Fetching EIA electricity demand data")
        if not Config.EIA_API_KEY:
            self.quality["eia"] = "NO_KEY"
            return {"error": "No EIA API key", "growth_yoy": None}

        try:
            rate_limiter.wait("eia")
            url    = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
            params = {
                "api_key":              Config.EIA_API_KEY,
                "frequency":            "monthly",
                "data[0]":              "generation",
                "facets[fueltypeid][]": "ALL",
                "sort[0][column]":      "period",
                "sort[0][direction]":   "desc",
                "length":               13,
            }
            r    = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json().get("response", {}).get("data", [])

            if len(data) < 13:
                self.quality["eia"] = "PARTIAL"
                return {"error": "Not enough EIA data", "growth_yoy": None}

            # Aggregiere über alle Fuel Types pro Monat
            by_month: dict = {}
            for row in data:
                period = row.get("period", "")
                gen    = row.get("generation")
                if period and gen is not None:
                    by_month[period] = by_month.get(period, 0) + float(gen)

            sorted_months = sorted(by_month.keys(), reverse=True)
            if len(sorted_months) < 13:
                self.quality["eia"] = "PARTIAL"
                return {"growth_yoy": None}

            latest    = by_month[sorted_months[0]]
            year_ago  = by_month[sorted_months[12]]
            growth    = (latest - year_ago) / year_ago if year_ago else None

            self.quality["eia"] = "OK"
            shulman_signal = growth is not None and growth > Config.SHULMAN_EIA_GROWTH_THRESHOLD

            return {
                "growth_yoy":      round(growth, 4) if growth else None,
                "latest_period":   sorted_months[0],
                "latest_gwh":      round(latest, 0),
                "shulman_signal":  shulman_signal,
                "empirical_point": 1 if shulman_signal else 0,
                "fetched_at":      datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"EIA fetch error: {e}")
            self.quality["eia"] = "ERROR"
            return {"error": str(e), "growth_yoy": None}

    # ── FRED MAKRO-DATEN ─────────────────────────────────────────

    def get_fred_data(self) -> dict:
        logger.info("Fetching FRED macro data")
        if not Config.FRED_API_KEY:
            self.quality["fred"] = "NO_KEY"
            return {"error": "No FRED API key"}

        results = {}
        series  = {
            "IPG2211A2N": "electric_power_production",
            "INDPRO":     "industrial_production",
            "CPIAUCSL":   "cpi_inflation",
        }

        for series_id, name in series.items():
            try:
                rate_limiter.wait("fred")
                url    = "https://api.stlouisfed.org/fred/series/observations"
                params = {
                    "series_id":         series_id,
                    "api_key":           Config.FRED_API_KEY,
                    "file_type":         "json",
                    "observation_start": (datetime.utcnow() - timedelta(days=400)).strftime("%Y-%m-%d"),
                    "sort_order":        "desc",
                    "limit":             14,
                }
                r    = requests.get(url, params=params, timeout=20)
                r.raise_for_status()
                obs  = r.json().get("observations", [])

                valid = [(o["date"], float(o["value"]))
                         for o in obs if o["value"] != "."]

                if len(valid) >= 13:
                    latest   = valid[0][1]
                    year_ago = valid[12][1]
                    growth   = (latest - year_ago) / year_ago
                    results[name] = {
                        "latest":      round(latest, 2),
                        "year_ago":    round(year_ago, 2),
                        "growth_yoy":  round(growth, 4),
                        "period":      valid[0][0],
                    }

            except Exception as e:
                logger.warning(f"FRED {series_id}: {e}")
                results[name] = {"error": str(e)}

        self.quality["fred"] = "OK" if results else "ERROR"
        results["fetched_at"] = datetime.utcnow().isoformat()
        return results

    # ── FINNHUB FUNDAMENTALS / CAPEX ─────────────────────────────

    def get_hyperscaler_capex(self) -> dict:
        logger.info("Fetching hyperscaler CapEx via Finnhub")
        results = {}

        for ticker in Config.HYPERSCALER_TICKERS:
            try:
                rate_limiter.wait("finnhub")
                data   = self.fh.company_basic_financials(ticker, "all")
                series = (data.get("series", {}) or {}).get("annual", {}) or {}
                capex  = series.get("capitalExpenditures", []) or []

                if len(capex) >= 2:
                    recent   = sorted(capex, key=lambda x: x.get("period", ""), reverse=True)
                    latest   = recent[0].get("v", 0) or 0
                    previous = recent[1].get("v", 0) or 0
                    growth   = (latest - previous) / abs(previous) if previous else None
                    results[ticker] = {
                        "latest_capex":  latest,
                        "prev_capex":    previous,
                        "growth_yoy":    round(growth, 4) if growth else None,
                        "period":        recent[0].get("period"),
                    }
                else:
                    results[ticker] = {"error": "Insufficient data"}

            except Exception as e:
                logger.warning(f"Finnhub CapEx {ticker}: {e}")
                results[ticker] = {"error": str(e)}

        # Aggregierter CapEx-Trend
        growths = [v["growth_yoy"] for v in results.values()
                   if isinstance(v, dict) and v.get("growth_yoy") is not None]

        if growths:
            avg_growth  = sum(growths) / len(growths)
            trend       = "rising" if avg_growth > 0.05 else \
                          "falling" if avg_growth < -0.05 else "stable"
            empirical_p = 1 if avg_growth > Config.SHULMAN_CAPEX_GROWTH_THRESHOLD else 0
        else:
            avg_growth  = None
            trend       = "unknown"
            empirical_p = 0

        self.quality["finnhub_capex"] = "OK" if growths else "PARTIAL"

        return {
            "by_ticker":       results,
            "avg_growth_yoy":  round(avg_growth, 4) if avg_growth else None,
            "capex_trend":     trend,
            "empirical_point": empirical_p,
            "fetched_at":      datetime.utcnow().isoformat(),
        }

    def get_nvda_revenue_growth(self) -> dict:
        try:
            rate_limiter.wait("finnhub")
            data   = self.fh.company_basic_financials("NVDA", "all")
            series = (data.get("series", {}) or {}).get("annual", {}) or {}
            rev    = series.get("revenue", []) or []

            if len(rev) >= 2:
                recent   = sorted(rev, key=lambda x: x.get("period", ""), reverse=True)
                latest   = recent[0].get("v", 0) or 0
                previous = recent[1].get("v", 0) or 0
                growth   = (latest - previous) / abs(previous) if previous else None
                empirical_p = 1 if (growth and growth > Config.SHULMAN_NVDA_GROWTH_THRESHOLD) else 0
                return {
                    "growth_yoy":    round(growth, 4) if growth else None,
                    "empirical_point": empirical_p,
                    "latest_revenue": latest,
                    "period":         recent[0].get("period"),
                }
        except Exception as e:
            logger.warning(f"NVDA revenue: {e}")

        return {"growth_yoy": None, "empirical_point": 0}

    # ── RSS CRAWLER ──────────────────────────────────────────────

    THIEL_KEYWORDS      = ["Peter Thiel", "Katechon", "Sovereign AI",
                           "Founders Fund", "Thiel Capital", "Anduril",
                           "Antichrist", "permanent crisis", "Palmer Luckey",
                           "David Sacks", "J.D. Vance", "Alex Karp"]
    SHULMAN_KEYWORDS    = ["Carl Shulman", "doubling times", "compute overhang",
                           "intelligence explosion", "robot doublings",
                           "algorithmic efficiency", "compute substitut"]
    CONTRARIAN_KEYWORDS = ["AI winter", "software overhang", "energy abundance",
                           "scaling plateau", "efficiency breakthrough",
                           "AI regulation", "AI moratorium", "AI ban"]
    BOTTLENECK_COMPUTE  = ["Nvidia", "H100", "H200", "GPU shortage",
                           "compute capacity", "TSMC", "chip demand"]
    BOTTLENECK_ENERGY   = ["power demand", "grid capacity", "SMR", "nuclear AI",
                           "data center energy", "hyperscaler power",
                           "electricity AI", "energy infrastructure"]
    SALP_KEYWORDS       = ["Situational Awareness", "Leopold Aschenbrenner",
                           "SALP", "AI safety fund", "Decade Ahead"]

    def fetch_rss(self) -> list:
        logger.info("Fetching RSS feeds")
        results = []
        cutoff  = datetime.utcnow() - timedelta(hours=36)

        for feed_name, (url, credibility) in Config.RSS_FEEDS.items():
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries:
                    try:
                        pub = datetime(*entry.published_parsed[:6])
                    except Exception:
                        continue
                    if pub < cutoff:
                        continue

                    text    = f"{entry.title} {entry.get('summary', '')}"
                    tickers = self.mapper.extract_tickers_from_text(text)

                    signals = {
                        "thiel":           any(k.lower() in text.lower() for k in self.THIEL_KEYWORDS),
                        "shulman":         any(k.lower() in text.lower() for k in self.SHULMAN_KEYWORDS),
                        "contrarian":      any(k.lower() in text.lower() for k in self.CONTRARIAN_KEYWORDS),
                        "bottleneck_compute": any(k.lower() in text.lower() for k in self.BOTTLENECK_COMPUTE),
                        "bottleneck_energy":  any(k.lower() in text.lower() for k in self.BOTTLENECK_ENERGY),
                        "salp":            any(k.lower() in text.lower() for k in self.SALP_KEYWORDS),
                    }

                    if any(signals.values()):
                        results.append({
                            "source":      feed_name,
                            "credibility": credibility,
                            "title":       entry.title,
                            "summary":     entry.get("summary", "")[:500],
                            "url":         entry.link,
                            "published":   pub.isoformat(),
                            "signals":     signals,
                            "tickers":     tickers,
                            "weighted_relevance": credibility * sum(signals.values()),
                        })
            except Exception as e:
                logger.warning(f"RSS {feed_name}: {e}")

        results.sort(key=lambda x: x["weighted_relevance"], reverse=True)
        self.quality["rss"] = f"{len(results)} articles"
        logger.info(f"RSS: {len(results)} relevant articles")
        return results

    # ── OPTIONS DATA (Tradier) ────────────────────────────────────

    def fetch_options_data(self, state_manager,
                           laufzeit_months: int = 6) -> dict:
        logger.info(f"Fetching Tradier option data ({laufzeit_months}M)")
        results = {}

        for ticker in Config.TARGET_TICKERS:
            try:
                data = self.tradier.analyze_ticker_options(
                    ticker, laufzeit_months, state_manager
                )
                rsi  = self.get_rsi(ticker)
                data["rsi"] = rsi
                results[ticker] = data
            except Exception as e:
                logger.error(f"Options {ticker}: {e}")
                results[ticker] = {"error": str(e)}

        self.quality["tradier"] = "OK"
        return results

    # ── MAIN FETCH ORCHESTRATION ─────────────────────────────────

    def fetch_all(self, state_manager, laufzeit_months: int = 6) -> dict:
        logger.info("=== DataFetcher: starting full fetch ===")
        Config.ensure_dirs()
        all_data = {}
        errors   = []

        # 1. Energy Breadth
        try:
            energy_breadth = self.get_energy_breadth()
            all_data["energy_breadth"] = energy_breadth
        except Exception as e:
            errors.append(f"energy_breadth: {e}")
            all_data["energy_breadth"] = {"energy_breadth": 0.5}

        # 2. EIA Energie-Daten
        try:
            eia = self.get_eia_electricity_growth()
            all_data["eia"] = eia
        except Exception as e:
            errors.append(f"eia: {e}")
            all_data["eia"] = {"growth_yoy": None, "empirical_point": 0}

        # 3. FRED Makro
        try:
            fred = self.get_fred_data()
            all_data["fred"] = fred
        except Exception as e:
            errors.append(f"fred: {e}")
            all_data["fred"] = {}

        # 4. Hyperscaler CapEx (Finnhub)
        try:
            capex = self.get_hyperscaler_capex()
            all_data["hyperscaler_capex"] = capex
        except Exception as e:
            errors.append(f"capex: {e}")
            all_data["hyperscaler_capex"] = {"capex_trend": "unknown", "empirical_point": 0}

        # 5. NVDA Revenue (Shulman-Proxy)
        try:
            nvda = self.get_nvda_revenue_growth()
            all_data["nvda_revenue"] = nvda
        except Exception as e:
            errors.append(f"nvda: {e}")
            all_data["nvda_revenue"] = {"empirical_point": 0}

        # 6. RSS Feeds
        try:
            rss = self.fetch_rss()
            all_data["rss"] = rss
        except Exception as e:
            errors.append(f"rss: {e}")
            all_data["rss"] = []

        # 7. Option Chain + IV (Tradier)
        try:
            options = self.fetch_options_data(state_manager, laufzeit_months)
            all_data["options"] = options
        except Exception as e:
            errors.append(f"options: {e}")
            all_data["options"] = {}

        # 8. Shulman Empirical Score berechnen
        empirical_score = (
            all_data["eia"].get("empirical_point", 0) +
            all_data["hyperscaler_capex"].get("empirical_point", 0) +
            all_data["nvda_revenue"].get("empirical_point", 0)
        )
        all_data["shulman_empirical_score"] = empirical_score

        # 9. Datenqualitäts-Report
        all_data["data_quality"] = {
            **self.quality,
            "errors":       errors,
            "fetched_at":   datetime.utcnow().isoformat(),
            "shulman_empirical_score": empirical_score,
        }

        # Output schreiben
        self._write_outputs(all_data)
        logger.info(f"=== DataFetcher done. Shulman Empirical: {empirical_score}/3. "
                    f"Errors: {len(errors)} ===")
        return all_data

    def _write_outputs(self, all_data: dict):
        sig = Config.SIGNALS_DIR

        outputs = {
            "market_regime.json": {
                "energy_breadth":         all_data.get("energy_breadth", {}),
                "hyperscaler_capex":       all_data.get("hyperscaler_capex", {}),
                "fred":                    all_data.get("fred", {}),
                "eia":                     all_data.get("eia", {}),
            },
            "options_data.json":    all_data.get("options", {}),
            "energy_data.json":     all_data.get("eia", {}),
            "rss_signals.json":     all_data.get("rss", []),
            "fundamentals.json": {
                "hyperscaler_capex": all_data.get("hyperscaler_capex", {}),
                "nvda_revenue":      all_data.get("nvda_revenue", {}),
            },
            "data_quality.json":    all_data.get("data_quality", {}),
            "shulman_empirical.json": {
                "score":      all_data.get("shulman_empirical_score", 0),
                "eia_point":  all_data["eia"].get("empirical_point", 0),
                "capex_point":all_data["hyperscaler_capex"].get("empirical_point", 0),
                "nvda_point": all_data["nvda_revenue"].get("empirical_point", 0),
            },
        }

        for filename, data in outputs.items():
            try:
                path = sig / filename
                path.write_text(json.dumps(data, indent=2, default=str))
            except Exception as e:
                logger.error(f"Write {filename}: {e}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    logging.basicConfig(level=logging.INFO)
    from scanner.utils.state_manager import StateManager
    with StateManager() as sm:
        fetcher = DataFetcher(sm)
        fetcher.fetch_all(sm)
