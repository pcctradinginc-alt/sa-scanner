"""
scanner/sources/data_fetcher.py

FIXES v2:
    R1: EIA Einheiten-Normalisierung — Series ELEC.GEN.TOTAL-US-99.M
        in Tausend MWh, konsistente Einheit, korrekte Wachstumsberechnung
    R2: Finnhub CapEx — explizites Logging wenn leer, Null-Data-Flag
    R6: NVDA Schwellenwert von 20% auf 10% reduziert (realistisch 2026)
    R7: Credibility-Score korrigiert — Qualität über Quantität
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

from ..utils.config import Config
from ..utils.rate_limiter import rate_limiter
from ..utils.ticker_mapper import TickerMapper
from .tradier_client import TradierClient

logger = logging.getLogger(__name__)

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
        self.quality = {}

    # ── ENERGY BREADTH ───────────────────────────────────────────

    def get_energy_breadth(self) -> dict:
        logger.info("Fetching energy breadth via yfinance")
        above_200d = 0
        total      = 0
        details    = {}

        for ticker in Config.ENERGY_TICKERS:
            try:
                rate_limiter.wait("yfinance")
                data = yf.Ticker(ticker).history(
                    period="1y", auto_adjust=True
                )
                if len(data) >= 200:
                    current = data["Close"].iloc[-1]
                    ma200   = data["Close"].rolling(200).mean().iloc[-1]
                    above   = bool(current > ma200)
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
            "energy_breadth":  round(breadth, 3),
            "above_200d":      above_200d,
            "total_checked":   total,
            "details":         details,
            "fetched_at":      datetime.utcnow().isoformat(),
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

    # ── R1 FIX: EIA MIT KORREKTER SERIE UND EINHEIT ──────────────

    def get_eia_electricity_growth(self) -> dict:
        """
        R1 FIX: Nutzt Series ELEC.GEN.TOTAL-US-99.M
        Einheit: Tausend MWh (konsistent, keine Konversionsprobleme)
        Explizites Logging bei Datenlücken.
        """
        logger.info("Fetching EIA electricity demand (Series: ELEC.GEN.TOTAL-US-99.M)")

        if not Config.EIA_API_KEY:
            self.quality["eia"] = "NO_KEY"
            logger.warning("EIA: No API key — empirical_point=0 (data gap, not signal)")
            return {
                "error":          "No EIA API key",
                "growth_yoy":     None,
                "empirical_point": 0,
                "data_gap":       True,  # R3: explizites Flag
            }

        try:
            rate_limiter.wait("eia")

            # R1 FIX: Spezifische Serie für US-Gesamtstrom in Tausend MWh
            # Konsistente Einheit, keine Aggregationsprobleme
            url    = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
            params = {
                "api_key":                Config.EIA_API_KEY,
                "frequency":              "monthly",
                "data[0]":                "generation",
                "facets[fueltypeid][]":   "ALL",
                "facets[location][]":     "US",  # R1: nur US-Gesamt
                "sort[0][column]":        "period",
                "sort[0][direction]":     "desc",
                "length":                 14,    # 13 Monate + Buffer
            }
            r    = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json().get("response", {}).get("data", [])

            if not data:
                # R3: Explizites Logging — Datenlücke vs. kein Wachstum
                logger.warning(
                    "EIA: Empty response — empirical_point=0 "
                    "(DATA GAP, not negative signal)"
                )
                self.quality["eia"] = "EMPTY_RESPONSE"
                return {
                    "growth_yoy":     None,
                    "empirical_point": 0,
                    "data_gap":        True,
                }

            # Aggregiere nach Periode (alle Fuel-Types summieren)
            by_month: dict = {}
            for row in data:
                period = row.get("period", "")
                gen    = row.get("generation")
                if period and gen is not None:
                    try:
                        # R1: Alle Werte in derselben Einheit (Tausend MWh)
                        by_month[period] = by_month.get(period, 0) + float(gen)
                    except (ValueError, TypeError):
                        continue

            sorted_months = sorted(by_month.keys(), reverse=True)

            if len(sorted_months) < 13:
                logger.warning(
                    f"EIA: Only {len(sorted_months)} months available, "
                    f"need 13 — empirical_point=0 (DATA GAP)"
                )
                self.quality["eia"] = "INSUFFICIENT_DATA"
                return {
                    "growth_yoy":     None,
                    "empirical_point": 0,
                    "data_gap":        True,
                    "months_available": len(sorted_months),
                }

            latest   = by_month[sorted_months[0]]
            year_ago = by_month[sorted_months[12]]

            if year_ago == 0:
                logger.warning("EIA: year_ago value is 0 — cannot calculate growth")
                return {"growth_yoy": None, "empirical_point": 0, "data_gap": True}

            growth = (latest - year_ago) / year_ago
            shulman_signal = growth > Config.SHULMAN_EIA_GROWTH_THRESHOLD

            logger.info(
                f"EIA: growth={growth:.1%} | "
                f"threshold={Config.SHULMAN_EIA_GROWTH_THRESHOLD:.1%} | "
                f"empirical_point={1 if shulman_signal else 0} | "
                f"unit=thousand_MWh"
            )

            self.quality["eia"] = "OK"
            return {
                "growth_yoy":      round(growth, 4),
                "latest_period":   sorted_months[0],
                "latest_value":    round(latest, 0),
                "year_ago_value":  round(year_ago, 0),
                "unit":            "thousand_MWh",  # R1: explizite Einheit
                "shulman_signal":  shulman_signal,
                "empirical_point": 1 if shulman_signal else 0,
                "data_gap":        False,
                "fetched_at":      datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"EIA fetch error: {e}")
            self.quality["eia"] = "ERROR"
            return {
                "error":          str(e),
                "growth_yoy":     None,
                "empirical_point": 0,
                "data_gap":        True,
            }

    # ── FRED MAKRO ───────────────────────────────────────────────

    def get_fred_data(self) -> dict:
        logger.info("Fetching FRED macro data")
        if not Config.FRED_API_KEY:
            self.quality["fred"] = "NO_KEY"
            return {"error": "No FRED API key", "data_gap": True}

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
                    "observation_start": (
                        datetime.utcnow() - timedelta(days=400)
                    ).strftime("%Y-%m-%d"),
                    "sort_order":        "desc",
                    "limit":             14,
                }
                r    = requests.get(url, params=params, timeout=20)
                r.raise_for_status()
                obs  = r.json().get("observations", [])
                valid = [
                    (o["date"], float(o["value"]))
                    for o in obs if o["value"] != "."
                ]

                if len(valid) >= 13:
                    latest   = valid[0][1]
                    year_ago = valid[12][1]
                    growth   = (latest - year_ago) / year_ago
                    results[name] = {
                        "latest":      round(latest, 2),
                        "year_ago":    round(year_ago, 2),
                        "growth_yoy":  round(growth, 4),
                        "period":      valid[0][0],
                        "data_gap":    False,
                    }
                else:
                    # R3: Explizites Data-Gap-Flag
                    logger.warning(
                        f"FRED {series_id}: only {len(valid)} observations "
                        f"(need 13) — DATA GAP"
                    )
                    results[name] = {
                        "data_gap":    True,
                        "obs_count":   len(valid),
                    }

            except Exception as e:
                logger.warning(f"FRED {series_id}: {e}")
                results[name] = {"error": str(e), "data_gap": True}

        self.quality["fred"] = "OK" if any(
            not v.get("data_gap") for v in results.values()
            if isinstance(v, dict)
        ) else "ERROR"

        results["fetched_at"] = datetime.utcnow().isoformat()
        return results

    # ── R2 FIX: FINNHUB CAPEX MIT EXPLIZITEM NULL-DATA-LOGGING ───

    def get_hyperscaler_capex(self) -> dict:
        """
        R2 FIX: Explizites Logging wenn Finnhub leere CapEx-Listen liefert.
        Unterscheidet zwischen DATA GAP und echtem Nicht-Wachstum.
        """
        logger.info("Fetching hyperscaler CapEx via Finnhub")
        results = {}

        for ticker in Config.HYPERSCALER_TICKERS:
            try:
                rate_limiter.wait("finnhub")
                data   = self.fh.company_basic_financials(ticker, "all")
                series = (data.get("series", {}) or {}).get("annual", {}) or {}
                capex  = series.get("capitalExpenditures", []) or []

                # R2 FIX: Explizites Logging bei leerer Liste
                if not capex:
                    logger.warning(
                        f"Finnhub CapEx {ticker}: empty list returned — "
                        f"DATA GAP (Finnhub Free Tier limitation)"
                    )
                    results[ticker] = {
                        "data_gap": True,
                        "reason":   "finnhub_free_tier_no_capex",
                    }
                    continue

                if len(capex) < 2:
                    logger.warning(
                        f"Finnhub CapEx {ticker}: only {len(capex)} "
                        f"quarters — need 2 for YoY (DATA GAP)"
                    )
                    results[ticker] = {
                        "data_gap":    True,
                        "reason":      "insufficient_quarters",
                        "quarters":    len(capex),
                    }
                    continue

                recent   = sorted(
                    capex,
                    key=lambda x: x.get("period", ""),
                    reverse=True
                )
                latest   = recent[0].get("v", 0) or 0
                previous = recent[1].get("v", 0) or 0

                if previous == 0:
                    logger.warning(
                        f"Finnhub CapEx {ticker}: previous quarter = 0 "
                        f"— cannot calculate growth (DATA GAP)"
                    )
                    results[ticker] = {"data_gap": True, "reason": "zero_previous"}
                    continue

                growth = (latest - previous) / abs(previous)
                results[ticker] = {
                    "latest_capex":  latest,
                    "prev_capex":    previous,
                    "growth_yoy":    round(growth, 4),
                    "period":        recent[0].get("period"),
                    "data_gap":      False,
                }
                logger.info(
                    f"Finnhub CapEx {ticker}: {growth:.1%} YoY"
                )

            except Exception as e:
                logger.warning(f"Finnhub CapEx {ticker}: {e}")
                results[ticker] = {"error": str(e), "data_gap": True}

        # Aggregierter Trend — nur aus Nicht-Gap-Daten
        valid_growths = [
            v["growth_yoy"] for v in results.values()
            if isinstance(v, dict) and not v.get("data_gap")
            and v.get("growth_yoy") is not None
        ]

        if valid_growths:
            avg_growth    = sum(valid_growths) / len(valid_growths)
            trend         = (
                "rising"  if avg_growth > 0.05 else
                "falling" if avg_growth < -0.05 else
                "stable"
            )
            empirical_p   = 1 if avg_growth > Config.SHULMAN_CAPEX_GROWTH_THRESHOLD else 0
            data_gap_total = False
            logger.info(
                f"CapEx aggregate: {avg_growth:.1%} | "
                f"trend={trend} | "
                f"empirical_point={empirical_p} | "
                f"from {len(valid_growths)}/{len(Config.HYPERSCALER_TICKERS)} tickers"
            )
        else:
            # R3: Klare Unterscheidung DATA GAP
            avg_growth    = None
            trend         = "unknown"
            empirical_p   = 0
            data_gap_total = True
            logger.warning(
                "CapEx: ALL tickers returned DATA GAP — "
                "empirical_point=0 (not a negative signal)"
            )

        self.quality["finnhub_capex"] = (
            "OK" if valid_growths else "DATA_GAP"
        )

        return {
            "by_ticker":        results,
            "avg_growth_yoy":   round(avg_growth, 4) if avg_growth else None,
            "capex_trend":      trend,
            "empirical_point":  empirical_p,
            "data_gap":         data_gap_total,
            "valid_tickers":    len(valid_growths),
            "total_tickers":    len(Config.HYPERSCALER_TICKERS),
            "fetched_at":       datetime.utcnow().isoformat(),
        }

    # ── R6 FIX: NVDA SCHWELLENWERT 10% STATT 20% ─────────────────

    def get_nvda_revenue_growth(self) -> dict:
        """
        R6 FIX: Schwellenwert von 20% auf 10% reduziert.
        Nach dem 2023/2024 Hyperwachstum ist 20% für 2026 zu hoch.
        10% YoY ist weiterhin sehr starkes Wachstum für ein 1T+ Unternehmen.
        """
        try:
            rate_limiter.wait("finnhub")
            data   = self.fh.company_basic_financials("NVDA", "all")
            series = (data.get("series", {}) or {}).get("annual", {}) or {}
            rev    = series.get("revenue", []) or []

            if not rev:
                logger.warning(
                    "NVDA Revenue: empty from Finnhub — DATA GAP"
                )
                return {
                    "growth_yoy":     None,
                    "empirical_point": 0,
                    "data_gap":        True,
                }

            if len(rev) < 2:
                logger.warning(
                    f"NVDA Revenue: only {len(rev)} quarters — DATA GAP"
                )
                return {
                    "growth_yoy":     None,
                    "empirical_point": 0,
                    "data_gap":        True,
                }

            recent   = sorted(
                rev,
                key=lambda x: x.get("period", ""),
                reverse=True
            )
            latest   = recent[0].get("v", 0) or 0
            previous = recent[1].get("v", 0) or 0

            if previous == 0:
                return {
                    "growth_yoy":     None,
                    "empirical_point": 0,
                    "data_gap":        True,
                }

            growth = (latest - previous) / abs(previous)

            # R6 FIX: 10% statt 20% — realistischer für 2026
            NVDA_THRESHOLD_2026 = 0.10
            empirical_p = 1 if growth > NVDA_THRESHOLD_2026 else 0

            logger.info(
                f"NVDA Revenue: {growth:.1%} YoY | "
                f"threshold={NVDA_THRESHOLD_2026:.0%} | "
                f"empirical_point={empirical_p}"
            )

            return {
                "growth_yoy":     round(growth, 4),
                "empirical_point": empirical_p,
                "threshold_used":  NVDA_THRESHOLD_2026,
                "latest_revenue":  latest,
                "period":          recent[0].get("period"),
                "data_gap":        False,
            }

        except Exception as e:
            logger.warning(f"NVDA revenue: {e}")
            return {
                "growth_yoy":     None,
                "empirical_point": 0,
                "data_gap":        True,
                "error":           str(e),
            }

    # ── R7 FIX: RSS MIT KORRIGIERTEM CREDIBILITY-SCORING ─────────

    # Signal-Keywords
    THIEL_KEYWORDS = [
        "Peter Thiel", "Katechon", "Sovereign AI",
        "Founders Fund", "Thiel Capital", "Anduril",
        "Antichrist", "permanent crisis", "Palmer Luckey",
        "David Sacks", "J.D. Vance", "Alex Karp",
    ]
    SHULMAN_KEYWORDS = [
        "Carl Shulman", "doubling times", "compute overhang",
        "intelligence explosion", "robot doublings",
        "algorithmic efficiency", "compute substitut",
    ]
    CONTRARIAN_KEYWORDS = [
        "AI winter", "software overhang", "energy abundance",
        "scaling plateau", "efficiency breakthrough",
        "AI regulation", "AI moratorium", "AI ban",
    ]
    BOTTLENECK_COMPUTE = [
        "Nvidia", "H100", "H200", "GPU shortage",
        "compute capacity", "TSMC", "chip demand",
    ]
    BOTTLENECK_ENERGY = [
        "power demand", "grid capacity", "SMR", "nuclear AI",
        "data center energy", "hyperscaler power",
        "electricity AI", "energy infrastructure",
    ]
    SALP_KEYWORDS = [
        "Situational Awareness", "Leopold Aschenbrenner",
        "SALP", "AI safety fund", "Decade Ahead",
    ]

    def fetch_rss(self) -> list:
        """
        R7 FIX: Credibility-Score korrigiert.
        Qualität (Credibility) wird nicht durch Quantität (Signal-Count) 
        überstimmt. Artikel werden nach Credibility-gewichtetem
        Primär-Signal sortiert, nicht nach rohem Produkt.
        """
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
                        "thiel":              any(
                            k.lower() in text.lower()
                            for k in self.THIEL_KEYWORDS
                        ),
                        "shulman":            any(
                            k.lower() in text.lower()
                            for k in self.SHULMAN_KEYWORDS
                        ),
                        "contrarian":         any(
                            k.lower() in text.lower()
                            for k in self.CONTRARIAN_KEYWORDS
                        ),
                        "bottleneck_compute": any(
                            k.lower() in text.lower()
                            for k in self.BOTTLENECK_COMPUTE
                        ),
                        "bottleneck_energy":  any(
                            k.lower() in text.lower()
                            for k in self.BOTTLENECK_ENERGY
                        ),
                        "salp":               any(
                            k.lower() in text.lower()
                            for k in self.SALP_KEYWORDS
                        ),
                    }

                    if not any(signals.values()):
                        continue

                    signal_count = sum(signals.values())

                    # R7 FIX: Credibility ist primärer Faktor
                    # Hohe Credibility + 1 Signal > Niedrige Credibility + 6 Signale
                    # Formel: credibility^2 * (1 + log(signal_count))
                    import math
                    quality_score = (
                        credibility ** 2 * (1 + math.log(signal_count + 1))
                    )

                    results.append({
                        "source":        feed_name,
                        "credibility":   credibility,
                        "title":         entry.title,
                        "summary":       entry.get("summary", "")[:500],
                        "url":           entry.link,
                        "published":     pub.isoformat(),
                        "signals":       signals,
                        "signal_count":  signal_count,
                        "tickers":       tickers,
                        # R7: quality_score statt weighted_relevance
                        "quality_score": round(quality_score, 4),
                        # Legacy-Feld für Kompatibilität
                        "weighted_relevance": quality_score,
                    })

            except Exception as e:
                logger.warning(f"RSS {feed_name}: {e}")

        # R7 FIX: Sortierung nach quality_score (credibility-dominant)
        results.sort(key=lambda x: x["quality_score"], reverse=True)
        self.quality["rss"] = f"{len(results)} articles"
        logger.info(
            f"RSS: {len(results)} relevant articles | "
            f"top source: {results[0]['source'] if results else 'none'}"
        )
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
                rsi        = self.get_rsi(ticker)
                data["rsi"] = rsi
                results[ticker] = data
            except Exception as e:
                logger.error(f"Options {ticker}: {e}")
                results[ticker] = {"error": str(e)}

        self.quality["tradier"] = "OK"
        return results

    # ── MAIN ORCHESTRATION ───────────────────────────────────────

    def fetch_all(self, state_manager, laufzeit_months: int = 6) -> dict:
        logger.info("=== DataFetcher: starting full fetch ===")
        Config.ensure_dirs()
        all_data = {}
        errors   = []

        # 1. Energy Breadth
        try:
            all_data["energy_breadth"] = self.get_energy_breadth()
        except Exception as e:
            errors.append(f"energy_breadth: {e}")
            all_data["energy_breadth"] = {
                "energy_breadth": 0.5, "data_gap": True
            }

        # 2. EIA (R1 Fix)
        try:
            all_data["eia"] = self.get_eia_electricity_growth()
        except Exception as e:
            errors.append(f"eia: {e}")
            all_data["eia"] = {
                "growth_yoy": None, "empirical_point": 0, "data_gap": True
            }

        # 3. FRED
        try:
            all_data["fred"] = self.get_fred_data()
        except Exception as e:
            errors.append(f"fred: {e}")
            all_data["fred"] = {"data_gap": True}

        # 4. Hyperscaler CapEx (R2 Fix)
        try:
            all_data["hyperscaler_capex"] = self.get_hyperscaler_capex()
        except Exception as e:
            errors.append(f"capex: {e}")
            all_data["hyperscaler_capex"] = {
                "capex_trend": "unknown", "empirical_point": 0,
                "data_gap": True
            }

        # 5. NVDA Revenue (R6 Fix)
        try:
            all_data["nvda_revenue"] = self.get_nvda_revenue_growth()
        except Exception as e:
            errors.append(f"nvda: {e}")
            all_data["nvda_revenue"] = {
                "empirical_point": 0, "data_gap": True
            }

        # 6. RSS (R7 Fix)
        try:
            all_data["rss"] = self.fetch_rss()
        except Exception as e:
            errors.append(f"rss: {e}")
            all_data["rss"] = []

        # 7. Options (Tradier)
        try:
            all_data["options"] = self.fetch_options_data(
                state_manager, laufzeit_months
            )
        except Exception as e:
            errors.append(f"options: {e}")
            all_data["options"] = {}

        # 8. Shulman Empirical Score
        eia_point   = all_data["eia"].get("empirical_point", 0)
        capex_point = all_data["hyperscaler_capex"].get("empirical_point", 0)
        nvda_point  = all_data["nvda_revenue"].get("empirical_point", 0)
        empirical   = eia_point + capex_point + nvda_point

        # R3: Data-Gap-Tracking für Shulman
        eia_gap   = all_data["eia"].get("data_gap", False)
        capex_gap = all_data["hyperscaler_capex"].get("data_gap", False)
        nvda_gap  = all_data["nvda_revenue"].get("data_gap", False)

        all_data["shulman_empirical_score"] = empirical
        all_data["shulman_data_gaps"] = {
            "eia_gap":   eia_gap,
            "capex_gap": capex_gap,
            "nvda_gap":  nvda_gap,
            "any_gap":   any([eia_gap, capex_gap, nvda_gap]),
            "all_gaps":  all([eia_gap, capex_gap, nvda_gap]),
        }

        # 9. Quality Report
        all_data["data_quality"] = {
            **self.quality,
            "errors":                  errors,
            "shulman_empirical_score": empirical,
            "shulman_data_gaps":       all_data["shulman_data_gaps"],
            "fetched_at":              datetime.utcnow().isoformat(),
        }

        self._write_outputs(all_data)

        logger.info(
            f"=== DataFetcher done | "
            f"Shulman Empirical: {empirical}/3 | "
            f"Data Gaps: EIA={eia_gap} CapEx={capex_gap} NVDA={nvda_gap} | "
            f"Errors: {len(errors)} ==="
        )
        return all_data

    def _write_outputs(self, all_data: dict):
        sig = Config.SIGNALS_DIR
        outputs = {
            "market_regime.json": {
                "energy_breadth":     all_data.get("energy_breadth", {}),
                "hyperscaler_capex":  all_data.get("hyperscaler_capex", {}),
                "fred":               all_data.get("fred", {}),
                "eia":                all_data.get("eia", {}),
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
                "score":       all_data.get("shulman_empirical_score", 0),
                "eia_point":   all_data["eia"].get("empirical_point", 0),
                "capex_point": all_data["hyperscaler_capex"].get("empirical_point", 0),
                "nvda_point":  all_data["nvda_revenue"].get("empirical_point", 0),
                "data_gaps":   all_data.get("shulman_data_gaps", {}),
            },
        }
        for filename, data in outputs.items():
            try:
                (sig / filename).write_text(
                    json.dumps(data, indent=2, default=str)
                )
            except Exception as e:
                logger.error(f"Write {filename}: {e}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    logging.basicConfig(level=logging.INFO)
    from scanner.utils.state_manager import StateManager
    with StateManager() as sm:
        fetcher  = DataFetcher(sm)
        all_data = fetcher.fetch_all(sm)
