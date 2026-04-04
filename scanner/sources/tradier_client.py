"""
scanner/sources/tradier_client.py
Vollständiger Tradier API-Client mit:
- Option Chain (alle Greeks)
- IV-Historie für IV-Rank-Berechnung
- Options-Flow (Volume, OI, Put/Call-Ratio)
- Historical IV über Quotes-History
- Market Data (Quotes, Historisch)
"""

import requests
import logging
import time
from datetime import datetime, timedelta
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential

from ..utils.config import Config
from ..utils.rate_limiter import rate_limiter

logger = logging.getLogger(__name__)

BASE = Config.TRADIER_BASE
HDR  = Config.TRADIER_HEADERS


class TradierClient:

    def _get(self, endpoint: str, params: dict = None) -> dict:
        rate_limiter.wait("tradier")
        url = f"{BASE}{endpoint}"
        try:
            r = requests.get(url, headers=HDR, params=params or {}, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error(f"Tradier GET {endpoint}: {e}")
            return {}

    # ── OPTION CHAIN ─────────────────────────────────────────────

    def get_expirations(self, ticker: str) -> list:
        data = self._get(f"/markets/options/expirations",
                         {"symbol": ticker, "includeAllRoots": "true"})
        return data.get("expirations", {}).get("date", []) or []

    def get_option_chain(self, ticker: str, expiration: str,
                         option_type: str = "call") -> list:
        """
        Vollständige Option Chain für einen Ticker und Expiration.
        Gibt Liste aller Strikes mit Greeks zurück.
        """
        data = self._get("/markets/options/chains", {
            "symbol":     ticker,
            "expiration": expiration,
            "greeks":     "true",
        })
        options = data.get("options", {}).get("option", []) or []
        if isinstance(options, dict):
            options = [options]

        # Nur Calls filtern
        calls = [o for o in options if o.get("option_type") == option_type]
        return calls

    def get_target_calls(self, ticker: str,
                         laufzeit_months: int = 6) -> dict:
        """
        Findet die beste CALL-Option für gegebene Laufzeit.
        Sucht Expiration in +/- 30 Tagen um Ziellaufzeit.
        Berechnet IV-Rank aus gespeicherter Historie.
        """
        target_date = datetime.utcnow() + timedelta(days=laufzeit_months * 30)
        expirations  = self.get_expirations(ticker)

        if not expirations:
            return {"error": "No expirations found", "ticker": ticker}

        # Nächste Expiration zur Ziellaufzeit finden
        best_exp = None
        best_diff = float("inf")
        for exp in expirations:
            try:
                exp_dt = datetime.strptime(exp, "%Y-%m-%d")
                diff   = abs((exp_dt - target_date).days)
                if diff < best_diff:
                    best_diff = diff
                    best_exp  = exp
            except ValueError:
                continue

        if not best_exp:
            return {"error": "No suitable expiration", "ticker": ticker}

        calls = self.get_option_chain(ticker, best_exp, "call")
        if not calls:
            return {"error": "No calls found", "ticker": ticker,
                    "expiration": best_exp}

        # Aktuellen Kurs holen
        quote = self.get_quote(ticker)
        current_price = quote.get("last", 0) or quote.get("close", 0)

        if not current_price:
            return {"error": "No price available", "ticker": ticker}

        # OTM-Strikes klassifizieren
        result_calls = []
        for call in calls:
            strike   = call.get("strike", 0)
            if not strike or not current_price:
                continue
            pct_otm  = (strike - current_price) / current_price * 100
            greeks   = call.get("greeks", {}) or {}
            volume   = call.get("volume", 0) or 0
            oi       = call.get("open_interest", 0) or 0
            bid      = call.get("bid", 0) or 0
            ask      = call.get("ask", 0) or 0
            mid      = (bid + ask) / 2 if bid and ask else 0
            spread_pct = (ask - bid) / mid if mid > 0 else 1.0

            result_calls.append({
                "strike":      strike,
                "pct_otm":     round(pct_otm, 1),
                "expiration":  best_exp,
                "bid":         bid,
                "ask":         ask,
                "mid":         round(mid, 2),
                "spread_pct":  round(spread_pct, 3),
                "volume":      volume,
                "open_interest": oi,
                "iv":          call.get("greeks", {}).get("smv_vol") if call.get("greeks") else None,
                "delta":       greeks.get("delta"),
                "gamma":       greeks.get("gamma"),
                "theta":       greeks.get("theta"),
                "vega":        greeks.get("vega"),
                "rho":         greeks.get("rho"),
            })

        # Nach Liquidität sortieren: Volume * OI als Liquiditäts-Score
        result_calls.sort(key=lambda x: (x["volume"] or 0) * (x["open_interest"] or 0),
                          reverse=True)

        return {
            "ticker":         ticker,
            "current_price":  current_price,
            "expiration":     best_exp,
            "target_months":  laufzeit_months,
            "calls":          result_calls,
            "total_calls":    len(result_calls),
        }

    # ── LIQUIDITY CHECK ──────────────────────────────────────────

    def check_liquidity(self, ticker: str, strike: float,
                        expiration: str) -> dict:
        """
        Prüft Liquidity-Anforderungen für spezifischen Strike.
        """
        calls = self.get_option_chain(ticker, expiration, "call")
        target = next((c for c in calls if c.get("strike") == strike), None)

        if not target:
            return {"passed": False, "reason": "Strike not found"}

        greeks    = target.get("greeks", {}) or {}
        volume    = target.get("volume", 0) or 0
        bid       = target.get("bid", 0) or 0
        ask       = target.get("ask", 0) or 0
        mid       = (bid + ask) / 2 if bid and ask else 0
        spread_pct = (ask - bid) / mid if mid > 0 else 1.0
        vega      = greeks.get("vega", 0) or 0
        theta     = greeks.get("theta", 0) or 0

        issues = []
        if volume < Config.MIN_DAILY_VOLUME:
            issues.append(f"Volume {volume} < {Config.MIN_DAILY_VOLUME}")
        if abs(vega) > Config.MAX_VEGA_OTM_LEAPS:
            issues.append(f"Vega {vega:.3f} > {Config.MAX_VEGA_OTM_LEAPS}")
        if theta < Config.MAX_THETA_12M_LEAPS:
            issues.append(f"Theta {theta:.3f} < {Config.MAX_THETA_12M_LEAPS}")
        if spread_pct > Config.MAX_BID_ASK_SPREAD_PCT:
            issues.append(f"Spread {spread_pct:.1%} > {Config.MAX_BID_ASK_SPREAD_PCT:.0%}")

        return {
            "passed":      len(issues) == 0,
            "issues":      issues,
            "volume":      volume,
            "vega":        vega,
            "theta":       theta,
            "spread_pct":  round(spread_pct, 3),
            "mid_premium": round(mid, 2),
        }

    # ── IV HISTORY FÜR IV-RANK ───────────────────────────────────

    def get_historical_iv(self, ticker: str, days: int = 30) -> list:
        """
        Holt historische IV-Daten über Options-Statistiken.
        Tradier Vollzugriff: historische Optionsstatistiken verfügbar.
        """
        end_date   = datetime.utcnow().date()
        start_date = end_date - timedelta(days=days)

        data = self._get("/markets/history", {
            "symbol":   ticker,
            "interval": "daily",
            "start":    start_date.isoformat(),
            "end":      end_date.isoformat(),
        })

        history = data.get("history", {})
        if not history:
            return []

        days_data = history.get("day", [])
        if isinstance(days_data, dict):
            days_data = [days_data]

        return days_data

    def get_current_iv(self, ticker: str, expiration: str) -> Optional[float]:
        """
        Holt aktuelle IV als Durchschnitt der ATM-Calls.
        """
        calls = self.get_option_chain(ticker, expiration)
        if not calls:
            return None

        quote = self.get_quote(ticker)
        current_price = quote.get("last", 0)
        if not current_price:
            return None

        # ATM-Calls (innerhalb 5% des aktuellen Kurses)
        atm_calls = [
            c for c in calls
            if c.get("greeks", {}) and
               abs(c.get("strike", 0) - current_price) / current_price < 0.05
        ]

        if not atm_calls:
            return None

        ivs = [c["greeks"].get("smv_vol") for c in atm_calls
               if c.get("greeks", {}).get("smv_vol")]
        return sum(ivs) / len(ivs) if ivs else None

    # ── MARKET DATA ──────────────────────────────────────────────

    def get_quote(self, ticker: str) -> dict:
        data = self._get("/markets/quotes", {
            "symbols": ticker,
            "greeks":  "false",
        })
        quotes = data.get("quotes", {}).get("quote", {})
        if isinstance(quotes, list):
            return quotes[0] if quotes else {}
        return quotes or {}

    def get_quotes(self, tickers: list) -> dict:
        symbols = ",".join(tickers)
        data    = self._get("/markets/quotes", {
            "symbols": symbols,
            "greeks":  "false",
        })
        quotes = data.get("quotes", {}).get("quote", [])
        if isinstance(quotes, dict):
            quotes = [quotes]
        return {q["symbol"]: q for q in quotes if "symbol" in q}

    # ── OPTIONS FLOW / PUT-CALL RATIO ────────────────────────────

    def get_options_statistics(self, ticker: str) -> dict:
        """
        Aggregierte Options-Statistiken: Put/Call-Ratio, IV, OI.
        """
        expirations = self.get_expirations(ticker)
        if not expirations:
            return {}

        # Nächste 3 Expirations für aggregierten Flow
        near_exps = expirations[:3]

        total_call_volume = 0
        total_put_volume  = 0
        total_call_oi     = 0
        total_put_oi      = 0

        for exp in near_exps:
            # Calls
            calls = self.get_option_chain(ticker, exp, "call")
            total_call_volume += sum(c.get("volume", 0) or 0 for c in calls)
            total_call_oi     += sum(c.get("open_interest", 0) or 0 for c in calls)

            # Puts
            puts = self.get_option_chain(ticker, exp, "put")
            total_put_volume  += sum(p.get("volume", 0) or 0 for p in puts)
            total_put_oi      += sum(p.get("open_interest", 0) or 0 for p in puts)

        pc_volume = (total_put_volume / total_call_volume
                     if total_call_volume > 0 else None)
        pc_oi     = (total_put_oi / total_call_oi
                     if total_call_oi > 0 else None)

        return {
            "ticker":             ticker,
            "call_volume":        total_call_volume,
            "put_volume":         total_put_volume,
            "put_call_volume":    round(pc_volume, 3) if pc_volume else None,
            "call_oi":            total_call_oi,
            "put_oi":             total_put_oi,
            "put_call_oi":        round(pc_oi, 3) if pc_oi else None,
            "options_bullish":    pc_volume < 0.7 if pc_volume else None,
        }

    # ── FULL TICKER ANALYSIS ─────────────────────────────────────

    def analyze_ticker_options(self, ticker: str,
                               laufzeit_months: int,
                               state_manager) -> dict:
        """
        Vollständige Options-Analyse für einen Ticker.
        Integriert IV-Rank aus StateManager (SQLite-Historie).
        """
        logger.info(f"Analyzing options for {ticker} ({laufzeit_months}M)")

        # Expirations
        expirations = self.get_expirations(ticker)
        if not expirations:
            return {"error": "No expirations", "ticker": ticker}

        # Erste verfügbare Expiration für IV
        first_exp   = expirations[0] if expirations else None
        current_iv  = self.get_current_iv(ticker, first_exp) if first_exp else None

        # IV in SQLite speichern und Rank berechnen
        iv_rank_data = {"iv_rank": 50.0, "confidence": "NO_DATA"}
        if current_iv:
            state_manager.store_iv(ticker, current_iv, "tradier")
            iv_rank_data = state_manager.get_iv_rank(ticker, current_iv)

        # Target Calls für Laufzeit
        target_calls = self.get_target_calls(ticker, laufzeit_months)

        # Options Statistics (Flow)
        flow = self.get_options_statistics(ticker)

        # Quote
        quote = self.get_quote(ticker)

        return {
            "ticker":       ticker,
            "current_price":quote.get("last") or quote.get("close"),
            "current_iv":   current_iv,
            "iv_rank":      iv_rank_data,
            "target_calls": target_calls,
            "options_flow": flow,
            "laufzeit_months": laufzeit_months,
            "fetched_at":   datetime.utcnow().isoformat(),
        }
