"""
scanner/analysis/pre_filter.py
Quick-Score ohne Claude. Filtert Kandidaten vor dem teuren API-Call.

Score-Logik:
    13F Filing Klasse A  → +4.0  (stärkstes Signal)
    13F Filing Klasse B  → +2.5
    13F Filing Klasse C  → +1.0
    RSI nicht überkauft  → +1.0
    Liquidity OK         → +1.0
    Ticker in RSS        → +1.0
    Allgemeine AI/Thiel RSS (min 2 Artikel) → +0.5
    Shulman empirical >= 2 → +0.5
    Stress ohne Filing   → -2.0
    Put/Call > 1.5       → -1.0

Schwellenwert: 2.0 (Anfangsphase ohne 13F)
Erhöhe auf 5.0 wenn regelmäßige 13F-Filings vorliegen.
"""

import logging
from ..utils.config import Config

logger = logging.getLogger(__name__)


class PreFilter:

    def quick_score(self, ticker: str, all_data: dict,
                    regime: dict, sec_data: dict) -> float:
        score = 0.0
        reasons = []

        # ── 13F FILING KLASSE ────────────────────────────────────
        classifications = sec_data.get("classifications", [])
        ticker_class    = next(
            (c for c in classifications if c["ticker"] == ticker), None
        )
        if ticker_class:
            cls_map = {"A": 4.0, "B": 2.5, "C": 1.0, "D": 0.0}
            filing_score = cls_map.get(ticker_class["class"], 0.0)
            score += filing_score
            if filing_score > 0:
                reasons.append(f"Filing-{ticker_class['class']}={filing_score}")

        # ── SC 13D / FORM4 OVERRIDE ──────────────────────────────
        # Sehr starke Signale aus neuen SEC Filing-Typen
        very_strong = sec_data.get("very_strong_signals", [])
        strong      = sec_data.get("strong_signals", [])
        if very_strong:
            score += 3.0
            reasons.append("SC13D_SIGNAL=+3.0")
        elif strong:
            score += 1.5
            reasons.append("STRONG_FILING=+1.5")

        # ── RSI (aus Tradier-Daten) ──────────────────────────────
        ticker_opts = all_data.get("options", {}).get(ticker, {})
        rsi         = ticker_opts.get("rsi", 50.0)
        if rsi < Config.CONTRARIAN_RSI_ELEVATED:
            score += 1.0
            reasons.append(f"RSI={rsi:.0f}<65")
        elif rsi > Config.CONTRARIAN_RSI_HIGH:
            score -= 0.5
            reasons.append(f"RSI={rsi:.0f}>75_PENALTY")

        # ── LIQUIDITY CHECK (Tradier) ────────────────────────────
        calls = (ticker_opts.get("target_calls", {})
                            .get("calls", []) or [])
        if calls:
            best_call = calls[0]
            vol = best_call.get("volume", 0) or 0
            if vol >= Config.MIN_DAILY_VOLUME:
                score += 1.0
                reasons.append(f"Volume={vol}>=500")

        # ── RSS: TICKER-SPEZIFISCHE ARTIKEL ─────────────────────
        rss      = all_data.get("rss", [])
        relevant = [
            a for a in rss
            if ticker in str(a.get("tickers", []))
        ]
        if len(relevant) >= 2:
            score += 1.0
            reasons.append(f"RSS_ticker={len(relevant)}_articles")
        elif len(relevant) == 1:
            score += 0.5
            reasons.append(f"RSS_ticker=1_article")

        # ── RSS: ALLGEMEINE THIEL/AI/ENERGIE SIGNALE ─────────────
        # Auch ohne direkten Ticker-Bezug relevant
        general_thiel = [
            a for a in rss
            if a.get("signals", {}).get("thiel")
        ]
        general_energy = [
            a for a in rss
            if a.get("signals", {}).get("bottleneck_energy")
        ]
        general_salp = [
            a for a in rss
            if a.get("signals", {}).get("salp")
        ]

        if len(general_thiel) >= 2:
            score += 0.5
            reasons.append(f"RSS_thiel={len(general_thiel)}")
        elif len(general_thiel) == 1:
            score += 0.25
            reasons.append(f"RSS_thiel=1")

        if len(general_energy) >= 2:
            # Extra Boost für Energie-Ticker
            sector = _get_sector(ticker)
            if sector == "energy_infrastructure":
                score += 0.75
                reasons.append(f"RSS_energy+sector_match={len(general_energy)}")
            else:
                score += 0.25
                reasons.append(f"RSS_energy={len(general_energy)}")

        if general_salp:
            score += 0.5
            reasons.append(f"RSS_salp={len(general_salp)}")

        # ── SHULMAN EMPIRICAL ────────────────────────────────────
        empirical = all_data.get("shulman_empirical_score", 0)
        if empirical >= 2:
            score += 0.5
            reasons.append(f"Shulman_empirical={empirical}")

        # ── EIA ENERGIE-WACHSTUM (für Energie-Ticker) ────────────
        eia_growth = all_data.get("eia", {}).get("growth_yoy")
        if eia_growth and eia_growth > Config.SHULMAN_EIA_GROWTH_THRESHOLD:
            sector = _get_sector(ticker)
            if sector == "energy_infrastructure":
                score += 0.5
                reasons.append(f"EIA_growth={eia_growth:.1%}")

        # ── STRESS-MODUS OHNE STARKES FILING ────────────────────
        if (regime.get("mode") == "STRESS" and
                (not ticker_class or ticker_class["class"] not in ["A", "B"]) and
                not very_strong):
            score -= 2.0
            reasons.append("STRESS_NO_FILING=-2.0")

        # ── PUT/CALL RATIO (bearisch) ────────────────────────────
        flow   = ticker_opts.get("options_flow", {})
        pc_vol = flow.get("put_call_volume")
        if pc_vol and pc_vol > 1.5:
            score -= 1.0
            reasons.append(f"PC_ratio={pc_vol:.2f}>1.5_BEARISH")

        final_score = round(score, 1)

        if final_score >= Config.PRE_FILTER_THRESHOLD:
            logger.info(
                f"Pre-Filter PASS: {ticker} score={final_score} | "
                f"{' | '.join(reasons)}"
            )
        else:
            logger.info(
                f"Pre-Filter: {ticker} score={final_score} "
                f"< {Config.PRE_FILTER_THRESHOLD} — skip"
            )

        return final_score

    def should_call_claude(self, ticker: str, all_data: dict,
                           regime: dict, sec_data: dict) -> tuple:
        score  = self.quick_score(ticker, all_data, regime, sec_data)
        should = score >= Config.PRE_FILTER_THRESHOLD
        return should, score


def _get_sector(ticker: str) -> str:
    """Hilfsfunktion für Sektor-Lookup ohne TickerMapper-Import."""
    sector_map = {
        "VST":  "energy_infrastructure",
        "CEG":  "energy_infrastructure",
        "NRG":  "energy_infrastructure",
        "XEL":  "energy_infrastructure",
        "NEE":  "energy_infrastructure",
        "PLTR": "sovereign_ai_defense",
        "LMT":  "sovereign_ai_defense",
        "RTX":  "sovereign_ai_defense",
        "NVDA": "compute_hardware",
        "TSM":  "compute_hardware",
        "AVGO": "compute_hardware",
    }
    return sector_map.get(ticker, "unknown")
