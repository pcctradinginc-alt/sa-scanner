"""
scanner/analysis/pre_filter.py
Quick-Score ohne Claude. Filtert Kandidaten vor dem teuren API-Call.
"""

import logging
from ..utils.config import Config

logger = logging.getLogger(__name__)


class PreFilter:

    def quick_score(self, ticker: str, all_data: dict,
                    regime: dict, sec_data: dict) -> float:
        score = 0.0

        # 13F Filing Klasse
        classifications = sec_data.get("classifications", [])
        ticker_class    = next((c for c in classifications if c["ticker"] == ticker), None)
        if ticker_class:
            cls_map = {"A": 4.0, "B": 2.5, "C": 1.0, "D": 0.0}
            score  += cls_map.get(ticker_class["class"], 0.0)

        # RSI nicht überkauft
        ticker_opts = all_data.get("options", {}).get(ticker, {})
        rsi         = ticker_opts.get("rsi", 50.0)
        if rsi < Config.CONTRARIAN_RSI_ELEVATED:
            score += 1.0
        elif rsi > Config.CONTRARIAN_RSI_HIGH:
            score -= 0.5

        # Liquidity-Check (Tradier)
        calls = ticker_opts.get("target_calls", {}).get("calls", [])
        if calls:
            best_call = calls[0]
            if (best_call.get("volume", 0) or 0) >= Config.MIN_DAILY_VOLUME:
                score += 1.0

        # Mindestens 2 RSS-Artikel
        rss      = all_data.get("rss", [])
        relevant = [a for a in rss if ticker in str(a.get("tickers", []))]
        if len(relevant) >= 2:
            score += 1.0

        # Shulman empirischer Score >= 2
        empirical = all_data.get("shulman_empirical_score", 0)
        if empirical >= 2:
            score += 0.5

        # Stress-Regime ohne starkes Filing
        if regime.get("mode") == "STRESS" and (not ticker_class or ticker_class["class"] not in ["A", "B"]):
            score -= 2.0

        # Put/Call-Ratio bearish
        flow   = ticker_opts.get("options_flow", {})
        pc_vol = flow.get("put_call_volume")
        if pc_vol and pc_vol > 1.5:
            score -= 1.0

        return round(score, 1)

    def should_call_claude(self, ticker: str, all_data: dict,
                           regime: dict, sec_data: dict) -> tuple:
        score   = self.quick_score(ticker, all_data, regime, sec_data)
        should  = score >= Config.PRE_FILTER_THRESHOLD
        if not should:
            logger.info(f"Pre-Filter: {ticker} score={score} < {Config.PRE_FILTER_THRESHOLD} — skip")
        return should, score
