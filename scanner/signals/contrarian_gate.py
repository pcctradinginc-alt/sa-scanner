"""
scanner/signals/contrarian_gate.py
Prüft Gegenthesen gegen die Hauptthese des Systems.
Verhindert geschlossene Überzeugungsschleifen.
"""

import logging
from datetime import datetime

from ..utils.config import Config

logger = logging.getLogger(__name__)

GEGENTHESEN = {
    "software_overhang": [
        "software efficiency", "algorithmic improvement",
        "fewer gpus", "software overhang", "compute efficient",
        "model compression", "distillation breakthrough",
    ],
    "regulatory_acceleration": [
        "ai regulation", "ai ban", "ai moratorium",
        "eu ai act", "ai safety law", "restrict ai",
        "compute governance", "frontier ai law",
    ],
    "shulman_timeline_error": [
        "ai slower than expected", "scaling plateau",
        "diminishing returns ai", "ai winter",
        "llm plateau", "benchmark saturation",
    ],
    "energy_abundance": [
        "energy surplus", "cheap power", "fusion breakthrough",
        "energy abundance", "power glut", "electricity oversupply",
        "smr cost reduction",
    ],
    "network_fragmentation": [
        "thiel musk conflict", "silicon valley split",
        "palantir controversy", "founders fund dispute",
        "thiel network break",
    ],
}


class ContrarianGate:

    def evaluate(self, rss_articles: list, ticker: str,
                 options_data: dict) -> dict:

        # 1. Gegenthesen aus RSS prüfen
        all_text = " ".join([
            a.get("title", "") + " " + a.get("summary", "")
            for a in rss_articles
        ]).lower()

        found_gegenthesen = {}
        for thesis, keywords in GEGENTHESEN.items():
            hits = sum(1 for kw in keywords if kw in all_text)
            if hits >= 2:
                found_gegenthesen[thesis] = hits

        n = len(found_gegenthesen)
        score_from_gegenthesen = {0: 2, 1: 0, 2: -2, 3: -4, 4: -5, 5: -5}.get(n, -5)

        # 2. RSI-Abwertung (aus Tradier-Daten)
        ticker_options = options_data.get(ticker, {})
        rsi           = ticker_options.get("rsi", 50.0)
        rsi_penalty   = (
            -1.0 if rsi > Config.CONTRARIAN_RSI_HIGH else
            -0.5 if rsi > Config.CONTRARIAN_RSI_ELEVATED else
            0.0
        )

        # 3. Hype-Abwertung aus RSS
        relevant    = [a for a in rss_articles if ticker in str(a.get("tickers", []))]
        hype_score  = 0.0
        hype_penalty = 0.0
        if relevant:
            import numpy as np
            avg_cred   = float(np.mean([a.get("credibility", 0.5) for a in relevant]))
            hype_score = len(relevant) * (1 - avg_cred)
            hype_score = min(hype_score / 10, 1.0)
            if hype_score > Config.CONTRARIAN_HYPE_HIGH:
                hype_penalty = -1.0

        # 4. Put/Call-Ratio Signal (Tradier-Vollzugriff)
        flow         = ticker_options.get("options_flow", {})
        pc_volume    = flow.get("put_call_volume")
        pc_penalty   = 0.0
        if pc_volume is not None and pc_volume > 1.5:
            # Sehr hohes Put/Call-Ratio = Markt erwartet Rückgang
            pc_penalty = -1.0
            logger.info(f"{ticker}: High put/call ratio {pc_volume:.2f} — contrarian penalty")

        final_score = max(
            score_from_gegenthesen + rsi_penalty + hype_penalty + pc_penalty,
            -5.0
        )
        final_score = round(final_score, 1)

        gate_blocked = final_score < Config.CONTRARIAN_BLOCK_THRESHOLD

        if gate_blocked:
            logger.warning(f"CONTRARIAN GATE BLOCKED {ticker}: score {final_score}")

        return {
            "contrarian_score":    final_score,
            "gate_blocked":        gate_blocked,
            "gegenthesen_aktiv":   list(found_gegenthesen.keys()),
            "gegenthesen_detail":  found_gegenthesen,
            "rsi":                 rsi,
            "rsi_penalty":         rsi_penalty,
            "hype_score":          round(hype_score, 2),
            "hype_penalty":        hype_penalty,
            "put_call_ratio":      pc_volume,
            "pc_penalty":          pc_penalty,
            "evaluated_at":        datetime.utcnow().isoformat(),
        }
