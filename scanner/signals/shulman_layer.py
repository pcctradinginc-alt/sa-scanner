"""
scanner/signals/shulman_layer.py
Shulman-Metriken mit ehrlicher Trennung zwischen
quantitativen Proxies und qualitativer Extraktion.
"""

import logging
from datetime import datetime

from ..utils.config import Config

logger = logging.getLogger(__name__)

SHULMAN_FORUM_KEYWORDS = [
    "doubling times", "compute-overhang", "intelligence explosion",
    "recursive self-improvement", "automated research",
    "robot doublings", "scaling laws", "algorithmic progress",
]

SEKTOR_WEIGHTS = {
    "energy_infrastructure": 0.35,
    "compute_hardware":       0.25,
    "physical_ai":            0.20,
    "sovereign_ai_defense":   0.12,
    "cyber_bio_nuclear":      0.08,
}


class ShulmanLayer:

    def evaluate(self, all_data: dict, ticker: str, sector: str) -> dict:

        # ── QUANTITATIVE PROXIES ──────────────────────────────────
        eia_point   = all_data.get("eia", {}).get("empirical_point", 0)
        capex_point = all_data.get("hyperscaler_capex", {}).get("empirical_point", 0)
        nvda_point  = all_data.get("nvda_revenue", {}).get("empirical_point", 0)

        empirical_score = eia_point + capex_point + nvda_point

        # ── QUALITATIVE EXTRAKTION AUS RSS ───────────────────────
        rss_articles     = all_data.get("rss", [])
        all_text         = " ".join([
            a.get("title", "") + " " + a.get("summary", "")
            for a in rss_articles
            if a.get("signals", {}).get("shulman")
        ]).lower()

        forum_keywords_found = [kw for kw in SHULMAN_FORUM_KEYWORDS
                                if kw.lower() in all_text]
        shulman_articles     = [a for a in rss_articles
                                if a.get("signals", {}).get("shulman")]
        ige_language         = any(k in all_text for k in
                                   ["intelligence explosion", "recursive self-improvement",
                                    "automated research"])
        doubling_language    = any(k in all_text for k in
                                   ["doubling times", "robot doublings", "doubling rate"])

        # ── SALP-BEGLEITTEXT-BONUS ───────────────────────────────
        shulman_bonus = all_data.get("shulman_empirical", {}).get("salp_begleit_bonus", 0.0)

        # ── SEKTOR-MULTIPLIKATOR ─────────────────────────────────
        sektor_mult = SEKTOR_WEIGHTS.get(sector, 0.10)

        # ── SHULMAN SUB-SCORE (1-10) ─────────────────────────────
        base_score = 1.0

        # Empirische Punkte
        base_score += empirical_score * 2.0  # Max +6 bei Score 3

        # Qualitative Signale (max +2)
        if forum_keywords_found:
            base_score += min(len(forum_keywords_found) * 0.5, 1.5)
        if ige_language:
            base_score += 0.5

        shulman_score = round(min(base_score, 10.0), 1)

        # ── GEWICHTS-ANPASSUNG ───────────────────────────────────
        weight_modifier = (
            0.5 if empirical_score < Config.SHULMAN_EMPIRICAL_FULL_WEIGHT else
            1.0
        )
        conviction_bonus = (
            Config.SHULMAN_EMPIRICAL_BONUS if empirical_score >= 3 else 0.0
        )

        logger.info(f"Shulman [{ticker}]: score={shulman_score} | "
                    f"empirical={empirical_score}/3 | "
                    f"weight_mod={weight_modifier} | "
                    f"forum_kw={len(forum_keywords_found)}")

        return {
            "shulman_score":        shulman_score,
            "empirical_score":      empirical_score,
            "weight_modifier":      weight_modifier,
            "conviction_bonus":     conviction_bonus,
            "shulman_bonus":        shulman_bonus,
            # Quantitative Proxies
            "eia_empirical_point":  eia_point,
            "capex_empirical_point":capex_point,
            "nvda_empirical_point": nvda_point,
            # Qualitativ — explizit nicht quantifiziert
            "qualitative": {
                "forum_keywords_found": forum_keywords_found,
                "shulman_articles":     len(shulman_articles),
                "ige_language":         ige_language,
                "doubling_language":    doubling_language,
                "confidence":           "LOW",  # Immer LOW für qualitative Signale
            },
            # Sektor
            "sektor_multiplikator": sektor_mult,
            "sektor":               sector,
            # Nicht messbare Metriken — explizit None
            "robot_doublings_verified": None,
            "flops_per_dollar_growth":  None,
            "ige_loop_confirmed":       None,
            "evaluated_at":             datetime.utcnow().isoformat(),
        }
