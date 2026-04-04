"""
scanner/signals/thiel_layer.py
Thiel-Analyse: Handlung vs. These, Katechon-Bonus, Deep Network Signal.
"""

import logging
from datetime import datetime, timedelta

from ..utils.config import Config

logger = logging.getLogger(__name__)

THIEL_ACTORS = ["Musk", "Sacks", "Vance", "Karp", "Luckey", "Stephens", "Lonsdale"]

ANTICHRIST_THESIS_KEYWORDS = [
    "katechon", "sovereign ai", "communistic technology",
    "centraliz", "permanent crisis", "end of modernity",
    "ai safety as control", "luddites", "antichrist",
]

HANDLUNG_KEYWORDS = [
    "founders fund invest", "thiel capital acquir",
    "anduril contract", "palantir contract",
    "founders fund deal", "thiel back",
]

MONOPOLY_KEYWORDS = [
    "monopoly", "moat", "hoheitlich", "defense contract",
    "government contract", "exclusive", "barrier to entry",
    "sovereign", "national security",
]


class ThielLayer:

    def evaluate(self, rss_articles: list, sec_data: dict,
                 state_manager) -> dict:

        all_text = " ".join([
            a.get("title", "") + " " + a.get("summary", "")
            for a in rss_articles
        ]).lower()

        # ── HANDLUNG vs. THESE ───────────────────────────────────
        handlung_signals = []
        these_signals    = []

        # Aus SEC-Filings (härteste Evidenz)
        if sec_data.get("new_filings_found", 0) > 0:
            for f in sec_data.get("new_filings", []):
                if "thiel" in f.get("entity", "").lower():
                    handlung_signals.append(f"13F Filing: {f['entity']}")

        # Aus RSS — Handlungen
        for kw in HANDLUNG_KEYWORDS:
            if kw in all_text:
                handlung_signals.append(f"RSS: {kw}")

        # Aus RSS — Thesen
        antichrist_hits = [kw for kw in ANTICHRIST_THESIS_KEYWORDS if kw in all_text]
        if antichrist_hits:
            these_signals.append(f"Antichrist-Thesis: {antichrist_hits}")

        monopoly_hits = [kw for kw in MONOPOLY_KEYWORDS if kw in all_text]
        if monopoly_hits:
            these_signals.append(f"Zero-to-One: {monopoly_hits}")

        has_handlung = len(handlung_signals) > 0
        has_these    = len(these_signals) > 0

        signal_type = (
            "HANDLUNG_UND_THESE" if has_handlung and has_these else
            "HANDLUNG"           if has_handlung else
            "THESE"              if has_these else
            "KEIN_SIGNAL"
        )

        # ── NETZWERK-AKTEURE ─────────────────────────────────────
        active_actors = []
        actor_30d     = []
        cutoff_30d    = datetime.utcnow() - timedelta(days=Config.KATECHON_WINDOW_DAYS)

        for article in rss_articles:
            text = article.get("title", "") + " " + article.get("summary", "")
            for actor in THIEL_ACTORS:
                if actor.lower() in text.lower():
                    if actor not in active_actors:
                        active_actors.append(actor)
                    try:
                        pub = datetime.fromisoformat(article.get("published", ""))
                        if pub > cutoff_30d:
                            actor_30d.append(actor)
                    except Exception:
                        pass

        deep_network_signal = len(set(active_actors)) >= 3

        # ── ANTICHRIST-CHECK ─────────────────────────────────────
        antichrist_aligned = len(antichrist_hits) >= 2

        # ── KATECHON-BONUS ───────────────────────────────────────
        katechon_bonus      = 0.0
        katechon_triggered  = False
        katechon_actors_30d = list(set(actor_30d))

        if (len(katechon_actors_30d) >= Config.KATECHON_MIN_ACTORS and
                state_manager.can_use_katechon_bonus()):
            katechon_bonus     = Config.KATECHON_BONUS_VALUE
            katechon_triggered = True
            state_manager.use_katechon_bonus()
            logger.info(f"Katechon bonus triggered: {katechon_actors_30d}")

        # ── THIEL SUB-SCORE (1-10) ───────────────────────────────
        score = 1.0
        if has_these and not has_handlung:
            score = 4.5
        elif has_handlung:
            score = 6.5
        if active_actors:
            score += min(len(active_actors) * 0.5, 2.0)
        if deep_network_signal:
            score += 1.0
        if antichrist_aligned:
            score += 0.5

        score = round(min(score, 10.0), 1)

        logger.info(f"Thiel: score={score} | type={signal_type} | "
                    f"actors={active_actors} | deep_network={deep_network_signal} | "
                    f"katechon_bonus={katechon_bonus}")

        return {
            "thiel_score":          score,
            "signal_type":          signal_type,
            "handlung_signals":     handlung_signals,
            "these_signals":        these_signals,
            "active_actors":        active_actors,
            "actors_last_30d":      katechon_actors_30d,
            "deep_network_signal":  deep_network_signal,
            "antichrist_aligned":   antichrist_aligned,
            "antichrist_keywords":  antichrist_hits,
            "monopoly_keywords":    monopoly_hits,
            "katechon_bonus":       katechon_bonus,
            "katechon_triggered":   katechon_triggered,
            "evaluated_at":         datetime.utcnow().isoformat(),
        }
