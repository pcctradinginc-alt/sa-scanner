"""
scanner/analysis/scoring_engine.py
Gewichteter Conviction-Score mit Regime-abhängigen Gewichten.
"""

import logging
from dataclasses import dataclass, field
from ..utils.config import Config

logger = logging.getLogger(__name__)


@dataclass
class ConvictionResult:
    ticker:           str   = ""
    salp_score:       float = 0.0
    thiel_score:      float = 0.0
    shulman_score:    float = 0.0
    multigate_score:  float = 0.0
    regime_score:     float = 0.0
    contrarian_score: float = 0.0
    katechon_bonus:   float = 0.0
    shulman_bonus:    float = 0.0
    conviction_total: float = 0.0
    gate_status:      str   = "PENDING"
    regime_mode:      str   = "NORMAL"
    laufzeit_months:  int   = 0
    weights_used:     dict  = field(default_factory=dict)


class ScoringEngine:

    def calculate(self, ticker: str, scores: dict, regime: dict,
                  shulman_data: dict) -> ConvictionResult:
        mode     = regime.get("mode", "NORMAL")
        weights  = regime.get("weights", Config.WEIGHTS_NORMAL)
        threshold = regime.get("conviction_threshold", Config.CONVICTION_NORMAL)

        result = ConvictionResult(
            ticker=ticker,
            salp_score=scores.get("salp", 3.0),
            thiel_score=scores.get("thiel", 0.0),
            shulman_score=scores.get("shulman", 0.0),
            multigate_score=scores.get("multigate", 5.0),
            regime_score=scores.get("regime", 5.0),
            contrarian_score=scores.get("contrarian", 0.0),
            katechon_bonus=scores.get("katechon_bonus", 0.0),
            shulman_bonus=shulman_data.get("conviction_bonus", 0.0),
            regime_mode=mode,
            weights_used=weights,
        )

        # Contrarian Gate — binär
        if result.contrarian_score < Config.CONTRARIAN_BLOCK_THRESHOLD:
            result.gate_status      = "BLOCKED_CONTRARIAN"
            result.conviction_total = 0.0
            logger.warning(f"{ticker}: BLOCKED by Contrarian Gate (score={result.contrarian_score})")
            return result

        # Shulman Gewichts-Anpassung
        shulman_weight_mod = shulman_data.get("weight_modifier", 1.0)
        eff_shulman_weight = weights["shulman"] * shulman_weight_mod
        surplus            = weights["shulman"] - eff_shulman_weight

        # Surplus proportional auf SALP und Regime verteilen
        eff_weights = dict(weights)
        eff_weights["shulman"] = eff_shulman_weight
        eff_weights["salp"]   += surplus * 0.6
        eff_weights["regime"] += surplus * 0.4

        # Contrarian-Score normalisieren (neg. = 0 im Gewicht, aber penalisiert)
        contrarian_normalized = max(result.contrarian_score, 0) / 5.0 * 10.0

        # Gewichteter Durchschnitt
        total = (
            eff_weights["salp"]       * result.salp_score +
            eff_weights["thiel"]      * result.thiel_score +
            eff_weights["shulman"]    * result.shulman_score +
            eff_weights["multigate"]  * result.multigate_score +
            eff_weights["regime"]     * result.regime_score +
            eff_weights["contrarian"] * contrarian_normalized
        )

        # Boni
        total += result.katechon_bonus + result.shulman_bonus

        # Contrarian-Malus (neg. Werte bestrafen)
        if result.contrarian_score < 0:
            total += result.contrarian_score * 0.3

        result.conviction_total = round(min(total, 10.0), 2)

        # Gate-Status
        if result.conviction_total >= threshold:
            result.gate_status = "PASS"
        elif result.conviction_total >= Config.CONVICTION_WATCHLIST_MIN:
            result.gate_status = "WATCHLIST"
        else:
            result.gate_status = "NO_SIGNAL"

        # Laufzeit
        if result.gate_status == "PASS":
            if result.conviction_total >= 9.0:
                result.laufzeit_months = 12
            elif result.conviction_total >= 8.0:
                result.laufzeit_months = 9
            else:
                result.laufzeit_months = 6

        logger.info(f"Conviction [{ticker}]: {result.conviction_total:.2f} | "
                    f"Status: {result.gate_status} | Laufzeit: {result.laufzeit_months}M | "
                    f"Mode: {mode}")
        return result
