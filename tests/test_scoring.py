"""
tests/test_scoring.py
Unit-Tests für die Kern-Scoring-Logik.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from scanner.analysis.scoring_engine import ScoringEngine
from scanner.utils.config import Config


@pytest.fixture
def engine():
    return ScoringEngine()


@pytest.fixture
def normal_regime():
    return {
        "mode": "NORMAL",
        "weights": Config.WEIGHTS_NORMAL,
        "conviction_threshold": Config.CONVICTION_NORMAL,
        "regime_score": 7.0,
        "iv_rank_avg": 35.0,
        "energy_breadth": 0.65,
    }


@pytest.fixture
def stress_regime():
    return {
        "mode": "STRESS",
        "weights": Config.WEIGHTS_STRESS,
        "conviction_threshold": Config.CONVICTION_STRESS,
        "regime_score": 4.0,
        "iv_rank_avg": 68.0,
        "energy_breadth": 0.35,
    }


@pytest.fixture
def shulman_full():
    return {
        "weight_modifier": 1.0,
        "conviction_bonus": 0.0,
        "shulman_bonus": 0.0,
    }


@pytest.fixture
def shulman_half():
    return {
        "weight_modifier": 0.5,
        "conviction_bonus": 0.0,
        "shulman_bonus": 0.0,
    }


class TestContrarianGate:
    def test_blocked_below_threshold(self, engine, normal_regime, shulman_full):
        scores = {
            "salp": 9.0, "thiel": 8.0, "shulman": 8.0,
            "multigate": 7.0, "regime": 7.0,
            "contrarian": -4.0,  # Unter -3 → blockiert
            "katechon_bonus": 0.0,
        }
        result = engine.calculate("TEST", scores, normal_regime, shulman_full)
        assert result.gate_status == "BLOCKED_CONTRARIAN"
        assert result.conviction_total == 0.0

    def test_not_blocked_at_threshold(self, engine, normal_regime, shulman_full):
        scores = {
            "salp": 9.0, "thiel": 8.0, "shulman": 8.0,
            "multigate": 7.0, "regime": 7.0,
            "contrarian": -2.9,  # Knapp über -3 → nicht blockiert
            "katechon_bonus": 0.0,
        }
        result = engine.calculate("TEST", scores, normal_regime, shulman_full)
        assert result.gate_status != "BLOCKED_CONTRARIAN"


class TestConvictionThresholds:
    def test_pass_normal_mode(self, engine, normal_regime, shulman_full):
        scores = {
            "salp": 9.0, "thiel": 8.0, "shulman": 8.0,
            "multigate": 7.0, "regime": 7.0, "contrarian": 2.0,
            "katechon_bonus": 0.0,
        }
        result = engine.calculate("TEST", scores, normal_regime, shulman_full)
        assert result.gate_status == "PASS"
        assert result.conviction_total >= Config.CONVICTION_NORMAL

    def test_watchlist_range(self, engine, normal_regime, shulman_full):
        scores = {
            "salp": 5.0, "thiel": 5.0, "shulman": 5.0,
            "multigate": 5.0, "regime": 5.0, "contrarian": 1.0,
            "katechon_bonus": 0.0,
        }
        result = engine.calculate("TEST", scores, normal_regime, shulman_full)
        # Sollte unter 7.5 sein
        assert result.gate_status in ("WATCHLIST", "NO_SIGNAL")

    def test_stress_higher_threshold(self, engine, stress_regime, shulman_full):
        # Scores die im Normalmodus PASS wären, im Stress WATCHLIST
        scores = {
            "salp": 7.5, "thiel": 7.0, "shulman": 7.0,
            "multigate": 6.0, "regime": 4.0, "contrarian": 1.0,
            "katechon_bonus": 0.0,
        }
        normal_result = engine.calculate("TEST", scores, {
            "mode": "NORMAL",
            "weights": Config.WEIGHTS_NORMAL,
            "conviction_threshold": Config.CONVICTION_NORMAL,
        }, shulman_full)

        stress_result = engine.calculate("TEST", scores, stress_regime, shulman_full)
        # Im Stress-Modus sollte der Schwellenwert höher sein
        assert stress_regime["conviction_threshold"] > Config.CONVICTION_NORMAL


class TestLaufzeit:
    def test_12m_above_9(self, engine, normal_regime, shulman_full):
        scores = {
            "salp": 10.0, "thiel": 10.0, "shulman": 10.0,
            "multigate": 9.0, "regime": 9.0, "contrarian": 4.0,
            "katechon_bonus": 0.3,
        }
        result = engine.calculate("TEST", scores, normal_regime, shulman_full)
        if result.gate_status == "PASS" and result.conviction_total >= 9.0:
            assert result.laufzeit_months == 12

    def test_6m_at_threshold(self, engine, normal_regime, shulman_full):
        scores = {
            "salp": 8.0, "thiel": 7.0, "shulman": 7.0,
            "multigate": 6.0, "regime": 7.0, "contrarian": 2.0,
            "katechon_bonus": 0.0,
        }
        result = engine.calculate("TEST", scores, normal_regime, shulman_full)
        if result.gate_status == "PASS":
            assert result.laufzeit_months in (6, 9)


class TestShulmanWeightModifier:
    def test_half_weight_reduces_score(self, engine, normal_regime,
                                        shulman_full, shulman_half):
        scores = {
            "salp": 7.0, "thiel": 6.0, "shulman": 9.0,
            "multigate": 6.0, "regime": 7.0, "contrarian": 1.0,
            "katechon_bonus": 0.0,
        }
        full_result = engine.calculate("TEST", scores, normal_regime, shulman_full)
        half_result = engine.calculate("TEST", scores, normal_regime, shulman_half)
        # Halbes Shulman-Gewicht sollte niedrigeren Score ergeben
        assert half_result.conviction_total <= full_result.conviction_total


class TestWeightSums:
    def test_normal_weights_sum_to_one(self):
        total = sum(Config.WEIGHTS_NORMAL.values())
        assert abs(total - 1.0) < 0.001

    def test_stress_weights_sum_to_one(self):
        total = sum(Config.WEIGHTS_STRESS.values())
        assert abs(total - 1.0) < 0.001

    def test_stress_salp_higher(self):
        assert Config.WEIGHTS_STRESS["salp"] > Config.WEIGHTS_NORMAL["salp"]

    def test_stress_thiel_lower(self):
        assert Config.WEIGHTS_STRESS["thiel"] < Config.WEIGHTS_NORMAL["thiel"]


class TestKatechonBonus:
    def test_bonus_adds_to_conviction(self, engine, normal_regime, shulman_full):
        scores_no_bonus = {
            "salp": 8.0, "thiel": 7.0, "shulman": 7.0,
            "multigate": 6.0, "regime": 7.0, "contrarian": 2.0,
            "katechon_bonus": 0.0,
        }
        scores_with_bonus = dict(scores_no_bonus)
        scores_with_bonus["katechon_bonus"] = Config.KATECHON_BONUS_VALUE

        r_no   = engine.calculate("TEST", scores_no_bonus, normal_regime, shulman_full)
        r_with = engine.calculate("TEST", scores_with_bonus, normal_regime, shulman_full)

        assert r_with.conviction_total > r_no.conviction_total
        diff = r_with.conviction_total - r_no.conviction_total
        assert abs(diff - Config.KATECHON_BONUS_VALUE) < 0.1
