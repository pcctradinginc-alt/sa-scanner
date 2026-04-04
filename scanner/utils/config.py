"""
scanner/utils/config.py
Zentrale Konfiguration. Alle Konstanten, Gewichte, Schwellenwerte.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── API KEYS ────────────────────────────────────────────────
    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
    TRADIER_API_KEY   = os.environ.get("TRADIER_API_KEY")
    FINNHUB_API_KEY   = os.environ.get("FINNHUB_API_KEY")
    EIA_API_KEY       = os.environ.get("EIA_API_KEY")
    FRED_API_KEY      = os.environ.get("FRED_API_KEY")

    # ── TRADIER ENDPOINTS (Vollzugriff) ─────────────────────────
    TRADIER_BASE      = "https://api.tradier.com/v1"
    TRADIER_HEADERS   = {
        "Authorization": f"Bearer {os.environ.get('TRADIER_API_KEY', '')}",
        "Accept": "application/json",
    }

    # ── PFADE ───────────────────────────────────────────────────
    BASE_DIR      = Path(__file__).parent.parent.parent
    DB_PATH       = BASE_DIR / "data" / "scanner.db"
    SIGNALS_DIR   = BASE_DIR / "data" / "signals"
    CARDS_DIR     = BASE_DIR / "data" / "cards"
    HIST_DIR      = BASE_DIR / "data" / "historical"
    DASH_DIR      = BASE_DIR / "dashboard"
    CACHE_DIR     = BASE_DIR / "data" / "cache"

    # ── SCORING SCHWELLENWERTE ──────────────────────────────────
    CONVICTION_NORMAL        = 7.5
    CONVICTION_STRESS        = 8.0
    CONVICTION_WATCHLIST_MIN = 6.5
    PRE_FILTER_THRESHOLD     = 5.0

    # ── REGIME-SCHWELLENWERTE ───────────────────────────────────
    IV_RANK_STRESS_THRESHOLD         = 60.0
    REGIME_STABILITY_STRESS          = 0.4
    CAPEX_FALLING_QUARTERS_FOR_STRESS = 2

    # ── GEWICHTE NORMALMODUS ────────────────────────────────────
    WEIGHTS_NORMAL = {
        "salp":       0.40,
        "thiel":      0.14,
        "shulman":    0.15,
        "multigate":  0.04,
        "regime":     0.15,
        "contrarian": 0.12,
    }

    # ── GEWICHTE STRESSMODUS ────────────────────────────────────
    WEIGHTS_STRESS = {
        "salp":       0.50,
        "thiel":      0.10,
        "shulman":    0.13,
        "multigate":  0.03,
        "regime":     0.12,
        "contrarian": 0.12,
    }

    # ── PORTFOLIO LIMITS ────────────────────────────────────────
    MAX_ACTIVE_POSITIONS      = 3
    MAX_CAPITAL_PER_POSITION  = 0.20
    MIN_PORTFOLIO_CONVICTION  = 8.0
    MAX_SAME_SECTOR_POSITIONS = 2

    # ── KATECHON BONUS ──────────────────────────────────────────
    KATECHON_BONUS_VALUE        = 0.3
    KATECHON_BONUS_PER_QUARTER  = 1
    KATECHON_WINDOW_DAYS        = 30
    KATECHON_MIN_ACTORS         = 2

    # ── SHULMAN EMPIRICAL ───────────────────────────────────────
    SHULMAN_EMPIRICAL_FULL_WEIGHT   = 2   # >= 2 Punkte: volles Gewicht
    SHULMAN_EMPIRICAL_BONUS         = 0.3 # Bei Score 3: Bonus auf Conviction
    SHULMAN_HALF_WEIGHT_MULTIPLIER  = 0.5
    SHULMAN_SALP_BEGLEIT_BONUS      = 0.5

    # Empirische Schwellenwerte
    SHULMAN_EIA_GROWTH_THRESHOLD    = 0.05  # 5% YoY Stromwachstum
    SHULMAN_CAPEX_GROWTH_THRESHOLD  = 0.10  # 10% YoY CapEx Wachstum
    SHULMAN_NVDA_GROWTH_THRESHOLD   = 0.20  # 20% YoY NVDA Umsatz

    # ── CONTRARIAN GATE ─────────────────────────────────────────
    CONTRARIAN_BLOCK_THRESHOLD   = -3.0
    CONTRARIAN_RSI_HIGH          = 75.0   # Abwertung -1.0
    CONTRARIAN_RSI_ELEVATED      = 65.0   # Abwertung -0.5
    CONTRARIAN_HYPE_HIGH         = 0.7    # Abwertung -1.0

    # ── GREEKS LIQUIDITY THRESHOLDS ─────────────────────────────
    MIN_DAILY_VOLUME             = 500    # Kontrakte/Tag
    MAX_VEGA_OTM_LEAPS           = 0.15
    MAX_THETA_12M_LEAPS          = -0.05
    MAX_BID_ASK_SPREAD_PCT       = 0.08   # 8% der Spread-Mitte

    # ── STOP LOSS REGELN ────────────────────────────────────────
    STOP_PREMIUM_LOSS_PCT        = -0.40  # -40% Prämie in 30 Tagen
    STOP_IV_CRUSH_POINTS         = 15.0   # IV-Crush >15 Punkte

    # ── LAUFZEIT CONVICTION BANDS ───────────────────────────────
    LAUFZEIT_6M_MAX  = 8.0
    LAUFZEIT_9M_MAX  = 9.0
    # >= 9.0 → 12 Monate LEAPS

    # ── IV RANK KONFIGURATION (Tradier) ─────────────────────────
    IV_RANK_LOOKBACK_DAYS        = 365
    IV_RANK_MIN_DATAPOINTS       = 30
    IV_RANK_HIGH_CONFIDENCE      = 200
    IV_RANK_WARMUP_DEFAULT       = 50.0

    # ── RATE LIMITS (Calls/Minute) ──────────────────────────────
    RATE_LIMITS = {
        "tradier":    60,
        "finnhub":    60,
        "yfinance":   30,
        "sec_edgar":  10,
        "anthropic":   5,
        "fred":       120,
        "eia":       5000,
    }

    # ── CACHE EXPIRE (Sekunden) ──────────────────────────────────
    CACHE_EXPIRE = {
        "yfinance":   21600,   # 6 Stunden
        "fred":       86400,   # 24 Stunden
        "eia":        86400,
        "finnhub":    21600,
        "rss":         3600,   # 1 Stunde
        "edgar":      21600,
        "tradier":     3600,
    }

    # ── ENERGIE TICKER (XLE-Komponenten) ────────────────────────
    ENERGY_TICKERS = [
        "VST", "CEG", "NRG", "XEL", "SO",
        "DUK", "NEE", "AEP", "EXC", "ETR",
        "D", "PCG", "ES", "FE", "PPL",
    ]

    # ── HYPERSCALER TICKER ───────────────────────────────────────
    HYPERSCALER_TICKERS = ["MSFT", "GOOGL", "AMZN", "META", "NVDA"]

    # ── ZIELTICKER FÜR OPTION CHAIN ─────────────────────────────
    TARGET_TICKERS = [
        "VST", "CEG", "NRG",           # Energie-Infrastruktur
        "PLTR",                         # Sovereign AI / Defense
        "NVDA", "TSM", "AVGO",         # Compute Hardware
        "LMT", "RTX",                  # Defense
    ]

    # ── RSS FEEDS MIT CREDIBILITY ────────────────────────────────
    RSS_FEEDS = {
        "Reuters_Markets":  ("https://feeds.reuters.com/reuters/businessNews", 0.92),
        "CNBC_Finance":     ("https://www.cnbc.com/id/10000664/device/rss/rss.html", 0.85),
        "CNBC_Investing":   ("https://www.cnbc.com/id/15839135/device/rss/rss.html", 0.84),
        "CNBC_Earnings":    ("https://www.cnbc.com/id/15839069/device/rss/rss.html", 0.82),
        "CNBC_Economy":     ("https://www.cnbc.com/id/20910258/device/rss/rss.html", 0.78),
        "CNBC_WorldNews":   ("https://www.cnbc.com/id/100003114/device/rss/rss.html", 0.72),
        "SeekingAlpha_En":  ("https://seekingalpha.com/feed/sectors/energy", 0.65),
        "SeekingAlpha_Tech":("https://seekingalpha.com/feed/sectors/technology", 0.65),
    }

    # ── SEC CIK TARGETS ──────────────────────────────────────────
    # CIK-Nummern über EDGAR Company Search ermitteln
    SEC_CIK_TARGETS = {
        "situational_awareness_lp": "0002014020",  # Bitte verifizieren
        "thiel_capital":            "0001418819",  # Bitte verifizieren
        "founders_fund":            "0001615175",  # Bitte verifizieren
    }

    # ── ANTHROPIC MODEL ──────────────────────────────────────────
    CLAUDE_MODEL     = "claude-sonnet-4-6"
    CLAUDE_MAX_TOKENS = 4000

    @classmethod
    def validate(cls) -> list:
        missing = []
        required = ["ANTHROPIC_API_KEY", "TRADIER_API_KEY", "FINNHUB_API_KEY"]
        for key in required:
            if not getattr(cls, key):
                missing.append(key)
        return missing

    @classmethod
    def ensure_dirs(cls):
        for d in [cls.SIGNALS_DIR, cls.CARDS_DIR,
                  cls.HIST_DIR, cls.DASH_DIR,
                  cls.CACHE_DIR, cls.DASH_DIR / "cards"]:
            d.mkdir(parents=True, exist_ok=True)
