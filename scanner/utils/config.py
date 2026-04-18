"""
scanner/utils/config.py
Zentrale Konfiguration. Alle Konstanten, Gewichte, Schwellenwerte.
"""

import os
import json
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
    TRADIER_BASE    = "https://api.tradier.com/v1"
    TRADIER_HEADERS = {
        "Authorization": f"Bearer {os.environ.get('TRADIER_API_KEY', '')}",
        "Accept": "application/json",
    }

    # ── PFADE ───────────────────────────────────────────────────
    BASE_DIR    = Path(__file__).parent.parent.parent
    DB_PATH     = BASE_DIR / "data" / "scanner.db"
    SIGNALS_DIR = BASE_DIR / "data" / "signals"
    CARDS_DIR   = BASE_DIR / "data" / "cards"
    HIST_DIR    = BASE_DIR / "data" / "historical"
    DASH_DIR    = BASE_DIR / "dashboard"
    CACHE_DIR   = BASE_DIR / "data" / "cache"

    # ── DYNAMISCHE TICKER (neu) ─────────────────────────────────
    DYNAMIC_TICKERS_PATH = BASE_DIR / "data" / "dynamic_tickers.json"

    # ── SCORING SCHWELLENWERTE ──────────────────────────────────
    CONVICTION_NORMAL        = 7.5
    CONVICTION_STRESS        = 8.0
    CONVICTION_WATCHLIST_MIN = 6.5
    PRE_FILTER_THRESHOLD     = 2.0

    # ── REGIME-SCHWELLENWERTE ───────────────────────────────────
    IV_RANK_STRESS_THRESHOLD          = 60.0
    REGIME_STABILITY_STRESS           = 0.4
    CAPEX_FALLING_QUARTERS_FOR_STRESS = 2

    # ── GEWICHTE NORMALMODUS / STRESSMODUS ──────────────────────
    WEIGHTS_NORMAL = { "salp": 0.40, "thiel": 0.14, "shulman": 0.15, "multigate": 0.04, "regime": 0.15, "contrarian": 0.12 }
    WEIGHTS_STRESS = { "salp": 0.50, "thiel": 0.10, "shulman": 0.13, "multigate": 0.03, "regime": 0.12, "contrarian": 0.12 }

    # ── PORTFOLIO LIMITS ────────────────────────────────────────
    MAX_ACTIVE_POSITIONS      = 3
    MAX_CAPITAL_PER_POSITION  = 0.20
    MIN_PORTFOLIO_CONVICTION  = 8.0
    MAX_SAME_SECTOR_POSITIONS = 2

    # ── KATECHON / SHULMAN / CONTRARIAN / GREEKS ───────────────
    KATECHON_BONUS_VALUE       = 0.3
    KATECHON_BONUS_PER_QUARTER = 1
    KATECHON_WINDOW_DAYS       = 30
    KATECHON_MIN_ACTORS        = 2

    SHULMAN_EMPIRICAL_FULL_WEIGHT  = 2
    SHULMAN_EMPIRICAL_BONUS        = 0.3
    SHULMAN_HALF_WEIGHT_MULTIPLIER = 0.5
    SHULMAN_SALP_BEGLEIT_BONUS     = 0.5

    SHULMAN_EIA_GROWTH_THRESHOLD   = 0.05
    SHULMAN_CAPEX_GROWTH_THRESHOLD = 0.10
    SHULMAN_NVDA_GROWTH_THRESHOLD  = 0.20

    CONTRARIAN_BLOCK_THRESHOLD = -3.0
    CONTRARIAN_RSI_HIGH        = 75.0
    CONTRARIAN_RSI_ELEVATED    = 65.0
    CONTRARIAN_HYPE_HIGH       = 0.7

    MIN_DAILY_VOLUME         = 500
    MAX_VEGA_OTM_LEAPS       = 0.15
    MAX_THETA_12M_LEAPS      = -0.05
    MAX_BID_ASK_SPREAD_PCT   = 0.08

    STOP_PREMIUM_LOSS_PCT    = -0.40
    STOP_IV_CRUSH_POINTS     = 15.0

    LAUFZEIT_6M_MAX = 8.0
    LAUFZEIT_9M_MAX = 9.0

    IV_RANK_LOOKBACK_DAYS   = 365
    IV_RANK_MIN_DATAPOINTS  = 30
    IV_RANK_HIGH_CONFIDENCE = 200
    IV_RANK_WARMUP_DEFAULT  = 50.0

    # ── RATE LIMITS & CACHE ─────────────────────────────────────
    RATE_LIMITS = { "tradier": 60, "finnhub": 60, "yfinance": 30, "sec_edgar": 10, "anthropic": 5, "fred": 120, "eia": 5000 }
    CACHE_EXPIRE = { "yfinance": 21600, "fred": 86400, "eia": 86400, "finnhub": 21600, "rss": 3600, "edgar": 21600, "tradier": 3600 }

    # ── FESTE TICKER (Basis-Pool) ───────────────────────────────
    TARGET_TICKERS = [
        "VST", "CEG", "NRG", "PLTR",
        "NVDA", "TSM", "AVGO",
        "LMT", "RTX",
    ]

    # ── RSS, SEC CIK, CLAUDE ────────────────────────────────────
    RSS_FEEDS = { ... }  # (unverändert – bleibt wie bisher)

    SEC_CIK_TARGETS = {
        "situational_awareness_lp": "0002014020",
        "thiel_capital":            "0001418819",
        "founders_fund":            "0001615175",
    }

    CLAUDE_MODEL      = "claude-sonnet-4-6"
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
        """Erstellt alle benötigten Ordner inkl. dynamic_tickers.json"""
        for d in [
            cls.SIGNALS_DIR,
            cls.CARDS_DIR,
            cls.HIST_DIR,
            cls.DASH_DIR,
            cls.CACHE_DIR,
            cls.DASH_DIR / "cards",
        ]:
            d.mkdir(parents=True, exist_ok=True)
        
        # Dynamische Ticker-Datei sicherstellen
        cls.DYNAMIC_TICKERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not cls.DYNAMIC_TICKERS_PATH.exists():
            default = {
                "tickers": cls.TARGET_TICKERS.copy(),
                "last_updated": "",
                "source": "initial"
            }
            cls.DYNAMIC_TICKERS_PATH.write_text(json.dumps(default, indent=2))

    @classmethod
    def get_all_target_tickers(cls) -> list:
        """Gibt feste + dynamische Ticker zurück (wird jetzt überall verwendet)"""
        base = cls.TARGET_TICKERS.copy()
        
        dynamic_path = cls.DYNAMIC_TICKERS_PATH
        if dynamic_path.exists():
            try:
                data = json.loads(dynamic_path.read_text())
                dynamic = data.get("tickers", [])
                # Duplikate entfernen, Reihenfolge erhalten
                return list(dict.fromkeys(base + dynamic))
            except Exception:
                pass
        return base
