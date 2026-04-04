"""
main.py
Vollständige Scanner-Pipeline Orchestrierung.
Einstiegspunkt für GitHub Actions und lokalen Betrieb.

Usage:
    python main.py                      # Vollständiger Daily Run
    python main.py --edgar-only         # Nur EDGAR-Check
    python main.py --ticker VST PLTR    # Spezifische Ticker
    python main.py --no-claude          # Nur Daten, kein Claude
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Sicherstellen dass Root-Verzeichnis im Python-Pfad ist
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


def run_full_pipeline(args=None):
    from scanner.utils.config import Config
    from scanner.utils.state_manager import StateManager
    from scanner.sources.data_fetcher import DataFetcher
    from scanner.sources.sec_edgar import run_edgar_monitor
    from scanner.signals.regime_detector import RegimeDetector
    from scanner.analysis.claude_analyzer import ClaudeAnalyzer
    from scanner.output.trading_card_generator import generate_all_cards
    from scanner.output.dashboard_generator import build_dashboard

    # Validierung
    missing = Config.validate()
    if missing:
        logger.warning(f"Missing API keys: {missing}")

    Config.ensure_dirs()

    errors = []
    start  = datetime.utcnow()
    logger.info(f"=== SA SCANNER START: {start.isoformat()} ===")

    with StateManager() as sm:
        try:
            # ── 1. SEC EDGAR MONITOR ─────────────────────────────
            logger.info("Step 1: SEC EDGAR monitor")
            sec_data = run_edgar_monitor(sm)

            if args and args.edgar_only:
                logger.info("--edgar-only: stopping after EDGAR check")
                sm.commit_state("EDGAR-only run")
                return

            # ── 2. DATEN FETCHEN ──────────────────────────────────
            logger.info("Step 2: Data fetch")
            fetcher  = DataFetcher(sm)
            all_data = fetcher.fetch_all(sm)

            # ── 3. REGIME BESTIMMEN ───────────────────────────────
            logger.info("Step 3: Regime detection")
            detector = RegimeDetector()
            regime   = detector.detect(all_data, sm)

            logger.info(f"Regime: {regime['mode']} | "
                        f"IV-Rank: {regime.get('iv_rank_avg',50):.1f}% | "
                        f"Energy: {regime.get('energy_breadth',0.5):.0%} | "
                        f"Threshold: {regime.get('conviction_threshold',7.5)}")

            # ── 4. CLAUDE ANALYSE ─────────────────────────────────
            cards = []
            if not (args and args.no_claude):
                logger.info("Step 4: Claude analysis")
                analyzer = ClaudeAnalyzer()

                # Ticker-Override
                if args and args.ticker:
                    from scanner.utils.config import Config as C
                    C.TARGET_TICKERS = args.ticker
                    logger.info(f"Ticker override: {args.ticker}")

                cards = analyzer.run_daily_analysis(all_data, regime, sec_data, sm)
            else:
                logger.info("Step 4: Skipped (--no-claude)")

            # ── 5. TRADING CARDS GENERIEREN ───────────────────────
            logger.info("Step 5: Trading card generation")

            # Watchlist-Cards aus DB laden
            import sqlite3
            conn = sqlite3.connect(str(Config.DB_PATH))
            conn.row_factory = sqlite3.Row
            today = datetime.utcnow().date().isoformat()
            db_cards = conn.execute(
                "SELECT card_json FROM trading_cards WHERE date = ? AND gate_status IN ('PASS','WATCHLIST')",
                (today,)
            ).fetchall()
            conn.close()

            all_cards = [json.loads(r["card_json"]) for r in db_cards]
            n_cards   = generate_all_cards(all_cards)
            logger.info(f"Generated {n_cards} HTML cards")

            # ── 6. DASHBOARD ─────────────────────────────────────
            logger.info("Step 6: Dashboard generation")
            build_dashboard(sm, regime)

            # ── 7. SUMMARY ───────────────────────────────────────
            duration = (datetime.utcnow() - start).total_seconds()
            logger.info(
                f"=== SA SCANNER DONE: {duration:.0f}s | "
                f"Cards: {n_cards} | "
                f"Errors: {len(errors)} | "
                f"Regime: {regime['mode']} ==="
            )

            # GitHub Actions Environment Variable für Commit-Message
            try:
                env_file = Path("/tmp/scanner_stats.env")
                env_file.write_text(
                    f"SIGNAL_COUNT={n_cards}\n"
                    f"REGIME_MODE={regime['mode']}\n"
                    f"CANDIDATE_COUNT={len(all_cards)}\n"
                )
            except Exception:
                pass

            # ── 8. GIT COMMIT ─────────────────────────────────────
            extra = f"Regime: {regime['mode']} | Cards: {n_cards}"
            if sec_data.get("new_filings_found", 0) > 0:
                extra += f" | 13F ALERT: {sec_data['new_filings_found']} new filings"
            sm.commit_state(extra)

        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            errors.append(str(e))
            try:
                sm.commit_state(f"Error run: {e}")
            except Exception:
                pass
            sys.exit(1)


def run_edgar_only():
    from scanner.utils.state_manager import StateManager
    from scanner.sources.sec_edgar import run_edgar_monitor

    logger.info("=== EDGAR-ONLY RUN ===")
    with StateManager() as sm:
        result = run_edgar_monitor(sm)
        if result.get("trigger_pipeline"):
            logger.info("New filings detected — triggering full pipeline")
            run_full_pipeline()
        else:
            sm.commit_state("EDGAR check — no new filings")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SA Scanner")
    parser.add_argument("--edgar-only",  action="store_true",
                        help="Run only EDGAR 13F check")
    parser.add_argument("--no-claude",   action="store_true",
                        help="Skip Claude analysis (data fetch only)")
    parser.add_argument("--ticker",      nargs="+",
                        help="Analyze specific tickers only")
    args = parser.parse_args()

    if args.edgar_only:
        run_edgar_only()
    else:
        run_full_pipeline(args)
