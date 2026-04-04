"""
scanner/sources/sec_edgar.py
SEC EDGAR 13F-Monitor. Erkennt neue Filings und klassifiziert Positions-Änderungen.
"""

import json
import logging
import requests
import feedparser
from datetime import datetime
from pathlib import Path

from ..utils.config import Config
from ..utils.rate_limiter import rate_limiter
from ..utils.ticker_mapper import TickerMapper

logger = logging.getLogger(__name__)
mapper = TickerMapper()

EDGAR_RSS = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13F&dateb=&owner=include&count=5&search_text=&output=atom"
HEADERS   = {"User-Agent": "SA-Scanner scanner@example.com"}

SHULMAN_KEYWORDS = [
    "doubling", "compute", "intelligence explosion",
    "recursive", "algorithmic", "scaling", "robot",
    "energy demand", "power", "sovereign",
]


def check_new_filings(state_manager) -> list:
    new_filings = []

    for entity, cik in Config.SEC_CIK_TARGETS.items():
        try:
            rate_limiter.wait("sec_edgar")
            url  = EDGAR_RSS.format(cik=cik)
            feed = feedparser.parse(url)

            last_date = state_manager.get_last_filing_date(entity) or ""

            for entry in feed.entries:
                filing_date = entry.get("updated", "")
                if filing_date > last_date:
                    new_filings.append({
                        "entity":      entity,
                        "cik":         cik,
                        "filing_date": filing_date,
                        "filing_url":  entry.link,
                        "title":       entry.title,
                    })
                    state_manager.update_filing(
                        entity, cik, filing_date, entry.link, "PENDING"
                    )
                    logger.info(f"NEW FILING: {entity} — {filing_date}")

        except Exception as e:
            logger.error(f"EDGAR check {entity}: {e}")

    return new_filings


def classify_position_delta(current: dict, previous: dict) -> list:
    """Klassifiziert Positions-Änderungen in Klassen A-D."""
    classifications = []
    all_tickers     = set(list(current.keys()) + list(previous.keys()))

    for ticker in all_tickers:
        curr_shares = current.get(ticker, 0)
        prev_shares = previous.get(ticker, 0)

        if prev_shares == 0 and curr_shares > 0:
            cls   = "A"
            score = 9.5
            desc  = "NEW_POSITION"
        elif curr_shares == 0 and prev_shares > 0:
            cls   = "A"
            score = 9.0
            desc  = "CLOSED_POSITION"
        elif prev_shares > 0:
            change_pct = abs(curr_shares - prev_shares) / prev_shares * 100
            if change_pct > 20:
                cls   = "B"
                score = min(7.5 + change_pct / 100, 9.0)
                desc  = f"INCREASED_{change_pct:.0f}pct" if curr_shares > prev_shares else f"REDUCED_{change_pct:.0f}pct"
            elif change_pct > 5:
                cls   = "C"
                score = 5.0 + change_pct / 20
                desc  = "MINOR_CHANGE"
            else:
                cls   = "D"
                score = 3.0
                desc  = "UNCHANGED"
        else:
            continue

        classifications.append({
            "ticker":      ticker,
            "class":       cls,
            "score":       round(score, 1),
            "prev_shares": prev_shares,
            "curr_shares": curr_shares,
            "change_pct":  round((curr_shares - prev_shares) / max(prev_shares, 1) * 100, 1),
            "description": desc,
            "sector":      mapper.get_sector(ticker),
        })

    classifications.sort(key=lambda x: x["score"], reverse=True)
    return classifications


def check_begleittext_for_shulman(text: str) -> dict:
    """Prüft 13F-Begleittext auf Shulman-Konzepte."""
    found   = [kw for kw in SHULMAN_KEYWORDS if kw.lower() in text.lower()]
    bonus   = Config.SHULMAN_SALP_BEGLEIT_BONUS if len(found) >= 2 else 0.0
    return {
        "keywords_found": found,
        "shulman_bonus":  bonus,
    }


def run_edgar_monitor(state_manager) -> dict:
    logger.info("Running SEC EDGAR monitor")
    new_filings = check_new_filings(state_manager)

    result = {
        "new_filings_found": len(new_filings),
        "new_filings":       new_filings,
        "checked_entities":  list(Config.SEC_CIK_TARGETS.keys()),
        "checked_at":        datetime.utcnow().isoformat(),
        "trigger_pipeline":  len(new_filings) > 0,
    }

    out = Config.SIGNALS_DIR / "sec_filings.json"
    out.write_text(json.dumps(result, indent=2))

    if new_filings:
        logger.info(f"ALERT: {len(new_filings)} new 13F filings detected")
    else:
        logger.info("No new 13F filings")

    return result


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    logging.basicConfig(level=logging.INFO)
    from scanner.utils.state_manager import StateManager
    with StateManager() as sm:
        run_edgar_monitor(sm)
