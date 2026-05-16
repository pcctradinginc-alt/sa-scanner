"""
scanner/analysis/position_monitor.py
Überwacht offene Positionen auf Thesis-Break, IV-Crush und Ablauf-Warnungen.
Läuft täglich nach der Regime-Erkennung, bevor Claude aufgerufen wird.
"""

import logging
import sqlite3
from datetime import datetime, timedelta

from ..utils.config import Config

logger = logging.getLogger(__name__)


class PositionMonitor:

    def check_open_positions(self, state_manager,
                             options_data: dict, regime: dict) -> list:
        """Prüft alle offenen Positionen auf Exit-Signale."""
        try:
            conn = sqlite3.connect(str(Config.DB_PATH))
            conn.row_factory = sqlite3.Row
            positions = conn.execute(
                "SELECT * FROM active_positions WHERE status = 'OPEN'"
            ).fetchall()
            conn.close()
        except Exception as e:
            logger.warning(f"PositionMonitor: DB read error: {e}")
            return []

        alerts = []
        for pos in positions:
            alerts.extend(self._check_position(dict(pos), options_data, regime))

        return alerts

    def _check_position(self, pos: dict, options_data: dict,
                        regime: dict) -> list:
        alerts = []
        ticker = pos.get("ticker", "?")

        # ── ABLAUF-WARNUNG ────────────────────────────────────────
        opened_at = pos.get("opened_at", "")
        laufzeit  = pos.get("laufzeit_months", 6)
        if opened_at:
            try:
                opened    = datetime.fromisoformat(opened_at[:10])
                expiry    = opened + timedelta(days=int(laufzeit) * 30)
                days_left = (expiry - datetime.utcnow()).days
                if days_left <= 30:
                    alerts.append({
                        "ticker":   ticker,
                        "type":     "EXPIRY_WARNING",
                        "message":  (
                            f"{ticker}: ca. {days_left} Tage bis "
                            f"geschätztem Ablauf ({expiry.date()})"
                        ),
                        "severity": "HIGH" if days_left <= 14 else "MEDIUM",
                    })
                    logger.warning(
                        f"EXPIRY WARNING [{ticker}]: ~{days_left}d left"
                    )
            except Exception as e:
                logger.debug(f"PositionMonitor expiry parse error {ticker}: {e}")

        # ── IV-CRUSH-CHECK ────────────────────────────────────────
        ticker_opts  = options_data.get(ticker, {})
        current_iv   = ticker_opts.get("current_iv")
        iv_at_open   = pos.get("iv_at_open")
        if current_iv and iv_at_open:
            iv_drop = float(iv_at_open) - float(current_iv)
            if iv_drop > Config.STOP_IV_CRUSH_POINTS:
                alerts.append({
                    "ticker":   ticker,
                    "type":     "IV_CRUSH",
                    "message":  (
                        f"{ticker}: IV-Crush {iv_drop:.1f}pt "
                        f"(war {iv_at_open:.1f}%, jetzt {current_iv:.1f}%)"
                    ),
                    "severity": "HIGH",
                })
                logger.warning(
                    f"IV CRUSH [{ticker}]: -{iv_drop:.1f}pt "
                    f"(threshold={Config.STOP_IV_CRUSH_POINTS}pt)"
                )

        # ── STRESS-REGIME MIT NIEDRIGER EINSTIEGS-CONVICTION ──────
        conviction_at_open = pos.get("conviction_at_open", 0) or 0
        if (regime.get("mode") == "STRESS" and
                float(conviction_at_open) < Config.CONVICTION_STRESS):
            alerts.append({
                "ticker":   ticker,
                "type":     "STRESS_REGIME_LOW_CONVICTION",
                "message":  (
                    f"{ticker}: Stress-Regime aktiv, "
                    f"Einstiegs-Conviction {conviction_at_open:.1f} "
                    f"< {Config.CONVICTION_STRESS} (Stress-Schwelle)"
                ),
                "severity": "MEDIUM",
            })

        # ── OI-UNUSUAL ACTIVITY (bearisch) ────────────────────────
        oi_unusual = ticker_opts.get("oi_unusual", False)
        oi_delta   = ticker_opts.get("oi_delta_pct")
        if oi_unusual and oi_delta:
            alerts.append({
                "ticker":   ticker,
                "type":     "OI_SPIKE_ON_OPEN_POSITION",
                "message":  (
                    f"{ticker}: Unusual OI-Anstieg +{oi_delta:.0%} "
                    f"bei offener Position — möglicher Smart-Money-Gegensatz"
                ),
                "severity": "LOW",
            })

        return alerts

    def run(self, state_manager, all_data: dict, regime: dict) -> list:
        """Vollständiger Position-Monitor-Run. Gibt Alert-Liste zurück."""
        options_data = all_data.get("options", {})
        alerts = self.check_open_positions(state_manager, options_data, regime)

        if alerts:
            high   = sum(1 for a in alerts if a["severity"] == "HIGH")
            medium = sum(1 for a in alerts if a["severity"] == "MEDIUM")
            low    = sum(1 for a in alerts if a["severity"] == "LOW")
            logger.warning(
                f"Position Monitor: {len(alerts)} Alerts "
                f"(HIGH={high} MEDIUM={medium} LOW={low})"
            )
            for a in alerts:
                logger.warning(f"  [{a['severity']}] {a['message']}")
        else:
            logger.info("Position Monitor: Keine Alerts — alle Positionen OK")

        return alerts
