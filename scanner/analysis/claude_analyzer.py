"""
scanner/analysis/claude_analyzer.py
Anthropic API Integration mit vollständigem Master-Prompt.
Nur Ticker die den Pre-Filter passiert haben werden analysiert.
"""

import json
import logging
from datetime import datetime

import anthropic

from ..utils.config import Config
from ..utils.ticker_mapper import TickerMapper
from .pre_filter import PreFilter
from .scoring_engine import ScoringEngine, ConvictionResult
from ..signals.regime_detector import RegimeDetector
from ..signals.contrarian_gate import ContrarianGate
from ..signals.shulman_layer import ShulmanLayer
from ..signals.thiel_layer import ThielLayer

logger  = logging.getLogger(__name__)
client  = anthropic.Anthropic(api_key=Config.ANTHROPIC_API_KEY)
mapper  = TickerMapper()


MASTER_PROMPT = """Du bist der Analyse-Layer des Situational Awareness Scanner Systems.
Deine Aufgabe ist eine strukturierte Analyse basierend auf Aschenbrenners "The Decade Ahead".

<analysis_request>

  <ticker>{ticker}</ticker>
  <sector>{sector}</sector>
  <regime_mode>{regime_mode}</regime_mode>
  <conviction_threshold>{threshold}</conviction_threshold>

  <pre_computed_scores>
{pre_scores_json}
  </pre_computed_scores>

  <raw_data>
{raw_data_json}
  </raw_data>

  <step_1_bottleneck>
    Ist diese News primär eine Lösung für einen RECHEN-Flaschenhals oder ENERGIE-Flaschenhals?
    Beziehe dich auf "The Decade Ahead" von Aschenbrenner.
    Antworte: "RECHEN", "ENERGIE" oder "BEIDE".
  </step_1_bottleneck>

  <step_2_salp_analysis>
    SA LP Filing-Klasse: {filing_class}
    Bewerte die Portfoliobewegung im Kontext der Energie/Rechen-Flaschenhals-These.
    Berücksichtige Shulman-Terminologie in Begleittexten: {shulman_begleit}
  </step_2_salp_analysis>

  <step_3_thiel_thesis>
    Antichrist-Check: Ist die News eine ZENTRALISIERENDE KRAFT
    (kommunistische KI / globale Regulation / AI-Safety-Frameworks) oder
    eine DEZENTRALE/SOVEREIGN LÖSUNG (Katechon)?

    Zero to One: Baut {ticker} ein echtes Monopol mit physischer/hoheitlicher Barriere auf?

    Katechon: Wird {ticker} als "das Aufhaltende" gegen Stagnation/Chaos positioniert?

    Netzwerk aktive Akteure: {active_actors}
    Deep Network Signal: {deep_network}
    Signal-Typ: {thiel_signal_type}
  </step_3_thiel_thesis>

  <step_4_shulman>
    Empirischer Score: {empirical_score}/3
    EIA Stromwachstum: {eia_growth}
    CapEx-Trend: {capex_trend}
    NVDA-Wachstum: {nvda_growth}

    Prüfe: Bestätigt {ticker} einen oder mehrere von:
    - Robot Doublings (Physical AI Produktion wächst exponentiell)
    - Compute-Substitutability (Software-Effizienz dämpft Hardware-Nachfrage)
    - Intelligence Explosion Loop (Energie-Nachfrage wächst quadratisch zu Modellgröße)
  </step_4_shulman>

  <step_5_contrarian>
    Contrarian-Score: {contrarian_score}
    Aktive Gegenthesen: {gegenthesen}
    RSI: {rsi} | Put/Call-Ratio: {put_call}

    Wenn Contrarian-Score < -3: Gate ist bereits blockiert — analysiere trotzdem warum.
  </step_5_contrarian>

  <step_6_call_recommendation>
    Laufzeit basierend auf Conviction: {laufzeit_months} Monate
    Aktuelle Optionsdaten (beste Calls nach Liquidität):
{top_calls_json}

    Empfehle:
    - Strike (% OTM vom aktuellen Kurs)
    - Expiration
    - Entry-Prämie (Midpoint)
    - Target (Prämien-Vielfaches)
    - Stop Schicht 1: spezifisches Thesis-Ereignis das Position schließt
    - Stop Schicht 2: -40% Prämie in 30 Tagen ODER IV-Crush >15 Punkte
    - Positions-Checkpoints: 90 Tage, 180 Tage, monatlich
  </step_6_call_recommendation>

  <output_instructions>
    Antworte NUR mit einem validen JSON-Objekt. KEIN Text davor oder danach.
    Kein Markdown, keine Backticks, kein "json" Prefix.

    Pflichtstruktur:
    {{
      "ticker": "{ticker}",
      "company_name": "...",
      "sector": "{sector}",
      "bottleneck_type": "RECHEN|ENERGIE|BEIDE",
      "regime_mode": "{regime_mode}",
      "scores": {{
        "salp": {{"score": float, "filing_class": "A|B|C|D|NONE", "rationale": "..."}},
        "thiel": {{"score": float, "signal_type": "...", "katechon_bonus": float, "rationale": "..."}},
        "shulman": {{"score": float, "empirical_score": int, "weight_modifier": float, "rationale": "..."}},
        "multigate": {{"score": float, "rationale": "..."}},
        "regime": {{"score": float, "rationale": "..."}},
        "contrarian": {{"score": float, "gate_blocked": bool, "gegenthesen": [], "rationale": "..."}}
      }},
      "conviction_total": float,
      "conviction_gate": "PASS|WATCHLIST|NO_SIGNAL|BLOCKED_CONTRARIAN",
      "deep_network_signal": bool,
      "laufzeit_months": int,
      "option": {{
        "type": "CALL",
        "strike_pct_otm": float,
        "strike_absolute": float,
        "expiration": "YYYY-MM-DD",
        "entry_premium": float,
        "target_multiplier": float,
        "laufzeit_begruendung": "...",
        "stop_thesis_trigger": "Spezifisches Ereignis das den Trade schließt",
        "stop_technical_trigger": "-40% Prämie in 30 Tagen ODER IV-Crush >15 Punkte",
        "checkpoint_90d": "...",
        "checkpoint_180d": "...",
        "checkpoint_monthly": "Greeks-Review: Delta-Schwellenwert und Maßnahme"
      }},
      "rationale": "3-5 Sätze Analysebegründung alle aktiven Layer",
      "gegen_szenario": "Spezifisches Ereignis das Thesis widerlegt",
      "signal_tags": [],
      "liquidity_flags": []
    }}
  </output_instructions>

</analysis_request>"""


class ClaudeAnalyzer:

    def __init__(self):
        self.pre_filter  = PreFilter()
        self.scorer      = ScoringEngine()
        self.contrarian  = ContrarianGate()
        self.shulman     = ShulmanLayer()
        self.thiel       = ThielLayer()

    def analyze_ticker(self, ticker: str, all_data: dict,
                       regime: dict, sec_data: dict,
                       state_manager) -> dict | None:

        sector = mapper.get_sector(ticker)

        # Pre-Filter
        should_call, pre_score = self.pre_filter.should_call_claude(
            ticker, all_data, regime, sec_data
        )
        if not should_call:
            return None

        logger.info(f"Claude analysis: {ticker} (pre_score={pre_score})")

        # Layer-Scores berechnen
        rss         = all_data.get("rss", [])
        options_data = all_data.get("options", {})

        contrarian_data = self.contrarian.evaluate(rss, ticker, options_data)
        if contrarian_data["gate_blocked"]:
            return {
                "ticker":          ticker,
                "conviction_gate": "BLOCKED_CONTRARIAN",
                "conviction_total":0.0,
                "contrarian_data": contrarian_data,
            }

        shulman_data = self.shulman.evaluate(all_data, ticker, sector)
        thiel_data   = self.thiel.evaluate(rss, sec_data, state_manager)

        # Ticker-spezifische Options-Daten
        ticker_opts = options_data.get(ticker, {})
        top_calls   = (ticker_opts.get("target_calls", {}).get("calls", []) or [])[:3]

        # SALP Score aus Filing
        classifications = sec_data.get("classifications", [])
        ticker_class    = next((c for c in classifications if c["ticker"] == ticker), None)
        salp_score      = ticker_class["score"] if ticker_class else 3.0
        filing_class    = ticker_class["class"] if ticker_class else "NONE"

        # Regime Score
        regime_score = regime.get("regime_score", 5.0)

        # Multi-Signal-Gate (vereinfacht aus RSS)
        multigate_articles = [a for a in rss
                              if a.get("signals", {}).get("salp") or
                                 a.get("credibility", 0) > 0.85]
        multigate_score    = min(5.0 + len(multigate_articles) * 0.5, 10.0)

        # Pre-Scores für Prompt
        pre_scores = {
            "salp":       salp_score,
            "thiel":      thiel_data["thiel_score"],
            "shulman":    shulman_data["shulman_score"],
            "multigate":  multigate_score,
            "regime":     regime_score,
            "contrarian": contrarian_data["contrarian_score"],
            "katechon_bonus": thiel_data["katechon_bonus"],
        }

        # Laufzeit bestimmen (vorläufig für Prompt)
        pre_conviction = sum(
            pre_scores[k] * v
            for k, v in regime.get("weights", Config.WEIGHTS_NORMAL).items()
            if k in pre_scores
        )
        laufzeit = 12 if pre_conviction >= 9.0 else 9 if pre_conviction >= 8.0 else 6

        # EIA + CapEx für Prompt
        eia_growth  = all_data.get("eia", {}).get("growth_yoy")
        capex_trend = all_data.get("hyperscaler_capex", {}).get("capex_trend", "unknown")
        nvda_growth = all_data.get("nvda_revenue", {}).get("growth_yoy")

        prompt = MASTER_PROMPT.format(
            ticker=ticker,
            sector=sector,
            regime_mode=regime.get("mode", "NORMAL"),
            threshold=regime.get("conviction_threshold", 7.5),
            pre_scores_json=json.dumps(pre_scores, indent=6),
            raw_data_json=json.dumps({
                "rss_relevant": [a for a in rss if ticker in str(a.get("tickers", []))][:5],
                "options_summary": {
                    "current_price": ticker_opts.get("current_price"),
                    "current_iv":    ticker_opts.get("current_iv"),
                    "iv_rank":       ticker_opts.get("iv_rank", {}),
                    "options_flow":  ticker_opts.get("options_flow", {}),
                },
            }, indent=6, default=str),
            filing_class=filing_class,
            shulman_begleit=str(shulman_data.get("shulman_bonus", 0) > 0),
            active_actors=", ".join(thiel_data.get("active_actors", [])),
            deep_network=thiel_data.get("deep_network_signal", False),
            thiel_signal_type=thiel_data.get("signal_type", "KEIN_SIGNAL"),
            empirical_score=shulman_data.get("empirical_score", 0),
            eia_growth=f"{eia_growth:.1%}" if eia_growth else "N/A",
            capex_trend=capex_trend,
            nvda_growth=f"{nvda_growth:.1%}" if nvda_growth else "N/A",
            contrarian_score=contrarian_data.get("contrarian_score", 0),
            gegenthesen=", ".join(contrarian_data.get("gegenthesen_aktiv", [])) or "keine",
            rsi=ticker_opts.get("rsi", 50),
            put_call=ticker_opts.get("options_flow", {}).get("put_call_volume", "N/A"),
            laufzeit_months=laufzeit,
            top_calls_json=json.dumps(top_calls, indent=8, default=str),
        )

        try:
            response = client.messages.create(
                model=Config.CLAUDE_MODEL,
                max_tokens=Config.CLAUDE_MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()

            # JSON-Bereinigung
            if "```" in raw:
                parts = raw.split("```")
                for part in parts:
                    part = part.strip()
                    if part.startswith("json"):
                        part = part[4:].strip()
                    if part.startswith("{"):
                        raw = part
                        break

            result = json.loads(raw)

            # Portfolio-Check
            ok, reason = state_manager.check_portfolio_limits(ticker, sector)
            result["portfolio_check"] = {"passed": ok, "reason": reason}
            if not ok:
                result["conviction_gate"] = f"PORTFOLIO_BLOCKED_{reason}"

            result["pre_filter_score"] = pre_score
            result["analyzed_at"]      = datetime.utcnow().isoformat()
            return result

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error {ticker}: {e}\nRaw: {raw[:200]}")
            return None
        except Exception as e:
            logger.error(f"Claude error {ticker}: {e}")
            return None

    def run_daily_analysis(self, all_data: dict, regime: dict,
                           sec_data: dict, state_manager) -> list:
        logger.info("=== Claude Analyzer: starting analysis ===")

        # Kandidaten-Ticker ermitteln
        candidates = set(Config.TARGET_TICKERS)

        # Aus SEC-Filings
        for cls in sec_data.get("classifications", []):
            if cls["class"] in ["A", "B"]:
                candidates.add(cls["ticker"])

        # Aus RSS
        for article in all_data.get("rss", []):
            for ticker in article.get("tickers", []):
                if ticker in Config.TARGET_TICKERS:
                    candidates.add(ticker)

        results       = []
        claude_calls  = 0
        errors        = []

        for ticker in sorted(candidates):
            try:
                result = self.analyze_ticker(
                    ticker, all_data, regime, sec_data, state_manager
                )
                if result:
                    claude_calls += 1
                    conviction = result.get("conviction_total", 0)
                    gate       = result.get("conviction_gate", "NO_SIGNAL")

                    state_manager.store_signal(
                        ticker, conviction, gate,
                        regime.get("mode", "NORMAL"),
                        result.get("bottleneck_type", "UNKNOWN"),
                        result,
                    )

                    if gate == "PASS":
                        state_manager.store_trading_card(
                            ticker, conviction, gate,
                            result.get("laufzeit_months", 6),
                            result,
                        )
                        results.append(result)
                        logger.info(f"TRADING CARD: {ticker} | {conviction:.2f} | "
                                    f"{result.get('laufzeit_months')}M CALL")

            except Exception as e:
                errors.append(f"{ticker}: {e}")
                logger.error(f"Analysis error {ticker}: {e}")

        state_manager.log_run_stats(
            candidates=len(candidates),
            claude_calls=claude_calls,
            cards_generated=len(results),
            regime_mode=regime.get("mode", "NORMAL"),
            errors=errors,
        )

        logger.info(f"=== Analysis done: {len(results)} cards | "
                    f"{claude_calls} Claude calls | {len(errors)} errors ===")
        return results
