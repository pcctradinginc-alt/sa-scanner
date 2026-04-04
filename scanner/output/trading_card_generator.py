"""
scanner/output/trading_card_generator.py
Generiert HTML Trading Cards aus Claude-Analyse-JSON.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from ..utils.config import Config

logger = logging.getLogger(__name__)


def score_color(score: float) -> str:
    if score >= 8.0:
        return "#00d4ff"
    elif score >= 6.5:
        return "#ffd166"
    elif score >= 5.0:
        return "#ff9f6e"
    return "#ff4444"


def gate_color(gate: str) -> str:
    return {
        "PASS":                 "#44ff88",
        "WATCHLIST":            "#ffd166",
        "NO_SIGNAL":            "#5a7a9a",
        "BLOCKED_CONTRARIAN":   "#ff4444",
    }.get(gate, "#5a7a9a")


def bottleneck_color(bt: str) -> str:
    return {
        "ENERGIE": "#ffd166",
        "RECHEN":  "#00d4ff",
        "BEIDE":   "#b48eff",
    }.get(bt, "#5a7a9a")


def render_score_bar(label: str, score: float, weight: float,
                     color: str, sub: str = "") -> str:
    pct = int(score / 10 * 100)
    return f"""
    <div class="score-row">
      <div class="score-label">
        <span class="score-name">{label}</span>
        <span class="score-weight">{int(weight*100)}%</span>
        {f'<span class="score-sub">{sub}</span>' if sub else ''}
      </div>
      <div class="score-track">
        <div class="score-fill" style="width:{pct}%;background:{color}"></div>
      </div>
      <div class="score-num" style="color:{color}">{score:.1f}</div>
    </div>"""


def generate_card_html(card: dict) -> str:
    ticker    = card.get("ticker", "???")
    company   = card.get("company_name", "")
    sector    = card.get("sector", "")
    bt        = card.get("bottleneck_type", "?")
    regime    = card.get("regime_mode", "NORMAL")
    gate      = card.get("conviction_gate", "NO_SIGNAL")
    conviction = card.get("conviction_total", 0.0)
    laufzeit  = card.get("laufzeit_months", 0)
    rationale = card.get("rationale", "")
    gegen     = card.get("gegen_szenario", "")
    deep_net  = card.get("deep_network_signal", False)
    tags      = card.get("signal_tags", [])
    liq_flags = card.get("liquidity_flags", [])
    analyzed  = card.get("analyzed_at", datetime.utcnow().isoformat())[:10]

    scores    = card.get("scores", {})
    option    = card.get("option", {})
    portfolio = card.get("portfolio_check", {})

    # Score-Werte
    s_salp      = scores.get("salp",       {}).get("score", 0)
    s_thiel     = scores.get("thiel",      {}).get("score", 0)
    s_shulman   = scores.get("shulman",    {}).get("score", 0)
    s_multi     = scores.get("multigate",  {}).get("score", 0)
    s_regime    = scores.get("regime",     {}).get("score", 0)
    s_contra    = scores.get("contrarian", {}).get("score", 0)
    k_bonus     = scores.get("thiel",      {}).get("katechon_bonus", 0)
    emp_score   = scores.get("shulman",    {}).get("empirical_score", 0)
    thiel_type  = scores.get("thiel",      {}).get("signal_type", "")
    filing_cls  = scores.get("salp",       {}).get("filing_class", "NONE")
    contra_block = scores.get("contrarian",{}).get("gate_blocked", False)
    gegenthesen = scores.get("contrarian", {}).get("gegenthesen", [])

    # Option-Felder
    opt_type    = option.get("type", "CALL")
    strike_pct  = option.get("strike_pct_otm", 0)
    strike_abs  = option.get("strike_absolute", 0)
    expiry      = option.get("expiration", "")
    entry       = option.get("entry_premium", 0)
    target_mult = option.get("target_multiplier", 0)
    stop_thesis = option.get("stop_thesis_trigger", "")
    stop_tech   = option.get("stop_technical_trigger", "")
    cp_90       = option.get("checkpoint_90d", "")
    cp_180      = option.get("checkpoint_180d", "")
    cp_mon      = option.get("checkpoint_monthly", "")
    laufzeit_b  = option.get("laufzeit_begruendung", "")

    gate_c  = gate_color(gate)
    bt_c    = bottleneck_color(bt)
    conv_c  = score_color(conviction)

    tags_html = "".join([
        f'<span class="tag">{t}</span>' for t in tags
    ])
    liq_html = "".join([
        f'<span class="tag tag-warn">{f}</span>' for f in liq_flags
    ]) if liq_flags else '<span class="tag tag-ok">LIQUIDITY OK</span>'

    scores_html = (
        render_score_bar("SA LP ALIGN", s_salp, 0.40,
                         score_color(s_salp), f"Filing: {filing_cls}") +
        render_score_bar("THIEL ALIGN", s_thiel, 0.14,
                         score_color(s_thiel),
                         f"{thiel_type} | K-Bonus: +{k_bonus}") +
        render_score_bar("SHULMAN", s_shulman, 0.15,
                         score_color(s_shulman),
                         f"Empirical: {emp_score}/3") +
        render_score_bar("MULTI-SIGNAL", s_multi, 0.04,
                         score_color(s_multi)) +
        render_score_bar("MARKT-REGIME", s_regime, 0.15,
                         score_color(s_regime), f"Mode: {regime}") +
        render_score_bar("CONTRARIAN",
                         max(s_contra, 0), 0.12,
                         "#ff4444" if contra_block else score_color(max(s_contra, 0)),
                         f"Score: {s_contra:+.1f}"
                         + (" | BLOCKED" if contra_block else ""))
    )

    gegenthesen_html = "".join([
        f'<li>{g}</li>' for g in gegenthesen
    ]) if gegenthesen else "<li>Keine aktiven Gegenthesen</li>"

    portfolio_ok = portfolio.get("passed", True)
    portfolio_c  = "#44ff88" if portfolio_ok else "#ff4444"
    portfolio_r  = portfolio.get("reason", "OK")

    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SA Scanner — {ticker} Trading Card</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;700&family=Bebas+Neue&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
  :root {{
    --bg:#0a0b0d;--surface:#10131a;--surface2:#161c28;--surface3:#1e2635;
    --border:#1f2d42;--border-bright:#2a3f5c;
    --accent:#00d4ff;--accent2:#ff6b35;--accent3:#7fff6e;--accent4:#ffd166;
    --text:#c8d8e8;--text-dim:#5a7a9a;--text-bright:#eaf4ff;
    --red:#ff4444;--green:#44ff88;
  }}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:'IBM Plex Sans',sans-serif;
        font-size:13px;line-height:1.5;padding:20px;max-width:900px;margin:0 auto}}
  .card{{background:var(--surface);border:1px solid {gate_c};position:relative;
         overflow:hidden;margin-bottom:16px}}
  .card-header{{background:var(--surface2);padding:16px 20px;
                border-bottom:1px solid var(--border-bright);
                display:flex;justify-content:space-between;align-items:flex-start}}
  .ticker{{font-family:'Bebas Neue',sans-serif;font-size:52px;
           color:var(--accent);letter-spacing:4px;line-height:1}}
  .company{{font-family:'IBM Plex Mono',monospace;font-size:10px;
            color:var(--text-dim);letter-spacing:1px;margin-top:4px}}
  .header-right{{text-align:right}}
  .gate-badge{{font-family:'IBM Plex Mono',monospace;font-size:9px;
               letter-spacing:3px;padding:4px 10px;
               border:1px solid {gate_c};color:{gate_c};margin-bottom:6px}}
  .regime-badge{{font-family:'IBM Plex Mono',monospace;font-size:9px;
                 color:var(--text-dim);letter-spacing:2px}}
  .conviction-block{{display:flex;align-items:center;gap:20px;padding:16px 20px;
                     border-bottom:1px solid var(--border)}}
  .conviction-num{{font-family:'Bebas Neue',sans-serif;font-size:72px;
                   line-height:1;color:{conv_c}}}
  .conviction-label{{font-family:'IBM Plex Mono',monospace;font-size:9px;
                     color:var(--text-dim);letter-spacing:3px}}
  .conviction-meta{{font-size:11px;color:var(--text);margin-top:4px}}
  .bt-tag{{display:inline-flex;align-items:center;gap:6px;
           border:1px solid {bt_c};padding:4px 10px;
           font-family:'IBM Plex Mono',monospace;font-size:10px;color:{bt_c};
           letter-spacing:1px;margin-bottom:8px}}
  .dot{{width:6px;height:6px;border-radius:50%;background:{bt_c}}}
  .body{{padding:16px 20px}}
  .section-title{{font-family:'IBM Plex Mono',monospace;font-size:9px;
                  letter-spacing:3px;color:var(--text-dim);margin-bottom:8px;
                  padding-bottom:4px;border-bottom:1px solid var(--border)}}
  .section{{margin-bottom:16px}}
  /* Score bars */
  .score-row{{display:flex;align-items:center;gap:10px;margin-bottom:8px}}
  .score-label{{min-width:220px}}
  .score-name{{font-family:'IBM Plex Mono',monospace;font-size:11px;color:var(--text)}}
  .score-weight{{font-family:'IBM Plex Mono',monospace;font-size:9px;
                 color:var(--text-dim);margin-left:6px}}
  .score-sub{{display:block;font-family:'IBM Plex Mono',monospace;font-size:9px;
              color:var(--text-dim)}}
  .score-track{{flex:1;height:6px;background:var(--surface3)}}
  .score-fill{{height:100%;transition:width 0.3s}}
  .score-num{{font-family:'Bebas Neue',sans-serif;font-size:22px;
              min-width:36px;text-align:right;line-height:1}}
  /* Option fields */
  .opt-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:12px}}
  .opt-field-label{{font-family:'IBM Plex Mono',monospace;font-size:9px;
                    color:var(--text-dim);letter-spacing:2px}}
  .opt-field-val{{font-family:'IBM Plex Mono',monospace;font-size:14px;
                  color:var(--text-bright);margin-top:2px}}
  .opt-field-val.green{{color:var(--green)}}
  .opt-field-val.red{{color:var(--red)}}
  .opt-field-val.amber{{color:var(--accent4)}}
  /* Stop blocks */
  .stop-block{{padding:10px 12px;font-size:11px;line-height:1.6;margin-bottom:8px}}
  .stop-thesis{{background:var(--surface2);border-left:2px solid var(--accent3)}}
  .stop-tech{{background:var(--surface2);border-left:2px solid var(--red)}}
  .rationale-block{{background:var(--surface2);border-left:2px solid var(--accent3);
                    padding:10px 12px;font-size:11px;line-height:1.6;margin-bottom:8px}}
  .against-block{{background:var(--surface2);border-left:2px solid var(--red);
                  padding:10px 12px;font-size:11px;line-height:1.6;margin-bottom:8px}}
  .block-label{{font-family:'IBM Plex Mono',monospace;font-size:9px;
                color:var(--text-dim);letter-spacing:2px;margin-bottom:5px}}
  .checkpoint-grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:12px}}
  .checkpoint{{background:var(--surface2);padding:8px 10px}}
  .checkpoint-title{{font-family:'IBM Plex Mono',monospace;font-size:9px;
                     color:var(--accent);letter-spacing:2px;margin-bottom:4px}}
  .checkpoint-body{{font-size:10px;color:var(--text)}}
  .gegenthesen-list{{list-style:none;padding:0}}
  .gegenthesen-list li{{font-family:'IBM Plex Mono',monospace;font-size:10px;
                        color:var(--red);padding:2px 0}}
  .gegenthesen-list li::before{{content:"▸ ";color:var(--red)}}
  .tags-row{{display:flex;flex-wrap:wrap;gap:4px;margin-top:10px}}
  .tag{{font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:1px;
        padding:2px 8px;border:1px solid var(--border-bright);color:var(--text-dim)}}
  .tag-warn{{border-color:var(--accent2);color:var(--accent2)}}
  .tag-ok{{border-color:var(--accent3);color:var(--accent3)}}
  .portfolio-row{{font-family:'IBM Plex Mono',monospace;font-size:10px;
                  color:{portfolio_c};padding:6px 10px;
                  background:var(--surface2);margin-bottom:8px}}
  .footer{{font-family:'IBM Plex Mono',monospace;font-size:9px;
           color:var(--text-dim);display:flex;justify-content:space-between;
           padding:10px 20px;border-top:1px solid var(--border)}}
  .deep-net{{font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:2px;
             color:var(--accent);margin-top:4px}}
  @media(max-width:600px){{
    .opt-grid,.checkpoint-grid{{grid-template-columns:1fr}}
    .conviction-num{{font-size:52px}}
    .ticker{{font-size:38px}}
  }}
</style>
</head>
<body>

<div class="card">

  <!-- HEADER -->
  <div class="card-header">
    <div>
      <div class="ticker">{ticker}</div>
      <div class="company">{company}</div>
      <div class="company" style="margin-top:2px">{sector.upper().replace("_"," ")}</div>
      {f'<div class="deep-net">◈ DEEP NETWORK SIGNAL AKTIV</div>' if deep_net else ''}
    </div>
    <div class="header-right">
      <div class="gate-badge">{gate.replace("_"," ")}</div>
      <div class="regime-badge">{regime} MODE · {analyzed}</div>
      {f'<div class="regime-badge" style="color:#ff4444;margin-top:4px">PORTFOLIO: {portfolio_r}</div>'
        if not portfolio_ok else ''}
    </div>
  </div>

  <!-- CONVICTION -->
  <div class="conviction-block">
    <div>
      <div class="conviction-label">GESAMT-CONVICTION</div>
      <div class="conviction-num">{conviction:.1f}</div>
      <div class="conviction-meta">Schwellenwert: {"8.0" if regime == "STRESS" else "7.5"} | Laufzeit: {laufzeit}M</div>
    </div>
    <div style="flex:1">
      <div class="bt-tag"><div class="dot"></div>{bt}-FLASCHENHALS</div>
      {f'<div style="font-family:IBM Plex Mono,monospace;font-size:10px;color:#b48eff">K-BONUS: +{k_bonus}</div>' if k_bonus > 0 else ''}
    </div>
  </div>

  <div class="body">

    <!-- SCORES -->
    <div class="section">
      <div class="section-title">SCORING BREAKDOWN</div>
      {scores_html}
    </div>

    <!-- OPTION PARAMETERS -->
    <div class="section">
      <div class="section-title">CALL OPTION PARAMETER</div>
      <div class="opt-grid">
        <div>
          <div class="opt-field-label">TYP</div>
          <div class="opt-field-val green">{opt_type}</div>
        </div>
        <div>
          <div class="opt-field-label">STRIKE (OTM)</div>
          <div class="opt-field-val">+{strike_pct:.1f}% {f'/ ${strike_abs:.0f}' if strike_abs else ''}</div>
        </div>
        <div>
          <div class="opt-field-label">EXPIRATION</div>
          <div class="opt-field-val">{expiry}</div>
        </div>
        <div>
          <div class="opt-field-label">ENTRY-PRÄMIE</div>
          <div class="opt-field-val">${entry:.2f}</div>
        </div>
        <div>
          <div class="opt-field-label">TARGET</div>
          <div class="opt-field-val green">{target_mult:.1f}x Prämie</div>
        </div>
        <div>
          <div class="opt-field-label">LAUFZEIT</div>
          <div class="opt-field-val amber">{laufzeit} Monate</div>
        </div>
      </div>
      <div style="font-family:'IBM Plex Mono',monospace;font-size:10px;
                  color:var(--text-dim);margin-bottom:10px">{laufzeit_b}</div>
    </div>

    <!-- STOP LOSS -->
    <div class="section">
      <div class="section-title">STOP-LOSS (ZWEI SCHICHTEN)</div>
      <div class="stop-block stop-thesis">
        <div class="block-label">SCHICHT 1 · THESIS-TRIGGER (VOLLSTÄNDIGER EXIT)</div>
        {stop_thesis}
      </div>
      <div class="stop-block stop-tech">
        <div class="block-label">SCHICHT 2 · TECHNISCHER TEIL-EXIT (50%)</div>
        {stop_tech}
      </div>
    </div>

    <!-- CHECKPOINTS -->
    <div class="section">
      <div class="section-title">POSITIONS-MANAGEMENT CHECKPOINTS</div>
      <div class="checkpoint-grid">
        <div class="checkpoint">
          <div class="checkpoint-title">90 TAGE</div>
          <div class="checkpoint-body">{cp_90}</div>
        </div>
        <div class="checkpoint">
          <div class="checkpoint-title">180 TAGE</div>
          <div class="checkpoint-body">{cp_180}</div>
        </div>
        <div class="checkpoint">
          <div class="checkpoint-title">MONATLICH</div>
          <div class="checkpoint-body">{cp_mon}</div>
        </div>
      </div>
    </div>

    <!-- RATIONALE -->
    <div class="section">
      <div class="section-title">ANALYSEBEGRÜNDUNG</div>
      <div class="rationale-block">
        <div class="block-label">THESIS</div>
        {rationale}
      </div>
    </div>

    <!-- GEGEN-SZENARIO -->
    <div class="section">
      <div class="section-title">GEGEN-SZENARIO</div>
      <div class="against-block">
        <div class="block-label">TRADE UNGÜLTIG WENN</div>
        {gegen}
        {f'<ul class="gegenthesen-list" style="margin-top:8px">{gegenthesen_html}</ul>' if gegenthesen else ''}
      </div>
    </div>

    <!-- LIQUIDITY + TAGS -->
    <div class="section">
      <div class="section-title">LIQUIDITY FLAGS</div>
      <div class="tags-row">{liq_html}</div>
    </div>

    <div class="tags-row">{tags_html}</div>
    <div class="portfolio-row">
      PORTFOLIO: {portfolio_r}
    </div>

  </div><!-- /body -->

  <div class="footer">
    <span>SA SCANNER v4.0</span>
    <span>{ticker} · {analyzed}</span>
    <span>CONVICTION {conviction:.2f} · {gate}</span>
  </div>

</div><!-- /card -->

</body>
</html>"""

    return html


def generate_all_cards(cards: list) -> int:
    Config.ensure_dirs()
    generated = 0

    for card in cards:
        ticker = card.get("ticker", "UNKNOWN")
        gate   = card.get("conviction_gate", "NO_SIGNAL")

        if gate not in ("PASS", "WATCHLIST"):
            continue

        try:
            html  = generate_card_html(card)
            date  = datetime.utcnow().strftime("%Y%m%d")
            fname = f"{ticker}_{date}_{gate}.html"
            path  = Config.DASH_DIR / "cards" / fname

            path.write_text(html, encoding="utf-8")

            # JSON auch speichern
            json_path = Config.CARDS_DIR / f"{ticker}_{date}.json"
            json_path.write_text(json.dumps(card, indent=2, default=str))

            generated += 1
            logger.info(f"Card generated: {fname}")

        except Exception as e:
            logger.error(f"Card generation error {ticker}: {e}")

    return generated
