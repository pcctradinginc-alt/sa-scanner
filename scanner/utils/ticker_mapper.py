"""
scanner/utils/ticker_mapper.py

FIXES v2:
    R8: Erweiterte Synonym-Liste für RSS-Ticker-Extraktion.
        Artikel die "GPU-Hersteller", "KI-Chip" oder "Energieversorger"
        schreiben ohne Firmennamen werden jetzt korrekt gemappt.
        Direkte $TICKER Erkennung + Firmennamen + Synonyme.
"""

import re

CIK_TO_TICKER = {
    "0001045810": "NVDA",
    "0001652044": "GOOGL",
    "0000789019": "MSFT",
    "0001018724": "AMZN",
    "0001326801": "META",
    "0001418819": "PLTR",
}

NAME_TO_TICKER = {
    # Energie-Infrastruktur
    "Palantir":             "PLTR",
    "Vistra":               "VST",
    "Vistra Energy":        "VST",
    "Constellation Energy": "CEG",
    "Constellation":        "CEG",
    "NRG Energy":           "NRG",
    "NRG":                  "NRG",
    "Talen Energy":         "TLN",
    # Compute Hardware
    "Nvidia":               "NVDA",
    "NVIDIA":               "NVDA",
    "Microsoft":            "MSFT",
    "Alphabet":             "GOOGL",
    "Google":               "GOOGL",
    "Amazon":               "AMZN",
    "Meta":                 "META",
    "Broadcom":             "AVGO",
    "Taiwan Semiconductor": "TSM",
    "TSMC":                 "TSM",
    # Defense
    "Lockheed":             "LMT",
    "Lockheed Martin":      "LMT",
    "Raytheon":             "RTX",
    "RTX":                  "RTX",
    # Private (kein Ticker)
    "Anduril":              None,
    "OpenAI":               None,
    "Anthropic":            None,
    "xAI":                  None,
    "Founders Fund":        None,
    "Thiel Capital":        None,
}

# R8 FIX: Synonyme und generische Beschreibungen
# Deutsch + Englisch — häufig in Finanzmedien
SYNONYM_TO_TICKER = {
    # NVDA Synonyme
    "gpu-hersteller":           "NVDA",
    "ki-chip":                  "NVDA",
    "ai chip":                  "NVDA",
    "gpu maker":                "NVDA",
    "chip giant":               "NVDA",
    "graphics chip":            "NVDA",
    "ki-prozessor":             "NVDA",
    "datacenter chip":          "NVDA",
    "data center chip":         "NVDA",
    "h100":                     "NVDA",
    "h200":                     "NVDA",
    "blackwell":                "NVDA",
    "hopper":                   "NVDA",
    # VST / CEG Synonyme
    "nuclear power plant":      "VST",
    "kernkraftwerk":            "VST",
    "atomkraftwerk":            "VST",
    "nuclear energy":           "CEG",
    "kernenergie":              "CEG",
    "zero-carbon power":        "CEG",
    "carbon-free energy":       "CEG",
    # PLTR Synonyme
    "palantir software":        "PLTR",
    "ai warfare":               "PLTR",
    "sovereign ai platform":    "PLTR",
    "government ai":            "PLTR",
    "defense ai":               "PLTR",
    # Energie allgemein → beide
    "hyperscaler power":        "VST",
    "data center energy":       "VST",
    "rechenzentrum energie":    "VST",
    "stromversorgung ki":       "VST",
    "energy infrastructure":    "VST",
    # Defense
    "autonomous weapons":       "LMT",
    "defense contractor":       "LMT",
    "pentagon contract":        "PLTR",
    "dod contract":             "PLTR",
}

TICKER_TO_SECTOR = {
    "VST":  "energy_infrastructure",
    "CEG":  "energy_infrastructure",
    "NRG":  "energy_infrastructure",
    "TLN":  "energy_infrastructure",
    "XEL":  "energy_infrastructure",
    "NEE":  "energy_infrastructure",
    "PLTR": "sovereign_ai_defense",
    "LMT":  "sovereign_ai_defense",
    "RTX":  "sovereign_ai_defense",
    "NVDA": "compute_hardware",
    "TSM":  "compute_hardware",
    "AVGO": "compute_hardware",
    "MSFT": "hyperscaler",
    "GOOGL":"hyperscaler",
    "AMZN": "hyperscaler",
    "META": "hyperscaler",
}


class TickerMapper:

    def cik_to_ticker(self, cik: str) -> str | None:
        return CIK_TO_TICKER.get(cik.zfill(10))

    def name_to_ticker(self, name: str) -> str | None:
        for key, ticker in NAME_TO_TICKER.items():
            if key.lower() in name.lower():
                return ticker
        return None

    def get_sector(self, ticker: str) -> str:
        return TICKER_TO_SECTOR.get(ticker, "unknown")

    def extract_tickers_from_text(self, text: str) -> list:
        """
        R8 FIX: Erweiterte Ticker-Extraktion mit Synonymen.
        Reihenfolge: $TICKER → Firmenname → Synonyme
        """
        found = set()
        text_lower = text.lower()

        # 1. Direkte $TICKER Erkennung
        direct = re.findall(r'\$([A-Z]{2,5})\b', text)
        found.update(direct)

        # 2. Firmennamen
        for name, ticker in NAME_TO_TICKER.items():
            if ticker and name.lower() in text_lower:
                found.add(ticker)

        # 3. R8 FIX: Synonyme und generische Beschreibungen
        for synonym, ticker in SYNONYM_TO_TICKER.items():
            if ticker and synonym.lower() in text_lower:
                found.add(ticker)

        # Ungültige Ticker-Symbole filtern
        blacklist = {
            "AI", "US", "EU", "UK", "SEC", "LLC", "INC",
            "CORP", "LTD", "LP", "ETF", "USA", "CEO", "CFO",
            "IPO", "ESG", "GDP", "CPI",
        }
        found -= blacklist

        return list(found)
