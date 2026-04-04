"""
scanner/utils/ticker_mapper.py
CIK → Ticker → Firmenname → Sektor Mapping.
"""

CIK_TO_TICKER = {
    "0001045810": "NVDA",
    "0001652044": "GOOGL",
    "0000789019": "MSFT",
    "0001018724": "AMZN",
    "0001326801": "META",
    "0001418819": "PLTR",
}

NAME_TO_TICKER = {
    "Palantir":             "PLTR",
    "Vistra":               "VST",
    "Vistra Energy":        "VST",
    "Constellation Energy": "CEG",
    "NRG Energy":           "NRG",
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
    "Lockheed":             "LMT",
    "Raytheon":             "RTX",
    "Anduril":              None,
    "OpenAI":               None,
    "Anthropic":            None,
    "xAI":                  None,
    "Founders Fund":        None,
    "Thiel Capital":        None,
}

TICKER_TO_SECTOR = {
    "VST":  "energy_infrastructure",
    "CEG":  "energy_infrastructure",
    "NRG":  "energy_infrastructure",
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
        found = []
        for name, ticker in NAME_TO_TICKER.items():
            if ticker and name.lower() in text.lower():
                found.append(ticker)
        # Direkte $TICKER Erkennung
        import re
        direct = re.findall(r'\$([A-Z]{2,5})\b', text)
        found.extend(direct)
        return list(set(found))
