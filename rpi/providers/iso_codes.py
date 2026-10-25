"""
ISO code conversion for providers that use 3-letter codes.

RPI stores ISO 3166-1 alpha-2 countries and ISO 639-1 languages. CreatorDB
returns and filters on ISO 3166-1 alpha-3 and ISO 639-3. Codes missing from
these tables are passed through unchanged on output and reported as
unapplied on input, never guessed.
"""

from __future__ import annotations

from typing import Optional

COUNTRY_A3_TO_A2 = {
    "USA": "US", "CAN": "CA", "GBR": "GB", "AUS": "AU", "NZL": "NZ", "IRL": "IE", "IND": "IN", "PHL": "PH",
    "SGP": "SG", "MYS": "MY", "IDN": "ID", "THA": "TH", "VNM": "VN", "JPN": "JP", "KOR": "KR", "CHN": "CN",
    "TWN": "TW", "HKG": "HK", "PAK": "PK", "BGD": "BD", "LKA": "LK", "NPL": "NP", "ARE": "AE", "SAU": "SA",
    "QAT": "QA", "KWT": "KW", "ISR": "IL", "TUR": "TR", "EGY": "EG", "NGA": "NG", "KEN": "KE", "ZAF": "ZA",
    "GHA": "GH", "MAR": "MA", "DEU": "DE", "FRA": "FR", "ESP": "ES", "ITA": "IT", "PRT": "PT", "NLD": "NL",
    "BEL": "BE", "LUX": "LU", "CHE": "CH", "AUT": "AT", "SWE": "SE", "NOR": "NO", "DNK": "DK", "FIN": "FI",
    "ISL": "IS", "POL": "PL", "CZE": "CZ", "SVK": "SK", "ROU": "RO", "BGR": "BG", "HUN": "HU", "GRC": "GR",
    "HRV": "HR", "SRB": "RS", "UKR": "UA", "RUS": "RU", "MEX": "MX", "BRA": "BR", "ARG": "AR", "COL": "CO",
    "CHL": "CL", "PER": "PE", "VEN": "VE", "ECU": "EC", "JAM": "JM", "TTO": "TT", "PRI": "PR", "DOM": "DO",
}
COUNTRY_A2_TO_A3 = {v: k for k, v in COUNTRY_A3_TO_A2.items()}

LANGUAGE_639_3_TO_1 = {
    "eng": "en", "spa": "es", "fra": "fr", "deu": "de", "por": "pt", "ita": "it", "nld": "nl", "rus": "ru",
    "jpn": "ja", "kor": "ko", "zho": "zh", "cmn": "zh", "hin": "hi", "ara": "ar", "tur": "tr", "ind": "id",
    "msa": "ms", "zsm": "ms", "vie": "vi", "tha": "th", "pol": "pl", "ukr": "uk", "swe": "sv", "nor": "no",
    "nob": "nb", "dan": "da", "fin": "fi", "tgl": "tl", "fil": "tl", "ben": "bn", "urd": "ur", "tam": "ta",
    "tel": "te", "mar": "mr", "heb": "he", "ell": "el", "ces": "cs", "ron": "ro", "hun": "hu", "fas": "fa",
    "pes": "fa", "swa": "sw",
}
# One canonical 639-3 code per 639-1 language for search filters.
LANGUAGE_639_1_TO_3 = {
    "en": "eng", "es": "spa", "fr": "fra", "de": "deu", "pt": "por", "it": "ita", "nl": "nld", "ru": "rus",
    "ja": "jpn", "ko": "kor", "zh": "zho", "hi": "hin", "ar": "ara", "tr": "tur", "id": "ind", "ms": "msa",
    "vi": "vie", "th": "tha", "pl": "pol", "uk": "ukr", "sv": "swe", "no": "nor", "nb": "nob", "da": "dan",
    "fi": "fin", "tl": "tgl", "bn": "ben", "ur": "urd", "ta": "tam", "te": "tel", "mr": "mar", "he": "heb",
    "el": "ell", "cs": "ces", "ro": "ron", "hu": "hun", "fa": "fas", "sw": "swa",
}


def country_to_a2(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    code = code.strip().upper()
    return COUNTRY_A3_TO_A2.get(code, code) if len(code) == 3 else code or None


def country_to_a3(code: str) -> Optional[str]:
    code = code.strip().upper()
    return code if code in COUNTRY_A3_TO_A2 else COUNTRY_A2_TO_A3.get(code)


def language_to_639_1(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    code = code.strip().lower()
    return LANGUAGE_639_3_TO_1.get(code, code) if len(code) == 3 else code or None


def language_to_639_3(code: str) -> Optional[str]:
    code = code.strip().lower()
    return code if code in LANGUAGE_639_3_TO_1 else LANGUAGE_639_1_TO_3.get(code)
