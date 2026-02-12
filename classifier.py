"""Market classification — European Digital vs Barrier One-Touch.

Pure functions, no I/O. Regex patterns copied from backtest_guide.md Section 2.
"""

import re
from datetime import datetime, timezone
from typing import Optional

from config import ASSETS, ASSET_KEYWORDS, BARRIER_KEYWORDS, EXCLUDE_TOPICS

# --- Number parsing ---

def _parse_number(s: str) -> Optional[float]:
    s = s.replace(",", "").strip()
    multiplier = 1
    if s.endswith("k") or s.endswith("K"):
        multiplier = 1000
        s = s[:-1]
    elif s.endswith("m") or s.endswith("M"):
        multiplier = 1000000
        s = s[:-1]
    try:
        return float(s) * multiplier
    except ValueError:
        return None


def _validate_threshold(asset: str, threshold: float) -> bool:
    cfg = ASSETS.get(asset)
    if not cfg:
        return False
    return cfg.threshold_min <= threshold <= cfg.threshold_max


# --- Asset detection ---

def _detect_asset(question: str) -> Optional[str]:
    q_lower = question.lower()
    for asset, keywords in ASSET_KEYWORDS.items():
        for kw in keywords:
            if re.search(rf"\b{re.escape(kw)}\b", q_lower):
                return asset
    return None


def _has_excluded_topic(question: str) -> bool:
    q_lower = question.lower()
    return any(topic in q_lower for topic in EXCLUDE_TOPICS)


# --- European Digital patterns ---

_ABOVE_PATTERNS = [
    r"(?:be\s+)?(?:at\s+or\s+)?above\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"(?:over|hit|reach|exceed|surpass)\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"(?:greater|more|higher)\s+than\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"\$?([\d,]+(?:\.\d+)?[kKmM]?)\s*(?:or\s+)?(?:higher|more|above)",
    r"at\s+least\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"(?:price|trading|trade)\s+above\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
]

_BELOW_PATTERNS = [
    r"(?:be\s+)?(?:at\s+or\s+)?below\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"(?:under|drop\s+to|fall\s+to|fall\s+below|drop\s+below)\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"(?:less|lower)\s+than\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"\$?([\d,]+(?:\.\d+)?[kKmM]?)\s*(?:or\s+)?(?:lower|less|below)",
    r"at\s+most\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"(?:price|trading|trade)\s+below\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
]

_BETWEEN_PATTERNS = [
    r"between\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)\s*(?:and|to|-)\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"be\s+between\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)\s*(?:and|to|-)\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"\$?([\d,]+(?:\.\d+)?[kKmM]?)\s*[-\u2013]\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"(?:in\s+the\s+)?range\s+(?:of\s+)?\$?([\d,]+(?:\.\d+)?[kKmM]?)\s*(?:to|-)\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
]

# --- Barrier One-Touch patterns ---

_REACH_PATTERNS = [
    r"reach\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"hit\s+\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"touch\s+\$?([\d,]+(?:\.\d+)?[kKmM]?)",
]

_DIP_PATTERNS = [
    r"dip\s+to\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"drop\s+to\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"fall\s+to\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
    r"dip\s+below\s*\$?([\d,]+(?:\.\d+)?[kKmM]?)",
]


def _try_match_patterns(question: str, patterns: list[str]) -> Optional[float]:
    """Return the first matched number from patterns, or None."""
    for pat in patterns:
        m = re.search(pat, question, re.IGNORECASE)
        if m:
            val = _parse_number(m.group(1))
            if val is not None:
                return val
    return None


def _try_match_between(question: str) -> Optional[tuple[float, float]]:
    for pat in _BETWEEN_PATTERNS:
        m = re.search(pat, question, re.IGNORECASE)
        if m:
            lo = _parse_number(m.group(1))
            hi = _parse_number(m.group(2))
            if lo is not None and hi is not None:
                return (min(lo, hi), max(lo, hi))
    return None


# --- Settlement date parsing ---

_DATE_FIELDS = [
    "endDate", "end_date_iso", "endDateIso",
    "resolutionDate", "resolution_date",
    "closeTime", "close_time",
]

_QUESTION_DATE_RE = re.compile(
    r"on\s+(\w+\s+\d{1,2}(?:,?\s+\d{4})?)", re.IGNORECASE
)


def parse_settlement_date(market_data: dict) -> Optional[str]:
    """Extract settlement date as ISO string from market data dict."""
    for field in _DATE_FIELDS:
        val = market_data.get(field)
        if val is None:
            continue
        dt = _parse_date_value(val)
        if dt:
            return dt.isoformat()

    # Fallback: parse from question text
    question = market_data.get("question", "")
    m = _QUESTION_DATE_RE.search(question)
    if m:
        try:
            dt = _parse_date_string(m.group(1))
            if dt:
                return dt.isoformat()
        except Exception:
            pass
    return None


def _parse_date_value(val) -> Optional[datetime]:
    if isinstance(val, (int, float)):
        ts = val / 1000 if val > 1e12 else val
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(val, str):
        # Try numeric string
        try:
            num = float(val)
            ts = num / 1000 if num > 1e12 else num
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except ValueError:
            pass
        # Try ISO string
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            pass
    return None


def _parse_date_string(s: str) -> Optional[datetime]:
    """Parse 'Mar 29' or 'April 5, 2024' style dates."""
    from datetime import datetime as dt
    for fmt in ("%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y", "%B %d", "%b %d"):
        try:
            parsed = dt.strptime(s.strip(), fmt)
            if parsed.year == 1900:
                parsed = parsed.replace(year=datetime.now(timezone.utc).year)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# --- Outcome determination ---

def determine_outcome(market_data: dict) -> Optional[int]:
    """Return 1 (YES won), 0 (NO won), or None (unsettled)."""
    # CLOB: winner field on tokens
    tokens = market_data.get("tokens", [])
    for token in tokens:
        if token.get("winner") is True:
            if token.get("outcome") == "Yes":
                return 1
            elif token.get("outcome") == "No":
                return 0

    # Gamma: resolvedTo field
    resolved = market_data.get("resolvedTo")
    if resolved == "Yes":
        return 1
    elif resolved == "No":
        return 0

    return None


# --- Main classification entry point ---

def classify_market(
    question: str, target_assets: list[str] | None = None
) -> Optional[dict]:
    """Classify a market question.

    Returns dict with keys: asset, direction, threshold, upper_threshold
    or None if not classifiable.
    """
    if _has_excluded_topic(question):
        return None

    asset = _detect_asset(question)
    if not asset:
        return None
    if target_assets and asset not in target_assets:
        return None

    q_lower = question.lower()

    # Pre-filter: must have 'price' or a barrier keyword
    has_price = "price" in q_lower
    has_barrier = any(kw in q_lower for kw in BARRIER_KEYWORDS)
    if not has_price and not has_barrier:
        return None

    # European Digital (when 'price' is in question)
    if has_price:
        # Between first (more specific)
        between = _try_match_between(question)
        if between:
            lo, hi = between
            if _validate_threshold(asset, lo) and _validate_threshold(asset, hi):
                return {
                    "asset": asset,
                    "direction": "between",
                    "threshold": lo,
                    "upper_threshold": hi,
                }

        # Above
        val = _try_match_patterns(question, _ABOVE_PATTERNS)
        if val and _validate_threshold(asset, val):
            return {
                "asset": asset,
                "direction": "above",
                "threshold": val,
                "upper_threshold": None,
            }

        # Below
        val = _try_match_patterns(question, _BELOW_PATTERNS)
        if val and _validate_threshold(asset, val):
            return {
                "asset": asset,
                "direction": "below",
                "threshold": val,
                "upper_threshold": None,
            }

    # Barrier One-Touch (only when 'price' is absent)
    if not has_price:
        val = _try_match_patterns(question, _REACH_PATTERNS)
        if val and _validate_threshold(asset, val):
            return {
                "asset": asset,
                "direction": "up_barrier",
                "threshold": val,
                "upper_threshold": None,
            }

        val = _try_match_patterns(question, _DIP_PATTERNS)
        if val and _validate_threshold(asset, val):
            return {
                "asset": asset,
                "direction": "down_barrier",
                "threshold": val,
                "upper_threshold": None,
            }

    return None
