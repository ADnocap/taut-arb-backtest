"""Cross-reference Polymarket sports markets with Odds API events."""

import logging
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher

from sports.config import SPORTS, TEAM_ALIASES
from sports.database import SportsDatabase

log = logging.getLogger(__name__)

MIN_MATCH_SCORE = 0.75

# Soccer question patterns — Polymarket structures soccer as binary Yes/No markets
_RE_SOCCER_WIN = re.compile(r"^Will (.+?) win on \d{4}-\d{2}-\d{2}\?$")
_RE_SOCCER_DRAW = re.compile(r"^Will (.+?) vs\. (.+?) end in a draw\?$")
DATE_TOLERANCE_DAYS = 1
DATE_TOLERANCE_SECS = DATE_TOLERANCE_DAYS * 86400


def normalize_team(name: str, sport: str) -> str:
    """Normalize a team name using sport-specific aliases."""
    if not name:
        return ""
    lower = name.strip().lower()
    aliases = TEAM_ALIASES.get(sport, {})
    # Check direct alias match
    if lower in aliases:
        return aliases[lower].lower()
    # Check if any alias key is contained in the name
    for alias_key, canonical in aliases.items():
        if alias_key in lower:
            return canonical.lower()
    return lower


def _name_similarity(a: str, b: str) -> float:
    """Fuzzy string similarity between two team names."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _lastname_match(poly_name: str, odds_name: str) -> float:
    """Player-name matching (Tennis/individual sports): match on last names.

    "Djokovic" should match "Novak Djokovic".
    """
    poly_parts = poly_name.strip().lower().split()
    odds_parts = odds_name.strip().lower().split()

    if not poly_parts or not odds_parts:
        return 0.0

    poly_last = poly_parts[-1]
    odds_last = odds_parts[-1]

    if poly_last == odds_last:
        return 1.0

    # Check if poly name is a substring of odds name or vice versa
    if poly_name.lower().strip() in odds_name.lower():
        return 0.95
    if odds_name.lower().strip() in poly_name.lower():
        return 0.95

    return _name_similarity(poly_name, odds_name)


def _match_teams(
    poly_a: str,
    poly_b: str,
    odds_home: str,
    odds_away: str,
    sport: str,
) -> tuple[float, bool] | None:
    """Try to match Polymarket teams to Odds API home/away.

    Returns (score, poly_a_is_home) or None if no match.
    """
    # Normalize names
    pa = normalize_team(poly_a, sport)
    pb = normalize_team(poly_b, sport)
    oh = normalize_team(odds_home, sport)
    oa = normalize_team(odds_away, sport)

    if sport == "Tennis":
        # Tennis: match players by last name
        # Try mapping: poly_a=home, poly_b=away
        s1_home = _lastname_match(poly_a, odds_home)
        s1_away = _lastname_match(poly_b, odds_away)
        score_1 = (s1_home + s1_away) / 2

        # Try mapping: poly_a=away, poly_b=home
        s2_home = _lastname_match(poly_b, odds_home)
        s2_away = _lastname_match(poly_a, odds_away)
        score_2 = (s2_home + s2_away) / 2

        if score_1 >= score_2 and score_1 >= MIN_MATCH_SCORE:
            return (score_1, True)  # poly_a is home
        elif score_2 >= MIN_MATCH_SCORE:
            return (score_2, False)  # poly_a is away
        return None

    # General team sports: normalize then fuzzy match
    # Try mapping: poly_a=home, poly_b=away
    if pa and oh:
        s1_home = 1.0 if pa == oh else _name_similarity(pa, oh)
    else:
        s1_home = 0.0
    if pb and oa:
        s1_away = 1.0 if pb == oa else _name_similarity(pb, oa)
    else:
        s1_away = 0.0
    score_1 = (s1_home + s1_away) / 2

    # Try mapping: poly_a=away, poly_b=home
    if pa and oa:
        s2_away = 1.0 if pa == oa else _name_similarity(pa, oa)
    else:
        s2_away = 0.0
    if pb and oh:
        s2_home = 1.0 if pb == oh else _name_similarity(pb, oh)
    else:
        s2_home = 0.0
    score_2 = (s2_away + s2_home) / 2

    if score_1 >= score_2 and score_1 >= MIN_MATCH_SCORE:
        return (score_1, True)
    elif score_2 >= MIN_MATCH_SCORE:
        return (score_2, False)
    return None


def _dates_close(
    poly_date: str | None,
    poly_start_time: int | None,
    odds_commence: int | None,
) -> bool:
    """Check if dates are within tolerance."""
    if odds_commence is None:
        return False

    # Use game_start_time if available
    if poly_start_time:
        return abs(poly_start_time - odds_commence) <= DATE_TOLERANCE_SECS

    # Fall back to game_date
    if poly_date:
        try:
            poly_ts = int(
                datetime.strptime(poly_date, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
            # Compare date midpoints (add 12h to poly date for midday)
            return abs((poly_ts + 43200) - odds_commence) <= DATE_TOLERANCE_SECS
        except ValueError:
            pass

    return False


def _parse_soccer_question(question: str):
    """Parse a Soccer market question to extract real team names.

    Returns:
        ("win", team_name) for win markets
        ("draw", team_a, team_b) for draw markets
        None if the question doesn't match known patterns
    """
    m = _RE_SOCCER_WIN.match(question)
    if m:
        return ("win", m.group(1))
    m = _RE_SOCCER_DRAW.match(question)
    if m:
        return ("draw", m.group(1), m.group(2))
    return None


def _match_soccer_team(
    team: str, odds_home: str, odds_away: str, sport: str,
) -> tuple[float, bool] | None:
    """Match a single Soccer team name against odds home/away.

    Returns (score, team_is_home) or None.
    """
    nt = normalize_team(team, sport)
    oh = normalize_team(odds_home, sport)
    oa = normalize_team(odds_away, sport)

    s_home = 1.0 if nt == oh else _name_similarity(nt, oh)
    s_away = 1.0 if nt == oa else _name_similarity(nt, oa)

    if s_home >= s_away and s_home >= MIN_MATCH_SCORE:
        return (s_home, True)
    elif s_away >= MIN_MATCH_SCORE:
        return (s_away, False)
    return None


async def match_events(
    db: SportsDatabase, sports: list[str],
    *, progress=None, task_id=None,
) -> dict:
    """Match Polymarket markets to Odds API events.

    Returns stats: {sport: {matched, unmatched, avg_score}}.
    """
    stats = {}

    for sport_name in sports:
        markets = await db.get_sports_markets(sport_name)
        odds_events = await db.get_odds_snapshots_for_matching(sport_name)

        if not markets or not odds_events:
            log.info("%s: no data to match (markets=%d, odds_events=%d)",
                     sport_name, len(markets), len(odds_events))
            stats[sport_name] = {"matched": 0, "unmatched": len(markets), "avg_score": 0}
            continue

        matched_rows = []
        matched_count = 0
        total_score = 0.0

        for mkt in markets:
            best_match = None
            best_score = 0.0
            best_a_is_home = True

            # Soccer: parse question to extract real team names
            soccer_parsed = None
            if sport_name == "Soccer":
                soccer_parsed = _parse_soccer_question(mkt.get("question", ""))
                if soccer_parsed is None:
                    log.debug("Soccer: unparseable question: %s", mkt.get("question"))

            for odds_ev in odds_events:
                # Date filter
                if not _dates_close(
                    mkt.get("game_date"),
                    mkt.get("game_start_time"),
                    odds_ev.get("commence_time"),
                ):
                    continue

                if soccer_parsed:
                    # Soccer win market: match single team
                    if soccer_parsed[0] == "win":
                        result = _match_soccer_team(
                            soccer_parsed[1],
                            odds_ev.get("home_team", ""),
                            odds_ev.get("away_team", ""),
                            sport_name,
                        )
                    else:
                        # Soccer draw market: match both teams
                        result = _match_teams(
                            soccer_parsed[1],
                            soccer_parsed[2],
                            odds_ev.get("home_team", ""),
                            odds_ev.get("away_team", ""),
                            sport_name,
                        )
                else:
                    # Standard team matching
                    result = _match_teams(
                        mkt.get("team_a", ""),
                        mkt.get("team_b", ""),
                        odds_ev.get("home_team", ""),
                        odds_ev.get("away_team", ""),
                        sport_name,
                    )

                if result and result[0] > best_score:
                    best_score = result[0]
                    best_a_is_home = result[1]
                    best_match = odds_ev

            if best_match and best_score >= MIN_MATCH_SCORE:
                matched_rows.append({
                    "condition_id": mkt["condition_id"],
                    "odds_event_id": best_match["odds_event_id"],
                    "poly_team_a_is_home": 1 if best_a_is_home else 0,
                    "match_score": round(best_score, 4),
                })
                matched_count += 1
                total_score += best_score

            if progress is not None and task_id is not None:
                progress.update(task_id, advance=1)

        if matched_rows:
            await db.insert_matched_events(matched_rows)

        avg_score = round(total_score / matched_count, 4) if matched_count else 0
        unmatched = len(markets) - matched_count
        stats[sport_name] = {
            "matched": matched_count,
            "unmatched": unmatched,
            "avg_score": avg_score,
        }
        log.info(
            "%s: matched %d/%d (avg score %.3f), unmatched %d",
            sport_name, matched_count, len(markets), avg_score, unmatched,
        )

    return stats
