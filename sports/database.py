"""Sports SQLite database — schema, batch inserts, query helpers."""

import aiosqlite

from sports.config import SPORTS_DB_PATH

DDL = """
-- Polymarket sports markets
CREATE TABLE IF NOT EXISTS sports_markets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL UNIQUE,
    sport TEXT NOT NULL,
    question TEXT,
    team_a TEXT,
    team_b TEXT,
    game_date TEXT,
    game_start_time INTEGER,
    token_a_id TEXT,
    token_b_id TEXT,
    winner TEXT,
    outcome TEXT
);

-- Price history from Polymarket (15-min granularity)
CREATE TABLE IF NOT EXISTS sports_price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    team_a_price REAL,
    team_b_price REAL,
    source TEXT NOT NULL DEFAULT 'clob',
    UNIQUE(condition_id, timestamp)
);

-- Odds API historical snapshots
CREATE TABLE IF NOT EXISTS odds_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    odds_event_id TEXT NOT NULL,
    sport TEXT NOT NULL,
    home_team TEXT,
    away_team TEXT,
    commence_time INTEGER,
    snapshot_ts INTEGER NOT NULL,
    home_odds REAL,
    away_odds REAL,
    draw_odds REAL,
    bookmaker TEXT NOT NULL,
    UNIQUE(odds_event_id, snapshot_ts, bookmaker)
);

-- Cross-reference between Polymarket and Odds API events
CREATE TABLE IF NOT EXISTS matched_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL UNIQUE,
    odds_event_id TEXT NOT NULL,
    poly_team_a_is_home INTEGER,
    match_score REAL
);

-- Credit usage tracking
CREATE TABLE IF NOT EXISTS credits_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    sport TEXT NOT NULL,
    sport_key TEXT,
    date_str TEXT,
    credits_used INTEGER NOT NULL,
    UNIQUE(sport_key, date_str)
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_sm_sport ON sports_markets(sport);
CREATE INDEX IF NOT EXISTS idx_sm_game_date ON sports_markets(game_date);
CREATE INDEX IF NOT EXISTS idx_sph_cond_ts ON sports_price_history(condition_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_os_event ON odds_snapshots(odds_event_id);
CREATE INDEX IF NOT EXISTS idx_os_sport_ts ON odds_snapshots(sport, snapshot_ts);
CREATE INDEX IF NOT EXISTS idx_os_commence ON odds_snapshots(commence_time);
CREATE INDEX IF NOT EXISTS idx_me_odds ON matched_events(odds_event_id);
"""


class SportsDatabase:
    def __init__(self, path: str = SPORTS_DB_PATH):
        self.path = path
        self._db: aiosqlite.Connection | None = None

    async def connect(self):
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._init_schema()

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _init_schema(self):
        await self._db.executescript(DDL)
        await self._db.executescript(INDEXES)
        await self._db.commit()

    # ---- Batch inserts ----

    async def insert_sports_markets(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO sports_markets
               (condition_id, sport, question, team_a, team_b, game_date,
                game_start_time, token_a_id, token_b_id, winner, outcome)
               VALUES (:condition_id, :sport, :question, :team_a, :team_b,
                       :game_date, :game_start_time, :token_a_id, :token_b_id,
                       :winner, :outcome)""",
            rows,
        )
        await self._db.commit()

    async def insert_sports_prices(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO sports_price_history
               (condition_id, timestamp, team_a_price, team_b_price, source)
               VALUES (:condition_id, :timestamp, :team_a_price, :team_b_price,
                       :source)""",
            rows,
        )
        await self._db.commit()

    async def insert_odds_snapshots(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO odds_snapshots
               (odds_event_id, sport, home_team, away_team, commence_time,
                snapshot_ts, home_odds, away_odds, draw_odds, bookmaker)
               VALUES (:odds_event_id, :sport, :home_team, :away_team,
                       :commence_time, :snapshot_ts, :home_odds, :away_odds,
                       :draw_odds, :bookmaker)""",
            rows,
        )
        await self._db.commit()

    async def insert_matched_events(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO matched_events
               (condition_id, odds_event_id, poly_team_a_is_home, match_score)
               VALUES (:condition_id, :odds_event_id, :poly_team_a_is_home,
                       :match_score)""",
            rows,
        )
        await self._db.commit()

    async def log_credits(
        self, sport: str, credits_used: int, timestamp: int,
        *, sport_key: str | None = None, date_str: str | None = None,
    ):
        await self._db.execute(
            """INSERT OR IGNORE INTO credits_log
               (timestamp, sport, sport_key, date_str, credits_used)
               VALUES (?, ?, ?, ?, ?)""",
            (timestamp, sport, sport_key, date_str, credits_used),
        )
        await self._db.commit()

    # ---- Query helpers ----

    async def get_sports_markets(self, sport: str | None = None) -> list[dict]:
        if sport:
            cur = await self._db.execute(
                "SELECT * FROM sports_markets WHERE sport = ? ORDER BY game_date",
                (sport,),
            )
        else:
            cur = await self._db.execute(
                "SELECT * FROM sports_markets ORDER BY sport, game_date"
            )
        return [dict(r) for r in await cur.fetchall()]

    async def get_markets_missing_prices(self, sport: str | None = None) -> list[dict]:
        query = """
            SELECT * FROM sports_markets
            WHERE condition_id NOT IN (
                SELECT DISTINCT condition_id FROM sports_price_history
            )
            AND token_a_id IS NOT NULL AND token_a_id != ''
        """
        params = ()
        if sport:
            query += " AND sport = ?"
            params = (sport,)
        query += " ORDER BY game_date"
        cur = await self._db.execute(query, params)
        return [dict(r) for r in await cur.fetchall()]

    async def get_all_markets_with_prices(self) -> list[dict]:
        """Markets that have at least one price row."""
        cur = await self._db.execute(
            """SELECT * FROM sports_markets
               WHERE condition_id IN (
                   SELECT DISTINCT condition_id FROM sports_price_history
               )
               ORDER BY sport, game_date"""
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_price_history(self, condition_id: str) -> list[dict]:
        cur = await self._db.execute(
            """SELECT timestamp, team_a_price, team_b_price, source
               FROM sports_price_history
               WHERE condition_id = ?
               ORDER BY timestamp""",
            (condition_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_odds_for_event(self, odds_event_id: str) -> list[dict]:
        cur = await self._db.execute(
            """SELECT snapshot_ts, home_odds, away_odds, draw_odds, bookmaker
               FROM odds_snapshots
               WHERE odds_event_id = ?
               ORDER BY snapshot_ts""",
            (odds_event_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_odds_event_ids_for_sport(self, sport: str) -> set[str]:
        cur = await self._db.execute(
            "SELECT DISTINCT odds_event_id FROM odds_snapshots WHERE sport = ?",
            (sport,),
        )
        return {row[0] for row in await cur.fetchall()}

    async def get_matched_events(self) -> list[dict]:
        cur = await self._db.execute(
            """SELECT me.condition_id, me.odds_event_id, me.poly_team_a_is_home,
                      me.match_score, sm.sport, sm.question, sm.team_a, sm.team_b,
                      sm.game_date
               FROM matched_events me
               JOIN sports_markets sm ON me.condition_id = sm.condition_id
               ORDER BY sm.sport, sm.game_date"""
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_total_credits_used(self) -> int:
        cur = await self._db.execute("SELECT SUM(credits_used) FROM credits_log")
        row = await cur.fetchone()
        return row[0] or 0

    async def get_completed_odds_queries(self, sport: str) -> set[tuple[str, str]]:
        """Return (sport_key, date_str) pairs already fetched for a sport."""
        cur = await self._db.execute(
            """SELECT sport_key, date_str FROM credits_log
               WHERE sport = ? AND sport_key IS NOT NULL AND date_str IS NOT NULL""",
            (sport,),
        )
        return {(row[0], row[1]) for row in await cur.fetchall()}

    async def get_table_counts(self) -> dict[str, int]:
        tables = [
            "sports_markets", "sports_price_history",
            "odds_snapshots", "matched_events", "credits_log",
        ]
        counts = {}
        for t in tables:
            cur = await self._db.execute(f"SELECT COUNT(*) FROM {t}")
            row = await cur.fetchone()
            counts[t] = row[0]
        return counts

    async def count_matched_events(self) -> int:
        cur = await self._db.execute("SELECT COUNT(*) FROM matched_events")
        row = await cur.fetchone()
        return row[0] or 0

    async def get_unique_game_dates_for_sport(self, sport: str) -> list[str]:
        """Get distinct game dates that have markets for a sport."""
        cur = await self._db.execute(
            """SELECT DISTINCT game_date FROM sports_markets
               WHERE sport = ? AND game_date IS NOT NULL
               ORDER BY game_date""",
            (sport,),
        )
        return [row[0] for row in await cur.fetchall()]

    async def get_odds_snapshots_for_matching(self, sport: str) -> list[dict]:
        """Get distinct events from odds_snapshots for matching."""
        cur = await self._db.execute(
            """SELECT DISTINCT odds_event_id, home_team, away_team, commence_time
               FROM odds_snapshots
               WHERE sport = ?""",
            (sport,),
        )
        return [dict(r) for r in await cur.fetchall()]
