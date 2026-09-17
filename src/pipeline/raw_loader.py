#!/usr/bin/env python3
"""Scoped raw-table loading for the weekly pipeline.

Semantics (the product, per the plan):
  - Every delete is scoped by (league_id, week) composite key - the
    predecessor's unscoped ``DELETE WHERE week = N`` would have wiped
    week N across all 20 seasons on its first successful run.
  - The full delta for a run commits in ONE caller-owned transaction,
    together with the pipeline_periods flags describing it.
  - Idempotency is database-enforced: unique indexes back every upsert.
  - Matchups are flattened from the raw Yahoo blob at load time (ported
    from heroku_deployer) - raw blobs never reach the database, which is
    also what keeps join passwords and account GUIDs out of snapshots.
  - Playoff round flags (championship/semifinal/quarterfinal) are
    recomputed per league from the rows in the database after each load,
    with two-round brackets supported (2007's championship was unflagged
    in production because the old detector required three rounds).
"""
import logging
from typing import Dict, List, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Unique indexes that make idempotency a database guarantee. Created
# lazily (IF NOT EXISTS) so first run against an existing dev/prod
# schema adopts them; creation FAILS LOUDLY if existing data violates
# one, which is a data problem we want surfaced, not papered over.
CONSTRAINT_DDL = [
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_leagues_league_id ON public.leagues (league_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_teams_team_id ON public.teams (team_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_rosters_key ON public.rosters (league_id, team_id, week, player_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_matchups_matchup_id ON public.matchups (matchup_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_statistics_stat_id ON public.statistics (stat_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_key ON public.transactions (transaction_id, player_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_draft_picks_id ON public.draft_picks (draft_pick_id)",
]

LEAGUE_UPDATE_COLS = ['name', 'current_week', 'draft_status', 'extracted_at']
TEAM_UPDATE_COLS = ['name', 'manager_name', 'wins', 'losses', 'ties',
                    'points_for', 'points_against', 'playoff_seed',
                    'waiver_priority', 'faab_balance', 'extracted_at']


def ensure_constraints(conn):
    for ddl in CONSTRAINT_DDL:
        conn.execute(text(ddl))


# ---------------------------------------------------------------------------
# Matchup flattening (ported from heroku_deployer.flatten_matchups_data)
# ---------------------------------------------------------------------------

def _detect_playoff_game(matchup: dict) -> bool:
    if str(matchup.get('matchup_type', '')).lower() in ('playoffs', 'championship'):
        return True
    if 'is_playoffs' in matchup:
        return str(matchup.get('is_playoffs', '0')) == '1'
    return False


def _detect_consolation_game(matchup: dict) -> bool:
    if str(matchup.get('matchup_type', '')).lower() == 'consolation':
        return True
    if 'is_consolation' in matchup:
        return str(matchup.get('is_consolation', '0')) == '1'
    return False


def flatten_matchups(matchups_data: List[dict]) -> List[dict]:
    """Raw week-blobs -> flat matchup rows.

    Round flags (championship/semifinal/quarterfinal/last-place) are left
    False here; refresh_playoff_flags computes them from database context
    after the load, because they depend on the whole bracket, not one row.
    """
    flat = []
    for league_matchup in matchups_data:
        league_id = league_matchup.get('league_id')
        week = league_matchup.get('week')
        blob = league_matchup.get('matchups', {})
        if not blob or not league_id or not week:
            continue

        league_node = blob.get('fantasy_content', {}).get('league', [{}])
        if not (isinstance(league_node, list) and len(league_node) > 1):
            continue
        scoreboard = league_node[1].get('scoreboard', {})
        if '0' not in scoreboard or 'matchups' not in scoreboard['0']:
            continue

        for match_key, match_data in scoreboard['0']['matchups'].items():
            if match_key == 'count' or not isinstance(match_data, dict):
                continue
            matchup = match_data.get('matchup', {})
            if not matchup:
                continue

            record = {
                'matchup_id': f"{league_id}_W{week}_{match_key}",
                'league_id': league_id,
                'week': int(week),
                'is_playoffs': _detect_playoff_game(matchup),
                'is_championship': False,
                'is_semifinal': False,
                'is_quarterfinal': False,
                'is_last_place_game': False,
                'is_consolation': _detect_consolation_game(matchup),
                'winner_team_id': matchup.get('winner_team_key'),
                'team1_id': None,
                'team2_id': None,
                'team1_score': 0.0,
                'team2_score': 0.0,
                'extracted_at': league_matchup.get('extracted_at'),
            }

            teams_node = matchup.get('0', {}).get('teams', {})
            team_ids, team_scores = [], []
            for idx in ('0', '1'):
                team_info = teams_node.get(idx, {}).get('team', [])
                if isinstance(team_info, list) and len(team_info) >= 2:
                    for item in team_info[0]:
                        if isinstance(item, dict) and 'team_key' in item:
                            team_ids.append(item['team_key'])
                            break
                    points = team_info[1].get('team_points', {})
                    team_scores.append(float(points.get('total', '0') or 0))

            if len(team_ids) >= 2:
                record['team1_id'], record['team2_id'] = team_ids[0], team_ids[1]
            if len(team_scores) >= 2:
                record['team1_score'], record['team2_score'] = team_scores[0], team_scores[1]

            if record['team1_id'] and record['team2_id']:
                flat.append(record)
    return flat


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _insert_rows(conn, table: str, rows: List[dict], conflict_clause: str):
    if not rows:
        return 0
    cols = list(rows[0].keys())
    col_list = ', '.join(f'"{c}"' for c in cols)
    placeholders = ', '.join(f':{c}' for c in cols)
    stmt = text(f'INSERT INTO public.{table} ({col_list}) '
                f'VALUES ({placeholders}) {conflict_clause}')
    conn.execute(stmt, rows)
    return len(rows)


def upsert_dimension(conn, table: str, key_col: str, rows: List[dict],
                     update_cols: List[str]) -> int:
    if not rows:
        return 0
    sets = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols
                     if c in rows[0])
    clause = f'ON CONFLICT ({key_col}) DO UPDATE SET {sets}'
    return _insert_rows(conn, table, rows, clause)


def replace_period_rows(conn, table: str, week_col: str, league_id: str,
                        weeks: List[int], rows: List[dict]) -> Dict[str, int]:
    """Delete-then-insert for time-series tables, scoped by league AND week.

    The scoping is the point: this is the fix for the history-destroying
    unscoped week delete.

    Rows must fall inside `weeks`. The insert deliberately carries no
    ON CONFLICT clause -- it relies on the delete above having cleared
    exactly what it is about to write -- so a row outside the scope would
    either collide with a live row or slip in unreplaced. That is a caller
    bug, so it raises rather than corrupting the period.
    """
    if not weeks:
        return {'deleted': 0, 'inserted': 0}

    scope = set(weeks)
    stray = {r.get(week_col) for r in rows} - scope
    if stray:
        raise ValueError(
            f"{table}: rows for week(s) {sorted(stray, key=str)} outside the "
            f"replacement scope {sorted(scope)}")

    deleted = conn.execute(
        text(f'DELETE FROM public.{table} '
             f'WHERE league_id = :l AND "{week_col}" = ANY(:weeks)'),
        {'l': league_id, 'weeks': weeks}).rowcount
    inserted = _insert_rows(conn, table, rows, '')
    return {'deleted': deleted, 'inserted': inserted}


def append_only(conn, table: str, rows: List[dict], conflict_cols: str) -> int:
    return _insert_rows(conn, table, rows,
                        f'ON CONFLICT ({conflict_cols}) DO NOTHING')


# Time-series entities: raw-data key -> (table, week column).
PERIOD_ENTITIES = {
    'matchups': ('matchups', 'week'),
    'rosters': ('rosters', 'week'),
    'statistics': ('statistics', 'week_number'),
}


def load_delta(conn, state, league_id: str, season: int, weeks: List[int],
               data: Dict[str, List[dict]]) -> Dict[str, int]:
    """Load one league's delta for the given weeks on the caller's
    transaction, marking pipeline_periods raw-complete on the same
    transaction. Caller owns commit/rollback - all tables land together
    or none do.

    Only entities PRESENT IN `data` are touched. A key's absence means the
    run did not fetch it, and a delete-then-insert for data that was never
    fetched deletes the period and puts nothing back: --stats-only once
    erased a week's matchups (championship game included) and rosters this
    way. Absent entities are left alone and are NOT marked raw-complete,
    so gap detection still knows they are outstanding.

    An entity that IS present but returns no rows for a given week is
    likewise left alone for that week. Only weeks with incoming rows are
    replaced; an empty fetch never deletes.

    data keys: leagues, teams, rosters, matchups (RAW week-blobs),
    transactions, draft_picks, statistics.
    """
    counts: Dict[str, int] = {}

    if 'leagues' in data:
        counts['leagues'] = upsert_dimension(
            conn, 'leagues', 'league_id', data['leagues'], LEAGUE_UPDATE_COLS)
    if 'teams' in data:
        counts['teams'] = upsert_dimension(
            conn, 'teams', 'team_id', data['teams'], TEAM_UPDATE_COLS)

    # Rows per entity, so each period records its own counts rather than
    # the run's totals (and a week with genuinely no rows is visible).
    per_week: Dict[int, Dict[str, int]] = {w: {} for w in weeks}

    scope = set(weeks)
    for entity, (table, week_col) in PERIOD_ENTITIES.items():
        if entity not in data:
            continue
        rows = flatten_matchups(data[entity]) if entity == 'matchups' else data[entity]

        by_week: Dict[int, List[dict]] = {w: [] for w in weeks}
        stray = set()
        for row in rows:
            week = row.get(week_col)
            if week in scope:
                by_week[week].append(row)
            else:
                stray.add(week)
        if stray:
            # Same contract replace_period_rows enforces, applied before the
            # rows are split so the message names the entity.
            raise ValueError(
                f"{table}: rows for week(s) {sorted(stray, key=str)} outside "
                f"the replacement scope {sorted(scope)}")
        for week in weeks:
            per_week[week][entity] = len(by_week[week])

        # Replace only the weeks this fetch actually returned rows for. A
        # delete-then-insert over a week that came back empty deletes what we
        # already hold and puts nothing back - and because raw_*_complete is
        # only ever set, never cleared, the week goes on claiming to be
        # complete and published, so gap detection never asks for it again.
        # The rolling reload window re-fetches the two most recent complete
        # weeks on EVERY run, which makes this the common case rather than
        # the rare one: one empty response (a swallowed rate-limit denial, a
        # league whose taken_players momentarily returns nothing) would erase
        # two weeks of raw data permanently and invisibly. An empty fetch is
        # not evidence that the source has nothing, so leave what we hold and
        # let the next run reconcile.
        present = [w for w in weeks if by_week[w]]
        empty = [w for w in weeks if not by_week[w]]
        if empty:
            logger.warning(
                "%s: no rows returned for week(s) %s - leaving the rows "
                "already held in place (an empty fetch is not a deletion)",
                table, empty)
        result = replace_period_rows(
            conn, table, week_col, league_id, present,
            [row for w in present for row in by_week[w]])
        counts[entity] = result['inserted']

    if 'transactions' in data:
        counts['transactions'] = append_only(
            conn, 'transactions', data['transactions'],
            'transaction_id, player_id')
    if 'draft_picks' in data:
        counts['draft_picks'] = append_only(
            conn, 'draft_picks', data['draft_picks'], 'draft_pick_id')

    if 'matchups' in data:
        refresh_playoff_flags(conn, league_id)

    for week in weeks:
        # Only entities that actually landed rows are claimed complete. A
        # fetched-but-empty entity is not evidence of completeness: marking
        # it would retire the week from gap detection and make a transient
        # Yahoo gap permanent. Leaving it unclaimed costs one re-fetch.
        loaded = {e: n for e, n in per_week[week].items() if n}
        if loaded:
            state.mark_raw_complete(conn, league_id, season, week, loaded)

    logger.info("Loaded delta for %s weeks %s: %s", league_id, weeks, counts)
    return counts


# ---------------------------------------------------------------------------
# Playoff round flags (database-context bracket walk; 2-round supported)
# ---------------------------------------------------------------------------

def _winner(row) -> Optional[str]:
    team1_id, team2_id, winner, s1, s2 = row
    if winner:
        return winner
    if s1 is not None and s2 is not None and s1 != s2:
        return team1_id if s1 > s2 else team2_id
    return None


def refresh_playoff_flags(conn, league_id: str) -> Optional[str]:
    """Recompute championship/semifinal/quarterfinal flags for a league
    from the matchup rows in the database.

    Brackets: 3+ playoff weeks -> quarterfinals/semifinals/championship
    (the last three); exactly 2 -> semifinals/championship (2007's league
    used this shape and went unflagged for years under the 3-week-minimum
    detector). Fewer than 2 playoff weeks (bracket still in progress):
    flags stay as they are until more weeks land.

    Returns the championship matchup_id, or None when undeterminable.
    """
    playoff_weeks = [r[0] for r in conn.execute(text(
        "SELECT DISTINCT week FROM public.matchups "
        "WHERE league_id = :l AND is_playoffs AND NOT is_consolation "
        "ORDER BY week"), {'l': league_id})]

    if len(playoff_weeks) < 2:
        return None

    # The bracket must be over. Mid-playoffs, a three-round bracket has only
    # its first two weeks loaded, which looks exactly like a finished
    # two-round bracket - and flagged a semifinal as the championship,
    # showing a champion for an unfinished season until the final week landed.
    # end_week is a text column and the extractor writes '' when Yahoo omits
    # the setting; a bare ::int cast on that raises and rolls back the whole
    # raw load. NULLIF makes an absent value behave as unknown, which the
    # guard below already handles.
    end_week = conn.execute(text(
        "SELECT max(NULLIF(end_week, '')::int) FROM public.leagues "
        "WHERE league_id = :l"), {'l': league_id}).scalar()
    if end_week is not None and playoff_weeks[-1] < end_week:
        logger.info("Playoffs still in progress for %s (last playoff week %s "
                    "< end_week %s) - deferring round flags",
                    league_id, playoff_weeks[-1], end_week)
        return None

    champ_week = playoff_weeks[-1]
    semi_week = playoff_weeks[-2]
    quarter_week = playoff_weeks[-3] if len(playoff_weeks) >= 3 else None

    def games(week):
        return conn.execute(text(
            "SELECT team1_id, team2_id, winner_team_id, team1_score, team2_score, matchup_id "
            "FROM public.matchups WHERE league_id = :l AND week = :w "
            "AND is_playoffs AND NOT is_consolation"),
            {'l': league_id, 'w': week}).fetchall()

    quarter_winners = set()
    if quarter_week is not None:
        for row in games(quarter_week):
            w = _winner(row[:5])
            if w:
                quarter_winners.add(w)

    semi_ids, semi_winners = [], set()
    for row in games(semi_week):
        team1_id, team2_id = row[0], row[1]
        if quarter_week is not None:
            # 3-round bracket: a semifinal involves a quarterfinal winner;
            # both-losers games are placement games.
            is_semi = team1_id in quarter_winners or team2_id in quarter_winners
        else:
            # 2-round bracket: every non-consolation playoff game in the
            # first playoff week is a semifinal.
            is_semi = True
        if is_semi:
            semi_ids.append(row[5])
            w = _winner(row[:5])
            if w:
                semi_winners.add(w)

    champ_id = None
    for row in games(champ_week):
        if row[0] in semi_winners and row[1] in semi_winners:
            champ_id = row[5]
            break

    if champ_id is None:
        logger.warning("No championship determinable yet for %s "
                       "(playoff weeks: %s)", league_id, playoff_weeks)
        return None

    conn.execute(text(
        "UPDATE public.matchups SET is_championship = false, "
        "is_semifinal = false, is_quarterfinal = false "
        "WHERE league_id = :l"), {'l': league_id})
    if quarter_week is not None:
        conn.execute(text(
            "UPDATE public.matchups SET is_quarterfinal = true "
            "WHERE league_id = :l AND week = :w AND is_playoffs "
            "AND NOT is_consolation"), {'l': league_id, 'w': quarter_week})
    if semi_ids:
        conn.execute(text(
            "UPDATE public.matchups SET is_semifinal = true "
            "WHERE matchup_id = ANY(:ids)"), {'ids': semi_ids})
    conn.execute(text(
        "UPDATE public.matchups SET is_championship = true "
        "WHERE matchup_id = :m"), {'m': champ_id})
    return champ_id
