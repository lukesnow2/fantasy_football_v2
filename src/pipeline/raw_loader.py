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
    """
    if not weeks:
        return {'deleted': 0, 'inserted': 0}
    deleted = conn.execute(
        text(f'DELETE FROM public.{table} '
             f'WHERE league_id = :l AND "{week_col}" = ANY(:weeks)'),
        {'l': league_id, 'weeks': weeks}).rowcount
    inserted = _insert_rows(conn, table, rows, '')
    return {'deleted': deleted, 'inserted': inserted}


def append_only(conn, table: str, rows: List[dict], conflict_cols: str) -> int:
    return _insert_rows(conn, table, rows,
                        f'ON CONFLICT ({conflict_cols}) DO NOTHING')


def load_delta(conn, state, league_id: str, season: int, weeks: List[int],
               data: Dict[str, List[dict]]) -> Dict[str, int]:
    """Load one league's delta for the given weeks on the caller's
    transaction, marking pipeline_periods raw-complete on the same
    transaction. Caller owns commit/rollback - all tables land together
    or none do.

    data keys: leagues, teams, rosters, matchups (RAW week-blobs),
    transactions, draft_picks, statistics.
    """
    counts: Dict[str, int] = {}

    counts['leagues'] = upsert_dimension(
        conn, 'leagues', 'league_id', data.get('leagues', []), LEAGUE_UPDATE_COLS)
    counts['teams'] = upsert_dimension(
        conn, 'teams', 'team_id', data.get('teams', []), TEAM_UPDATE_COLS)

    flat_matchups = flatten_matchups(data.get('matchups', []))
    m = replace_period_rows(conn, 'matchups', 'week', league_id, weeks, flat_matchups)
    counts['matchups'] = m['inserted']

    r = replace_period_rows(conn, 'rosters', 'week', league_id, weeks,
                            data.get('rosters', []))
    counts['rosters'] = r['inserted']

    s = replace_period_rows(conn, 'statistics', 'week_number', league_id, weeks,
                            data.get('statistics', []))
    counts['statistics'] = s['inserted']

    counts['transactions'] = append_only(
        conn, 'transactions', data.get('transactions', []),
        'transaction_id, player_id')
    counts['draft_picks'] = append_only(
        conn, 'draft_picks', data.get('draft_picks', []), 'draft_pick_id')

    refresh_playoff_flags(conn, league_id)

    for week in weeks:
        state.mark_raw_complete(conn, league_id, season, week, {
            'matchups': m['inserted'],
            'rosters': r['inserted'],
            'statistics': s['inserted'],
        })

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
