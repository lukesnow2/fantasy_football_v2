#!/usr/bin/env python3
"""Read-only probe of the Yahoo Fantasy API for fields the pipeline doesn't keep.

Dumps raw JSON responses so field names and their availability by season can
be checked against what Yahoo actually returns, not what the docs say.

    python scripts/probe_yahoo_api.py OUT_DIR --phase settings
    python scripts/probe_yahoo_api.py OUT_DIR --phase deep --seasons 2026 2025 2015 2005

GET requests only, paced through the extractor's ReliableYHandler.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'extractors'))

from yahoo_oauth import OAuth2  # noqa: E402
from yahoo_http import ReliableYHandler, RequestPacer, YahooApiError  # noqa: E402

LEAGUES = {
    2026: None,  # found at run time from the logged-in user's leagues
    2025: "461.l.654923", 2024: "449.l.674707", 2023: "423.l.841006",
    2022: "414.l.1194955", 2021: "406.l.1065326", 2020: "399.l.837311",
    2019: "390.l.777720", 2018: "380.l.1143665", 2017: "371.l.1025465",
    2016: "359.l.696366", 2015: "348.l.655822", 2014: "331.l.355899",
    2013: "314.l.319572", 2012: "273.l.107980", 2011: "257.l.89145",
    2010: "242.l.413666", 2009: "222.l.222935", 2008: "199.l.42364",
    2007: "175.l.658531", 2006: "153.l.76788", 2005: "124.l.109785",
}


class Probe:
    def __init__(self, out_dir):
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)
        oauth = OAuth2(None, None, from_file='oauth2.json')
        self.http = ReliableYHandler(oauth, pacer=RequestPacer(min_interval=1.5))
        self.calls = 0

    def get(self, label, uri):
        path = os.path.join(self.out_dir, f"{label}.json")
        if os.path.exists(path):  # resumable: never re-spend a call
            with open(path) as f:
                return json.load(f)
        self.calls += 1
        try:
            body = self.http.get(uri)
        except YahooApiError as e:
            body = {'_error': str(e), '_status': e.status_code,
                    '_body': (e.body or b'').decode('utf-8', 'replace')}
        body = {'_uri': uri, **body} if isinstance(body, dict) else {'_uri': uri, 'data': body}
        with open(path, 'w') as f:
            json.dump(body, f, indent=1)
        print(f"  {'ERR' if '_error' in body else 'ok '} {label}  <- {uri}", flush=True)
        return body


def find_current_league(p):
    body = p.get('users_nfl_leagues', 'users;use_login=1/games;game_codes=nfl/leagues')
    keys = []

    def walk(o):
        if isinstance(o, dict):
            if 'league_key' in o and 'season' in o:
                keys.append((o['season'], o['league_key'], o.get('name')))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(body)
    return keys


def phase_settings(p):
    for season, lk in sorted(LEAGUES.items()):
        if not lk:
            continue
        gk = lk.split('.')[0]
        p.get(f"{season}_league_settings", f"league/{lk}/settings")
        p.get(f"{season}_game_meta",
              f"game/{gk};out=game_weeks,stat_categories,position_types,roster_positions")


def first_team_key(teams_body):
    s = json.dumps(teams_body)
    i = s.find('"team_key": "')
    return s[i + 13:s.find('"', i + 13)] if i >= 0 else None


def player_keys_from(body, n):
    s, keys, i = json.dumps(body), [], 0
    while len(keys) < n:
        i = s.find('"player_key": "', i)
        if i < 0:
            break
        k = s[i + 15:s.find('"', i + 15)]
        if k not in keys:
            keys.append(k)
        i += 1
    return keys


def phase_deep(p, seasons):
    for season in seasons:
        lk = LEAGUES[season]
        if not lk:
            print(f"skip {season}: no league key")
            continue
        print(f"== {season} {lk}")
        s = p.get(f"{season}_league_settings", f"league/{lk}/settings")
        txt = json.dumps(s)

        def setting(name, default):
            i = txt.find(f'"{name}": "')
            return txt[i + len(name) + 5:txt.find('"', i + len(name) + 5)] if i >= 0 else default
        wk = 3 if season != 2026 else min(3, int(setting('current_week', '3')))

        p.get(f"{season}_standings", f"league/{lk}/standings")
        p.get(f"{season}_scoreboard_w{wk}", f"league/{lk}/scoreboard;week={wk}")
        teams = p.get(f"{season}_teams", f"league/{lk}/teams")
        tk = first_team_key(teams)
        if tk:
            p.get(f"{season}_team_full",
                  f"team/{tk};out=metadata,stats,standings,draftresults")
            p.get(f"{season}_roster_w{wk}_stats",
                  f"team/{tk}/roster;week={wk}/players/stats;type=week;week={wk}")
            p.get(f"{season}_team_matchups", f"team/{tk}/matchups;weeks={wk}")
        p.get(f"{season}_trades", f"league/{lk}/transactions;types=trade;count=5")
        draft = p.get(f"{season}_draftresults", f"league/{lk}/draftresults")
        pk = player_keys_from(draft, 3)
        if pk:
            keys = ','.join(pk)
            p.get(f"{season}_players_meta",
                  f"league/{lk}/players;player_keys={keys};out=metadata,percent_owned,ownership,draft_analysis")
            p.get(f"{season}_players_stats_w{wk}",
                  f"league/{lk}/players;player_keys={keys}/stats;type=week;week={wk}")
        # Player pool: top scorers that week regardless of status, to test
        # whether a past week's best available player can be reconstructed.
        p.get(f"{season}_pool_top_w{wk}",
              f"league/{lk}/players;sort=PTS;sort_type=week;sort_week={wk};count=25/stats;type=week;week={wk}")
        p.get(f"{season}_pool_FA_now",
              f"league/{lk}/players;status=FA;sort=PTS;sort_type=week;sort_week={wk};count=5")


def phase_extras(p):
    """Per-season coverage: team/manager metadata, every trade (for trade
    notes), and whether week stat lines survive (top scorers of week 3)."""
    for season, lk in sorted(LEAGUES.items()):
        if not lk:
            continue
        p.get(f"{season}_teams", f"league/{lk}/teams")
        p.get(f"{season}_trades_all", f"league/{lk}/transactions;types=trade")
        p.get(f"{season}_pool_top3_w3",
              f"league/{lk}/players;sort=PTS;sort_type=week;sort_week=3;count=3/stats;type=week;week=3")
    p.get("2015_pool_top3_season",
          f"league/{LEAGUES[2015]}/players;sort=PTS;sort_type=season;count=3/stats;type=season")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('out_dir')
    ap.add_argument('--phase', choices=['settings', 'deep', 'extras', 'find'], required=True)
    ap.add_argument('--seasons', type=int, nargs='*', default=[2026, 2025, 2015, 2005])
    ap.add_argument('--current-league')
    a = ap.parse_args()
    p = Probe(a.out_dir)
    if a.current_league:
        LEAGUES[2026] = a.current_league
    if a.phase == 'find':
        for row in find_current_league(p):
            print(row)
    elif a.phase == 'extras':
        phase_extras(p)
    elif a.phase == 'settings':
        phase_settings(p)
    else:
        phase_deep(p, a.seasons)
    print(f"{p.calls} API calls made")


if __name__ == '__main__':
    main()
