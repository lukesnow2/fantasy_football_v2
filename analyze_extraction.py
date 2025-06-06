#!/usr/bin/env python3
import json

# Load the data
with open('data/current/yahoo_fantasy_FINAL_complete_data_20250606_151834.json', 'r') as f:
    data = json.load(f)

print('=== EXTRACTION ANALYSIS ===')
print(f'Total Records: {sum(len(v) for v in data.values() if isinstance(v, list)):,}')
print()

leagues = data.get('leagues', [])
teams = data.get('teams', [])
rosters = data.get('rosters', [])
matchups = data.get('matchups', [])
transactions = data.get('transactions', [])
drafts = data.get('draft_picks', [])

print('=== SUCCESS vs FAILURE ANALYSIS ===')
successful_leagues = []
failed_leagues = []

for league in leagues:
    league_id = league.get('league_id')
    league_teams = [t for t in teams if t.get('league_id') == league_id]
    league_rosters = [r for r in rosters if r.get('league_id') == league_id]
    
    if len(league_teams) > 0 and len(league_rosters) > 0:
        successful_leagues.append(league)
        print(f'✅ {league.get("season")} - {league.get("name")} ({league_id})')
        print(f'   Teams: {len(league_teams)}, Rosters: {len(league_rosters)}')
    else:
        failed_leagues.append(league)
        print(f'❌ {league.get("season")} - {league.get("name")} ({league_id})')
        print(f'   Teams: {len(league_teams)}, Rosters: {len(league_rosters)}')

print(f'\n=== SUMMARY ===')
print(f'Successful leagues: {len(successful_leagues)}')
print(f'Failed leagues: {len(failed_leagues)}')

if failed_leagues:
    print(f'\n=== FIRST FAILED LEAGUE ===')
    first_failed = failed_leagues[0]
    print(f'League: {first_failed.get("name")} ({first_failed.get("season")})')
    print(f'League ID: {first_failed.get("league_id")}')
    print('This is where we should resume extraction from.') 