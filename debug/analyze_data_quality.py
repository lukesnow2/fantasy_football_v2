#!/usr/bin/env python3
"""
Analyze the extracted data to identify patterns where team data is missing or zero
"""

import json
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_data_quality():
    """Analyze the data quality patterns in the extracted data"""
    
    # Load the fixed extraction data
    filename = 'yahoo_fantasy_fixed_complete_data_20250605_094453.json'
    
    with open(filename, 'r') as f:
        data = json.load(f)
    
    leagues = data.get('leagues', [])
    teams = data.get('teams', [])
    
    logger.info(f"📊 Analyzing {len(leagues)} leagues and {len(teams)} teams")
    
    # Create league lookup
    league_lookup = {l['league_id']: l for l in leagues}
    
    # Analysis by year
    year_stats = defaultdict(lambda: {
        'total_teams': 0,
        'teams_with_records': 0,
        'teams_with_points': 0,
        'teams_with_managers': 0,
        'zero_records': 0,
        'zero_points': 0,
        'missing_managers': 0,
        'leagues': set()
    })
    
    # Analysis by league
    league_stats = {}
    
    # Detailed analysis
    problematic_teams = []
    good_teams = []
    
    for team in teams:
        league_id = team['league_id']
        league_info = league_lookup.get(league_id, {})
        year = league_info.get('season', 'unknown')
        league_name = league_info.get('name', 'unknown')
        
        # Add to year stats
        year_stats[year]['total_teams'] += 1
        year_stats[year]['leagues'].add(league_id)
        
        # Check for records
        has_records = team['wins'] > 0 or team['losses'] > 0 or team['ties'] > 0
        if has_records:
            year_stats[year]['teams_with_records'] += 1
        else:
            year_stats[year]['zero_records'] += 1
            
        # Check for points
        has_points = team['points_for'] > 0 or team['points_against'] > 0
        if has_points:
            year_stats[year]['teams_with_points'] += 1
        else:
            year_stats[year]['zero_points'] += 1
            
        # Check for managers
        has_manager = team['manager_name'] and team['manager_name'].strip() and team['manager_name'] != '--hidden--'
        if has_manager:
            year_stats[year]['teams_with_managers'] += 1
        else:
            year_stats[year]['missing_managers'] += 1
        
        # League-specific stats
        if league_id not in league_stats:
            league_stats[league_id] = {
                'name': league_name,
                'year': year,
                'total_teams': 0,
                'good_teams': 0,
                'zero_data_teams': 0,
                'teams': []
            }
        
        league_stats[league_id]['total_teams'] += 1
        
        # Classify team quality
        is_good = has_records and has_points and has_manager
        if is_good:
            league_stats[league_id]['good_teams'] += 1
            good_teams.append({
                'team_id': team['team_id'],
                'name': team['name'],
                'manager': team['manager_name'],
                'league': league_name,
                'year': year,
                'record': f"{team['wins']}-{team['losses']}-{team['ties']}",
                'points_for': team['points_for']
            })
        else:
            league_stats[league_id]['zero_data_teams'] += 1
            problematic_teams.append({
                'team_id': team['team_id'],
                'name': team['name'],
                'manager': team['manager_name'],
                'league': league_name,
                'year': year,
                'wins': team['wins'],
                'losses': team['losses'],
                'ties': team['ties'],
                'points_for': team['points_for'],
                'points_against': team['points_against'],
                'issues': []
            })
            
            # Identify specific issues
            if not has_records:
                problematic_teams[-1]['issues'].append('no_records')
            if not has_points:
                problematic_teams[-1]['issues'].append('no_points')
            if not has_manager:
                problematic_teams[-1]['issues'].append('no_manager')
        
        league_stats[league_id]['teams'].append(team)
    
    # Print analysis results
    logger.info("\n" + "="*80)
    logger.info("DATA QUALITY ANALYSIS")
    logger.info("="*80)
    
    total_teams = len(teams)
    total_good = len(good_teams)
    total_problematic = len(problematic_teams)
    
    logger.info(f"📊 Overall Statistics:")
    logger.info(f"  Total Teams: {total_teams}")
    logger.info(f"  Complete Teams: {total_good} ({total_good/total_teams*100:.1f}%)")
    logger.info(f"  Problematic Teams: {total_problematic} ({total_problematic/total_teams*100:.1f}%)")
    
    # Year breakdown
    logger.info(f"\n📅 Quality by Year:")
    for year in sorted(year_stats.keys()):
        stats = year_stats[year]
        total = stats['total_teams']
        good = min(stats['teams_with_records'], stats['teams_with_points'], stats['teams_with_managers'])
        
        logger.info(f"  {year}: {good}/{total} complete teams ({good/total*100:.1f}%)")
        logger.info(f"    Records: {stats['teams_with_records']}/{total} ({stats['teams_with_records']/total*100:.1f}%)")
        logger.info(f"    Points: {stats['teams_with_points']}/{total} ({stats['teams_with_points']/total*100:.1f}%)")
        logger.info(f"    Managers: {stats['teams_with_managers']}/{total} ({stats['teams_with_managers']/total*100:.1f}%)")
    
    # League-specific analysis
    logger.info(f"\n🏆 Problematic Leagues:")
    problematic_leagues = [(lid, stats) for lid, stats in league_stats.items() 
                          if stats['zero_data_teams'] > 0]
    problematic_leagues.sort(key=lambda x: x[1]['zero_data_teams'], reverse=True)
    
    for league_id, stats in problematic_leagues[:10]:  # Top 10 most problematic
        total = stats['total_teams']
        bad = stats['zero_data_teams']
        logger.info(f"  {stats['year']} - {stats['name']}: {bad}/{total} teams missing data ({bad/total*100:.1f}%)")
    
    # Detailed problematic teams
    logger.info(f"\n❌ Sample Problematic Teams:")
    for i, team in enumerate(problematic_teams[:15]):  # First 15 problematic teams
        issues = ', '.join(team['issues'])
        logger.info(f"  {i+1}. {team['year']} - {team['league']}")
        logger.info(f"     Team: {team['name']} (Manager: {team['manager']})")
        logger.info(f"     Record: {team['wins']}-{team['losses']}-{team['ties']}, Points: {team['points_for']}")
        logger.info(f"     Issues: {issues}")
    
    # Pattern analysis
    logger.info(f"\n🔍 Pattern Analysis:")
    
    # Group by year to see if older leagues have more issues
    year_issue_counts = defaultdict(int)
    for team in problematic_teams:
        year_issue_counts[team['year']] += 1
    
    logger.info(f"  Issues by Year:")
    for year in sorted(year_issue_counts.keys()):
        total_teams_year = year_stats[year]['total_teams']
        issues = year_issue_counts[year]
        logger.info(f"    {year}: {issues}/{total_teams_year} teams with issues ({issues/total_teams_year*100:.1f}%)")
    
    # Group by issue type
    issue_counts = defaultdict(int)
    for team in problematic_teams:
        for issue in team['issues']:
            issue_counts[issue] += 1
    
    logger.info(f"  Issues by Type:")
    for issue, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"    {issue}: {count} teams ({count/total_teams*100:.1f}%)")
    
    # Look for specific patterns
    logger.info(f"\n🎯 Specific Patterns:")
    
    # Check if certain league names have more issues
    league_name_issues = defaultdict(list)
    for team in problematic_teams:
        league_name_issues[team['league']].append(team)
    
    logger.info(f"  Leagues with most issues:")
    for league_name, team_list in sorted(league_name_issues.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
        logger.info(f"    {league_name}: {len(team_list)} problematic teams")
    
    # Check if teams in same league all have issues (suggests league-level problem)
    logger.info(f"\n  Leagues where ALL teams have issues:")
    for league_id, stats in league_stats.items():
        if stats['total_teams'] > 0 and stats['zero_data_teams'] == stats['total_teams']:
            logger.info(f"    {stats['year']} - {stats['name']}: {stats['total_teams']} teams all missing data")
    
    # Save detailed analysis
    analysis_output = {
        'summary': {
            'total_teams': total_teams,
            'complete_teams': total_good,
            'problematic_teams': total_problematic
        },
        'by_year': dict(year_stats),
        'by_league': league_stats,
        'problematic_teams': problematic_teams,
        'good_teams': good_teams[:10]  # Sample of good teams
    }
    
    with open('data_quality_analysis.json', 'w') as f:
        json.dump(analysis_output, f, indent=2, default=str)
    
    logger.info(f"\n💾 Detailed analysis saved to: data_quality_analysis.json")

if __name__ == "__main__":
    analyze_data_quality() 