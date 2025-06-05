#!/usr/bin/env python3
"""
Comprehensive debug script to examine data availability patterns across multiple leagues
"""

import json
import logging
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_league_data_availability(league, league_id, year):
    """Check what data is available for a specific league"""
    results = {
        'league_id': league_id,
        'year': year,
        'standings_available': False,
        'standings_has_records': False,
        'teams_available': False,
        'teams_has_managers': False,
        'num_teams': 0,
        'sample_team_data': None,
        'errors': []
    }
    
    try:
        # Test standings
        try:
            standings = league.standings()
            if standings:
                results['standings_available'] = True
                if isinstance(standings, list):
                    results['num_teams'] = len(standings)
                    if len(standings) > 0:
                        sample_team = standings[0]
                        results['sample_team_data'] = sample_team
                        
                        # Check if we have meaningful record data
                        outcome_totals = sample_team.get('outcome_totals', {})
                        wins = outcome_totals.get('wins', 0)
                        losses = outcome_totals.get('losses', 0)
                        
                        if wins > 0 or losses > 0:
                            results['standings_has_records'] = True
                            
        except Exception as e:
            results['errors'].append(f"standings error: {str(e)}")
        
        # Test teams method
        try:
            if hasattr(league, 'teams'):
                teams = league.teams()
                if teams:
                    results['teams_available'] = True
                    
                    # Check if we have manager data
                    if isinstance(teams, list) and len(teams) > 0:
                        sample_team = teams[0]
                        managers = sample_team.get('managers', [])
                        if managers and len(managers) > 0:
                            manager = managers[0].get('manager', {})
                            nickname = manager.get('nickname')
                            if nickname:
                                results['teams_has_managers'] = True
                                
        except Exception as e:
            results['errors'].append(f"teams error: {str(e)}")
            
    except Exception as e:
        results['errors'].append(f"general error: {str(e)}")
    
    return results

def comprehensive_debug():
    """Debug data availability across multiple leagues and years"""
    try:
        # Authenticate
        oauth = OAuth2(None, None, from_file='oauth2.json')
        game = yfa.Game(oauth, 'nfl')
        
        logger.info("✅ Authentication successful!")
        
        # Years to test (recent years first)
        years_to_test = [2024, 2023, 2022, 2021, 2020, 2019, 2018, 2015, 2010, 2005]
        
        all_results = []
        
        for year in years_to_test:
            logger.info(f"\n🔍 Testing year {year}...")
            
            try:
                league_ids = game.league_ids(year=year)
                logger.info(f"Found {len(league_ids)} leagues for {year}")
                
                # Test first few leagues for this year
                for i, league_id in enumerate(league_ids[:3]):  # Test up to 3 leagues per year
                    logger.info(f"  Testing league {i+1}/{min(3, len(league_ids))}: {league_id}")
                    
                    try:
                        league = game.to_league(league_id)
                        results = check_league_data_availability(league, league_id, year)
                        all_results.append(results)
                        
                        # Log summary
                        status = []
                        if results['standings_available']:
                            status.append("standings✅")
                        if results['standings_has_records']:
                            status.append("records✅")
                        if results['teams_available']:
                            status.append("teams✅")
                        if results['teams_has_managers']:
                            status.append("managers✅")
                        if results['errors']:
                            status.append(f"errors❌({len(results['errors'])})")
                            
                        logger.info(f"    {league_id}: {', '.join(status) if status else 'no data❌'}")
                        
                    except Exception as e:
                        logger.error(f"    Error with league {league_id}: {e}")
                        
            except Exception as e:
                logger.error(f"Error getting leagues for {year}: {e}")
        
        # Summary analysis
        logger.info("\n" + "="*80)
        logger.info("COMPREHENSIVE ANALYSIS")
        logger.info("="*80)
        
        total_leagues = len(all_results)
        standings_available = sum(1 for r in all_results if r['standings_available'])
        standings_with_records = sum(1 for r in all_results if r['standings_has_records'])
        teams_available = sum(1 for r in all_results if r['teams_available'])
        teams_with_managers = sum(1 for r in all_results if r['teams_has_managers'])
        
        logger.info(f"📊 Total leagues tested: {total_leagues}")
        logger.info(f"📊 Standings available: {standings_available}/{total_leagues} ({standings_available/total_leagues*100:.1f}%)")
        logger.info(f"📊 Standings with records: {standings_with_records}/{total_leagues} ({standings_with_records/total_leagues*100:.1f}%)")
        logger.info(f"📊 Teams method available: {teams_available}/{total_leagues} ({teams_available/total_leagues*100:.1f}%)")
        logger.info(f"📊 Teams with managers: {teams_with_managers}/{total_leagues} ({teams_with_managers/total_leagues*100:.1f}%)")
        
        # Year breakdown
        logger.info("\n📅 YEAR BREAKDOWN:")
        year_stats = {}
        for result in all_results:
            year = result['year']
            if year not in year_stats:
                year_stats[year] = {'total': 0, 'good_data': 0}
            year_stats[year]['total'] += 1
            if result['standings_has_records'] or result['teams_has_managers']:
                year_stats[year]['good_data'] += 1
        
        for year in sorted(year_stats.keys(), reverse=True):
            stats = year_stats[year]
            pct = stats['good_data']/stats['total']*100 if stats['total'] > 0 else 0
            logger.info(f"  {year}: {stats['good_data']}/{stats['total']} leagues with good data ({pct:.1f}%)")
        
        # Save detailed results
        output_file = 'debug_analysis_results.json'
        with open(output_file, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        logger.info(f"\n💾 Detailed results saved to: {output_file}")
        
        # Show some examples of good vs bad data
        logger.info("\n🔍 SAMPLE DATA EXAMPLES:")
        
        good_example = None
        bad_example = None
        
        for result in all_results:
            if result['standings_has_records'] and not good_example:
                good_example = result
            elif not result['standings_has_records'] and not result['teams_has_managers'] and not bad_example:
                bad_example = result
                
        if good_example:
            logger.info("\n✅ EXAMPLE OF GOOD DATA:")
            logger.info(f"League: {good_example['league_id']} ({good_example['year']})")
            if good_example['sample_team_data']:
                logger.info("Sample team data:")
                logger.info(json.dumps(good_example['sample_team_data'], indent=2, default=str))
        
        if bad_example:
            logger.info("\n❌ EXAMPLE OF MISSING DATA:")
            logger.info(f"League: {bad_example['league_id']} ({bad_example['year']})")
            logger.info(f"Errors: {bad_example['errors']}")
            if bad_example['sample_team_data']:
                logger.info("Sample team data:")
                logger.info(json.dumps(bad_example['sample_team_data'], indent=2, default=str))
        
    except Exception as e:
        logger.error(f"Comprehensive debug failed: {e}")

if __name__ == "__main__":
    comprehensive_debug() 