#!/usr/bin/env python3
"""
Test script to demonstrate working extraction methods for missing data
"""

import json
import logging
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_working_methods():
    """Test the working extraction methods"""
    try:
        # Authenticate
        oauth = OAuth2(None, None, from_file='oauth2.json')
        game = yfa.Game(oauth, 'nfl')
        
        logger.info("✅ Authentication successful!")
        
        # Test with 2024 league
        league_id = "449.l.674707"
        logger.info(f"🔍 Testing working methods for league: {league_id}")
        
        league = game.to_league(league_id)
        settings = league.settings()
        
        # Get league info
        current_week = settings.get('current_week', 17)
        start_week = settings.get('start_week', 1) 
        end_week = settings.get('end_week', 17)
        
        logger.info(f"📅 League: {settings.get('name')} - Weeks {start_week}-{end_week}, Current: {current_week}")
        
        # Get teams
        teams_data = league.teams()
        logger.info(f"👥 Found {len(teams_data)} teams")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 TESTING ROSTER EXTRACTION")
        logger.info("="*60)
        
        sample_rosters = {}
        for team_id, team_data in list(teams_data.items())[:2]:  # Test first 2 teams
            try:
                team = league.to_team(team_id)
                
                # Test current roster
                roster = team.roster()
                logger.info(f"✅ {team_id} roster: {len(roster)} players")
                
                # Test specific week roster
                roster_w1 = team.roster(week=1)
                logger.info(f"✅ {team_id} week 1 roster: {len(roster_w1)} players")
                
                # Save sample
                sample_rosters[team_id] = {
                    'current': roster[:2],  # First 2 players
                    'week_1': roster_w1[:2]
                }
                
            except Exception as e:
                logger.error(f"❌ Error getting roster for {team_id}: {e}")
        
        logger.info(f"📋 Sample roster data: {json.dumps(sample_rosters, indent=2)}")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 TESTING MATCHUP EXTRACTION")
        logger.info("="*60)
        
        # Test matchups for different weeks
        sample_matchups = {}
        for week in [1, 10, int(current_week)]:
            if week <= int(end_week):
                try:
                    matchups = league.matchups(week=week)
                    logger.info(f"✅ Week {week} matchups: {type(matchups)} - {len(str(matchups))} chars")
                    
                    # Extract meaningful data from matchups
                    if 'fantasy_content' in matchups:
                        content = matchups['fantasy_content']
                        if 'league' in content and isinstance(content['league'], list):
                            league_data = content['league'][0]
                            if 'scoreboard' in league_data:
                                scoreboard = league_data['scoreboard']
                                if '0' in scoreboard and 'matchups' in scoreboard['0']:
                                    matchup_count = scoreboard['0']['matchups']['count']
                                    logger.info(f"  📊 Week {week}: {matchup_count} matchups found")
                                    sample_matchups[f"week_{week}"] = {
                                        'count': matchup_count,
                                        'structure': list(scoreboard['0']['matchups'].keys())[:5]
                                    }
                                    
                except Exception as e:
                    logger.error(f"❌ Error getting matchups for week {week}: {e}")
        
        logger.info(f"📋 Sample matchup data: {json.dumps(sample_matchups, indent=2)}")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 TESTING TRANSACTION EXTRACTION")
        logger.info("="*60)
        
        # Try different transaction parameter formats
        transaction_formats = [
            "add,drop",
            "add",
            "drop", 
            "trade"
        ]
        
        for format_type in transaction_formats:
            try:
                logger.info(f"Testing transactions with '{format_type}'...")
                transactions = league.transactions(format_type, 10)
                logger.info(f"✅ league.transactions('{format_type}', 10) returned: {type(transactions)} - {len(str(transactions))} chars")
                
                # Look for transaction data
                if 'fantasy_content' in transactions:
                    content = transactions['fantasy_content']
                    if 'league' in content and isinstance(content['league'], list):
                        league_data = content['league'][0]
                        if 'transactions' in league_data:
                            trans_data = league_data['transactions']
                            if isinstance(trans_data, dict) and 'count' in trans_data:
                                count = trans_data['count']
                                logger.info(f"  📊 Found {count} {format_type} transactions")
                            
            except Exception as e:
                logger.error(f"❌ league.transactions('{format_type}', 10) failed: {str(e)[:100]}...")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 TESTING STATISTICS EXTRACTION")
        logger.info("="*60)
        
        # Test stat categories
        try:
            stat_cats = league.stat_categories()
            logger.info(f"✅ league.stat_categories() returned: {len(stat_cats)} categories")
            
            # Show sample categories
            sample_cats = stat_cats[:5]
            logger.info(f"📋 Sample stat categories: {json.dumps(sample_cats, indent=2)}")
            
        except Exception as e:
            logger.error(f"❌ Error getting stat categories: {e}")
        
        # Test player stats methods
        try:
            logger.info("Testing league.player_stats()...")
            # This might need specific parameters
            player_stats = league.player_stats([32671], week=1)  # Joe Burrow's ID from roster
            logger.info(f"✅ league.player_stats() for Joe Burrow: {type(player_stats)} - {len(str(player_stats))} chars")
            
        except Exception as e:
            logger.error(f"❌ league.player_stats() failed: {e}")
        
        logger.info("\n" + "="*60)
        logger.info("🎯 SUMMARY OF WORKING METHODS")
        logger.info("="*60)
        
        logger.info("✅ ROSTERS: team.roster() and team.roster(week=X)")
        logger.info("✅ MATCHUPS: league.matchups(week=X)")
        logger.info("❓ TRANSACTIONS: Need to test parameter formats")
        logger.info("✅ STATISTICS: league.stat_categories() + need player stats method")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")

if __name__ == "__main__":
    test_working_methods() 