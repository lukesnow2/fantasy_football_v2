#!/usr/bin/env python3
"""
Debug script to investigate API methods for rosters, matchups, transactions, and statistics
"""

import json
import logging
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_missing_data():
    """Debug the missing data extraction methods"""
    try:
        # Authenticate
        oauth = OAuth2(None, None, from_file='oauth2.json')
        game = yfa.Game(oauth, 'nfl')
        
        logger.info("✅ Authentication successful!")
        
        # Test with 2024 league (most likely to have complete data)
        league_id = "449.l.674707"
        logger.info(f"🔍 Testing missing data methods for league: {league_id}")
        
        league = game.to_league(league_id)
        
        # Get teams for testing individual team methods
        teams_data = league.teams()
        if teams_data:
            sample_team_id = list(teams_data.keys())[0]
            sample_team = league.to_team(sample_team_id)
            logger.info(f"📋 Sample team ID: {sample_team_id}")
        else:
            logger.error("❌ No teams found!")
            return
        
        logger.info("\n" + "="*60)
        logger.info("🔍 INVESTIGATING ROSTER METHODS")
        logger.info("="*60)
        
        # Test roster methods
        try:
            # Try different roster methods
            logger.info("Testing league.rosters()...")
            rosters = league.rosters()
            logger.info(f"✅ league.rosters() returned: {type(rosters)} with {len(rosters) if hasattr(rosters, '__len__') else 'N/A'} items")
            if rosters:
                logger.info(f"Sample roster keys: {list(rosters.keys())[:3] if isinstance(rosters, dict) else 'Not a dict'}")
        except Exception as e:
            logger.error(f"❌ league.rosters() failed: {e}")
        
        try:
            logger.info("Testing team.roster()...")
            team_roster = sample_team.roster()
            logger.info(f"✅ team.roster() returned: {type(team_roster)} with {len(team_roster) if hasattr(team_roster, '__len__') else 'N/A'} items")
            if team_roster:
                logger.info(f"Sample roster data: {str(team_roster)[:200]}...")
        except Exception as e:
            logger.error(f"❌ team.roster() failed: {e}")
            
        try:
            logger.info("Testing team.roster(week=1)...")
            team_roster_w1 = sample_team.roster(week=1)
            logger.info(f"✅ team.roster(week=1) returned: {type(team_roster_w1)} with {len(team_roster_w1) if hasattr(team_roster_w1, '__len__') else 'N/A'} items")
        except Exception as e:
            logger.error(f"❌ team.roster(week=1) failed: {e}")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 INVESTIGATING MATCHUP METHODS")
        logger.info("="*60)
        
        # Test matchup methods
        try:
            logger.info("Testing league.matchups()...")
            matchups = league.matchups()
            logger.info(f"✅ league.matchups() returned: {type(matchups)} with {len(matchups) if hasattr(matchups, '__len__') else 'N/A'} items")
        except Exception as e:
            logger.error(f"❌ league.matchups() failed: {e}")
            
        try:
            logger.info("Testing league.matchups(week=1)...")
            matchups_w1 = league.matchups(week=1)
            logger.info(f"✅ league.matchups(week=1) returned: {type(matchups_w1)} with {len(matchups_w1) if hasattr(matchups_w1, '__len__') else 'N/A'} items")
            if matchups_w1:
                logger.info(f"Sample matchup data: {str(matchups_w1)[:200]}...")
        except Exception as e:
            logger.error(f"❌ league.matchups(week=1) failed: {e}")
        
        try:
            logger.info("Testing league.scoreboard()...")
            scoreboard = league.scoreboard()
            logger.info(f"✅ league.scoreboard() returned: {type(scoreboard)}")
        except Exception as e:
            logger.error(f"❌ league.scoreboard() failed: {e}")
            
        try:
            logger.info("Testing league.scoreboard(week=1)...")
            scoreboard_w1 = league.scoreboard(week=1)
            logger.info(f"✅ league.scoreboard(week=1) returned: {type(scoreboard_w1)} with {len(scoreboard_w1) if hasattr(scoreboard_w1, '__len__') else 'N/A'} items")
            if scoreboard_w1:
                logger.info(f"Sample scoreboard data: {str(scoreboard_w1)[:200]}...")
        except Exception as e:
            logger.error(f"❌ league.scoreboard(week=1) failed: {e}")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 INVESTIGATING TRANSACTION METHODS")
        logger.info("="*60)
        
        # Test transaction methods
        try:
            logger.info("Testing league.transactions(['add', 'drop'], 50)...")
            transactions = league.transactions(['add', 'drop'], 50)
            logger.info(f"✅ league.transactions(['add', 'drop'], 50) returned: {type(transactions)} with {len(transactions) if hasattr(transactions, '__len__') else 'N/A'} items")
            if transactions:
                logger.info(f"Sample transaction data: {str(transactions)[:200]}...")
        except Exception as e:
            logger.error(f"❌ league.transactions(['add', 'drop'], 50) failed: {e}")
            
        try:
            logger.info("Testing league.transactions(['trade'], 25)...")
            trades = league.transactions(['trade'], 25)
            logger.info(f"✅ league.transactions(['trade'], 25) returned: {type(trades)} with {len(trades) if hasattr(trades, '__len__') else 'N/A'} items")
        except Exception as e:
            logger.error(f"❌ league.transactions(['trade'], 25) failed: {e}")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 INVESTIGATING STATISTICS METHODS")
        logger.info("="*60)
        
        # Test statistics methods
        try:
            logger.info("Testing league.stat_categories()...")
            stat_cats = league.stat_categories()
            logger.info(f"✅ league.stat_categories() returned: {type(stat_cats)} with {len(stat_cats) if hasattr(stat_cats, '__len__') else 'N/A'} items")
            if stat_cats:
                logger.info(f"Sample stat categories: {str(stat_cats)[:200]}...")
        except Exception as e:
            logger.error(f"❌ league.stat_categories() failed: {e}")
            
        try:
            logger.info("Testing team.stats()...")
            team_stats = sample_team.stats()
            logger.info(f"✅ team.stats() returned: {type(team_stats)}")
            if team_stats:
                logger.info(f"Sample team stats: {str(team_stats)[:200]}...")
        except Exception as e:
            logger.error(f"❌ team.stats() failed: {e}")
            
        try:
            logger.info("Testing team.stats(week=1)...")
            team_stats_w1 = sample_team.stats(week=1)
            logger.info(f"✅ team.stats(week=1) returned: {type(team_stats_w1)}")
        except Exception as e:
            logger.error(f"❌ team.stats(week=1) failed: {e}")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 CHECKING AVAILABLE METHODS")
        logger.info("="*60)
        
        # List all available methods
        league_methods = [method for method in dir(league) if not method.startswith('_')]
        team_methods = [method for method in dir(sample_team) if not method.startswith('_')]
        
        logger.info(f"📋 League methods: {league_methods}")
        logger.info(f"📋 Team methods: {team_methods}")
        
        # Check league settings for week info
        try:
            settings = league.settings()
            current_week = settings.get('current_week', 1)
            start_week = settings.get('start_week', 1)
            end_week = settings.get('end_week', 17)
            logger.info(f"📅 League weeks: current={current_week}, start={start_week}, end={end_week}")
        except Exception as e:
            logger.error(f"❌ Could not get league settings: {e}")
        
    except Exception as e:
        logger.error(f"❌ Debug failed: {e}")

if __name__ == "__main__":
    debug_missing_data() 