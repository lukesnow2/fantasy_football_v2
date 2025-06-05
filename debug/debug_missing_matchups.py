#!/usr/bin/env python3
"""
Debug why matchups and transactions are 0
"""

import json
import logging
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_missing_data():
    """Debug why matchups and transactions are empty"""
    try:
        # Authenticate
        oauth = OAuth2(None, None, from_file='oauth2.json')
        game = yfa.Game(oauth, 'nfl')
        
        logger.info("✅ Authentication successful!")
        
        # Test with 2024 league
        league_id = "449.l.674707"
        logger.info(f"🔍 Debugging missing data for: {league_id}")
        
        league = game.to_league(league_id)
        
        logger.info("\n" + "="*60)
        logger.info("🔍 DEBUGGING MATCHUPS")
        logger.info("="*60)
        
        # Test different weeks for matchups
        for week in [1, 8, 14, 16, 17]:
            try:
                logger.info(f"Testing matchups for week {week}...")
                matchup_data = league.matchups(week=week)
                
                logger.info(f"Week {week} response type: {type(matchup_data)}")
                logger.info(f"Week {week} response size: {len(str(matchup_data))} chars")
                
                # Save a sample for analysis
                if week == 1:
                    with open(f'matchup_week_{week}_sample.json', 'w') as f:
                        json.dump(matchup_data, f, indent=2)
                    logger.info(f"Saved week {week} sample to matchup_week_{week}_sample.json")
                
                # Try to parse structure
                if 'fantasy_content' in matchup_data:
                    content = matchup_data['fantasy_content']
                    logger.info(f"Week {week} has fantasy_content")
                    
                    if 'league' in content:
                        league_data = content['league']
                        logger.info(f"Week {week} has league data: {type(league_data)}")
                        
                        if isinstance(league_data, list) and len(league_data) > 0:
                            first_league = league_data[0]
                            logger.info(f"Week {week} league keys: {list(first_league.keys())}")
                            
                            if 'scoreboard' in first_league:
                                scoreboard = first_league['scoreboard']
                                logger.info(f"Week {week} scoreboard keys: {list(scoreboard.keys())}")
                                
                                if '0' in scoreboard:
                                    sb_data = scoreboard['0']
                                    logger.info(f"Week {week} scoreboard[0] keys: {list(sb_data.keys())}")
                                    
                                    if 'matchups' in sb_data:
                                        matchups = sb_data['matchups']
                                        logger.info(f"Week {week} matchups keys: {list(matchups.keys())}")
                                        logger.info(f"Week {week} matchup count: {matchups.get('count', 'N/A')}")
                
            except Exception as e:
                logger.error(f"Week {week} matchups failed: {e}")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 DEBUGGING TRANSACTIONS")
        logger.info("="*60)
        
        # Test different transaction types
        trans_types = ['add', 'drop', 'add,drop', 'trade']
        
        for trans_type in trans_types:
            try:
                logger.info(f"Testing transactions type '{trans_type}'...")
                trans_data = league.transactions(trans_type, 10)
                
                logger.info(f"Type '{trans_type}' response: {type(trans_data)}")
                logger.info(f"Type '{trans_type}' length: {len(trans_data) if isinstance(trans_data, list) else 'N/A'}")
                
                if isinstance(trans_data, list) and len(trans_data) > 0:
                    logger.info(f"Type '{trans_type}' sample keys: {list(trans_data[0].keys()) if isinstance(trans_data[0], dict) else 'Not dict'}")
                    
                    # Save sample
                    with open(f'transactions_{trans_type.replace(",", "_")}_sample.json', 'w') as f:
                        json.dump(trans_data[:2], f, indent=2)  # First 2 transactions
                    logger.info(f"Saved {trans_type} sample to transactions_{trans_type.replace(',', '_')}_sample.json")
                
            except Exception as e:
                logger.error(f"Transactions type '{trans_type}' failed: {e}")
        
        logger.info("\n" + "="*60)
        logger.info("🔍 SUMMARY")
        logger.info("="*60)
        
        logger.info("Check the saved sample files to understand the data structure!")
        
    except Exception as e:
        logger.error(f"❌ Debug failed: {e}")

if __name__ == "__main__":
    debug_missing_data() 