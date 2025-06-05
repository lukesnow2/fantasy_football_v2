#!/usr/bin/env python3
"""
Debug script to examine Yahoo Fantasy API team data structure
"""

import json
import logging
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_team_data():
    """Debug team data extraction to see actual API structure"""
    try:
        # Authenticate
        oauth = OAuth2(None, None, from_file='oauth2.json')
        game = yfa.Game(oauth, 'nfl')
        
        logger.info("✅ Authentication successful!")
        
        # Get a recent league for testing
        leagues_2024 = game.league_ids(year=2024)
        if not leagues_2024:
            logger.info("No 2024 leagues, trying 2023...")
            leagues_2023 = game.league_ids(year=2023)
            test_league_id = leagues_2023[0] if leagues_2023 else None
        else:
            test_league_id = leagues_2024[0]
        
        if not test_league_id:
            logger.error("No leagues found for testing")
            return
        
        logger.info(f"🔍 Testing with league: {test_league_id}")
        
        # Get league object
        league = game.to_league(test_league_id)
        
        # Test different methods to see what data is available
        logger.info("\n" + "="*60)
        logger.info("TESTING LEAGUE METHODS")
        logger.info("="*60)
        
        # 1. League settings
        try:
            settings = league.settings()
            logger.info("\n📋 LEAGUE SETTINGS:")
            logger.info(json.dumps(settings, indent=2, default=str))
        except Exception as e:
            logger.error(f"Error getting settings: {e}")
        
        # 2. League standings  
        try:
            standings = league.standings()
            logger.info("\n🏆 LEAGUE STANDINGS:")
            logger.info(f"Type: {type(standings)}")
            logger.info(f"Length: {len(standings) if isinstance(standings, list) else 'Not a list'}")
            
            if isinstance(standings, list) and len(standings) > 0:
                logger.info("\nFirst team in standings:")
                logger.info(json.dumps(standings[0], indent=2, default=str))
                
                if len(standings) > 1:
                    logger.info("\nSecond team in standings:")
                    logger.info(json.dumps(standings[1], indent=2, default=str))
            else:
                logger.info("Standings data:")
                logger.info(json.dumps(standings, indent=2, default=str))
                
        except Exception as e:
            logger.error(f"Error getting standings: {e}")
        
        # 3. Try to get teams another way
        try:
            logger.info("\n👥 TRYING ALTERNATIVE TEAM METHODS:")
            
            # Check what methods are available on league object
            league_methods = [method for method in dir(league) if not method.startswith('_')]
            logger.info(f"Available league methods: {league_methods}")
            
            # Try teams() method if it exists
            if hasattr(league, 'teams'):
                try:
                    teams = league.teams()
                    logger.info(f"\nleague.teams() result:")
                    logger.info(f"Type: {type(teams)}")
                    if isinstance(teams, (list, dict)):
                        logger.info(json.dumps(teams, indent=2, default=str))
                except Exception as e:
                    logger.error(f"Error with league.teams(): {e}")
            
        except Exception as e:
            logger.error(f"Error exploring team methods: {e}")
        
        # 4. Try individual team access
        try:
            logger.info("\n🔍 TESTING INDIVIDUAL TEAM ACCESS:")
            
            # If we have standings data, try to get individual team
            standings = league.standings()
            if isinstance(standings, list) and len(standings) > 0:
                first_team_data = standings[0]
                team_id = first_team_data.get('team_key') or first_team_data.get('team_id')
                
                if team_id:
                    logger.info(f"Trying to get team object for: {team_id}")
                    try:
                        team = league.to_team(team_id)
                        logger.info(f"Team object created successfully")
                        
                        # Check team methods
                        team_methods = [method for method in dir(team) if not method.startswith('_')]
                        logger.info(f"Available team methods: {team_methods}")
                        
                        # Try different team methods
                        if hasattr(team, 'get_team_metadata'):
                            try:
                                metadata = team.get_team_metadata()
                                logger.info(f"\nTeam metadata:")
                                logger.info(json.dumps(metadata, indent=2, default=str))
                            except Exception as e:
                                logger.error(f"Error getting team metadata: {e}")
                        
                        if hasattr(team, 'team_name'):
                            try:
                                name = team.team_name()
                                logger.info(f"Team name: {name}")
                            except Exception as e:
                                logger.error(f"Error getting team name: {e}")
                        
                        if hasattr(team, 'managers'):
                            try:
                                managers = team.managers()
                                logger.info(f"Team managers:")
                                logger.info(json.dumps(managers, indent=2, default=str))
                            except Exception as e:
                                logger.error(f"Error getting team managers: {e}")
                                
                    except Exception as e:
                        logger.error(f"Error creating team object: {e}")
                        
        except Exception as e:
            logger.error(f"Error testing individual team access: {e}")
            
    except Exception as e:
        logger.error(f"Debug failed: {e}")

if __name__ == "__main__":
    debug_team_data() 