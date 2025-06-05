#!/usr/bin/env python3
"""
Debug script to examine teams API response structure specifically
"""

import json
import logging
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_teams_api():
    """Debug teams API to understand structure"""
    try:
        # Authenticate
        oauth = OAuth2(None, None, from_file='oauth2.json')
        game = yfa.Game(oauth, 'nfl')
        
        logger.info("✅ Authentication successful!")
        
        # Test with 2024 league
        league_id = "449.l.674707"
        logger.info(f"🔍 Testing teams API for league: {league_id}")
        
        league = game.to_league(league_id)
        
        # Get teams data
        logger.info("\n" + "="*60)
        logger.info("TESTING league.teams() METHOD")
        logger.info("="*60)
        
        try:
            teams_data = league.teams()
            
            logger.info(f"Teams data type: {type(teams_data)}")
            logger.info(f"Teams data length: {len(teams_data) if isinstance(teams_data, (list, dict)) else 'N/A'}")
            
            # Show full structure
            logger.info("\nFull teams data structure:")
            logger.info(json.dumps(teams_data, indent=2, default=str))
            
            # Try to extract manager names manually
            if isinstance(teams_data, list):
                logger.info("\n🔍 ATTEMPTING MANAGER EXTRACTION:")
                for i, team in enumerate(teams_data[:2]):  # Check first 2 teams
                    logger.info(f"\nTeam {i+1}:")
                    
                    team_id = team.get('team_key', 'unknown')
                    team_name = team.get('name', 'unknown')
                    logger.info(f"  Team ID: {team_id}")
                    logger.info(f"  Team Name: {team_name}")
                    
                    # Check different possible manager fields
                    managers = team.get('managers', [])
                    logger.info(f"  Managers field: {managers}")
                    
                    if managers and isinstance(managers, list):
                        for j, manager_entry in enumerate(managers):
                            logger.info(f"    Manager {j+1}: {manager_entry}")
                            
                            if isinstance(manager_entry, dict):
                                manager = manager_entry.get('manager', {})
                                logger.info(f"      Manager nested: {manager}")
                                
                                if isinstance(manager, dict):
                                    nickname = manager.get('nickname')
                                    display_name = manager.get('display_name')
                                    guid = manager.get('guid')
                                    logger.info(f"        Nickname: {nickname}")
                                    logger.info(f"        Display Name: {display_name}")
                                    logger.info(f"        GUID: {guid}")
                    
                    # Check other possible manager fields
                    owner = team.get('owner', {})
                    if owner:
                        logger.info(f"  Owner field: {owner}")
                    
                    manager_field = team.get('manager', {})
                    if manager_field:
                        logger.info(f"  Manager field: {manager_field}")
                    
        except Exception as e:
            logger.error(f"Error with teams() method: {e}")
        
        # Also test individual team access
        logger.info("\n" + "="*60)
        logger.info("TESTING INDIVIDUAL TEAM ACCESS")
        logger.info("="*60)
        
        try:
            standings = league.standings()
            if standings and len(standings) > 0:
                first_team_data = standings[0]
                team_id = first_team_data.get('team_key')
                
                if team_id:
                    logger.info(f"Getting individual team: {team_id}")
                    team = league.to_team(team_id)
                    
                    # Check team methods
                    team_methods = [method for method in dir(team) if not method.startswith('_')]
                    logger.info(f"Available team methods: {team_methods}")
                    
                    # Try managers method if it exists
                    if hasattr(team, 'managers'):
                        try:
                            managers = team.managers()
                            logger.info(f"Individual team managers(): {managers}")
                        except Exception as e:
                            logger.error(f"Error with team.managers(): {e}")
                    
                    # Try metadata method if it exists
                    if hasattr(team, 'metadata'):
                        try:
                            metadata = team.metadata()
                            logger.info(f"Individual team metadata(): {metadata}")
                        except Exception as e:
                            logger.error(f"Error with team.metadata(): {e}")
                            
        except Exception as e:
            logger.error(f"Error with individual team access: {e}")
            
    except Exception as e:
        logger.error(f"Debug failed: {e}")

if __name__ == "__main__":
    debug_teams_api() 