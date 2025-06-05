#!/usr/bin/env python3
"""
Debug specific problematic leagues to understand why they have no standings data
"""

import json
import logging
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_specific_leagues():
    """Debug specific problematic leagues to understand data availability"""
    try:
        # Authenticate
        oauth = OAuth2(None, None, from_file='oauth2.json')
        game = yfa.Game(oauth, 'nfl')
        
        logger.info("✅ Authentication successful!")
        
        # Test problematic leagues from different years
        problematic_leagues = [
            ("423.l.367879", "2023 - Wet Hot Tahoe Summer"),
            ("414.l.1088817", "2022 - Rocky Mountain High"), 
            ("399.l.440905", "2020 - Women & Women First"),
            ("371.l.722293", "2017 - The Great SF Draft"),
            ("242.l.10035622710", "2010 - Christian Fellowship League")
        ]
        
        # Also test a good league for comparison
        good_leagues = [
            ("449.l.674707", "2024 - Idaho's DEI Quota"),
            ("414.l.1194955", "2022 - Wet Hot Tahoe Summer")
        ]
        
        logger.info("\n" + "="*80)
        logger.info("TESTING PROBLEMATIC LEAGUES")
        logger.info("="*80)
        
        for league_id, description in problematic_leagues:
            logger.info(f"\n🔍 Testing: {description} ({league_id})")
            
            try:
                league = game.to_league(league_id)
                
                # Test league settings
                try:
                    settings = league.settings()
                    logger.info(f"  Settings: ✅ Available")
                    logger.info(f"    Current Week: {settings.get('current_week', 'N/A')}")
                    logger.info(f"    Start Week: {settings.get('start_week', 'N/A')}")
                    logger.info(f"    End Week: {settings.get('end_week', 'N/A')}")
                    logger.info(f"    Draft Status: {settings.get('draft_status', 'N/A')}")
                    logger.info(f"    Is Finished: {settings.get('is_finished', 'N/A')}")
                    logger.info(f"    Season Type: {settings.get('season_type', 'N/A')}")
                except Exception as e:
                    logger.error(f"  Settings: ❌ Error - {e}")
                
                # Test standings
                try:
                    standings = league.standings()
                    logger.info(f"  Standings: ✅ Available ({len(standings)} teams)")
                    
                    if len(standings) > 0:
                        sample_team = standings[0]
                        outcome_totals = sample_team.get('outcome_totals', {})
                        logger.info(f"    Sample team wins: {outcome_totals.get('wins', 'N/A')}")
                        logger.info(f"    Sample team losses: {outcome_totals.get('losses', 'N/A')}")
                        logger.info(f"    Sample team points_for: {sample_team.get('points_for', 'N/A')}")
                        
                        # Check if ALL teams have zero records
                        all_zero = True
                        for team in standings:
                            ot = team.get('outcome_totals', {})
                            wins = ot.get('wins', 0)
                            losses = ot.get('losses', 0)
                            if isinstance(wins, str) and wins.isdigit():
                                wins = int(wins)
                            if isinstance(losses, str) and losses.isdigit():
                                losses = int(losses)
                            if wins > 0 or losses > 0:
                                all_zero = False
                                break
                        
                        if all_zero:
                            logger.info(f"    ⚠️ ALL teams have zero records - likely draft-only league")
                        else:
                            logger.info(f"    ✅ Some teams have records")
                    
                except Exception as e:
                    logger.error(f"  Standings: ❌ Error - {e}")
                
                # Test teams
                try:
                    teams = league.teams()
                    if isinstance(teams, dict):
                        logger.info(f"  Teams: ✅ Available ({len(teams)} teams)")
                        
                        # Check if managers are available
                        manager_count = 0
                        for team_id, team_data in teams.items():
                            managers = team_data.get('managers', [])
                            if managers and len(managers) > 0:
                                manager = managers[0].get('manager', {})
                                if manager.get('nickname'):
                                    manager_count += 1
                        
                        logger.info(f"    Managers available: {manager_count}/{len(teams)}")
                        
                    else:
                        logger.info(f"  Teams: ❌ Unexpected format - {type(teams)}")
                        
                except Exception as e:
                    logger.error(f"  Teams: ❌ Error - {e}")
                    
            except Exception as e:
                logger.error(f"  League access: ❌ Error - {e}")
        
        logger.info("\n" + "="*80)
        logger.info("TESTING GOOD LEAGUES (FOR COMPARISON)")
        logger.info("="*80)
        
        for league_id, description in good_leagues:
            logger.info(f"\n✅ Testing: {description} ({league_id})")
            
            try:
                league = game.to_league(league_id)
                
                # Test standings
                standings = league.standings()
                logger.info(f"  Standings: ✅ {len(standings)} teams")
                
                # Check sample team data
                if len(standings) > 0:
                    sample_team = standings[0]
                    outcome_totals = sample_team.get('outcome_totals', {})
                    wins = outcome_totals.get('wins', 0)
                    losses = outcome_totals.get('losses', 0)
                    points_for = sample_team.get('points_for', 0)
                    
                    logger.info(f"    Sample: {wins}-{losses} record, {points_for} points")
                
            except Exception as e:
                logger.error(f"  Error: {e}")
                
    except Exception as e:
        logger.error(f"Debug failed: {e}")

if __name__ == "__main__":
    debug_specific_leagues() 