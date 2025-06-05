#!/usr/bin/env python3
"""
Test the fixed team extraction logic
"""

import json
import logging
from comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_fixed_extraction():
    """Test the fixed extraction on a few leagues"""
    
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    if not extractor.authenticate():
        logger.error("Authentication failed")
        return
    
    # Test with a few recent leagues
    test_leagues = [
        "449.l.674707",  # 2024
        "423.l.841006",  # 2023
        "414.l.1194955"  # 2022
    ]
    
    for league_id in test_leagues:
        logger.info(f"\n{'='*60}")
        logger.info(f"🔍 Testing league: {league_id}")
        logger.info(f"{'='*60}")
        
        try:
            teams = extractor.extract_teams_for_league(league_id)
            
            logger.info(f"📊 Extracted {len(teams)} teams")
            
            # Show details for first few teams
            for i, team in enumerate(teams[:3]):
                logger.info(f"\n  Team {i+1}:")
                logger.info(f"    ID: {team.team_id}")
                logger.info(f"    Name: {team.name}")
                logger.info(f"    Manager: {team.manager_name}")
                logger.info(f"    Record: {team.wins}-{team.losses}-{team.ties}")
                logger.info(f"    Points For: {team.points_for}")
                logger.info(f"    Points Against: {team.points_against}")
                logger.info(f"    Playoff Seed: {team.playoff_seed}")
                logger.info(f"    FAAB Balance: {team.faab_balance}")
                logger.info(f"    Waiver Priority: {team.waiver_priority}")
                logger.info(f"    Logo URL: {team.team_logo_url}")
            
            # Summary stats
            teams_with_records = sum(1 for t in teams if t.wins > 0 or t.losses > 0)
            teams_with_managers = sum(1 for t in teams if t.manager_name)
            teams_with_points = sum(1 for t in teams if t.points_for > 0)
            
            logger.info(f"\n📈 SUMMARY STATS:")
            logger.info(f"  Teams with records: {teams_with_records}/{len(teams)} ({teams_with_records/len(teams)*100:.1f}%)")
            logger.info(f"  Teams with managers: {teams_with_managers}/{len(teams)} ({teams_with_managers/len(teams)*100:.1f}%)")
            logger.info(f"  Teams with points: {teams_with_points}/{len(teams)} ({teams_with_points/len(teams)*100:.1f}%)")
            
        except Exception as e:
            logger.error(f"❌ Error testing league {league_id}: {e}")

if __name__ == "__main__":
    test_fixed_extraction() 