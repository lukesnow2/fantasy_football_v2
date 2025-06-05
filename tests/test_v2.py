#!/usr/bin/env python3
"""
Quick test of v2 with a year that has data
"""
from yahoo_fantasy_oauth_v2 import YahooFantasyOAuthAutomation, LeagueDataCollectionTask, load_credentials
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Load credentials
    client_key, client_secret = load_credentials()
    
    if not client_key or not client_secret:
        logger.error("No credentials found")
        return
    
    # Create OAuth handler
    oauth_handler = YahooFantasyOAuthAutomation(client_key, client_secret)
    
    # Authenticate
    game = oauth_handler.authenticate(silent=True)
    
    if game:
        logger.info("✅ Authentication successful!")
        
        # Get ALL leagues with no year limit
        logger.info("🔍 Getting ALL leagues across all years...")
        all_leagues = oauth_handler.get_leagues()  # No year parameter = all years
        
        logger.info(f"✅ Found {len(all_leagues)} total leagues across all years")
        
        # Filter out Yahoo Public leagues and get detailed data
        logger.info("🔍 Filtering out Yahoo Public leagues...")
        filtered_leagues = []
        seasons = {}
        
        for league_id in all_leagues:
            league_data = oauth_handler.get_league_data(league_id)
            if league_data and league_data['season'] != 'N/A':
                # Filter out Yahoo Public leagues
                if "Yahoo Public" not in league_data['name']:
                    filtered_leagues.append(league_data)
                    season = league_data['season']
                    if season not in seasons:
                        seasons[season] = 0
                    seasons[season] += 1
                else:
                    logger.debug(f"Filtered out: {league_data['name']} (Yahoo Public)")
        
        logger.info(f"✅ After filtering: {len(filtered_leagues)} leagues (removed {len(all_leagues) - len(filtered_leagues)} Yahoo Public leagues)")
        
        # Show ALL non-public leagues
        logger.info("📋 ALL non-public leagues:")
        for i, league in enumerate(filtered_leagues, 1):
            logger.info(f"{i:2d}. {league['name']} ({league['season']}) - {league['num_teams']} teams - ID: {league['league_id']}")
        
        logger.info(f"\n📈 League distribution by season:")
        for season in sorted(seasons.keys()):
            logger.info(f"  {season}: {seasons[season]} leagues")
    else:
        logger.error("❌ Authentication failed")

if __name__ == "__main__":
    main() 