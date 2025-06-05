#!/usr/bin/env python3
"""
Test draft data extraction functionality
"""

import logging
from comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_draft_extraction():
    """Test draft extraction on a single league"""
    
    logger.info("🎯 Testing draft data extraction...")
    
    # Create extractor
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    if not extractor.authenticate():
        logger.error("❌ Authentication failed")
        return
    
    # Get first league to test
    leagues = extractor.get_all_leagues()
    
    if not leagues:
        logger.error("❌ No leagues found")
        return
    
    # Test on first league
    test_league = leagues[0]
    league_id = test_league['league_id']
    league_name = test_league['name']
    
    logger.info(f"🧪 Testing draft extraction on: {league_name} ({league_id})")
    
    # Extract draft data
    draft_picks = extractor.extract_draft_for_league(league_id)
    
    if not draft_picks:
        logger.warning("⚠️ No draft picks found - this might be expected for some leagues")
        return
    
    # Display results
    logger.info(f"✅ Successfully extracted {len(draft_picks)} draft picks!")
    
    # Show first few picks
    logger.info("📋 First 10 draft picks:")
    for i, pick in enumerate(draft_picks[:10]):
        cost_str = f" (${pick.cost})" if pick.cost else ""
        logger.info(f"  {pick.pick_number}. Round {pick.round_number}: {pick.player_name} ({pick.position}) -> Team {pick.team_id}{cost_str}")
    
    # Summary stats
    total_picks = len(draft_picks)
    rounds = len(set(pick.round_number for pick in draft_picks))
    teams = len(set(pick.team_id for pick in draft_picks))
    auction_picks = sum(1 for pick in draft_picks if pick.is_auction_draft)
    
    logger.info(f"\n📊 Draft Summary:")
    logger.info(f"  Total picks: {total_picks}")
    logger.info(f"  Rounds: {rounds}")
    logger.info(f"  Teams: {teams}")
    logger.info(f"  Auction picks: {auction_picks}")
    logger.info(f"  Snake picks: {total_picks - auction_picks}")
    
    if auction_picks > 0:
        costs = [pick.cost for pick in draft_picks if pick.cost is not None]
        if costs:
            total_cost = sum(costs)
            avg_cost = total_cost / len(costs)
            max_cost = max(costs)
            logger.info(f"  Total spent: ${total_cost}")
            logger.info(f"  Average cost: ${avg_cost:.1f}")
            logger.info(f"  Highest pick: ${max_cost}")

if __name__ == "__main__":
    test_draft_extraction() 