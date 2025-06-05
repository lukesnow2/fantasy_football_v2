#!/usr/bin/env python3
"""
Test the complete extraction with all data types
"""

import json
import logging
from datetime import datetime
from comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_complete_extraction():
    """Test the complete extraction on a recent league"""
    
    start_time = datetime.now()
    logger.info(f"🚀 Starting complete extraction test at {start_time}")
    
    # Create extractor
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    logger.info("🔑 Authenticating...")
    if not extractor.authenticate():
        logger.error("❌ Authentication failed")
        return
    
    # Test with just 2024 league to validate all data types
    logger.info("📊 Testing complete extraction on 2024 league...")
    
    # Manually set up a single league for testing
    test_league = {
        'league_id': '449.l.674707',
        'name': "Idaho's DEI Quota",
        'season': '2024',
        'game_code': 'nfl',
        'game_id': 449,
        'num_teams': 10,
        'current_week': 17,
        'start_week': 1,
        'end_week': 17,
        'league_type': 'private',
        'draft_status': 'postdraft',
        'is_pro_league': False,
        'is_cash_league': False,
        'url': '',
        'logo_url': None
    }
    
    try:
        # Extract league data
        league_data = extractor.extract_league_data(test_league)
        extractor.extracted_data['leagues'].append(league_data.__dict__)
        logger.info(f"✅ League data extracted")
        
        # Extract teams
        teams = extractor.extract_teams_for_league(test_league['league_id'])
        for team in teams:
            extractor.extracted_data['teams'].append(team.__dict__)
        logger.info(f"✅ Teams extracted: {len(teams)}")
        
        # Extract rosters
        rosters = extractor.extract_rosters_for_league(test_league['league_id'], teams)
        for roster in rosters:
            extractor.extracted_data['rosters'].append(roster.__dict__)
        logger.info(f"✅ Rosters extracted: {len(rosters)}")
        
        # Extract matchups
        matchups = extractor.extract_matchups_for_league(test_league['league_id'])
        for matchup in matchups:
            extractor.extracted_data['matchups'].append(matchup.__dict__)
        logger.info(f"✅ Matchups extracted: {len(matchups)}")
        
        # Extract transactions
        transactions = extractor.extract_transactions_for_league(test_league['league_id'])
        for transaction in transactions:
            extractor.extracted_data['transactions'].append(transaction.__dict__)
        logger.info(f"✅ Transactions extracted: {len(transactions)}")
        

        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'yahoo_fantasy_complete_test_{timestamp}.json'
        
        logger.info(f"💾 Saving results to {filename}...")
        extractor.save_to_json(filename)
        
        # Calculate runtime
        end_time = datetime.now()
        runtime = end_time - start_time
        
        # Print final statistics
        logger.info("\n" + "="*70)
        logger.info("🎯 COMPLETE EXTRACTION TEST RESULTS!")
        logger.info("="*70)
        
        logger.info(f"📅 Total Runtime: {runtime}")
        logger.info(f"🏆 League: {test_league['name']} ({test_league['season']})")
        logger.info(f"👥 Teams: {len(teams)}")
        logger.info(f"📋 Rosters: {len(rosters)}")
        logger.info(f"🏆 Matchups: {len(matchups)}")
        logger.info(f"💰 Transactions: {len(transactions)}")
        
        # Sample data analysis
        if rosters:
            logger.info(f"\n📋 ROSTER SAMPLE:")
            sample_roster = rosters[0]
            logger.info(f"  Player: {sample_roster.player_name}")
            logger.info(f"  Position: {sample_roster.position}")
            logger.info(f"  Eligible: {sample_roster.eligible_positions}")
            logger.info(f"  Week: {sample_roster.week}")
            logger.info(f"  Team: {sample_roster.team_id}")
        
        if matchups:
            logger.info(f"\n🏆 MATCHUP SAMPLE:")
            sample_matchup = matchups[0]
            logger.info(f"  Week: {sample_matchup.week}")
            logger.info(f"  Teams: {sample_matchup.team1_id} vs {sample_matchup.team2_id}")
            logger.info(f"  Score: {sample_matchup.team1_score} - {sample_matchup.team2_score}")
            logger.info(f"  Winner: {sample_matchup.winner_team_id}")
        
        if transactions:
            logger.info(f"\n💰 TRANSACTION SAMPLE:")
            sample_trans = transactions[0]
            logger.info(f"  Type: {sample_trans.type}")
            logger.info(f"  Player: {sample_trans.player_name}")
            logger.info(f"  From: {sample_trans.source_team_id}")
            logger.info(f"  To: {sample_trans.destination_team_id}")
        

        
        logger.info(f"\n💾 Complete test data saved to: {filename}")
        logger.info(f"🎯 All data types successfully extracted!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_complete_extraction() 