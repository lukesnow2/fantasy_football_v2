#!/usr/bin/env python3
"""
Test script for comprehensive Yahoo Fantasy data extractor
Tests on a small subset of leagues first
"""

import json
import logging
from comprehensive_data_extractor import YahooFantasyExtractor
from database_schema import save_schema_to_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_extractor_limited():
    """Test the extractor on a limited subset of leagues"""
    logger.info("🧪 Testing comprehensive data extractor (limited scope)...")
    
    # Create extractor instance
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    if not extractor.authenticate():
        logger.error("❌ Authentication failed")
        return False
    
    # Get all leagues first
    all_leagues = extractor.get_all_leagues()
    
    if not all_leagues:
        logger.error("❌ No leagues found")
        return False
    
    logger.info(f"📋 Found {len(all_leagues)} total leagues")
    
    # Test on just the most recent 3 leagues for validation
    test_leagues = all_leagues[-3:] if len(all_leagues) >= 3 else all_leagues
    
    logger.info(f"🧪 Testing extraction on {len(test_leagues)} recent leagues:")
    for league in test_leagues:
        logger.info(f"  - {league['name']} ({league['season']})")
    
    # Process test leagues
    test_data = {
        'leagues': [],
        'teams': [],
        'rosters': [],
        'matchups': [],
        'transactions': [],
        'statistics': []
    }
    
    for i, league_info in enumerate(test_leagues, 1):
        league_id = league_info['league_id']
        league_name = league_info['name']
        logger.info(f"📋 Processing test league {i}/{len(test_leagues)}: {league_name} ({league_info['season']})")
        
        try:
            # Extract league data
            league_data = extractor.extract_league_data(league_info)
            test_data['leagues'].append(league_data.__dict__)
            
            # Extract teams
            teams = extractor.extract_teams_for_league(league_id)
            for team in teams:
                test_data['teams'].append(team.__dict__)
            
            # Extract rosters (limit to avoid too much data in test)
            if len(teams) <= 12:  # Only extract rosters for smaller leagues
                rosters = extractor.extract_rosters_for_league(league_id, teams)
                for roster in rosters:
                    test_data['rosters'].append(roster.__dict__)
            else:
                logger.info(f"  👥 Skipping roster extraction for large league ({len(teams)} teams)")
            
            # Extract matchups
            matchups = extractor.extract_matchups_for_league(league_id)
            for matchup in matchups:
                test_data['matchups'].append(matchup.__dict__)
            
            # Extract transactions
            transactions = extractor.extract_transactions_for_league(league_id)
            for transaction in transactions:
                test_data['transactions'].append(transaction.__dict__)
            
        except Exception as e:
            logger.error(f"❌ Error processing test league {league_name}: {e}")
            continue
    
    # Log test results
    logger.info("✅ Test extraction completed!")
    logger.info(f"📊 Test Results Summary:")
    logger.info(f"  - Leagues: {len(test_data['leagues'])}")
    logger.info(f"  - Teams: {len(test_data['teams'])}")
    logger.info(f"  - Rosters: {len(test_data['rosters'])}")
    logger.info(f"  - Matchups: {len(test_data['matchups'])}")
    logger.info(f"  - Transactions: {len(test_data['transactions'])}")
    
    # Save test data to file
    try:
        # Convert dataclass objects to dicts for JSON serialization
        json_data = {}
        for key, value in test_data.items():
            json_data[key] = []
            for item in value:
                if hasattr(item, '__dict__'):
                    json_item = {}
                    for k, v in item.__dict__.items():
                        json_item[k] = v.isoformat() if hasattr(v, 'isoformat') else v
                    json_data[key].append(json_item)
                else:
                    json_data[key].append(item)
        
        with open('test_extraction_results.json', 'w') as f:
            json.dump(json_data, f, indent=2, default=str)
        
        logger.info("💾 Test data saved to test_extraction_results.json")
        
    except Exception as e:
        logger.error(f"❌ Error saving test data: {e}")
    
    # Show sample data
    if test_data['leagues']:
        logger.info("📋 Sample League Data:")
        sample_league = test_data['leagues'][0]
        for key, value in sample_league.items():
            logger.info(f"  {key}: {value}")
    
    if test_data['teams']:
        logger.info("📊 Sample Team Data:")
        sample_team = test_data['teams'][0]
        for key, value in sample_team.items():
            logger.info(f"  {key}: {value}")
    
    return True

def generate_schema():
    """Generate the database schema file"""
    logger.info("🗃️ Generating database schema...")
    schema_file = save_schema_to_file()
    logger.info(f"✅ Database schema generated: {schema_file}")

def main():
    """Main test function"""
    logger.info("🚀 Starting comprehensive data extractor test...")
    
    # Generate database schema
    generate_schema()
    
    # Test extraction
    success = test_extractor_limited()
    
    if success:
        logger.info("✅ Test completed successfully!")
        logger.info("🎯 Next steps:")
        logger.info("  1. Review test_extraction_results.json to validate data structure")
        logger.info("  2. Check yahoo_fantasy_schema.sql for database setup")
        logger.info("  3. Run comprehensive_data_extractor.py for full extraction")
        logger.info("  4. Provide database endpoint for data loading")
    else:
        logger.error("❌ Test failed. Check logs for errors.")

if __name__ == "__main__":
    main() 