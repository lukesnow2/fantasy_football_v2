#!/usr/bin/env python3
"""
Weekly Fantasy Football Data Extraction
Optimized for in-season updates focusing on current data
"""

import json
import logging
from datetime import datetime, timedelta
from comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_current_season_year():
    """Get the current fantasy football season year"""
    now = datetime.now()
    # Fantasy season starts in August, so if we're in Jan-July, it's the previous year's season
    if now.month <= 7:
        return now.year - 1
    else:
        return now.year

def is_fantasy_season():
    """Check if we're currently in fantasy football season (Aug 18 - Jan 18)"""
    now = datetime.now()
    current_year = now.year
    
    # Fantasy season dates
    season_start = datetime(current_year, 8, 18)
    season_end = datetime(current_year + 1, 1, 18)
    
    # If we're in January, check against previous season
    if now.month == 1:
        season_start = datetime(current_year - 1, 8, 18)
        season_end = datetime(current_year, 1, 18)
    
    return season_start <= now <= season_end

def main():
    """Run weekly data extraction focusing on current season"""
    
    start_time = datetime.now()
    logger.info(f"🗓️ Starting WEEKLY fantasy football data extraction at {start_time}")
    
    # Check if we're in season
    if not is_fantasy_season():
        logger.info("⏸️ Not currently in fantasy season (Aug 18 - Jan 18) - skipping extraction")
        return
    
    current_season = get_current_season_year()
    logger.info(f"🏈 Current fantasy season: {current_season}")
    
    # Create extractor
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    logger.info("🔑 Authenticating...")
    if not extractor.authenticate():
        logger.error("❌ Authentication failed")
        return
    
    # Get all leagues but filter for current season
    logger.info("📋 Getting current season leagues...")
    all_leagues = extractor.get_all_leagues()
    
    # Filter for current season only
    current_season_leagues = [
        league for league in all_leagues 
        if league.get('season') == str(current_season) and league.get('draft_status') == 'postdraft'
    ]
    
    if not current_season_leagues:
        logger.warning(f"⚠️ No active leagues found for {current_season} season")
        return
    
    logger.info(f"📊 Found {len(current_season_leagues)} active leagues for {current_season} season")
    
    # Initialize data structure
    weekly_data = {
        'leagues': [],
        'teams': [],
        'rosters': [],
        'matchups': [],
        'transactions': [],
        'statistics': []
    }
    
    # Extract data for current season leagues only
    for i, league_info in enumerate(current_season_leagues, 1):
        league_id = league_info['league_id']
        league_name = league_info['name']
        logger.info(f"📋 Processing league {i}/{len(current_season_leagues)}: {league_name}")
        
        try:
            # Extract league data
            league_data = extractor.extract_league_data(league_info)
            weekly_data['leagues'].append(league_data.__dict__)
            
            # Extract teams (always needed for current standings)
            teams = extractor.extract_teams_for_league(league_id)
            for team in teams:
                weekly_data['teams'].append(team.__dict__)
            
            # Extract current week rosters (more relevant than all historical rosters)
            current_week = league_info.get('current_week', 1)
            logger.info(f"  📅 Extracting rosters for current week: {current_week}")
            
            # Get rosters for current week only (much faster)
            rosters = extractor.extract_rosters_for_league(league_id, teams)
            for roster in rosters:
                weekly_data['rosters'].append(roster.__dict__)
            
            # Extract recent matchups (last 2 weeks)
            matchups = extractor.extract_matchups_for_league(league_id)
            # Filter for recent matchups only
            recent_matchups = [m for m in matchups if m.week >= max(1, current_week - 2)]
            for matchup in recent_matchups:
                weekly_data['matchups'].append(matchup.__dict__)
            
            # Extract recent transactions (last 7 days)
            recent_date = datetime.now() - timedelta(days=7)
            transactions = extractor.extract_transactions_for_league(league_id)
            # Filter for recent transactions
            recent_transactions = [
                t for t in transactions 
                if t.timestamp >= recent_date
            ]
            for transaction in recent_transactions:
                weekly_data['transactions'].append(transaction.__dict__)
            
            # Rate limiting between leagues
            import time
            time.sleep(0.3)
            
        except Exception as e:
            logger.error(f"❌ Error processing league {league_name}: {e}")
            continue
    
    # Convert datetime objects for JSON serialization
    def serialize_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj
    
    # Prepare JSON data
    json_data = {}
    for key, value in weekly_data.items():
        json_data[key] = []
        for item in value:
            if isinstance(item, dict):
                json_item = {k: serialize_datetime(v) for k, v in item.items()}
            else:
                json_item = {k: serialize_datetime(v) for k, v in item.__dict__.items()}
            json_data[key].append(json_item)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'yahoo_fantasy_weekly_data_{timestamp}.json'
    
    logger.info(f"💾 Saving weekly results to {filename}...")
    with open(filename, 'w') as f:
        json.dump(json_data, f, indent=2, default=str)
    
    # Calculate runtime and show summary
    end_time = datetime.now()
    runtime = end_time - start_time
    
    logger.info("\n" + "="*80)
    logger.info("🗓️ WEEKLY DATA EXTRACTION COMPLETE!")
    logger.info("="*80)
    logger.info(f"⏱️ Total Runtime: {runtime}")
    logger.info(f"🏈 Season: {current_season}")
    logger.info(f"🏆 Active Leagues: {len(current_season_leagues)}")
    logger.info(f"👥 Teams Updated: {len(json_data['teams'])}")
    logger.info(f"📋 Current Rosters: {len(json_data['rosters'])}")
    logger.info(f"🏆 Recent Matchups: {len(json_data['matchups'])}")
    logger.info(f"💰 Recent Transactions: {len(json_data['transactions'])}")
    
    # Calculate totals
    total_records = sum(len(json_data.get(table, [])) for table in json_data.keys())
    logger.info(f"📈 TOTAL WEEKLY RECORDS: {total_records:,}")
    
    # File size
    file_size_mb = len(json.dumps(json_data, default=str)) / (1024 * 1024)
    logger.info(f"\n💾 Weekly data saved to: {filename}")
    logger.info(f"💾 File size: {file_size_mb:.1f} MB")
    logger.info(f"🚀 Ready for deployment!")
    
    return filename

if __name__ == "__main__":
    main() 