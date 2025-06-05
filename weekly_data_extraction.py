#!/usr/bin/env python3
"""
Weekly Fantasy Football Data Extraction
Optimized for in-season updates focusing on current data
"""

import json
import logging
import time
from datetime import datetime, timedelta
from comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WeeklyDataExtractor:
    """Optimized weekly fantasy football data extractor"""
    
    def __init__(self):
        self.extractor = YahooFantasyExtractor()
        self.start_time = datetime.now()
    
    @staticmethod
    def get_current_season_year():
        """Get the current fantasy football season year"""
        now = datetime.now()
        return now.year - 1 if now.month <= 7 else now.year
    
    @staticmethod
    def is_fantasy_season():
        """Check if we're currently in fantasy football season (Aug 18 - Jan 18)"""
        now = datetime.now()
        current_year = now.year
        
        # Fantasy season dates
        if now.month == 1:
            season_start = datetime(current_year - 1, 8, 18)
            season_end = datetime(current_year, 1, 18)
        else:
            season_start = datetime(current_year, 8, 18)
            season_end = datetime(current_year + 1, 1, 18)
        
        return season_start <= now <= season_end
    
    def authenticate(self):
        """Authenticate with Yahoo API"""
        logger.info("🔑 Authenticating...")
        if not self.extractor.authenticate():
            logger.error("❌ Authentication failed")
            return False
        return True
    
    def get_active_leagues(self):
        """Get current season active leagues"""
        current_season = self.get_current_season_year()
        logger.info(f"📋 Getting active leagues for {current_season} season...")
        
        all_leagues = self.extractor.get_all_leagues()
        active_leagues = [
            league for league in all_leagues 
            if league.get('season') == str(current_season) and league.get('draft_status') == 'postdraft'
        ]
        
        if not active_leagues:
            logger.warning(f"⚠️ No active leagues found for {current_season} season")
            return []
        
        logger.info(f"📊 Found {len(active_leagues)} active leagues")
        return active_leagues
    
    def extract_league_data(self, league_info, current_week):
        """Extract data for a single league"""
        league_id = league_info['league_id']
        league_name = league_info['name']
        
        data = {
            'leagues': [],
            'teams': [],
            'rosters': [],
            'matchups': [],
            'transactions': []
        }
        
        try:
            # League data
            league_data = self.extractor.extract_league_data(league_info)
            data['leagues'].append(league_data.__dict__)
            
            # Teams data
            teams = self.extractor.extract_teams_for_league(league_id)
            data['teams'].extend([team.__dict__ for team in teams])
            
            # Current rosters
            rosters = self.extractor.extract_rosters_for_league(league_id, teams)
            data['rosters'].extend([roster.__dict__ for roster in rosters])
            
            # Recent matchups (last 2 weeks)
            matchups = self.extractor.extract_matchups_for_league(league_id)
            recent_matchups = [m for m in matchups if m.week >= max(1, current_week - 2)]
            data['matchups'].extend([matchup.__dict__ for matchup in recent_matchups])
            
            # Recent transactions (last 7 days)
            recent_date = datetime.now() - timedelta(days=7)
            transactions = self.extractor.extract_transactions_for_league(league_id)
            recent_transactions = [t for t in transactions if t.timestamp >= recent_date]
            data['transactions'].extend([transaction.__dict__ for transaction in recent_transactions])
            
            # Rate limiting
            time.sleep(0.3)
            
        except Exception as e:
            logger.error(f"❌ Error processing league {league_name}: {e}")
        
        return data
    
    def serialize_data(self, data):
        """Convert datetime objects for JSON serialization"""
        def serialize_datetime(obj):
            return obj.isoformat() if isinstance(obj, datetime) else obj
        
        json_data = {}
        for key, value in data.items():
            json_data[key] = []
            for item in value:
                if isinstance(item, dict):
                    json_item = {k: serialize_datetime(v) for k, v in item.items()}
                else:
                    json_item = {k: serialize_datetime(v) for k, v in item.__dict__.items()}
                json_data[key].append(json_item)
        
        return json_data
    
    def save_results(self, data):
        """Save extraction results to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'yahoo_fantasy_weekly_data_{timestamp}.json'
        
        logger.info(f"💾 Saving weekly results to {filename}...")
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        return filename
    
    def log_summary(self, data, active_leagues, filename):
        """Log extraction summary"""
        runtime = datetime.now() - self.start_time
        total_records = sum(len(data.get(table, [])) for table in data.keys())
        file_size_mb = len(json.dumps(data, default=str)) / (1024 * 1024)
        
        logger.info("\n" + "="*80)
        logger.info("🗓️ WEEKLY DATA EXTRACTION COMPLETE!")
        logger.info("="*80)
        logger.info(f"⏱️ Total Runtime: {runtime}")
        logger.info(f"🏈 Season: {self.get_current_season_year()}")
        logger.info(f"🏆 Active Leagues: {len(active_leagues)}")
        logger.info(f"👥 Teams Updated: {len(data['teams'])}")
        logger.info(f"📋 Current Rosters: {len(data['rosters'])}")
        logger.info(f"🏆 Recent Matchups: {len(data['matchups'])}")
        logger.info(f"💰 Recent Transactions: {len(data['transactions'])}")
        logger.info(f"📈 TOTAL WEEKLY RECORDS: {total_records:,}")
        logger.info(f"💾 File: {filename} ({file_size_mb:.1f} MB)")
        logger.info(f"🚀 Ready for deployment!")
    
    def run(self):
        """Run weekly data extraction"""
        logger.info(f"🗓️ Starting WEEKLY fantasy football data extraction at {self.start_time}")
        
        # Check if we're in season
        if not self.is_fantasy_season():
            logger.info("⏸️ Not currently in fantasy season (Aug 18 - Jan 18) - skipping extraction")
            return None
        
        # Authenticate
        if not self.authenticate():
            return None
        
        # Get active leagues
        active_leagues = self.get_active_leagues()
        if not active_leagues:
            return None
        
        # Initialize combined data structure
        combined_data = {
            'leagues': [],
            'teams': [],
            'rosters': [],
            'matchups': [],
            'transactions': [],
            'statistics': []
        }
        
        # Extract data for each league
        for i, league_info in enumerate(active_leagues, 1):
            league_name = league_info['name']
            current_week = league_info.get('current_week', 1)
            
            logger.info(f"📋 Processing league {i}/{len(active_leagues)}: {league_name}")
            logger.info(f"  📅 Current week: {current_week}")
            
            league_data = self.extract_league_data(league_info, current_week)
            
            # Combine league data
            for key in combined_data.keys():
                if key in league_data:
                    combined_data[key].extend(league_data[key])
        
        # Serialize and save data
        json_data = self.serialize_data(combined_data)
        filename = self.save_results(json_data)
        
        # Log summary
        self.log_summary(json_data, active_leagues, filename)
        
        return filename

def main():
    """Main entry point"""
    extractor = WeeklyDataExtractor()
    return extractor.run()

if __name__ == "__main__":
    main() 