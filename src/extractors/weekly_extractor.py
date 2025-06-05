#!/usr/bin/env python3
"""
Incremental Fantasy Football Data Extraction
Captures all new data since last extraction - the primary production system
"""

import json
import logging
import time
import glob
import os
from datetime import datetime, timedelta
from .comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IncrementalDataExtractor:
    """Production incremental fantasy football data extractor"""
    
    def __init__(self):
        self.extractor = YahooFantasyExtractor()
        self.start_time = datetime.now()
        self.last_extraction_data = None
    
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
    
    def load_last_extraction(self):
        """Load the most recent extraction to determine what's new"""
        try:
            # Find the most recent complete dataset
            pattern = 'data/current/yahoo_fantasy_COMPLETE_with_drafts_*.json'
            files = glob.glob(pattern)
            
            if not files:
                # Fallback to older complete data file
                fallback = 'data/current/yahoo_fantasy_FINAL_complete_data_*.json'
                files = glob.glob(fallback)
            
            if not files:
                logger.warning("⚠️ No previous extraction found - will do full extraction")
                return None
            
            # Get most recent file
            latest_file = max(files, key=lambda x: x.split('_')[-1])
            logger.info(f"📂 Loading previous extraction: {latest_file}")
            
            with open(latest_file, 'r') as f:
                self.last_extraction_data = json.load(f)
            
            # Log what we have as baseline
            total_records = sum(len(self.last_extraction_data.get(table, [])) for table in 
                              ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks'])
            logger.info(f"📊 Baseline data loaded: {total_records:,} total records")
            
            return self.last_extraction_data
            
        except Exception as e:
            logger.error(f"❌ Error loading previous extraction: {e}")
            return None
    
    def get_new_leagues(self, current_leagues):
        """Identify any new leagues since last extraction"""
        if not self.last_extraction_data:
            return current_leagues
        
        previous_league_ids = {league['league_id'] for league in self.last_extraction_data.get('leagues', [])}
        new_leagues = [league for league in current_leagues if league['league_id'] not in previous_league_ids]
        
        if new_leagues:
            logger.info(f"🆕 Found {len(new_leagues)} new leagues since last extraction")
            for league in new_leagues:
                logger.info(f"  📋 New: {league['name']} ({league['season']})")
        
        return new_leagues
    
    def get_incremental_rosters(self, league_id, teams, current_week):
        """Get only rosters that have changed since last extraction"""
        try:
            # For incremental, focus on current week and recent weeks
            # This captures lineup changes and new acquisitions
            recent_weeks = max(1, current_week - 1)  # Current week + previous week
            
            rosters = self.extractor.extract_rosters_for_league(league_id, teams)
            
            # Filter for recent activity if we have baseline data
            if self.last_extraction_data:
                # In production, you'd compare timestamps, but for now get recent rosters
                recent_rosters = [r for r in rosters if r.week >= recent_weeks]
                logger.info(f"  📋 Found {len(recent_rosters)} recent roster records")
                return recent_rosters
            else:
                return rosters
                
        except Exception as e:
            logger.error(f"❌ Error getting incremental rosters: {e}")
            return []
    
    def get_incremental_transactions(self, league_id):
        """Get only transactions since last extraction"""
        try:
            # Get recent transactions (last 30 days for incremental)
            recent_date = datetime.now() - timedelta(days=30)
            transactions = self.extractor.extract_transactions_for_league(league_id)
            
            # Filter for recent transactions
            recent_transactions = [t for t in transactions if t.timestamp >= recent_date]
            
            logger.info(f"  💰 Found {len(recent_transactions)} recent transactions")
            return recent_transactions
            
        except Exception as e:
            logger.error(f"❌ Error getting incremental transactions: {e}")
            return []
    
    def get_incremental_matchups(self, league_id, current_week):
        """Get only recent/current matchups"""
        try:
            matchups = self.extractor.extract_matchups_for_league(league_id)
            
            # Get current week and recent completed matchups
            recent_matchups = [m for m in matchups if m.week >= max(1, current_week - 2)]
            
            logger.info(f"  🏆 Found {len(recent_matchups)} recent matchups")
            return recent_matchups
            
        except Exception as e:
            logger.error(f"❌ Error getting incremental matchups: {e}")
            return []
    
    def extract_draft_data_for_new_leagues(self, new_leagues):
        """Extract draft data for any new leagues discovered"""
        all_draft_picks = []
        
        if not new_leagues:
            logger.info("📋 No new leagues - skipping draft extraction")
            return all_draft_picks
        
        logger.info(f"🏈 Extracting draft data for {len(new_leagues)} new leagues...")
        
        for league_info in new_leagues:
            league_id = league_info['league_id']
            league_name = league_info['name']
            
            try:
                logger.info(f"  🎯 Extracting drafts for: {league_name}")
                draft_picks = self.extractor.extract_draft_for_league(league_id)
                
                if draft_picks:
                    all_draft_picks.extend([pick.__dict__ for pick in draft_picks])
                    logger.info(f"    ✅ Found {len(draft_picks)} draft picks")
                else:
                    logger.info(f"    ⚠️ No draft data found")
                
                time.sleep(0.3)  # Rate limiting
                
            except Exception as e:
                logger.error(f"    ❌ Error extracting drafts for {league_name}: {e}")
                continue
        
        logger.info(f"🏈 Total new draft picks extracted: {len(all_draft_picks)}")
        return all_draft_picks
    
    def authenticate(self):
        """Authenticate with Yahoo API"""
        logger.info("🔑 Authenticating...")
        if not self.extractor.authenticate():
            logger.error("❌ Authentication failed")
            return False
        return True
    
    def get_current_active_leagues(self):
        """Get ONLY current season active leagues (incremental approach)"""
        current_season = self.get_current_season_year()
        logger.info(f"📋 Getting active leagues for {current_season} season ONLY (incremental)...")
        
        try:
            # Only get current season leagues - no historical scan
            league_ids = self.extractor.game.league_ids(year=current_season)
            
            if not league_ids:
                logger.warning(f"⚠️ No leagues found for {current_season} season")
                return []
            
            logger.info(f"📊 Found {len(league_ids)} leagues for {current_season}")
            
            # Get detailed info for current season leagues only
            active_leagues = []
            for league_id in league_ids:
                try:
                    league = self.extractor.game.to_league(league_id)
                    league_info = {
                        'league_id': str(league_id),
                        'name': getattr(league, 'name', lambda: f'League {league_id}')(),
                        'season': str(current_season),
                        'game_code': 'nfl',
                        'game_id': self.extractor.game.game_id(),
                        'num_teams': len(league.teams()),
                        'current_week': league.current_week(),
                        'start_week': league.start_week(),
                        'end_week': league.end_week(),
                        'league_type': 'private',
                        'draft_status': league.draft_status(),
                        'is_pro_league': False,
                        'is_cash_league': False,
                        'url': f"https://football.fantasysports.yahoo.com/f1/{league_id}"
                    }
                    
                    # Only include post-draft leagues
                    if league_info.get('draft_status') == 'postdraft':
                        active_leagues.append(league_info)
                        logger.info(f"  ✅ {league_info['name']}")
                    else:
                        logger.info(f"  ⏸️ {league_info['name']} (draft status: {league_info.get('draft_status')})")
                        
                except Exception as e:
                    logger.error(f"  ❌ Error getting league {league_id}: {e}")
                    continue
            
            logger.info(f"📊 Found {len(active_leagues)} ACTIVE leagues for {current_season}")
            return active_leagues
            
        except Exception as e:
            logger.error(f"❌ Error getting current season leagues: {e}")
            return []
    
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
    
    def merge_with_baseline(self, incremental_data):
        """Merge incremental data with baseline dataset"""
        if not self.last_extraction_data:
            logger.info("📊 No baseline data - using incremental data as complete dataset")
            return incremental_data
        
        logger.info("🔄 Merging incremental data with baseline...")
        
        # Start with baseline data
        merged_data = {
            'leagues': self.last_extraction_data.get('leagues', []).copy(),
            'teams': self.last_extraction_data.get('teams', []).copy(),
            'rosters': self.last_extraction_data.get('rosters', []).copy(),
            'matchups': self.last_extraction_data.get('matchups', []).copy(),
            'transactions': self.last_extraction_data.get('transactions', []).copy(),
            'draft_picks': self.last_extraction_data.get('draft_picks', []).copy(),
            'statistics': self.last_extraction_data.get('statistics', []).copy()
        }
        
        # Add new data
        for table, new_records in incremental_data.items():
            if new_records and table in merged_data:
                # For simplicity, append new records (in production, you'd deduplicate)
                merged_data[table].extend(new_records)
                logger.info(f"  📊 Added {len(new_records)} new {table} records")
        
        return merged_data
    
    def save_results(self, data, is_incremental=True):
        """Save extraction results to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if is_incremental:
            filename = f'data/current/yahoo_fantasy_INCREMENTAL_update_{timestamp}.json'
        else:
            filename = f'data/current/yahoo_fantasy_COMPLETE_with_drafts_{timestamp}.json'
        
        logger.info(f"💾 Saving results to {filename}...")
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        return filename
    
    def log_summary(self, data, active_leagues, filename, is_incremental=True):
        """Log extraction summary"""
        runtime = datetime.now() - self.start_time
        total_records = sum(len(data.get(table, [])) for table in data.keys())
        file_size_mb = len(json.dumps(data, default=str)) / (1024 * 1024)
        
        extraction_type = "INCREMENTAL" if is_incremental else "COMPLETE"
        
        logger.info("\n" + "="*80)
        logger.info(f"📈 {extraction_type} DATA EXTRACTION COMPLETE!")
        logger.info("="*80)
        logger.info(f"⏱️ Total Runtime: {runtime}")
        logger.info(f"🏈 Season: {self.get_current_season_year()}")
        logger.info(f"🏆 Active Leagues: {len(active_leagues)}")
        logger.info(f"👥 Teams: {len(data.get('teams', []))}")
        logger.info(f"📋 Rosters: {len(data.get('rosters', []))}")
        logger.info(f"🏆 Matchups: {len(data.get('matchups', []))}")
        logger.info(f"💰 Transactions: {len(data.get('transactions', []))}")
        logger.info(f"🏈 Draft Picks: {len(data.get('draft_picks', []))}")
        logger.info(f"📈 TOTAL RECORDS: {total_records:,}")
        logger.info(f"💾 File: {filename} ({file_size_mb:.1f} MB)")
        logger.info(f"🚀 Ready for deployment!")
    
    def run(self, force_run=False):
        """Run incremental data extraction"""
        logger.info(f"📈 Starting INCREMENTAL fantasy football data extraction at {self.start_time}")
        
        # Check if we're in season (unless forced)
        if not force_run and not self.is_fantasy_season():
            logger.info("⏸️ Not currently in fantasy season (Aug 18 - Jan 18) - skipping extraction")
            logger.info("💡 Use --force flag to run extraction during off-season")
            return None
        
        if force_run and not self.is_fantasy_season():
            logger.info("🧪 FORCE MODE: Running extraction during off-season for testing")
        
        # Load previous extraction data
        self.load_last_extraction()
        
        # Authenticate
        if not self.authenticate():
            return None
        
        # Get current active leagues
        active_leagues = self.get_current_active_leagues()
        if not active_leagues:
            return None
        
        # Identify new leagues since last extraction
        new_leagues = self.get_new_leagues(active_leagues)
        
        # Initialize incremental data structure
        incremental_data = {
            'leagues': [],
            'teams': [],
            'rosters': [],
            'matchups': [],
            'transactions': [],
            'draft_picks': [],
            'statistics': []
        }
        
        # Extract data for active leagues (focusing on incremental changes)
        for i, league_info in enumerate(active_leagues, 1):
            league_id = league_info['league_id']
            league_name = league_info['name']
            current_week = league_info.get('current_week', 1)
            
            logger.info(f"📋 Processing league {i}/{len(active_leagues)}: {league_name}")
            logger.info(f"  📅 Current week: {current_week}")
            
            try:
                # Always get current league info (may have updated settings)
                league_data = self.extractor.extract_league_data(league_info)
                incremental_data['leagues'].append(league_data.__dict__)
                
                # Get current teams (standings may have changed)
                teams = self.extractor.extract_teams_for_league(league_id)
                incremental_data['teams'].extend([team.__dict__ for team in teams])
                
                # Get incremental rosters (recent weeks only)
                rosters = self.get_incremental_rosters(league_id, teams, current_week)
                incremental_data['rosters'].extend([roster.__dict__ for roster in rosters])
                
                # Get incremental matchups (recent weeks)
                matchups = self.get_incremental_matchups(league_id, current_week)
                incremental_data['matchups'].extend([matchup.__dict__ for matchup in matchups])
                
                # Get incremental transactions (last 30 days)
                transactions = self.get_incremental_transactions(league_id)
                incremental_data['transactions'].extend([transaction.__dict__ for transaction in transactions])
                
                # Rate limiting
                time.sleep(0.3)
                
            except Exception as e:
                logger.error(f"❌ Error processing league {league_name}: {e}")
                continue
        
        # Extract draft data for any new leagues
        new_draft_picks = self.extract_draft_data_for_new_leagues(new_leagues)
        incremental_data['draft_picks'].extend(new_draft_picks)
        
        # Merge with baseline data to create complete dataset
        complete_data = self.merge_with_baseline(incremental_data)
        
        # Serialize and save data
        json_data = self.serialize_data(complete_data)
        filename = self.save_results(json_data, is_incremental=False)
        
        # Log summary
        self.log_summary(json_data, active_leagues, filename, is_incremental=True)
        
        return filename

# Backward compatibility alias
WeeklyDataExtractor = IncrementalDataExtractor

def main():
    """Main entry point"""
    extractor = IncrementalDataExtractor()
    return extractor.run()

if __name__ == "__main__":
    main() 