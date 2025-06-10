#!/usr/bin/env python3
"""
Deploy Yahoo Fantasy Data to Heroku Postgres
Streamlined deployment of fantasy football data to PostgreSQL
"""

import json
import logging
import os
import sys
import glob
from datetime import datetime
from typing import Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HerokuPostgresDeployer:
    """Streamlined Heroku Postgres deployer for fantasy football data"""
    
    # Data type mappings for cleaning
    DATETIME_FIELDS = {'extracted_at', 'timestamp', 'acquisition_date'}
    BOOLEAN_FIELDS = {'is_pro_league', 'is_cash_league', 'is_starter', 'is_playoffs', 
                      'is_championship', 'is_consolation', 'is_keeper', 'is_auction_draft'}
    NUMERIC_FIELDS = {'wins', 'losses', 'ties', 'points_for', 'points_against', 
                      'team1_score', 'team2_score', 'faab_bid', 'faab_balance', 
                      'pick_number', 'round_number', 'cost', 'total_fantasy_points', 
                      'season_year'}
    
    TABLE_ORDER = ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks', 'statistics']
    
    def __init__(self, data_file: str, database_url: str = None):
        self.data_file = data_file
        self.database_url = database_url or os.getenv('DATABASE_URL')
        self.engine = None
        self.data = None
        
        if not self.database_url:
            raise ValueError("DATABASE_URL required: set as environment variable or pass directly")
    
    def connect(self) -> bool:
        """Connect to Heroku Postgres database"""
        try:
            logger.info("🔌 Connecting to Heroku Postgres...")
            
            # Fix URL for newer SQLAlchemy
            url = self.database_url.replace('postgres://', 'postgresql://', 1)
            self.engine = create_engine(url)
            
            # Test connection
            with self.engine.connect() as conn:
                version = conn.execute(text("SELECT version()")).fetchone()[0]
                logger.info(f"✅ Connected: {version.split()[0:2]}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            return False
    
    def load_data(self) -> bool:
        """Load data from JSON file"""
        try:
            logger.info(f"📂 Loading data from {self.data_file}...")
            
            with open(self.data_file, 'r') as f:
                self.data = json.load(f)
            
            # Log summary
            total_records = sum(len(records) for records in self.data.values() if records)
            logger.info(f"✅ Data loaded: {total_records:,} total records")
            for table, records in self.data.items():
                if records:
                    logger.info(f"  📊 {table}: {len(records):,}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Data loading failed: {e}")
            return False
    
    def create_schema(self) -> bool:
        """Create database schema"""
        try:
            logger.info("🏗️ Creating database schema...")
            
            with open('src/utils/yahoo_fantasy_schema.sql', 'r') as f:
                schema_sql = f.read()
            
            with self.engine.connect() as conn:
                statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
                
                for stmt in statements:
                    if stmt.upper().startswith(('CREATE TABLE', 'CREATE VIEW', 'CREATE INDEX')):
                        try:
                            conn.execute(text(stmt))
                        except Exception as e:
                            if "already exists" not in str(e).lower():
                                logger.warning(f"Schema warning: {e}")
                
                conn.commit()
            
            logger.info("✅ Schema created successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Schema creation failed: {e}")
            return False
    
    def clean_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Clean DataFrame for database upload"""
        # Handle datetime fields
        for field in self.DATETIME_FIELDS:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce')
        
        # Handle boolean fields
        for field in self.BOOLEAN_FIELDS:
            if field in df.columns:
                df[field] = df[field].astype(bool)
        
        # Handle numeric fields
        for field in self.NUMERIC_FIELDS:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        return df
    
    def flatten_matchups_data(self, matchups_data: list) -> list:
        """
        Flatten nested Yahoo API matchup responses into database-ready format
        Converts complex nested API structure to simple flat records
        """
        flattened_matchups = []
        
        for league_matchup in matchups_data:
            league_id = league_matchup.get('league_id')
            week = league_matchup.get('week')
            matchups = league_matchup.get('matchups', {})
            
            # Skip if no matchups data or missing required fields
            if not matchups or not league_id or not week:
                continue
                
            # Extract nested matchup data
            scoreboard = matchups.get('fantasy_content', {}).get('league', [{}])
            if isinstance(scoreboard, list) and len(scoreboard) > 1:
                scoreboard_data = scoreboard[1].get('scoreboard', {})
                if '0' in scoreboard_data and 'matchups' in scoreboard_data['0']:
                    week_matchups = scoreboard_data['0']['matchups']
                    
                    # Process each matchup in the week
                    for match_key, match_data in week_matchups.items():
                        if match_key == 'count' or not isinstance(match_data, dict):
                            continue
                            
                        matchup = match_data.get('matchup', {})
                        if not matchup:
                            continue
                            
                        # Extract basic matchup info
                        matchup_record = {
                            'matchup_id': f"{league_id}_W{week}_{match_key}",
                            'league_id': league_id,
                            'week': int(week),
                            'is_playoffs': matchup.get('is_playoffs', '0') == '1',
                            'is_championship': self._detect_championship_game(matchup, week),
                            'is_consolation': matchup.get('is_consolation', '0') == '1',
                            'winner_team_id': matchup.get('winner_team_key'),
                            'team1_id': None,
                            'team2_id': None,
                            'team1_score': 0.0,
                            'team2_score': 0.0,
                            'extracted_at': league_matchup.get('extracted_at')
                        }
                        
                        # Extract team data from nested structure
                        if '0' in matchup and 'teams' in matchup['0']:
                            teams_data = matchup['0']['teams']
                            team_scores = []
                            team_ids = []
                            
                            for team_idx in ['0', '1']:
                                if team_idx in teams_data:
                                    team_info = teams_data[team_idx].get('team', [])
                                    if isinstance(team_info, list) and len(team_info) >= 2:
                                        # Extract team ID
                                        team_basic = team_info[0]
                                        for item in team_basic:
                                            if isinstance(item, dict) and 'team_key' in item:
                                                team_ids.append(item['team_key'])
                                                break
                                        
                                        # Extract team score
                                        team_stats = team_info[1]
                                        if 'team_points' in team_stats:
                                            score = team_stats['team_points'].get('total', '0')
                                            team_scores.append(float(score))
                            
                            # Assign team data
                            if len(team_ids) >= 2:
                                matchup_record['team1_id'] = team_ids[0]
                                matchup_record['team2_id'] = team_ids[1]
                            if len(team_scores) >= 2:
                                matchup_record['team1_score'] = team_scores[0]  
                                matchup_record['team2_score'] = team_scores[1]
                        
                        # Only add if we have essential data
                        if matchup_record['team1_id'] and matchup_record['team2_id']:
                            flattened_matchups.append(matchup_record)
        
        logger.info(f"📊 Flattened {len(flattened_matchups)} matchup records from {len(matchups_data)} league-weeks")
        return flattened_matchups

    def preprocess_data(self) -> bool:
        """Preprocess data to match expected database format"""
        try:
            logger.info("🔄 Preprocessing data for database compatibility...")
            
            # Step 1: Filter out non-NFL leagues and collect their IDs
            non_nfl_league_ids = set()
            if 'leagues' in self.data and self.data['leagues']:
                original_leagues = len(self.data['leagues'])
                
                # Identify non-NFL leagues
                for league in self.data['leagues']:
                    if league.get('game_code') != 'nfl':
                        non_nfl_league_ids.add(league.get('league_id'))
                
                # Filter out non-NFL leagues (keep only NFL)
                self.data['leagues'] = [
                    league for league in self.data['leagues'] 
                    if league.get('game_code') == 'nfl'
                ]
                
                filtered_leagues = len(self.data['leagues'])
                logger.info(f"🏈 Filtered out {original_leagues - filtered_leagues} non-NFL leagues, keeping {filtered_leagues} NFL leagues only")
            
            # Step 2: Filter out all data associated with non-NFL leagues
            if non_nfl_league_ids:
                logger.info(f"🗑️ Removing all data associated with {len(non_nfl_league_ids)} non-NFL leagues...")
                
                # Filter teams
                if 'teams' in self.data and self.data['teams']:
                    original_teams = len(self.data['teams'])
                    self.data['teams'] = [
                        team for team in self.data['teams']
                        if team.get('league_id') not in non_nfl_league_ids
                    ]
                    logger.info(f"  👥 Teams: {original_teams} → {len(self.data['teams'])}")
                
                # Filter matchups
                if 'matchups' in self.data and self.data['matchups']:
                    original_matchups = len(self.data['matchups'])
                    self.data['matchups'] = [
                        matchup for matchup in self.data['matchups']
                        if matchup.get('league_id') not in non_nfl_league_ids
                    ]
                    logger.info(f"  🏟️ Matchups: {original_matchups} → {len(self.data['matchups'])}")
                
                # Filter transactions
                if 'transactions' in self.data and self.data['transactions']:
                    original_transactions = len(self.data['transactions'])
                    self.data['transactions'] = [
                        transaction for transaction in self.data['transactions']
                        if transaction.get('league_id') not in non_nfl_league_ids
                    ]
                    logger.info(f"  💱 Transactions: {original_transactions} → {len(self.data['transactions'])}")
                
                # Filter draft picks
                if 'draft_picks' in self.data and self.data['draft_picks']:
                    original_draft_picks = len(self.data['draft_picks'])
                    self.data['draft_picks'] = [
                        pick for pick in self.data['draft_picks']
                        if pick.get('league_id') not in non_nfl_league_ids
                    ]
                    logger.info(f"  🎯 Draft picks: {original_draft_picks} → {len(self.data['draft_picks'])}")
                
                # Filter statistics
                if 'statistics' in self.data and self.data['statistics']:
                    original_statistics = len(self.data['statistics'])
                    self.data['statistics'] = [
                        stat for stat in self.data['statistics']
                        if stat.get('league_id') not in non_nfl_league_ids
                    ]
                    logger.info(f"  📈 Statistics: {original_statistics} → {len(self.data['statistics'])}")
            
            # Step 3: Flatten matchups data if it exists and is nested
            if 'matchups' in self.data and self.data['matchups']:
                sample_matchup = self.data['matchups'][0] if self.data['matchups'] else {}
                
                # Check if data is nested (has 'matchups' key in records)
                if 'matchups' in sample_matchup:
                    logger.info("📊 Detected nested matchups data, flattening...")
                    self.data['matchups'] = self.flatten_matchups_data(self.data['matchups'])
                else:
                    logger.info("📊 Matchups data already in flat format")
            
            # Final summary
            total_records = sum(len(records) for records in self.data.values() if isinstance(records, list))
            logger.info(f"✅ Preprocessing complete: {total_records:,} total records ready for deployment")
            
            return True
        except Exception as e:
            logger.error(f"❌ Data preprocessing failed: {e}")
            return False
    
    def upload_data(self) -> bool:
        """Upload data to database"""
        try:
            logger.info("📤 Uploading data...")
            
            total_uploaded = 0
            
            for table_name in self.TABLE_ORDER:
                records = self.data.get(table_name, [])
                if not records:
                    continue
                
                logger.info(f"  📊 {table_name}: {len(records):,} records")
                
                # Convert and clean data
                df = pd.DataFrame(records)
                df = self.clean_dataframe(df, table_name)
                
                # Upload to database
                df.to_sql(table_name, self.engine, if_exists='replace', 
                         index=False, method='multi', chunksize=1000)
                
                total_uploaded += len(records)
                logger.info(f"  ✅ {table_name} uploaded")
            
            logger.info(f"✅ Upload complete: {total_uploaded:,} records")
            return True
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            return False
    
    def verify_and_summarize(self) -> bool:
        """Verify upload and create summary"""
        try:
            logger.info("🔍 Verifying and summarizing...")
            
            with self.engine.connect() as conn:
                logger.info("\n📊 DEPLOYMENT SUMMARY:")
                logger.info("=" * 50)
                
                # Verify record counts
                total_db_records = 0
                for table_name in self.TABLE_ORDER:
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
                        expected = len(self.data.get(table_name, []))
                        status = "✅" if count == expected else "⚠️"
                        logger.info(f"{status} {table_name.capitalize()}: {count:,} records")
                        total_db_records += count
                    except:
                        logger.info(f"❌ {table_name}: Table not found")
                
                # League summary
                try:
                    result = conn.execute(text("""
                        SELECT season, COUNT(*) as leagues, SUM(num_teams) as teams
                        FROM leagues GROUP BY season ORDER BY season
                    """))
                    
                    logger.info("\n📈 LEAGUES BY SEASON:")
                    total_leagues = total_teams = 0
                    for season, leagues, teams in result:
                        logger.info(f"  {season}: {leagues} leagues, {teams} teams")
                        total_leagues += leagues
                        total_teams += teams or 0
                    
                    logger.info(f"\nTOTAL: {total_leagues} leagues, {total_teams} teams")
                except:
                    logger.info("League summary not available")
                
                logger.info(f"\nGRAND TOTAL: {total_db_records:,} database records")
                logger.info("=" * 50)
            
            return True
        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return False
    
    def deploy(self) -> bool:
        """Execute complete deployment"""
        steps = [
            ("Connect", self.connect),
            ("Load Data", self.load_data),
            ("Create Schema", self.create_schema),
            ("Preprocess Data", self.preprocess_data),
            ("Upload Data", self.upload_data),
            ("Verify & Summarize", self.verify_and_summarize)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"🚀 Step: {step_name}")
            if not step_func():
                logger.error(f"❌ Step '{step_name}' failed")
                return False
        
        return True

    def _detect_championship_game(self, matchup, week):
        """
        Detect if a matchup is a championship game based on week number and playoff status.
        
        Championship detection logic:
        1. Must be a playoff game (is_playoffs = True)
        2. Must be in the final week of playoffs (typically week 16 or 17)
        3. Week 17 is almost always championship for modern leagues
        4. Week 16 can be championship for shorter seasons
        """
        # Must be a playoff game first
        is_playoffs = matchup.get('is_playoffs', '0') == '1'
        if not is_playoffs:
            return False
        
        # Check for explicit championship indicator in Yahoo API data
        # Some Yahoo leagues may have this field
        if 'is_championship' in matchup:
            return matchup.get('is_championship', '0') == '1'
        
        # Week-based championship detection
        week_num = int(week)
        
        # Week 17 is championship week for most modern leagues (2021+)
        if week_num == 17:
            return True
        
        # Week 16 was championship week for many leagues (pre-2021)
        # and still is for some shorter seasons
        if week_num == 16:
            # Additional logic: if it's playoffs and week 16, likely championship
            # unless there's evidence of week 17 playoffs
            return True
        
        # Week 15 can be championship for very short seasons
        if week_num == 15:
            # Only if it's playoffs - this is rare but possible
            return True
        
        return False

def auto_detect_data_file(pattern: str) -> str:
    """Auto-detect the most recent data file"""
    if '*' not in pattern:
        return pattern
    
    matching_files = glob.glob(pattern)
    if matching_files:
        # Get most recent file
        latest = max(matching_files, key=lambda x: x.split('_')[-1])
        logger.info(f"🔍 Auto-detected: {latest}")
        return latest
    
    # Fallback
    fallback = 'data/current/yahoo_fantasy_FINAL_complete_data_20250605_101225.json'
    logger.warning(f"⚠️ No files match pattern, using: {fallback}")
    return fallback

def main():
    """Main deployment entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy Yahoo Fantasy data to Heroku Postgres')
    parser.add_argument('--data-file', 
                       default='data/current/yahoo_fantasy_COMPLETE_with_drafts_*.json',
                       help='Data file path (supports wildcards)')
    parser.add_argument('--database-url', 
                       help='Heroku DATABASE_URL (or set DATABASE_URL env var)')
    
    args = parser.parse_args()
    
    # Auto-detect data file
    data_file = auto_detect_data_file(args.data_file)
    
    start_time = datetime.now()
    logger.info(f"🚀 Starting Heroku Postgres deployment")
    logger.info(f"📊 Data file: {data_file}")
    
    try:
        deployer = HerokuPostgresDeployer(data_file, args.database_url)
        
        if deployer.deploy():
            runtime = datetime.now() - start_time
            logger.info(f"\n🎉 DEPLOYMENT SUCCESSFUL!")
            logger.info(f"⏱️ Runtime: {runtime}")
            logger.info(f"🗄️ Fantasy football data is now live in Heroku Postgres!")
        else:
            logger.error("❌ Deployment failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Deployment error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 