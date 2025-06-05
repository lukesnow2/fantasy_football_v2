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
                      'pick_number', 'round_number', 'cost'}
    
    TABLE_ORDER = ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks']
    
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
            
            with open('utils/yahoo_fantasy_schema.sql', 'r') as f:
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
            ("Upload Data", self.upload_data),
            ("Verify & Summarize", self.verify_and_summarize)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"🚀 Step: {step_name}")
            if not step_func():
                logger.error(f"❌ Step '{step_name}' failed")
                return False
        
        return True

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
    fallback = 'yahoo_fantasy_FINAL_complete_data_20250605_101225.json'
    logger.warning(f"⚠️ No files match pattern, using: {fallback}")
    return fallback

def main():
    """Main deployment entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy Yahoo Fantasy data to Heroku Postgres')
    parser.add_argument('--data-file', 
                       default='yahoo_fantasy_COMPLETE_with_drafts_*.json',
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