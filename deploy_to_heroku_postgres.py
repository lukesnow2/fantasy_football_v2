#!/usr/bin/env python3
"""
Deploy Yahoo Fantasy Data to Heroku Postgres
Uploads the complete 20-year dataset to a Heroku PostgreSQL database
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HerokuPostgresDeployer:
    """Deploy Yahoo Fantasy data to Heroku Postgres"""
    
    def __init__(self, data_file: str, database_url: str = None):
        """
        Initialize deployer
        
        Args:
            data_file (str): Path to extracted JSON data file
            database_url (str): Heroku DATABASE_URL (if not provided, will look for env var)
        """
        self.data_file = data_file
        self.database_url = database_url or os.getenv('DATABASE_URL')
        self.engine = None
        self.data = None
        
        if not self.database_url:
            raise ValueError("DATABASE_URL not provided. Set it as environment variable or pass directly.")
    
    def connect_to_database(self) -> bool:
        """Connect to Heroku Postgres database"""
        try:
            logger.info("🔌 Connecting to Heroku Postgres database...")
            
            # Fix URL for newer SQLAlchemy (postgres:// -> postgresql://)
            database_url = self.database_url
            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql://', 1)
            
            # Create SQLAlchemy engine
            self.engine = create_engine(database_url)
            
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                logger.info(f"✅ Connected to PostgreSQL: {version}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def load_data(self) -> bool:
        """Load extracted data from JSON file"""
        try:
            logger.info(f"📂 Loading data from {self.data_file}...")
            
            with open(self.data_file, 'r') as f:
                self.data = json.load(f)
            
            logger.info("✅ Data loaded successfully!")
            logger.info(f"📊 Data Summary:")
            for table, records in self.data.items():
                if records:  # Only show non-empty tables
                    logger.info(f"  - {table}: {len(records):,} records")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            return False
    
    def create_schema(self) -> bool:
        """Create database tables using our schema"""
        try:
            logger.info("🏗️  Creating database schema...")
            
            # Read schema SQL
            schema_file = 'utils/yahoo_fantasy_schema.sql'
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            # Execute schema creation
            with self.engine.connect() as conn:
                # Split and execute each statement
                statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
                
                for statement in statements:
                    if statement.upper().startswith(('CREATE TABLE', 'CREATE VIEW', 'CREATE INDEX')):
                        try:
                            conn.execute(text(statement))
                            logger.info(f"✅ Executed: {statement[:50]}...")
                        except Exception as e:
                            if "already exists" in str(e).lower():
                                logger.warning(f"⚠️  Table/view already exists: {statement[:50]}...")
                            else:
                                logger.error(f"❌ Error executing: {statement[:50]}... - {e}")
                
                conn.commit()
            
            logger.info("✅ Database schema created successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating schema: {e}")
            return False
    
    def upload_data(self) -> bool:
        """Upload data to database tables"""
        try:
            logger.info("📤 Uploading data to database...")
            
            # Define table order (to handle foreign key constraints)
            table_order = ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks']
            
            total_records = 0
            
            for table_name in table_order:
                if table_name not in self.data or not self.data[table_name]:
                    logger.info(f"  📄 Skipping empty table: {table_name}")
                    continue
                
                records = self.data[table_name]
                logger.info(f"  📤 Uploading {table_name}: {len(records):,} records...")
                
                # Convert to DataFrame
                df = pd.DataFrame(records)
                
                # Handle data type conversions
                df = self._clean_dataframe(df, table_name)
                
                # Upload to database
                try:
                    df.to_sql(
                        table_name, 
                        self.engine, 
                        if_exists='replace',  # Replace existing data
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                    logger.info(f"  ✅ {table_name}: {len(records):,} records uploaded")
                    total_records += len(records)
                    
                except Exception as e:
                    logger.error(f"  ❌ Error uploading {table_name}: {e}")
                    return False
            
            logger.info(f"✅ Data upload completed! Total records: {total_records:,}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading data: {e}")
            return False
    
    def _clean_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Clean and convert DataFrame for database upload"""
        # Handle datetime fields
        if 'extracted_at' in df.columns:
            df['extracted_at'] = pd.to_datetime(df['extracted_at'])
        
        if table_name == 'transactions' and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        if table_name == 'rosters' and 'acquisition_date' in df.columns:
            df['acquisition_date'] = pd.to_datetime(df['acquisition_date'], errors='coerce')
        
        # Handle boolean fields
        bool_fields = ['is_pro_league', 'is_cash_league', 'is_starter', 'is_playoffs', 'is_championship', 'is_consolation', 'is_keeper', 'is_auction_draft']
        for field in bool_fields:
            if field in df.columns:
                df[field] = df[field].astype(bool)
        
        # Handle numeric fields
        numeric_fields = ['wins', 'losses', 'ties', 'points_for', 'points_against', 'team1_score', 'team2_score', 'faab_bid', 'faab_balance', 'pick_number', 'round_number', 'cost']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        return df
    
    def verify_upload(self) -> bool:
        """Verify that data was uploaded correctly"""
        try:
            logger.info("🔍 Verifying data upload...")
            
            with self.engine.connect() as conn:
                # Check each table
                for table_name in ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks']:
                    try:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                        count = result.fetchone()[0]
                        expected_count = len(self.data.get(table_name, []))
                        
                        if count == expected_count:
                            logger.info(f"  ✅ {table_name}: {count:,} records (matches expected)")
                        else:
                            logger.warning(f"  ⚠️  {table_name}: {count:,} records (expected {expected_count:,})")
                    
                    except Exception as e:
                        logger.error(f"  ❌ Error checking {table_name}: {e}")
            
            logger.info("✅ Data verification completed!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error verifying upload: {e}")
            return False
    
    def create_summary_report(self) -> bool:
        """Create a summary report of the deployed data"""
        try:
            logger.info("📊 Creating deployment summary...")
            
            with self.engine.connect() as conn:
                # Get league summary
                result = conn.execute(text("""
                    SELECT 
                        season,
                        COUNT(*) as league_count,
                        SUM(num_teams) as total_teams
                    FROM leagues 
                    GROUP BY season 
                    ORDER BY season
                """))
                
                logger.info("\n📈 DEPLOYMENT SUMMARY:")
                logger.info("=" * 50)
                logger.info("LEAGUES BY SEASON:")
                
                total_leagues = 0
                total_teams = 0
                
                for row in result:
                    season, league_count, teams = row
                    logger.info(f"  {season}: {league_count} leagues, {teams} teams")
                    total_leagues += league_count
                    total_teams += teams
                
                logger.info(f"\nTOTAL: {total_leagues} leagues, {total_teams} teams")
                
                # Get overall record counts
                tables = ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks']
                logger.info("\nRECORD COUNTS:")
                grand_total = 0
                
                for table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    logger.info(f"  {table.capitalize()}: {count:,}")
                    grand_total += count
                
                logger.info(f"\nGRAND TOTAL: {grand_total:,} database records")
                logger.info("=" * 50)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating summary: {e}")
            return False

def main():
    """Main deployment function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy Yahoo Fantasy data to Heroku Postgres')
    parser.add_argument('--data-file', 
                       default='yahoo_fantasy_COMPLETE_with_drafts_*.json',
                       help='Path to extracted data JSON file (use most recent with draft data)')
    parser.add_argument('--database-url', 
                       help='Heroku DATABASE_URL (or set DATABASE_URL env var)')
    
    args = parser.parse_args()
    
    # Auto-detect the most recent draft data file if wildcard is used
    data_file = args.data_file
    if '*' in data_file:
        import glob
        matching_files = glob.glob(data_file)
        if matching_files:
            # Get the most recent file
            data_file = max(matching_files, key=lambda x: x.split('_')[-1])
            logger.info(f"🔍 Auto-detected most recent file: {data_file}")
        else:
            # Fall back to original final data file
            data_file = 'yahoo_fantasy_FINAL_complete_data_20250605_101225.json'
            logger.warning(f"⚠️ No draft data files found, using: {data_file}")
    
    start_time = datetime.now()
    logger.info(f"🚀 Starting Heroku Postgres deployment at {start_time}")
    logger.info(f"📊 Data file: {data_file}")
    
    try:
        # Create deployer
        deployer = HerokuPostgresDeployer(data_file, args.database_url)
        
        # Step 1: Connect to database
        if not deployer.connect_to_database():
            sys.exit(1)
        
        # Step 2: Load data
        if not deployer.load_data():
            sys.exit(1)
        
        # Step 3: Create schema
        if not deployer.create_schema():
            sys.exit(1)
        
        # Step 4: Upload data
        if not deployer.upload_data():
            sys.exit(1)
        
        # Step 5: Verify upload
        if not deployer.verify_upload():
            sys.exit(1)
        
        # Step 6: Create summary
        if not deployer.create_summary_report():
            sys.exit(1)
        
        # Success!
        end_time = datetime.now()
        runtime = end_time - start_time
        
        logger.info(f"\n🎉 DEPLOYMENT SUCCESSFUL!")
        logger.info(f"⏱️  Total runtime: {runtime}")
        logger.info(f"🗄️  Your 20-year Yahoo Fantasy dataset is now live in Heroku Postgres!")
        
    except Exception as e:
        logger.error(f"❌ Deployment failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 