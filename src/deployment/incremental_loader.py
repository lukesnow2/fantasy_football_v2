#!/usr/bin/env python3
"""
Incremental Database Loader with Hybrid Strategies
Implements table-specific loading strategies to optimize performance and prevent duplicates
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Set
import pandas as pd
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IncrementalDatabaseLoader:
    """Hybrid incremental database loader with table-specific strategies"""
    
    # Data type mappings for cleaning
    DATETIME_FIELDS = {'extracted_at', 'timestamp', 'acquisition_date'}
    BOOLEAN_FIELDS = {'is_pro_league', 'is_cash_league', 'is_starter', 'is_playoffs', 
                      'is_championship', 'is_consolation', 'is_keeper', 'is_auction_draft'}
    NUMERIC_FIELDS = {'wins', 'losses', 'ties', 'points_for', 'points_against', 
                      'team1_score', 'team2_score', 'faab_bid', 'faab_balance', 
                      'pick_number', 'round_number', 'cost'}
    
    # Table loading strategies
    TABLE_STRATEGIES = {
        'leagues': {
            'strategy': 'UPSERT',
            'primary_key': 'league_id',
            'update_fields': ['name', 'current_week', 'draft_status', 'extracted_at'],
            'description': 'UPSERT on league_id - leagues change infrequently'
        },
        'teams': {
            'strategy': 'UPSERT',
            'primary_key': 'team_id', 
            'update_fields': ['wins', 'losses', 'ties', 'points_for', 'points_against', 
                            'playoff_seed', 'faab_balance', 'extracted_at'],
            'description': 'UPSERT on team_id - team stats update weekly'
        },
        'rosters': {
            'strategy': 'INCREMENTAL_APPEND',
            'primary_key': 'roster_id',
            'filter_field': 'week',
            'composite_key': ['team_id', 'week', 'player_id'],
            'description': 'Delete current week records, append new - weekly time-series data'
        },
        'matchups': {
            'strategy': 'INCREMENTAL_APPEND', 
            'primary_key': 'matchup_id',
            'filter_field': 'week',
            'composite_key': ['league_id', 'week'],
            'description': 'Delete current week records, append new - weekly time-series data'
        },
        'transactions': {
            'strategy': 'APPEND_ONLY',
            'primary_key': 'transaction_id',
            'description': 'Append new transactions only - immutable historical events'
        },
        'draft_picks': {
            'strategy': 'APPEND_ONLY',
            'primary_key': 'draft_pick_id',
            'description': 'Append new draft picks only - immutable historical events'
        }
    }
    
    def __init__(self, data_file: str, database_url: str = None):
        self.data_file = data_file
        self.database_url = database_url or os.getenv('DATABASE_URL')
        self.engine = None
        self.data = None
        self.load_stats = {
            'tables_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'records_deleted': 0,
            'errors': []
        }
        
        if not self.database_url:
            raise ValueError("DATABASE_URL required: set as environment variable or pass directly")
    
    def connect(self) -> bool:
        """Connect to database"""
        try:
            logger.info("🔌 Connecting to database...")
            
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
        
        # Convert numpy types to Python native types to avoid psycopg2 issues
        for col in df.columns:
            if df[col].dtype.name.startswith('int'):
                df[col] = df[col].astype(int)
            elif df[col].dtype.name.startswith('float'):
                df[col] = df[col].astype(float)
        
        return df
    
    def execute_upsert_strategy(self, table_name: str, df: pd.DataFrame, strategy: Dict) -> bool:
        """Execute UPSERT strategy for leagues and teams"""
        try:
            logger.info(f"  🔄 UPSERT strategy for {table_name}")
            
            primary_key = strategy['primary_key']
            update_fields = strategy.get('update_fields', [])
            
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    # Get existing primary keys
                    existing_keys_query = f"SELECT {primary_key} FROM {table_name}"
                    existing_keys = set(row[0] for row in conn.execute(text(existing_keys_query)))
                    
                    new_records = df[~df[primary_key].isin(existing_keys)]
                    update_records = df[df[primary_key].isin(existing_keys)]
                    
                    # Insert new records
                    if not new_records.empty:
                        new_records.to_sql(table_name, conn, if_exists='append', 
                                          index=False, method='multi')
                        logger.info(f"    ✅ Inserted {len(new_records)} new records")
                        self.load_stats['records_inserted'] += len(new_records)
                    
                    # Update existing records
                    if not update_records.empty and update_fields:
                        for _, record in update_records.iterrows():
                            set_clause = ', '.join([f"{field} = :{field}" for field in update_fields if field in record])
                            if set_clause:
                                update_query = f"""
                                    UPDATE {table_name} 
                                    SET {set_clause}
                                    WHERE {primary_key} = :{primary_key}
                                """
                                
                                # Prepare parameters
                                params = {field: record[field] for field in update_fields + [primary_key] if field in record}
                                conn.execute(text(update_query), params)
                        
                        logger.info(f"    ✅ Updated {len(update_records)} existing records")
                        self.load_stats['records_updated'] += len(update_records)
                    
                    trans.commit()
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    raise e
                    
        except Exception as e:
            logger.error(f"❌ UPSERT failed for {table_name}: {e}")
            self.load_stats['errors'].append(f"{table_name}: UPSERT failed - {e}")
            return False
    
    def execute_incremental_append_strategy(self, table_name: str, df: pd.DataFrame, strategy: Dict) -> bool:
        """Execute INCREMENTAL_APPEND strategy for time-series data"""
        try:
            logger.info(f"  📈 INCREMENTAL_APPEND strategy for {table_name}")
            
            filter_field = strategy['filter_field']
            
            # Get unique weeks/periods in new data
            if filter_field in df.columns:
                periods_to_update = df[filter_field].unique()
                logger.info(f"    📅 Updating periods: {sorted(periods_to_update)}")
                
                with self.engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        # Delete existing records for these periods
                        deleted_count = 0
                        for period in periods_to_update:
                            # Convert numpy types to Python native types
                            period_value = int(period) if hasattr(period, 'item') else period
                            delete_query = f"DELETE FROM {table_name} WHERE {filter_field} = :period"
                            result = conn.execute(text(delete_query), {'period': period_value})
                            deleted_count += result.rowcount
                        
                        if deleted_count > 0:
                            logger.info(f"    🗑️ Deleted {deleted_count} existing records for update periods")
                            self.load_stats['records_deleted'] += deleted_count
                        
                        # Insert all new records
                        df.to_sql(table_name, conn, if_exists='append', 
                                 index=False, method='multi')
                        
                        logger.info(f"    ✅ Inserted {len(df)} new records")
                        self.load_stats['records_inserted'] += len(df)
                        
                        trans.commit()
                        return True
                        
                    except Exception as e:
                        trans.rollback()
                        raise e
            else:
                logger.warning(f"⚠️ Filter field '{filter_field}' not found in {table_name}, using append-only")
                return self.execute_append_only_strategy(table_name, df, strategy)
                
        except Exception as e:
            logger.error(f"❌ INCREMENTAL_APPEND failed for {table_name}: {e}")
            self.load_stats['errors'].append(f"{table_name}: INCREMENTAL_APPEND failed - {e}")
            return False
    
    def execute_append_only_strategy(self, table_name: str, df: pd.DataFrame, strategy: Dict) -> bool:
        """Execute APPEND_ONLY strategy for immutable data"""
        try:
            logger.info(f"  ➕ APPEND_ONLY strategy for {table_name}")
            
            primary_key = strategy['primary_key']
            
            with self.engine.connect() as conn:
                # Get existing primary keys to avoid duplicates
                existing_keys_query = f"SELECT {primary_key} FROM {table_name}"
                existing_keys = set(row[0] for row in conn.execute(text(existing_keys_query)))
                
                # Filter out existing records
                new_records = df[~df[primary_key].isin(existing_keys)]
                
                if not new_records.empty:
                    new_records.to_sql(table_name, conn, if_exists='append', 
                                      index=False, method='multi')
                    logger.info(f"    ✅ Appended {len(new_records)} new records")
                    self.load_stats['records_inserted'] += len(new_records)
                else:
                    logger.info(f"    ✅ No new records to append (all exist)")
                
                skipped = len(df) - len(new_records)
                if skipped > 0:
                    logger.info(f"    ⏭️ Skipped {skipped} existing records")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ APPEND_ONLY failed for {table_name}: {e}")
            self.load_stats['errors'].append(f"{table_name}: APPEND_ONLY failed - {e}")
            return False
    
    def load_table(self, table_name: str, records: List[Dict]) -> bool:
        """Load a table using its defined strategy"""
        if not records:
            logger.info(f"  ⏭️ Skipping {table_name} (no records)")
            return True
        
        strategy = self.TABLE_STRATEGIES.get(table_name)
        if not strategy:
            logger.warning(f"⚠️ No strategy defined for {table_name}, using REPLACE")
            df = pd.DataFrame(records)
            df = self.clean_dataframe(df, table_name)
            df.to_sql(table_name, self.engine, if_exists='replace', 
                     index=False, method='multi', chunksize=1000)
            self.load_stats['records_inserted'] += len(df)
            return True
        
        logger.info(f"📊 Loading {table_name}: {len(records):,} records")
        logger.info(f"    🎯 Strategy: {strategy['strategy']} - {strategy['description']}")
        
        # Convert to DataFrame and clean
        df = pd.DataFrame(records)
        df = self.clean_dataframe(df, table_name)
        
        # Execute strategy
        strategy_type = strategy['strategy']
        
        if strategy_type == 'UPSERT':
            return self.execute_upsert_strategy(table_name, df, strategy)
        elif strategy_type == 'INCREMENTAL_APPEND':
            return self.execute_incremental_append_strategy(table_name, df, strategy)
        elif strategy_type == 'APPEND_ONLY':
            return self.execute_append_only_strategy(table_name, df, strategy)
        else:
            logger.error(f"❌ Unknown strategy: {strategy_type}")
            return False
    
    def create_schema_if_needed(self) -> bool:
        """Create database schema if it doesn't exist"""
        try:
            logger.info("🏗️ Checking database schema...")
            
            # Check if tables exist
            with self.engine.connect() as conn:
                tables_query = """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """
                existing_tables = set(row[0] for row in conn.execute(text(tables_query)))
                
                required_tables = set(self.TABLE_STRATEGIES.keys())
                missing_tables = required_tables - existing_tables
                
                if missing_tables:
                    logger.info(f"📋 Missing tables: {missing_tables}")
                    logger.info("🏗️ Creating database schema...")
                    
                    # Try to read schema file
                    schema_file = 'src/utils/yahoo_fantasy_schema.sql'
                    if os.path.exists(schema_file):
                        with open(schema_file, 'r') as f:
                            schema_sql = f.read()
                        
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
                    else:
                        logger.warning(f"⚠️ Schema file not found: {schema_file}")
                        logger.info("📋 Tables will be created automatically during data loading")
                else:
                    logger.info("✅ All required tables exist")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ Schema check/creation failed: {e}")
            return False
    
    def load_incremental_data(self) -> bool:
        """Load all data using hybrid incremental strategies"""
        try:
            logger.info("📤 Starting incremental data loading...")
            
            # Defined loading order to respect foreign key constraints
            table_order = ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks']
            
            for table_name in table_order:
                records = self.data.get(table_name, [])
                if records or table_name in self.data:  # Process even if empty to handle deletions
                    if self.load_table(table_name, records):
                        self.load_stats['tables_processed'] += 1
                    else:
                        logger.error(f"❌ Failed to load {table_name}")
                        return False
            
            # Load any additional tables not in the standard order
            for table_name, records in self.data.items():
                if table_name not in table_order and records:
                    if self.load_table(table_name, records):
                        self.load_stats['tables_processed'] += 1
            
            logger.info("✅ Incremental loading completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Incremental loading failed: {e}")
            return False
    
    def verify_and_summarize(self) -> bool:
        """Verify loading and create summary"""
        try:
            logger.info("🔍 Verifying and summarizing...")
            
            with self.engine.connect() as conn:
                logger.info("\n📊 INCREMENTAL LOADING SUMMARY:")
                logger.info("=" * 60)
                
                # Loading statistics
                stats = self.load_stats
                logger.info(f"Tables processed: {stats['tables_processed']}")
                logger.info(f"Records inserted: {stats['records_inserted']:,}")
                logger.info(f"Records updated: {stats['records_updated']:,}")
                logger.info(f"Records deleted: {stats['records_deleted']:,}")
                
                if stats['errors']:
                    logger.info(f"\n❌ Errors encountered: {len(stats['errors'])}")
                    for error in stats['errors']:
                        logger.info(f"  - {error}")
                
                # Current database state
                logger.info(f"\n📈 CURRENT DATABASE STATE:")
                total_db_records = 0
                
                for table_name in self.TABLE_STRATEGIES.keys():
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
                        logger.info(f"  {table_name.capitalize()}: {count:,} records")
                        total_db_records += count
                    except:
                        logger.info(f"  {table_name}: Table not found")
                
                logger.info(f"\nTOTAL DATABASE RECORDS: {total_db_records:,}")
                logger.info("=" * 60)
            
            return True
        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return False
    
    def deploy_incremental(self, run_edw: bool = True) -> bool:
        """Execute complete incremental deployment with optional EDW processing"""
        steps = [
            ("Connect", self.connect),
            ("Load Data", self.load_data),
            ("Create Schema", self.create_schema_if_needed),
            ("Load Incremental Data", self.load_incremental_data),
            ("Verify & Summarize", self.verify_and_summarize)
        ]
        
        # Add EDW step if requested
        if run_edw:
            steps.append(("Process EDW", self.process_edw_updates))
        
        start_time = datetime.now()
        
        for step_name, step_func in steps:
            logger.info(f"🚀 Step: {step_name}")
            if not step_func():
                logger.error(f"❌ Step '{step_name}' failed")
                return False
        
        runtime = datetime.now() - start_time
        logger.info(f"\n🎉 INCREMENTAL DEPLOYMENT SUCCESSFUL!")
        logger.info(f"⏱️ Runtime: {runtime}")
        logger.info(f"🗄️ Database updated with incremental loading strategies!")
        
        if run_edw:
            logger.info(f"📊 EDW updated with latest operational data!")
        
        return True
    
    def process_edw_updates(self) -> bool:
        """Process EDW updates based on operational table changes"""
        try:
            logger.info("🚀 Starting EDW processing...")
            
            # Import EDW processor
            sys.path.append('src/edw_schema')
            from edw_etl_processor import EdwEtlProcessor
            
            # Determine which tables were changed during incremental loading
            changed_tables = set()
            for table_name in self.TABLE_STRATEGIES.keys():
                if table_name in self.data and self.data[table_name]:
                    changed_tables.add(table_name)
            
            if not changed_tables:
                logger.info("✅ No operational table changes - skipping EDW processing")
                return True
            
            logger.info(f"📊 Operational tables changed: {changed_tables}")
            
            # Initialize EDW processor
            edw = EdwEtlProcessor(database_url=self.database_url, data_file=self.data_file)
            
            # Connect to database (reuse existing connection info)
            if not edw.connect():
                logger.error("❌ Failed to connect to EDW database")
                return False
            
            # Load operational data
            if not edw.load_data():
                logger.error("❌ Failed to load operational data for EDW")
                return False
            
            # Process incremental EDW updates
            if not edw.process_incremental_edw(changed_tables):
                logger.error("❌ EDW processing failed")
                return False
            
            logger.info("✅ EDW processing completed successfully")
            return True
            
        except ImportError as e:
            logger.warning(f"⚠️ EDW processor not available: {e}")
            logger.info("📝 Skipping EDW processing - install EDW components if needed")
            return True
        except Exception as e:
            logger.error(f"❌ EDW processing failed: {e}")
            return False

def main():
    """Main incremental loading entry point"""
    import argparse
    import glob
    
    parser = argparse.ArgumentParser(description='Load fantasy football data with incremental strategies')
    parser.add_argument('--data-file', 
                       default='data/current/yahoo_fantasy_COMPLETE_with_drafts_*.json',
                       help='Data file path (supports wildcards)')
    parser.add_argument('--database-url', 
                       help='Database URL (or set DATABASE_URL env var)')
    parser.add_argument('--skip-edw', action='store_true',
                       help='Skip EDW processing (only load operational data)')
    parser.add_argument('--edw-only', action='store_true',
                       help='Only process EDW updates (skip operational loading)')
    
    args = parser.parse_args()
    
    # Determine EDW processing mode
    run_edw = not args.skip_edw
    
    # Auto-detect data file
    data_file = args.data_file
    if '*' in data_file:
        matching_files = glob.glob(data_file)
        if matching_files:
            data_file = max(matching_files, key=lambda x: x.split('_')[-1])
            logger.info(f"🔍 Auto-detected: {data_file}")
        else:
            data_file = 'data/current/yahoo_fantasy_FINAL_complete_data_20250605_101225.json'
            logger.warning(f"⚠️ No files match pattern, using: {data_file}")
    
    # Handle EDW-only mode
    if args.edw_only:
        logger.info(f"🚀 Starting EDW-only processing")
        logger.info(f"📊 Data file: {data_file}")
        
        try:
            # Import and run EDW processor directly
            sys.path.append('src/edw_schema')
            from edw_etl_processor import EdwEtlProcessor
            
            database_url = args.database_url or os.getenv('DATABASE_URL')
            if not database_url:
                logger.error("❌ DATABASE_URL required for EDW processing")
                sys.exit(1)
            
            edw = EdwEtlProcessor(database_url=database_url, data_file=data_file)
            
            if edw.connect() and edw.load_data() and edw.process_incremental_edw():
                logger.info("🏆 EDW processing completed successfully!")
            else:
                logger.error("❌ EDW processing failed")
                sys.exit(1)
                
        except Exception as e:
            logger.error(f"❌ EDW error: {e}")
            sys.exit(1)
    else:
        logger.info(f"🚀 Starting incremental database loading")
        logger.info(f"📊 Data file: {data_file}")
        if run_edw:
            logger.info("🏢 EDW processing: ENABLED")
        else:
            logger.info("🏢 EDW processing: DISABLED")
        
        try:
            loader = IncrementalDatabaseLoader(data_file, args.database_url)
            
            if loader.deploy_incremental(run_edw=run_edw):
                logger.info("🏆 Incremental loading system operational!")
            else:
                logger.error("❌ Incremental loading failed")
                sys.exit(1)
                
        except Exception as e:
            logger.error(f"❌ Loading error: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main() 