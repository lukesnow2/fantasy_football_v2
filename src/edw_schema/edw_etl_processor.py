#!/usr/bin/env python3
"""
Fantasy Football EDW ETL Processor - Integrated with Incremental Loader
Transforms operational data into analytical dimensional model
"""

import json
import logging
import os
import sys
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Set
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DECIMAL, Boolean, DateTime, Date
from sqlalchemy.orm import sessionmaker
from dataclasses import dataclass
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ETLStats:
    """Track ETL processing statistics"""
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class EdwEtlProcessor:
    """
    EDW ETL processor that integrates with incremental loading workflow
    
    League of Record Filtering:
    - Historical leagues (2005-2024): Always included via HISTORICAL_LEAGUE_IDS
    - Future leagues (2025+): Automatically included
    - Manual exclusions: Add league IDs to EXCLUDED_LEAGUE_IDS to exclude specific leagues
    
    To exclude a mistakenly included future league:
    1. Add the league ID to EXCLUDED_LEAGUE_IDS set
    2. Re-run the ETL to rebuild the EDW without that league
    """
    
    # Hard-coded historical league of record IDs (2005-2024)
    # These core leagues spanning 20 years are permanently included
    HISTORICAL_LEAGUE_IDS = {
        "449.l.674707",    # Idaho's DEI Quota (2024)
        "423.l.841006",    # Move the Raiders to PDX (2023)
        "414.l.1194955",   # Wet Hot Tahoe Summer (2022)
        "406.l.1065326",   # Rocky Mountain High (2021)
        "399.l.837311",    # The Lost Year (2020)
        "390.l.777720",    # Women & Women First (2019)
        "380.l.1143665",   # Sleepless In Seattle (2018)
        "371.l.1025465",   # Go Fuck Yourself San Diego (2017)
        "359.l.696366",    # The Great SF Draft (2016)
        "348.l.655822",    # Luke's Kingdom (2015)
        "331.l.355899",    # 10 Years 10 Assholes (2014)
        "314.l.319572",    # Rosterbaters Anonymous (2013)
        "273.l.107980",    # The League About Nothing (2012)
        "257.l.89145",     # Lock It Up (2011)
        "242.l.413666",    # Round 6 (2010)
        "222.l.222935",    # Engaged (2009)
        "199.l.42364",     # The Draft (2008)
        "175.l.658531",    # Oakdale Park (2007)
        "153.l.76788",     # Oakdale Park (2006)
        "124.l.109785"     # Oakdale Park (2005)
    }
    
    # Future seasons threshold - leagues from 2025+ are automatically included
    FUTURE_SEASON_THRESHOLD = 2025
    
    # Manual exclusion list - add league IDs here to exclude them if needed
    # Example: EXCLUDED_LEAGUE_IDS = {"449.l.999999"}  # Remove if added by mistake
    EXCLUDED_LEAGUE_IDS = set()
    
    # EDW table processing strategies aligned with operational table changes
    EDW_PROCESSING_STRATEGIES = {
        'leagues': {
            'triggers_refresh': ['dim_league', 'mart_league_summary'],
            'refresh_type': 'INCREMENTAL',  # Only changed leagues
            'depends_on': 'operational_leagues'
        },
        'teams': {
            'triggers_refresh': ['dim_team', 'fact_team_performance', 'mart_manager_performance'],
            'refresh_type': 'INCREMENTAL',  # Only changed teams
            'depends_on': 'operational_teams'
        },

        'matchups': {
            'triggers_refresh': ['fact_matchup', 'mart_league_summary', 'vw_league_competitiveness'],
            'refresh_type': 'WEEKLY',  # Full refresh for current week
            'depends_on': 'operational_matchups'
        },
        'transactions': {
            'triggers_refresh': ['fact_transaction', 'mart_player_value', 'vw_trade_analysis'],
            'refresh_type': 'APPEND',  # Only new transactions
            'depends_on': 'operational_transactions'
        },
        'draft_picks': {
            'triggers_refresh': ['fact_draft', 'mart_player_value'],
            'refresh_type': 'APPEND',  # Only new draft picks
            'depends_on': 'operational_draft_picks'
        },
        'statistics': {
            'triggers_refresh': ['fact_player_statistics', 'fact_draft', 'mart_player_value', 'fact_team_performance'],
            'refresh_type': 'UPSERT',  # Update existing, insert new
            'depends_on': 'operational_statistics'
        }
    }
    
    # Smart View Management: Maps views to their table dependencies
    VIEW_DEPENDENCIES = {
        # EDW Analytical Views
        'vw_current_season_dashboard': {
            'depends_on_tables': ['fact_team_performance', 'dim_team', 'dim_league', 'dim_week', 'fact_draft'],
            'depends_on_marts': [],
            'refresh_priority': 'HIGH',  # Critical for dashboards
            'description': 'Current season team performance dashboard'
        },
        'vw_manager_hall_of_fame': {
            'depends_on_tables': ['fact_team_performance', 'dim_team', 'dim_league'],
            'depends_on_marts': ['mart_manager_performance'],
            'refresh_priority': 'MEDIUM',
            'description': 'Manager career statistics and rankings'
        },
        'vw_league_competitiveness': {
            'depends_on_tables': ['fact_matchup'],
            'depends_on_marts': ['mart_league_summary'],
            'refresh_priority': 'LOW',
            'description': 'League competitiveness analysis'
        },
        'vw_player_breakout_analysis': {
            'depends_on_tables': ['dim_player'],
            'depends_on_marts': ['mart_player_value'],
            'refresh_priority': 'LOW',
            'description': 'Player breakout and value analysis'
        },
        'vw_trade_analysis': {
            'depends_on_tables': ['fact_transaction', 'dim_league', 'dim_player', 'dim_team'],
            'depends_on_marts': [],
            'refresh_priority': 'MEDIUM',
            'description': 'Trade transaction analysis'
        },
        'vw_league_record_book': {
            'depends_on_tables': ['fact_team_performance', 'fact_matchup', 'fact_transaction', 'dim_team', 'dim_league', 'dim_week', 'dim_manager'],
            'depends_on_marts': ['mart_manager_performance'],
            'refresh_priority': 'LOW',
            'description': 'Comprehensive fantasy football league records'
        }
    }
    
    def __init__(self, database_url: str = None, data_file: str = None, force_rebuild: bool = False):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        self.data_file = data_file
        self.force_rebuild = force_rebuild  # Add force_rebuild parameter
        self.engine = None
        self.session = None
        self.data = None
        self.load_stats = {
            'tables_processed': 0,
            'dimensions_processed': 0,
            'facts_processed': 0,
            'marts_processed': 0,
            'records_processed': 0,
            'errors': []
        }
        self.changed_tables = set()  # Track which operational tables changed
        self.refreshed_tables = set()  # Track which EDW tables were refreshed
        self.view_refresh_stats = {
            'views_checked': 0,
            'views_refreshed': 0,
            'views_skipped': 0,
            'views_failed': 0
        }
        
        if not self.database_url:
            raise ValueError("DATABASE_URL required: set as environment variable or pass directly")
        
        # Track processing statistics
        self.stats = {
            'dimensions': ETLStats(),
            'facts': ETLStats(),
            'marts': ETLStats(),
            'total': ETLStats()
        }
        
        # Dimension key mappings (operational ID -> EDW key)
        self.dim_mappings = {
            'season_keys': {},     # season_year -> season_key
            'league_keys': {},     # league_id -> league_key
            'team_keys': {},       # team_id -> team_key
            'player_keys': {},     # player_id -> player_key
            'manager_keys': {},    # manager_name -> manager_key
            'week_keys': {}        # (season_year, week_number) -> week_key
        }
    
    def connect(self) -> bool:
        """Connect to database"""
        try:
            logger.info("🔌 Connecting to EDW database...")
            
            # Fix URL for newer SQLAlchemy
            url = self.database_url.replace('postgres://', 'postgresql://', 1)
            self.engine = create_engine(url)
            
            # Create session
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            
            # Test connection
            with self.engine.connect() as conn:
                version = conn.execute(text("SELECT version()")).fetchone()[0]
                logger.info(f"✅ EDW Connected: {version.split()[0:2]}")
            
            return True
        except Exception as e:
            logger.error(f"❌ EDW Connection failed: {e}")
            return False
    
    def load_data(self) -> bool:
        """Load data from JSON file or operational database tables"""
        logger.info(f"🔍 DEBUG: load_data() called with data_file = '{self.data_file}'")
        logger.info(f"🔍 DEBUG: data_file type = {type(self.data_file)}")
        logger.info(f"🔍 DEBUG: data_file is None? {self.data_file is None}")
        logger.info(f"🔍 DEBUG: bool(data_file) = {bool(self.data_file)}")
        
        if not self.data_file:
            logger.info("📊 No data file provided - using database-only mode")
            return self.load_data_from_database()
            
        try:
            logger.info(f"📂 Loading operational data from {self.data_file}...")
            
            with open(self.data_file, 'r') as f:
                self.data = json.load(f)
            
            # Log summary
            total_records = sum(len(records) for records in self.data.values() if records)
            logger.info(f"✅ Operational data loaded: {total_records:,} total records")
            for table, records in self.data.items():
                if records:
                    logger.info(f"  📊 {table}: {len(records):,}")
                    self.changed_tables.add(table)  # Mark as changed for EDW processing
            
            return True
        except Exception as e:
            logger.error(f"❌ Data loading failed: {e}")
            return False
    
    def load_data_from_database(self) -> bool:
        """Load data directly from operational database tables"""
        try:
            logger.info("📊 Loading data from operational database tables...")
            logger.info(f"🔗 Using database engine: {self.engine.url}")
            
            self.data = {}
            operational_tables = ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks', 'statistics']
            
            with self.engine.connect() as conn:
                logger.info("🔌 Database connection established successfully")
                
                # Test connection with a simple query
                try:
                    test_result = conn.execute(text("SELECT 1 as test"))
                    logger.info(f"✅ Database test query successful: {test_result.fetchone()}")
                except Exception as test_e:
                    logger.error(f"❌ Database test query failed: {test_e}")
                    return False
                
                for table in operational_tables:
                    try:
                        logger.info(f"🔍 Loading table: {table}")
                        
                        # Check if table exists first
                        table_check = conn.execute(text("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = :table_name
                            )
                        """), {"table_name": table})
                        table_exists = table_check.fetchone()[0]
                        
                        if not table_exists:
                            logger.error(f"❌ Table {table} does not exist in public schema!")
                            self.data[table] = []
                            continue
                        
                        # Load all records from operational table
                        logger.info(f"🔍 Executing: SELECT * FROM public.{table}")
                        result = conn.execute(text(f"SELECT * FROM public.{table}"))
                        records = []
                        row_count = 0
                        
                        for row in result:
                            row_count += 1
                            record = dict(row._mapping)
                            # Convert any datetime fields to strings for consistency
                            for key, value in record.items():
                                if isinstance(value, (datetime, date)):
                                    record[key] = value.isoformat()
                            records.append(record)
                        
                        self.data[table] = records
                        logger.info(f"✅ {table}: {len(records):,} records loaded successfully")
                        
                        if records:
                            self.changed_tables.add(table)
                            # Show first record as sample
                            logger.info(f"📄 Sample record from {table}: {records[0] if records else 'None'}")
                        else:
                            logger.warning(f"⚠️ {table} table exists but contains no records")
                        
                    except Exception as e:
                        logger.error(f"❌ Error loading {table}: {str(e)}")
                        logger.error(f"❌ Exception type: {type(e).__name__}")
                        import traceback
                        logger.error(f"❌ Full traceback: {traceback.format_exc()}")
                        self.data[table] = []
            
            total_records = sum(len(records) for records in self.data.values())
            logger.info(f"✅ Database data loaded: {total_records:,} total records")
            
            # Log summary of what was loaded
            for table, records in self.data.items():
                logger.info(f"📊 Final count - {table}: {len(records)} records")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Database data loading failed: {e}")
            import traceback
            logger.error(f"❌ Full traceback: {traceback.format_exc()}")
            return False
    
    def detect_operational_changes(self) -> bool:
        """Detect which operational tables have changed since last EDW run"""
        try:
            logger.info("🔍 Detecting operational table changes...")
            
            with self.engine.connect() as conn:
                # Check if we have EDW metadata table to track last run
                metadata_check = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'edw_metadata'
                    );
                """
                
                has_metadata = conn.execute(text(metadata_check)).fetchone()[0]
                
                if not has_metadata:
                    # First run - process all tables
                    logger.info("🆕 First EDW run - processing all operational tables")
                    self.changed_tables = set(['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks'])
                    
                    # Create metadata table
                    create_metadata = """
                        CREATE TABLE IF NOT EXISTS edw.edw_metadata (
                            table_name VARCHAR(50) PRIMARY KEY,
                            last_processed_at TIMESTAMP,
                            record_count INTEGER,
                            checksum VARCHAR(32)
                        );
                    """
                    conn.execute(text(create_metadata))
                    conn.commit()
                else:
                    # Check for changes since last run
                    operational_tables = ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks']
                    
                    for table in operational_tables:
                        try:
                            # Get current record count and basic checksum
                            current_count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
                            
                            # Get last processed info
                            last_info = conn.execute(text("""
                                SELECT record_count, last_processed_at 
                                FROM edw.edw_metadata 
                                WHERE table_name = :table
                            """), {"table": table}).fetchone()
                            
                            if not last_info or last_info[0] != current_count:
                                logger.info(f"📈 {table}: Changed (count: {last_info[0] if last_info else 0} → {current_count})")
                                self.changed_tables.add(table)
                            else:
                                logger.info(f"✅ {table}: No changes (count: {current_count})")
                                
                        except Exception as e:
                            logger.warning(f"⚠️ Could not check {table}: {e}")
                            self.changed_tables.add(table)  # Include in processing to be safe
                
                logger.info(f"🎯 Tables to process: {self.changed_tables}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Change detection failed: {e}")
            # Process all tables as fallback
            self.changed_tables = set(['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks'])
            return True
    
    def update_metadata(self, table_name: str) -> None:
        """Update metadata after processing a table"""
        try:
            with self.engine.connect() as conn:
                current_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
                
                upsert_metadata = """
                    INSERT INTO edw.edw_metadata (table_name, last_processed_at, record_count, checksum)
                    VALUES (:table, :timestamp, :count, 'placeholder')
                    ON CONFLICT (table_name) 
                    DO UPDATE SET 
                        last_processed_at = :timestamp,
                        record_count = :count,
                        checksum = 'placeholder'
                """
                
                conn.execute(text(upsert_metadata), {
                    "table": table_name,
                    "timestamp": datetime.now(),
                    "count": current_count
                })
                conn.commit()
                
        except Exception as e:
            logger.warning(f"⚠️ Could not update metadata for {table_name}: {e}")
    
    def extract_seasons(self) -> List[Dict]:
        """Extract unique seasons from leagues data with derived week information"""
        seasons = {}
        
        if not self.data:
            logger.warning("⚠️ No data available for season extraction")
            return list(seasons.values())
        
        # Build season metrics from actual matchup data
        season_metrics = {}
        
        # Analyze matchups to get actual week ranges and playoff info
        for matchup in self.data.get('matchups', []):
            # Get season from league data
            season_year = None
            for league in self.data.get('leagues', []):
                if league['league_id'] == matchup['league_id']:
                    season_year = int(league['season'])
                    break
            
            if not season_year:
                continue
            
            if season_year not in season_metrics:
                season_metrics[season_year] = {
                    'weeks': set(),
                    'playoff_weeks': set(),
                    'championship_weeks': set(),
                    'start_week': None,
                    'end_week': None
                }
            
            week = matchup['week']
            season_metrics[season_year]['weeks'].add(week)
            
            if matchup.get('is_playoffs'):
                season_metrics[season_year]['playoff_weeks'].add(week)
            if matchup.get('is_championship'):
                season_metrics[season_year]['championship_weeks'].add(week)
        
        # Calculate derived metrics for each season
        for season_year, metrics in season_metrics.items():
            if metrics['weeks']:
                metrics['start_week'] = min(metrics['weeks'])
                metrics['end_week'] = max(metrics['weeks'])
                metrics['total_weeks'] = len(metrics['weeks'])
                
                # Determine playoff start week (first week where playoffs=true)
                if metrics['playoff_weeks']:
                    metrics['playoff_start_week'] = min(metrics['playoff_weeks'])
                else:
                    # Fallback: assume playoffs start at week total_weeks - 2
                    metrics['playoff_start_week'] = max(1, metrics['end_week'] - 2)
                
                # Championship week is the latest week with championship games
                if metrics['championship_weeks']:
                    metrics['championship_week'] = max(metrics['championship_weeks'])
                else:
                    # Fallback: championship is the last week
                    metrics['championship_week'] = metrics['end_week']
        
        # Use league configuration data as additional source
        league_configs = {}
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if season_year not in league_configs:
                league_configs[season_year] = {
                    'start_weeks': [],
                    'end_weeks': [],
                    'current_weeks': []
                }
            
            if league.get('start_week'):
                league_configs[season_year]['start_weeks'].append(league['start_week'])
            if league.get('end_week'):
                league_configs[season_year]['end_weeks'].append(league['end_week'])
            if league.get('current_week'):
                league_configs[season_year]['current_weeks'].append(league['current_week'])
        
        # Create final season records
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if season_year not in seasons:
                metrics = season_metrics.get(season_year, {})
                config = league_configs.get(season_year, {})
                
                # Use derived data or intelligent fallbacks
                total_weeks = metrics.get('total_weeks', 17)
                start_week = metrics.get('start_week', 1)
                end_week = metrics.get('end_week', total_weeks)
                playoff_start_week = metrics.get('playoff_start_week', max(1, end_week - 2))
                championship_week = metrics.get('championship_week', end_week)
                
                # Override with league config if available (prefer mode/most common value)
                if config.get('start_weeks'):
                    start_week = max(set(config['start_weeks']), key=config['start_weeks'].count)
                if config.get('end_weeks'):
                    end_week = max(set(config['end_weeks']), key=config['end_weeks'].count)
                
                seasons[season_year] = {
                    'season_year': season_year,
                    'season_start_date': date(season_year, 9, 1),  # NFL season typically starts early September
                    'season_end_date': date(season_year + 1, 1, 31),  # Fantasy typically ends in January
                    'playoff_start_week': playoff_start_week,
                    'championship_week': championship_week,
                    'total_weeks': total_weeks,
                    'is_current_season': season_year == datetime.now().year,
                    'season_status': 'completed' if season_year < datetime.now().year else 'active'
                }
        
        logger.info(f"📅 Extracted {len(seasons)} seasons with derived week information")
        for season_year in sorted(seasons.keys()):
            s = seasons[season_year]
            logger.info(f"  {season_year}: {s['total_weeks']} weeks, playoffs start week {s['playoff_start_week']}, championship week {s['championship_week']}")
        
        return list(seasons.values())
    
    def extract_weeks(self) -> List[Dict]:
        """Extract week dimension from available data with calculated week dates"""
        weeks = {}
        
        # Build league-to-season mapping
        league_to_season = {}
        for league in self.data.get('leagues', []):
            league_to_season[league['league_id']] = int(league['season'])
        
        # Build season info for week classification (use the updated season logic)
        season_playoff_starts = {}
        season_championships = {}
        
        # Use the same logic as extract_seasons to get playoff/championship week info
        season_metrics = {}
        for matchup in self.data.get('matchups', []):
            season_year = None
            for league in self.data.get('leagues', []):
                if league['league_id'] == matchup['league_id']:
                    season_year = int(league['season'])
                    break
            
            if not season_year:
                continue
            
            if season_year not in season_metrics:
                season_metrics[season_year] = {
                    'playoff_weeks': set(),
                    'championship_weeks': set()
                }
            
            if matchup.get('is_playoffs'):
                season_metrics[season_year]['playoff_weeks'].add(matchup['week'])
            if matchup.get('is_championship'):
                season_metrics[season_year]['championship_weeks'].add(matchup['week'])
        
        # Calculate playoff/championship weeks for each season
        for season_year, metrics in season_metrics.items():
            if metrics['playoff_weeks']:
                season_playoff_starts[season_year] = min(metrics['playoff_weeks'])
            if metrics['championship_weeks']:
                season_championships[season_year] = max(metrics['championship_weeks'])
        
        # Extract from matchup data (has week information)
        for matchup in self.data.get('matchups', []):
            # Get season from league mapping instead of defaulting to 2024
            season_year = league_to_season.get(matchup['league_id'], 2024)
            week_number = matchup['week']
            
            key = (season_year, week_number)
            if key not in weeks:
                # Calculate week dates based on NFL season patterns
                week_start_date, week_end_date = self.calculate_week_dates(season_year, week_number)
                
                # Determine week type based on actual playoff data for this season
                week_type = self.classify_week_type_for_season(
                    season_year, 
                    week_number, 
                    season_playoff_starts.get(season_year), 
                    season_championships.get(season_year)
                )
                
                weeks[key] = {
                    'season_year': season_year,
                    'week_number': week_number,
                    'week_type': week_type,
                    'week_start_date': week_start_date,
                    'week_end_date': week_end_date,
                    'is_current_week': self.is_current_week(season_year, week_number)
                }
        
        logger.info(f"📅 Extracted {len(weeks)} weeks with calculated dates")
        
        return list(weeks.values())
    
    def classify_week_type(self, week_number: int) -> str:
        """Classify week type based on week number (legacy - use classify_week_type_for_season)"""
        if week_number <= 14:
            return 'regular'
        elif week_number <= 16:
            return 'playoffs'
        else:
            return 'championship'
    
    def classify_week_type_for_season(self, season_year: int, week_number: int, 
                                    playoff_start_week: int = None, championship_week: int = None) -> str:
        """Classify week type based on actual season data"""
        if championship_week and week_number >= championship_week:
            return 'championship'
        elif playoff_start_week and week_number >= playoff_start_week:
            return 'playoffs'
        else:
            return 'regular'
    
    def calculate_week_dates(self, season_year: int, week_number: int) -> tuple:
        """Calculate week start and end dates based on NFL season patterns"""
        from datetime import date, timedelta
        
        # NFL season typically starts the first or second week of September
        # Week 1 usually starts on the Tuesday after Labor Day (first Monday in September)
        
        # Calculate Labor Day (first Monday in September)
        labor_day = date(season_year, 9, 1)
        while labor_day.weekday() != 0:  # 0 = Monday
            labor_day += timedelta(days=1)
        
        # NFL Week 1 typically starts the Tuesday after Labor Day
        nfl_week_1_start = labor_day + timedelta(days=1)  # Tuesday
        
        # However, some seasons start a week earlier or later
        # Adjust based on historical patterns
        if season_year <= 2020:
            # Pre-2021: Often started earlier
            if season_year <= 2010:
                nfl_week_1_start = date(season_year, 8, 31)  # Late August start
            else:
                nfl_week_1_start = date(season_year, 9, 1)   # Early September
        else:
            # 2021+: More standardized start around Labor Day
            nfl_week_1_start = labor_day + timedelta(days=1)
        
        # Ensure it's a Tuesday (fantasy weeks typically run Tuesday-Monday)
        while nfl_week_1_start.weekday() != 1:  # 1 = Tuesday
            nfl_week_1_start += timedelta(days=1)
        
        # Calculate this week's dates (each week is 7 days, Tuesday-Monday)
        week_start_date = nfl_week_1_start + timedelta(weeks=week_number - 1)
        week_end_date = week_start_date + timedelta(days=6)  # Monday
        
        return week_start_date, week_end_date
    
    def is_current_week(self, season_year: int, week_number: int) -> bool:
        """Determine if this is the current week"""
        from datetime import date
        
        current_date = date.today()
        current_year = current_date.year
        
        # Only current season can have current week
        if season_year != current_year:
            return False
        
        # Calculate the current week dates
        week_start_date, week_end_date = self.calculate_week_dates(season_year, week_number)
        
        # Check if today falls within this week
        return week_start_date <= current_date <= week_end_date
    
    def get_week_from_date(self, transaction_date: date, season_year: int) -> int:
        """Find which NFL week a transaction date falls into using dim_week data"""
        from datetime import date
        
        try:
            # Query dim_week to find the week that contains this date
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT week_number 
                    FROM edw.dim_week 
                    WHERE season_year = :season_year 
                    AND :transaction_date BETWEEN week_start_date AND week_end_date
                    LIMIT 1
                """), {
                    "season_year": season_year,
                    "transaction_date": transaction_date
                })
                
                row = result.fetchone()
                if row:
                    return row[0]
                else:
                    # If no exact match found, find the closest week by date
                    # This handles edge cases where transaction might fall between weeks
                    result = conn.execute(text("""
                        SELECT week_number
                        FROM edw.dim_week 
                        WHERE season_year = :season_year 
                        ORDER BY ABS(week_start_date - :transaction_date)
                        LIMIT 1
                    """), {
                        "season_year": season_year,
                        "transaction_date": transaction_date
                    })
                    
                    row = result.fetchone()
                    if row:
                        return row[0]
                    else:
                        # Fallback: return week 1 if no weeks found for this season
                        logger.warning(f"⚠️ No weeks found for season {season_year}, defaulting to week 1")
                        return 1
                        
        except Exception as e:
            logger.warning(f"⚠️ Could not determine week for date {transaction_date} in season {season_year}: {e}")
            # Fallback to simple calculation if database query fails
            return self.calculate_week_from_date_fallback(transaction_date, season_year)
    
    def get_week_from_preloaded_data(self, transaction_date: date, season_year: int, week_lookup: dict) -> int:
        """Fast in-memory lookup of week number from preloaded week data"""
        
        # Get weeks for this season
        season_weeks = week_lookup.get(season_year, [])
        if not season_weeks:
            # Fallback to calculation if no weeks available for this season
            return self.calculate_week_from_date_fallback(transaction_date, season_year)
        
        # Find the week that contains this date
        for week_info in season_weeks:
            if week_info['start_date'] <= transaction_date <= week_info['end_date']:
                return week_info['week_number']
        
        # If no exact match, find the closest week
        closest_week = None
        min_diff = float('inf')
        
        for week_info in season_weeks:
            start_diff = abs((transaction_date - week_info['start_date']).days)
            end_diff = abs((transaction_date - week_info['end_date']).days)
            week_diff = min(start_diff, end_diff)
            
            if week_diff < min_diff:
                min_diff = week_diff
                closest_week = week_info['week_number']
        
        return closest_week if closest_week else 1

    def get_championship_week_for_season(self, season_year: int) -> int:
        """Get the championship week number for a given season"""
        # Championship week logic based on season structure
        if season_year <= 2009 or season_year in [2006, 2007]:
            return 16  # Shorter seasons had championship in week 16
        elif season_year <= 2021:
            return 16  # Standard seasons had championship in week 16
        else:
            return 17  # Modern seasons have championship in week 17

    def calculate_week_from_date_fallback(self, transaction_date: date, season_year: int) -> int:
        """Fallback method to calculate week if dim_week lookup fails"""
        from datetime import date, timedelta
        
        # Get the start date of NFL Week 1 for this season
        week_1_start, _ = self.calculate_week_dates(season_year, 1)
        
        # Calculate days since week 1 started
        days_since_start = (transaction_date - week_1_start).days
        
        # Each week is 7 days, so calculate which week this falls into
        if days_since_start < 0:
            # Transaction happened before the season started (probably preseason)
            return 0  # Week 0 for preseason transactions
        else:
            # Add 1 because week numbering starts at 1
            week_number = (days_since_start // 7) + 1
            
            # Cap at reasonable max week number (17 for modern seasons, 16 for older)
            max_weeks = 17 if season_year >= 2021 else 16
            return min(week_number, max_weeks)
    
    def transform_leagues(self) -> List[Dict]:
        """Transform leagues into dimension format"""
        transformed = []
        
        if not self.data:
            logger.warning("⚠️ No data available for league transformation")
            return transformed
        
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            # Use the comprehensive league of record check
            if not self.is_league_of_record(league['league_id'], season_year):
                continue
                
            transformed.append({
                'league_id': league['league_id'],
                'league_name': league['name'],
                'season_year': season_year,
                'num_teams': league['num_teams'],
                'league_type': league.get('league_type', 'private'),
                'scoring_type': 'standard',  # Default, could be extracted from settings
                'draft_type': 'snake',  # Default, could be extracted from draft data
                'is_active': True,
                'valid_from': datetime.now(),
                'valid_to': datetime(9999, 12, 31)
            })
        
        return transformed
    
    def transform_teams(self) -> List[Dict]:
        """Transform teams into dimension format"""
        transformed = []
        
        if not self.data:
            logger.warning("⚠️ No data available for team transformation")
            return transformed
        
        # Build set of league of record IDs for efficient lookup
        league_of_record_ids = set()
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if self.is_league_of_record(league['league_id'], season_year):
                league_of_record_ids.add(league['league_id'])
        
        # Cache manager keys for lookup
        manager_keys = self.dim_mappings.get('manager_keys', {})
        
        for team in self.data.get('teams', []):
            # Only include teams from leagues of record
            if team['league_id'] not in league_of_record_ids:
                continue
            
            # Get manager name using team_id override logic, then consolidation
            raw_manager_name = team.get('manager_name')
            team_id = team['team_id']
            if raw_manager_name:
                consolidated_manager_name = self.get_manager_name_by_team_id(team_id, raw_manager_name)
                manager_key = manager_keys.get(consolidated_manager_name)
            else:
                consolidated_manager_name = None
                manager_key = None
                
            transformed.append({
                'team_id': team['team_id'],
                'league_id': team['league_id'],  # Will be mapped to league_key later
                'manager_key': manager_key,
                'team_name': team['name'],
                'manager_name': consolidated_manager_name,
                'manager_id': team.get('manager_id', team.get('manager_name', '').replace(' ', '_').lower()),
                'team_logo_url': team.get('team_logo_url'),
                'is_active': True,
                'valid_from': datetime.now(),
                'valid_to': datetime(9999, 12, 31)
            })
        
        return transformed
    
    def transform_players(self) -> List[Dict]:
        """Transform players from transaction and draft data with position lookup"""
        transformed = []
        
        if not self.data:
            logger.warning("⚠️ No data available for player transformation")
            return transformed
        
        # First, build a position lookup map from sources that have position data
        position_lookup = {}
        
        # Extract position data from draft picks (highest quality data)
        for draft_pick in self.data.get('draft_picks', []):
            player_id = draft_pick['player_id']
            position = draft_pick.get('position')
            if position and position.strip() and position != 'Unknown':
                # Normalize position (e.g., "S,CB" -> "S")
                normalized_position = position.split(',')[0] if ',' in position else position
                position_lookup[player_id] = normalized_position
        
        # Extract position data from rosters (fallback for players not in draft)
        for roster in self.data.get('rosters', []):
            raw_player_id = roster['player_id']
            
            # Extract numeric player ID from Yahoo format  
            if '.p.' in raw_player_id:
                numeric_player_id = raw_player_id.split('.p.')[-1]
            else:
                numeric_player_id = raw_player_id
                
            position = roster.get('position')
            if position and position.strip() and position != 'Unknown':
                # Only use if we don't already have position data from draft
                if numeric_player_id not in position_lookup:
                    normalized_position = position.split(',')[0] if ',' in position else position
                    position_lookup[numeric_player_id] = normalized_position
        
        logger.info(f"🏈 Built position lookup for {len(position_lookup)} players")
        
        # Collect unique players from all sources
        unique_players = {}
        
        # Extract players from transactions (most comprehensive player list)
        for transaction in self.data.get('transactions', []):
            raw_player_id = transaction['player_id']
            
            # Extract numeric player ID from Yahoo format (e.g., "124.p.5994" -> "5994")
            if '.p.' in raw_player_id:
                numeric_player_id = raw_player_id.split('.p.')[-1]
            else:
                numeric_player_id = raw_player_id
            
            if numeric_player_id not in unique_players:
                # Use position lookup to get position data
                position = position_lookup.get(numeric_player_id, 'Unknown')
                
                unique_players[numeric_player_id] = {
                    'player_id': numeric_player_id,
                    'player_name': transaction.get('player_name', f'Player {numeric_player_id}'),
                    'primary_position': position,
                    'eligible_positions': [position] if position != 'Unknown' else [],
                    'nfl_team': 'Unknown',
                    'jersey_number': None,
                    'rookie_year': None,
                    'is_active': True,
                    'valid_from': date.today(),
                    'valid_to': None
                }
        
        # Extract players from draft picks (ensure we have all drafted players)
        for draft_pick in self.data.get('draft_picks', []):
            player_id = draft_pick['player_id']
            
            if player_id not in unique_players:
                position = position_lookup.get(player_id, 'Unknown')
                
                unique_players[player_id] = {
                    'player_id': player_id,
                    'player_name': draft_pick.get('player_name', f'Player {player_id}'),
                    'primary_position': position,
                    'eligible_positions': [position] if position != 'Unknown' else [],
                    'nfl_team': 'Unknown',
                    'jersey_number': None,
                    'rookie_year': None,
                    'is_active': True,
                    'valid_from': date.today(),
                    'valid_to': None
                }
        
        transformed = list(unique_players.values())
        
        # Count how many players have position data
        players_with_positions = sum(1 for player in transformed if player['primary_position'] != 'Unknown')
        logger.info(f"🏈 Extracted {len(transformed)} unique players from transactions and draft data")
        logger.info(f"🏈 {players_with_positions} players have position data ({players_with_positions/len(transformed)*100:.1f}%)")
        
        return transformed
    
    def transform_fact_roster(self) -> List[Dict]:
        """Transform roster data into fact_roster format with proper data sources"""
        facts = []
        
        if not self.data:
            logger.warning("⚠️ No data available for roster fact transformation")
            return facts

        # Check if roster data exists
        rosters_data = self.data.get('rosters', [])
        if not rosters_data:
            logger.info("🏈 No roster data available - skipping fact_roster")
            return facts

        logger.info(f"📋 Processing {len(rosters_data)} roster records with enhanced data sources...")

        # Build set of league of record IDs for efficient lookup
        league_of_record_ids = set()
        league_to_season = {}
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if self.is_league_of_record(league['league_id'], season_year):
                league_of_record_ids.add(league['league_id'])
                league_to_season[league['league_id']] = season_year

        # Build acquisition data from transactions and drafts
        acquisition_data = {}
        logger.info("📊 Building acquisition data from transactions and drafts...")
        
        # Process draft data for acquisition info
        for draft in self.data.get('draft_picks', []):
            if draft['league_id'] not in league_of_record_ids:
                continue
            
            player_id = str(draft['player_id'])
            team_id = f"{draft['league_id']}.t.{draft['team_id']}"
            season_year = league_to_season.get(draft['league_id'])
            
            if not season_year:
                continue
                
            # Store draft acquisition info
            key = (draft['league_id'], team_id, player_id, season_year)
            acquisition_data[key] = {
                'type': 'draft',
                'date': None,  # Draft date not available in current data
                'cost': float(draft.get('cost', 0)) if draft.get('cost') else 0.0
            }
        
        # Process transaction data for acquisition info
        for transaction in self.data.get('transactions', []):
            if transaction['league_id'] not in league_of_record_ids:
                continue
                
            # Extract numeric player_id from full format (e.g., "153.p.7755" -> "7755")
            raw_player_id = transaction['player_id']
            if '.p.' in raw_player_id:
                player_id = raw_player_id.split('.p.')[-1]
            else:
                player_id = str(raw_player_id)
                
            to_team_id = transaction.get('destination_team_id')  # Already in full format
            season_year = league_to_season.get(transaction['league_id'])
            
            if not to_team_id or not season_year:
                continue
            
            # Map transaction type (Fixed: use actual transaction types from database)
            trans_type = transaction.get('type', 'unknown')
            if trans_type in ['add', 'add/drop']:
                acq_type = 'waiver'  # Yahoo adds are typically waiver/FAAB
            elif trans_type in ['trade']:
                acq_type = 'trade'
            elif trans_type in ['drop']:
                acq_type = None  # Don't track drops as acquisitions
                continue
            else:
                acq_type = 'unknown'
            
            # Store transaction acquisition info (to_team_id already in full format)
            key = (transaction['league_id'], to_team_id, player_id, season_year)
            acquisition_data[key] = {
                'type': acq_type,
                'date': transaction.get('timestamp'),  # Fixed: use timestamp
                'cost': float(transaction.get('faab_bid', 0)) if transaction.get('faab_bid') else 0.0
            }
        
        # Build weekly points data from statistics
        weekly_points_data = {}
        logger.info("📊 Building weekly points data from statistics...")
        
        for stat in self.data.get('statistics', []):
            if stat['league_id'] not in league_of_record_ids:
                continue
                
            player_id = str(stat['player_id'])
            week = stat.get('week_number')  # Fixed: use week_number instead of week
            season_year = league_to_season.get(stat['league_id'])
            
            if not week or not season_year:
                continue
                
            weekly_points = float(stat.get('weekly_fantasy_points', 0))
            
            # Store weekly points
            key = (stat['league_id'], player_id, season_year, week)
            weekly_points_data[key] = weekly_points

        # Use cached dimension mappings (no database queries in loop)
        league_keys = self.dim_mappings.get('league_keys', {})
        team_keys = self.dim_mappings.get('team_keys', {})
        player_keys = self.dim_mappings.get('player_keys', {})
        manager_keys = self.dim_mappings.get('manager_keys', {})
        week_keys = self.dim_mappings.get('week_keys', {})
        
        debug_count = 0
        filtered_league = 0
        missing_season = 0
        missing_keys = 0
        successful = 0
        
        for roster in rosters_data:
            debug_count += 1
            
            # Only include rosters from leagues of record
            if roster['league_id'] not in league_of_record_ids:
                filtered_league += 1
                continue
                
            season_year = league_to_season.get(roster['league_id'])
            if not season_year:
                missing_season += 1
                continue
                
            league_key = league_keys.get(roster['league_id'])
            
            # Map team_id to full team_id format for lookup (same pattern as transform_fact_draft)
            raw_team_id = roster['team_id']
            league_id = roster['league_id']
            # Construct full team_id: league_id + ".t." + team_id
            full_team_id = f"{league_id}.t.{raw_team_id}"
            team_key = team_keys.get(full_team_id)
            
            # Player ID is already numeric in roster data
            numeric_player_id = str(roster['player_id'])
            player_key = player_keys.get(numeric_player_id)
            week_key = week_keys.get((season_year, roster['week']))
            
            # Get manager_key from team data (roster doesn't have manager_name)
            manager_key = None
            raw_manager_name = None  # Initialize to avoid reference errors
            for team in self.data.get('teams', []):
                if team['team_id'] == full_team_id:  # Use full_team_id for lookup
                    raw_manager_name = team.get('manager_name')
                    if raw_manager_name:
                        consolidated_manager_name = self.get_manager_name_by_team_id(full_team_id, raw_manager_name)
                        manager_key = manager_keys.get(consolidated_manager_name)
                    break
            
            if not all([league_key, team_key, player_key, week_key, manager_key]):
                missing_keys += 1
                continue
            
            successful += 1
            
            # Calculate roster status flags
            player_status = roster.get('status', 'active')
            player_position = roster.get('position', 'BN')
            is_starter = roster.get('is_starter', False)
            
            # Determine bench and IR status
            is_bench = (player_status == 'bench') or (not is_starter)
            is_ir = (player_position in ['IR', 'INJ', 'INJURED']) or (player_status in ['ir', 'injured', 'inactive'])
            
            # Get acquisition info from our built data (use full_team_id to match transactions)
            acq_key = (league_id, full_team_id, numeric_player_id, season_year)
            acquisition_info = acquisition_data.get(acq_key, {
                'type': None,
                'date': None,
                'cost': None
            })
            
            # Get weekly points from statistics data
            points_key = (league_id, numeric_player_id, season_year, roster['week'])
            weekly_points = weekly_points_data.get(points_key, 0.0)
            
            facts.append({
                'league_key': league_key,
                'team_key': team_key,
                'manager_key': manager_key,
                'player_key': player_key,
                'week_key': week_key,
                'season_year': season_year,
                'is_starter': is_starter,
                'is_bench': is_bench,
                'is_ir': is_ir,
                'roster_position': player_position,
                'weekly_points': weekly_points,  # Now from statistics data
                'projected_points': float(roster.get('projected_points', 0)) if roster.get('projected_points') is not None else None,
                'acquisition_type': acquisition_info['type'],  # Now from transactions/drafts
                'acquisition_date': acquisition_info['date'],  # Now from transactions/drafts
                'acquisition_cost': acquisition_info['cost']   # Now from transactions/drafts (FAAB)
            })
        
        logger.info(f"📋 Transformed {len(facts)} roster facts from {len(rosters_data)} roster records")
        logger.info(f"🔍 Roster Debug: filtered_league={filtered_league}, missing_season={missing_season}, missing_keys={missing_keys}, successful={successful}")
        logger.info(f"📊 Enhanced with {len(acquisition_data)} acquisition records and {len(weekly_points_data)} weekly points")
        return facts
    
    def transform_fact_matchup(self) -> List[Dict]:
        """Transform matchup data into fact_matchup format"""
        facts = []
        
        if not self.data:
            logger.warning("⚠️ No data available for matchup fact transformation")
            return facts

        # Build league-to-season mapping and league of record set for efficient lookup
        league_to_season = {}
        league_of_record_ids = set()
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if self.is_league_of_record(league['league_id'], season_year):
                league_to_season[league['league_id']] = season_year
                league_of_record_ids.add(league['league_id'])

        # Use cached dimension mappings (no database queries in loop)
        league_keys = self.dim_mappings.get('league_keys', {})
        team_keys = self.dim_mappings.get('team_keys', {})
        manager_keys = self.dim_mappings.get('manager_keys', {})
        week_keys = self.dim_mappings.get('week_keys', {})
        
        for matchup in self.data.get('matchups', []):
            # Only include matchups from leagues of record
            if matchup['league_id'] not in league_of_record_ids:
                continue
                
            # Get season from league mapping
            season_year = league_to_season.get(matchup['league_id'], 2024)
            
            league_key = league_keys.get(matchup['league_id'])
            team1_key = team_keys.get(matchup['team1_id'])
            team2_key = team_keys.get(matchup['team2_id'])
            week_key = week_keys.get((season_year, matchup['week']))
            
            # Get manager keys for both teams
            manager1_key = None
            manager2_key = None
            for team in self.data.get('teams', []):
                if team['team_id'] == matchup['team1_id']:
                    raw_manager_name = team.get('manager_name')
                    if raw_manager_name:
                        consolidated_manager_name = self.get_manager_name_by_team_id(matchup['team1_id'], raw_manager_name)
                        manager1_key = manager_keys.get(consolidated_manager_name)
                elif team['team_id'] == matchup['team2_id']:
                    raw_manager_name = team.get('manager_name')
                    if raw_manager_name:
                        consolidated_manager_name = self.get_manager_name_by_team_id(matchup['team2_id'], raw_manager_name)
                        manager2_key = manager_keys.get(consolidated_manager_name)
            
            if not all([league_key, team1_key, team2_key, week_key, manager1_key, manager2_key]):
                continue
            
            team1_points = float(matchup.get('team1_score', 0))
            team2_points = float(matchup.get('team2_score', 0))
            
            # Determine matchup type based on operational flags (SIMPLIFIED)
            # The operational data is already fixed, so just trust it
            is_source_championship = matchup.get('is_championship', False)
            is_consolation = matchup.get('is_consolation', False)
            is_playoffs = matchup.get('is_playoffs', False)
            
            # Simple logic: trust the operational data
            if is_source_championship:
                matchup_type = 'championship'
            elif is_consolation:
                matchup_type = 'consolation'
            elif is_playoffs:
                matchup_type = 'playoffs'
            else:
                matchup_type = 'regular'
            
            # Determine winner manager key
            winner_manager_key = None
            if team1_points > team2_points:
                winner_manager_key = manager1_key
            elif team2_points > team1_points:
                winner_manager_key = manager2_key
            
            # Extract playoff flags from operational data
            is_semifinal = matchup.get('is_semifinal', False)
            is_quarterfinal = matchup.get('is_quarterfinal', False)
            is_last_place_game = matchup.get('is_last_place_game', False)
            
            facts.append({
                'league_key': int(league_key),
                'week_key': int(week_key),
                'season_year': int(season_year),
                'team1_key': int(team1_key),
                'team2_key': int(team2_key),
                'manager1_key': int(manager1_key),
                'manager2_key': int(manager2_key),
                'team1_points': float(team1_points),
                'team2_points': float(team2_points),
                'point_difference': float(abs(team1_points - team2_points)),
                'total_points': float(team1_points + team2_points),
                'winner_team_key': int(team1_key) if team1_points > team2_points else int(team2_key) if team2_points > team1_points else None,
                'winner_manager_key': int(winner_manager_key) if winner_manager_key else None,
                'is_tie': bool(team1_points == team2_points),
                'margin_of_victory': float(abs(team1_points - team2_points)),
                'matchup_type': str(matchup_type),
                'is_playoffs': bool(matchup_type in ['playoffs', 'championship']),
                'is_championship': bool(matchup_type == 'championship'),
                'is_semifinal': bool(is_semifinal),
                'is_quarterfinal': bool(is_quarterfinal),
                'is_last_place_game': bool(is_last_place_game),
                'is_consolation': bool(matchup_type == 'consolation')
            })
        
        # No post-processing needed - operational data is already correct
        return facts
    
    def _fix_championship_duplicates(self, facts: List[Dict]) -> List[Dict]:
        """
        Fix championship duplicates using proper playoff bracket logic.
        Championship game = teams that won semifinals (second-to-last week).
        """
        # Group potential championships by league and season
        potential_championships = {}
        regular_facts = []
        
        for fact in facts:
            if fact['matchup_type'] == 'potential_championship':
                key = (fact['league_key'], fact['season_year'])
                if key not in potential_championships:
                    potential_championships[key] = []
                potential_championships[key].append(fact)
            else:
                regular_facts.append(fact)
        
        # For each league-season, use bracket logic to identify championship
        for key, candidates in potential_championships.items():
            league_key, season_year = key
            
            if len(candidates) == 1:
                candidates[0]['matchup_type'] = 'championship'
                candidates[0]['is_championship'] = True
                regular_facts.append(candidates[0])
            else:
                logger.info(f"🏆 Resolving {len(candidates)} championship candidates for league {league_key} season {season_year}")
                
                # Step 1: Find the three key weeks
                championship_week = self.get_championship_week_for_season(season_year)
                semifinals_week = championship_week - 1
                quarterfinals_week = championship_week - 2
                
                # Step 2: Get week keys for lookups
                championship_week_key = self.dim_mappings['week_keys'].get((season_year, championship_week))
                semifinals_week_key = self.dim_mappings['week_keys'].get((season_year, semifinals_week))
                quarterfinals_week_key = self.dim_mappings['week_keys'].get((season_year, quarterfinals_week))
                
                # Step 3: Find quarterfinals winners and losers
                quarterfinals_winners = set()
                quarterfinals_losers = set()
                
                for fact in facts:
                    if (fact['league_key'] == league_key and 
                        fact['season_year'] == season_year and 
                        fact['week_key'] == quarterfinals_week_key and
                        fact['is_playoffs'] and 
                        not fact['is_consolation']):
                        
                        # This is a quarterfinals game - get the winner
                        if fact['winner_team_key']:
                            quarterfinals_winners.add(fact['winner_team_key'])
                            # Add the loser (the team that didn't win)
                            if fact['team1_key'] == fact['winner_team_key']:
                                quarterfinals_losers.add(fact['team2_key'])
                            else:
                                quarterfinals_losers.add(fact['team1_key'])
                
                logger.info(f"  🏆 Found {len(quarterfinals_winners)} quarterfinals winners, {len(quarterfinals_losers)} losers")
                
                # Step 4: Find semifinals winners (exclude 5th place game)
                semifinals_winners = set()
                
                for fact in facts:
                    if (fact['league_key'] == league_key and 
                        fact['season_year'] == season_year and 
                        fact['week_key'] == semifinals_week_key and
                        fact['is_playoffs'] and 
                        not fact['is_consolation']):
                        
                        # Check if this is a semifinals game or 5th place game
                        team1_won_quarters = fact['team1_key'] in quarterfinals_winners
                        team2_won_quarters = fact['team2_key'] in quarterfinals_winners
                        team1_lost_quarters = fact['team1_key'] in quarterfinals_losers
                        team2_lost_quarters = fact['team2_key'] in quarterfinals_losers
                        
                        # Semifinals game = at least one team won quarterfinals
                        # 5th place game = both teams lost quarterfinals
                        if team1_won_quarters or team2_won_quarters:
                            # This is a semifinals game - record the winner
                            if fact['winner_team_key']:
                                semifinals_winners.add(fact['winner_team_key'])
                            logger.info(f"  🏆 Semifinals game: teams {fact['team1_key']} vs {fact['team2_key']}")
                        elif team1_lost_quarters and team2_lost_quarters:
                            logger.info(f"  5️⃣ 5th place game: teams {fact['team1_key']} vs {fact['team2_key']}")
                
                logger.info(f"  🏆 Found {len(semifinals_winners)} semifinals winners")
                
                # Step 5: Championship game = game with both teams being semifinals winners
                championship_candidate = None
                for candidate in candidates:
                    team1_won_semis = candidate['team1_key'] in semifinals_winners
                    team2_won_semis = candidate['team2_key'] in semifinals_winners
                    
                    if team1_won_semis and team2_won_semis:
                        championship_candidate = candidate
                        logger.info(f"  🏆 Championship identified: both teams won semifinals")
                        break
                    else:
                        logger.info(f"  🥉 3rd place game: teams {candidate['team1_key']} vs {candidate['team2_key']}")
                
                # Step 6: Mark championship and convert others
                if championship_candidate:
                    championship_candidate['matchup_type'] = 'championship'
                    championship_candidate['is_championship'] = True
                    regular_facts.append(championship_candidate)
                    
                    # Convert others to regular playoff games (3rd place, etc.)
                    for candidate in candidates:
                        if candidate != championship_candidate:
                            candidate['matchup_type'] = 'playoffs'
                            candidate['is_championship'] = False
                            regular_facts.append(candidate)
                else:
                    # Fallback: if logic fails, pick the first one and log warning
                    logger.warning(f"  ⚠️ Could not determine championship using bracket logic for league {league_key} season {season_year}")
                    candidates[0]['matchup_type'] = 'championship'
                    candidates[0]['is_championship'] = True
                    regular_facts.append(candidates[0])
                    
                    for candidate in candidates[1:]:
                        candidate['matchup_type'] = 'playoffs'
                        candidate['is_championship'] = False
                        regular_facts.append(candidate)
        
        logger.info(f"🏆 Fixed championship duplicates: {len(potential_championships)} league-seasons processed")
        return regular_facts
    
    def transform_fact_transaction(self) -> List[Dict]:
        """Transform transaction data into fact_transaction format"""
        facts = []
        
        if not self.data:
            logger.warning("⚠️ No data available for transaction fact transformation")
            return facts

        # Build set of league of record IDs for efficient lookup
        league_of_record_ids = set()
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if self.is_league_of_record(league['league_id'], season_year):
                league_of_record_ids.add(league['league_id'])

        # Preload dim_week data to avoid N+1 queries
        logger.info("📅 Preloading week data for fast transaction week lookup...")
        week_lookup = {}
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT season_year, week_number, week_start_date, week_end_date 
                    FROM edw.dim_week 
                    ORDER BY season_year, week_number
                """))
                
                for row in result:
                    season_year = row[0]
                    week_number = row[1]
                    week_start = row[2]
                    week_end = row[3]
                    
                    if season_year not in week_lookup:
                        week_lookup[season_year] = []
                    week_lookup[season_year].append({
                        'week_number': week_number,
                        'start_date': week_start,
                        'end_date': week_end
                    })
                    
            logger.info(f"✅ Loaded week data for {len(week_lookup)} seasons")
        except Exception as e:
            logger.warning(f"⚠️ Could not preload week data: {e}")
            week_lookup = {}

        # Use cached dimension mappings (no database queries in loop)
        league_keys = self.dim_mappings.get('league_keys', {})
        team_keys = self.dim_mappings.get('team_keys', {})
        manager_keys = self.dim_mappings.get('manager_keys', {})
        player_keys = self.dim_mappings.get('player_keys', {})
        
        for transaction in self.data.get('transactions', []):
            # Only include transactions from leagues of record
            if transaction['league_id'] not in league_of_record_ids:
                continue
                
            league_key = league_keys.get(transaction['league_id'])
            
            # Extract numeric player ID from Yahoo format (e.g., "124.p.5994" -> "5994")
            raw_player_id = transaction['player_id']
            if '.p.' in raw_player_id:
                numeric_player_id = raw_player_id.split('.p.')[-1]
            else:
                numeric_player_id = raw_player_id
            
            player_key = player_keys.get(numeric_player_id)
            
            if not all([league_key, player_key]):
                logger.debug(f"⚠️ Missing keys for transaction: league={league_key}, player={player_key} (raw={raw_player_id}, numeric={numeric_player_id})")
                continue
            
            # Parse timestamp properly - handle different formats
            try:
                if 'T' in transaction['timestamp']:
                    # ISO format: 2005-12-31T02:26:03
                    transaction_date = datetime.fromisoformat(transaction['timestamp'].replace('T', ' ')).date()
                else:
                    # Already in datetime format
                    transaction_date = datetime.strptime(transaction['timestamp'], '%Y-%m-%d %H:%M:%S').date()
            except (ValueError, KeyError):
                logger.warning(f"⚠️ Could not parse timestamp for transaction: {transaction.get('timestamp')}")
                continue
            
            # Extract season from the timestamp (assume NFL season starts in September)
            season_year = transaction_date.year
            if transaction_date.month >= 9:  # September or later = current NFL season
                season_year = transaction_date.year
            else:  # January-August = previous NFL season
                season_year = transaction_date.year - 1
            
            # Ensure we don't assign transactions to seasons before our earliest league (2005)
            if season_year < 2005:
                season_year = 2005
            
            # Find transaction week using preloaded week data
            transaction_week = self.get_week_from_preloaded_data(transaction_date, season_year, week_lookup)
            
            # Get manager keys for from/to teams
            from_manager_key = None
            to_manager_key = None
            
            if transaction.get('source_team_id'):
                for team in self.data.get('teams', []):
                    if team['team_id'] == transaction['source_team_id']:
                        raw_manager_name = team.get('manager_name')
                        if raw_manager_name:
                            consolidated_manager_name = self.get_manager_name_by_team_id(transaction['source_team_id'], raw_manager_name)
                            from_manager_key = manager_keys.get(consolidated_manager_name)
                        break
                        
            if transaction.get('destination_team_id'):
                for team in self.data.get('teams', []):
                    if team['team_id'] == transaction['destination_team_id']:
                        raw_manager_name = team.get('manager_name')
                        if raw_manager_name:
                            consolidated_manager_name = self.get_manager_name_by_team_id(transaction['destination_team_id'], raw_manager_name)
                            to_manager_key = manager_keys.get(consolidated_manager_name)
                        break
            
            # Handle faab_bid properly - check for None vs 0
            faab_bid_value = transaction.get('faab_bid')
            if faab_bid_value is not None and faab_bid_value != '':
                try:
                    faab_bid = float(faab_bid_value)
                except (ValueError, TypeError):
                    faab_bid = None
            else:
                faab_bid = None
            
            facts.append({
                'league_key': int(league_key),
                'player_key': int(player_key),
                'season_year': int(season_year),
                'transaction_date': transaction_date,
                'transaction_week': transaction_week,  # Now calculated using dim_week
                'transaction_type': str(transaction['type']),
                'from_team_key': int(team_keys.get(transaction.get('source_team_id'))) if team_keys.get(transaction.get('source_team_id')) else None,
                'to_team_key': int(team_keys.get(transaction.get('destination_team_id'))) if team_keys.get(transaction.get('destination_team_id')) else None,
                'from_manager_key': int(from_manager_key) if from_manager_key else None,
                'to_manager_key': int(to_manager_key) if to_manager_key else None,
                'faab_bid': faab_bid,  # Properly handled None vs 0
                'trade_group_id': None,  # Not available in data
                'transaction_status': str(transaction.get('status', 'completed'))
            })
        
        return facts
    
    def transform_fact_draft(self) -> List[Dict]:
        """Transform draft picks into fact_draft format with performance metrics"""
        facts = []
        
        if not self.data:
            logger.warning("⚠️ No data available for draft transformation")
            return facts

        # Build set of league of record IDs for efficient lookup
        league_of_record_ids = set()
        league_to_season = {}
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if self.is_league_of_record(league['league_id'], season_year):
                league_of_record_ids.add(league['league_id'])
                league_to_season[league['league_id']] = season_year

        if not league_of_record_ids:
            logger.warning("⚠️ No league of record data found")
            return facts

        # Build player performance lookup following business rules:
        # - season_points: from public.statistics (fact_stats)
        # - fantasy_games_played: count of non-bench weeks from rosters (starting lineup games)
        # - points_per_week: season_points / total_weeks_in_season (average weekly impact)
        player_performance = {}
        
        # Get season weeks count for points_per_game calculation
        season_weeks = {}
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if league_year := league_to_season.get(league['league_id']):
                # Get total weeks for this season (varies by year)
                if season_year <= 2009 or season_year in [2006, 2007]:
                    season_weeks[season_year] = 15  # Shorter seasons
                elif season_year <= 2021:
                    season_weeks[season_year] = 16  # Standard 16-week seasons
                else:
                    season_weeks[season_year] = 17  # Modern 17-week seasons
        
        logger.info("📊 Loading player performance data from public.statistics...")
        try:
            with self.engine.connect() as conn:
                # Aggregate weekly points to get season totals
                league_ids_str = "', '".join(league_of_record_ids)
                sql = f"""
                    SELECT 
                        s.league_id,
                        s.player_id,
                        s.season_year,
                        SUM(s.weekly_fantasy_points) as total_fantasy_points
                    FROM public.statistics s
                    WHERE s.league_id IN ('{league_ids_str}')
                    GROUP BY s.league_id, s.player_id, s.season_year
                """
                
                result = conn.execute(text(sql))
                for row in result:
                    league_id = row[0]
                    player_id = str(row[1])
                    season_year = row[2]
                    total_points = float(row[3] or 0)
                    
                    perf_key = (league_id, season_year, player_id)
                    player_performance[perf_key] = {
                        'season_points': total_points,
                        'games_played': 0,  # Will calculate from roster data
                        'points_per_game': 0.0  # Will calculate
                    }
                
                logger.info(f"✅ Loaded season_points for {len(player_performance)} player-season combinations")
                
        except Exception as e:
            logger.warning(f"⚠️ Could not load from public.statistics: {e}")
            logger.info("📊 Season points will default to 0")
        
        # Calculate fantasy_games_played from roster data (starter positions only)
        logger.info("📊 Calculating fantasy_games_played from roster data (starter positions only)...")
        fantasy_games_played_counts = {}
        
        for roster in self.data.get('rosters', []):
            if roster['league_id'] not in league_of_record_ids:
                continue
            # Only count players who were in starting positions
            if not roster.get('is_starter', False):
                continue
                
            season_year = league_to_season.get(roster['league_id'])
            if not season_year:
                continue
                
            # Extract numeric player ID
            raw_player_id = roster['player_id']
            if '.p.' in raw_player_id:
                player_id = raw_player_id.split('.p.')[-1]
            else:
                player_id = raw_player_id
            
            perf_key = (roster['league_id'], season_year, player_id)
            if perf_key not in fantasy_games_played_counts:
                fantasy_games_played_counts[perf_key] = 0
            fantasy_games_played_counts[perf_key] += 1
        
        # Apply fantasy_games_played counts to performance data
        for perf_key, count in fantasy_games_played_counts.items():
            if perf_key in player_performance:
                player_performance[perf_key]['games_played'] = count
        
        # Calculate points_per_week = season_points / total_weeks_in_season
        for perf_key, perf_data in player_performance.items():
            season_year = perf_key[1]
            total_weeks = season_weeks.get(season_year, 17)  # Default to 17 weeks
            season_points = perf_data['season_points']
            
            if total_weeks > 0:
                perf_data['points_per_week'] = season_points / total_weeks
            else:
                perf_data['points_per_week'] = 0.0
        
        logger.info(f"📊 Built performance data for {len(player_performance)} player-season combinations")
        logger.info(f"📊 Fantasy games played calculated from {len(fantasy_games_played_counts)} starting roster entries")

        # Ensure dimension mappings are cached
        if not self.dim_mappings:
            self.cache_dimension_mappings()

        # Use cached dimension mappings (no database queries in loop)
        league_keys = self.dim_mappings.get('league_keys', {})
        team_keys = self.dim_mappings.get('team_keys', {})
        manager_keys = self.dim_mappings.get('manager_keys', {})
        player_keys = self.dim_mappings.get('player_keys', {})
        
        for draft_pick in self.data.get('draft_picks', []):
            # Only include draft picks from leagues of record
            if draft_pick['league_id'] not in league_of_record_ids:
                continue
                
            league_key = league_keys.get(draft_pick['league_id'])
            player_key = player_keys.get(draft_pick['player_id'])
            
            # Map team_id to full team_id format for lookup
            raw_team_id = draft_pick['team_id']
            league_id = draft_pick['league_id']
            # Construct full team_id: league_id + ".t." + team_id
            full_team_id = f"{league_id}.t.{raw_team_id}"
            team_key = team_keys.get(full_team_id)
            
            # Get manager_key from team's manager_name
            manager_key = None
            for team in self.data.get('teams', []):
                if team['team_id'] == full_team_id:
                    raw_manager_name = team.get('manager_name')
                    if raw_manager_name:
                        consolidated_manager_name = self.get_manager_name_by_team_id(full_team_id, raw_manager_name)
                        manager_key = manager_keys.get(consolidated_manager_name)
                    break
            
            if not all([league_key, team_key, player_key, manager_key]):
                logger.debug(f"⚠️ Missing keys for draft pick: league={league_key}, team={team_key} (raw={raw_team_id}, full={full_team_id}), player={player_key}, manager={manager_key}")
                continue
            
            # Get season year from league data (not extracted_at timestamp!)
            season_year = league_to_season.get(league_id, 2024)
            
            # Get player performance data for this season
            # Match player_id format - extract numeric ID from draft pick
            draft_player_id = draft_pick['player_id']
            if '.p.' in draft_player_id:
                numeric_player_id = draft_player_id.split('.p.')[-1]
            else:
                numeric_player_id = draft_player_id
                
            perf_key = (league_id, season_year, numeric_player_id)
            perf_data = player_performance.get(perf_key, {
                'season_points': 0.0,
                'games_played': 0,
                'points_per_week': 0.0
            })
            
            facts.append({
                'league_key': int(league_key),
                'team_key': int(team_key),
                'manager_key': int(manager_key),
                'player_key': int(player_key),
                'season_year': int(season_year),
                'overall_pick': int(draft_pick['pick_number']),
                'round_number': int(draft_pick.get('round_number', 1)),
                'pick_in_round': int(draft_pick.get('pick_in_round', (draft_pick['pick_number'] - 1) % 12 + 1)),
                'draft_type': 'auction' if draft_pick.get('is_auction_draft') else 'snake',
                'draft_cost': float(draft_pick.get('cost', 0)) if draft_pick.get('cost') else None,
                'is_keeper_pick': bool(draft_pick.get('is_keeper', False)),
                'season_points': float(perf_data['season_points']),
                'fantasy_games_played': int(perf_data['games_played']),
                'points_per_week': float(perf_data['points_per_week'])
            })
        
        return facts
    
    def transform_fact_player_statistics(self) -> List[Dict]:
        """Transform player statistics data into weekly fact_player_statistics format"""
        facts = []
        
        if not self.data:
            logger.warning("⚠️ No data available for player statistics transformation")
            return facts

        # Build set of league of record IDs for efficient lookup
        league_of_record_ids = set()
        league_to_season = {}
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if self.is_league_of_record(league['league_id'], season_year):
                league_of_record_ids.add(league['league_id'])
                league_to_season[league['league_id']] = season_year

        if not league_of_record_ids:
            logger.warning("⚠️ No league of record data found")
            return facts

        # Process statistics data
        statistics_data = self.data.get('statistics', [])
        if not statistics_data:
            logger.warning("⚠️ No statistics data found")
            return facts

        logger.info(f"📊 Processing {len(statistics_data)} weekly player statistics records...")

        # Aggregate by player-season for season-level rankings
        player_season_totals = {}
        
        # First pass: aggregate weekly data to season totals for ranking purposes
        for stat in statistics_data:
            league_id = stat['league_id']
            if league_id not in league_of_record_ids:
                continue

            season_year = league_to_season.get(league_id)
            if not season_year:
                continue

            weekly_points = float(stat.get('weekly_fantasy_points', 0))
            position_type = stat.get('position_type', 'Unknown')
            player_id = stat['player_id']
            
            # Aggregate to season level for rankings
            season_key = (league_id, player_id, season_year, position_type)
            if season_key not in player_season_totals:
                player_season_totals[season_key] = {
                    'total_points': 0,
                    'weeks_played': 0,
                    'position_type': position_type
                }
            
            player_season_totals[season_key]['total_points'] += weekly_points
            player_season_totals[season_key]['weeks_played'] += 1

        # Calculate season-level rankings
        league_season_rankings = {}
        position_season_rankings = {}
        
        # Group for league rankings
        league_season_players = {}
        for (league_id, player_id, season_year, position_type), totals in player_season_totals.items():
            league_season_key = (league_id, season_year)
            if league_season_key not in league_season_players:
                league_season_players[league_season_key] = []
            league_season_players[league_season_key].append(
                (player_id, totals['total_points'])
            )

        # Calculate league rankings
        for league_season_key, players in league_season_players.items():
            sorted_players = sorted(players, key=lambda x: x[1], reverse=True)
            for rank, (player_id, points) in enumerate(sorted_players, 1):
                ranking_key = (league_season_key[0], player_id, league_season_key[1])
                league_season_rankings[ranking_key] = rank

        # Group for position rankings
        position_season_players = {}
        for (league_id, player_id, season_year, position_type), totals in player_season_totals.items():
            position_key = (league_id, season_year, position_type)
            if position_key not in position_season_players:
                position_season_players[position_key] = []
            position_season_players[position_key].append(
                (player_id, totals['total_points'])
            )

        # Calculate position rankings
        for position_key, players in position_season_players.items():
            sorted_players = sorted(players, key=lambda x: x[1], reverse=True)
            for rank, (player_id, points) in enumerate(sorted_players, 1):
                ranking_key = (position_key[0], player_id, position_key[1])
                position_season_rankings[ranking_key] = rank

        # Second pass: create weekly fact records with season-level context
        for stat in statistics_data:
            league_id = stat['league_id']
            if league_id not in league_of_record_ids:
                continue

            season_year = league_to_season.get(league_id)
            if not season_year:
                continue

            # Get dimension keys
            league_key = self.dim_mappings['league_keys'].get(league_id)
            if not league_key:
                continue

            # Extract player ID (handle Yahoo format)
            raw_player_id = stat['player_id']
            if '.p.' in raw_player_id:
                numeric_player_id = raw_player_id.split('.p.')[-1]
            else:
                numeric_player_id = raw_player_id

            player_key = self.dim_mappings['player_keys'].get(numeric_player_id)
            if not player_key:
                continue

            # Weekly stats
            weekly_points = float(stat.get('weekly_fantasy_points', 0))
            week_number = int(stat.get('week_number', 1))
            position_type = stat.get('position_type', 'Unknown')

            # Get season-level rankings
            ranking_key = (league_id, stat['player_id'], season_year)
            league_rank = league_season_rankings.get(ranking_key)
            position_rank = position_season_rankings.get(ranking_key)

            # Get season totals for calculations
            season_key = (league_id, stat['player_id'], season_year, position_type)
            season_totals = player_season_totals.get(season_key, {
                'total_points': weekly_points,
                'weeks_played': 1
            })

            # Calculate season-level metrics
            season_total_points = season_totals['total_points']
            weeks_played = season_totals['weeks_played']
            avg_points_per_game = season_total_points / weeks_played if weeks_played > 0 else 0

            # Calculate points above replacement for this week
            # Use bottom 25% of position as replacement level
            position_key = (league_id, season_year, position_type)
            if position_key in position_season_players:
                position_players = position_season_players[position_key]
                if len(position_players) > 3:
                    replacement_threshold = int(len(position_players) * 0.75)
                    sorted_position = sorted(position_players, key=lambda x: x[1], reverse=True)
                    replacement_season_total = sorted_position[replacement_threshold][1] if replacement_threshold < len(sorted_position) else 0
                    replacement_weekly_avg = replacement_season_total / 17  # Assume 17 weeks
                    points_above_replacement = weekly_points - replacement_weekly_avg
                else:
                    points_above_replacement = weekly_points
            else:
                points_above_replacement = weekly_points

            # Calculate weekly consistency score (simplified for weekly data)
            consistency_score = self._calculate_weekly_consistency_score(
                weekly_points, avg_points_per_game, position_type
            )

            # Calculate draft value score based on season performance
            draft_value_score = self._calculate_draft_value_score(
                league_id, numeric_player_id, season_year, season_total_points, position_rank
            )

            # Create weekly fact record (no calculated fields)
            fact = {
                'league_key': league_key,
                'player_key': player_key,
                'season_year': season_year,
                'week_number': week_number,
                'weekly_fantasy_points': weekly_points,
                'position_type': position_type,
                'position_rank': position_rank,  # Season-level rank
                'league_rank': league_rank,     # Season-level rank
                'points_above_replacement': round(points_above_replacement, 2),
                'source_stat_id': stat.get('stat_id'),
                'game_code': stat.get('game_code', 'nfl')
            }

            facts.append(fact)

        logger.info(f"✅ Player statistics facts transformed: {len(facts)} records")
        return facts

    def _calculate_consistency_score(self, total_points: float, points_per_game: float, 
                                   position_type: str, games_played: int) -> float:
        """
        Calculate consistency score using simplified method without weekly data.
        
        This uses position-based variance estimates since we don't have weekly data.
        Lower scores indicate more consistent performance.
        """
        if total_points <= 0 or games_played <= 0:
            return None
        
        # Position-based consistency multipliers (estimated from historical data)
        # QBs tend to be more consistent, RBs/WRs more volatile, K/DEF very volatile
        position_volatility = {
            'O': 0.25,    # Offensive players (QB, RB, WR, TE) - moderate volatility
            'K': 0.45,    # Kickers - high volatility  
            'DT': 0.40,   # Defense/Team - high volatility
            'DP': 0.35,   # Individual defensive players - moderate-high volatility
        }
        
        base_volatility = position_volatility.get(position_type, 0.30)
        
        # Estimate standard deviation as percentage of points per game
        estimated_std_dev = points_per_game * base_volatility
        
        # Normalize by points per game to get coefficient of variation
        consistency_score = estimated_std_dev / points_per_game if points_per_game > 0 else None
        
        return consistency_score

    def _calculate_weekly_consistency_score(self, weekly_points: float, season_avg: float, 
                                          position_type: str) -> float:
        """
        Calculate weekly consistency score by comparing this week's performance to season average.
        
        Returns a score indicating how much this week deviates from the player's season average.
        Lower scores indicate performance closer to average (more consistent).
        """
        if season_avg <= 0:
            return None
        
        # Calculate deviation from season average as percentage
        deviation = abs(weekly_points - season_avg) / season_avg
        
        # Position-based volatility expectations
        position_volatility = {
            'O': 0.30,    # Offensive players - moderate expected volatility
            'K': 0.50,    # Kickers - high volatility  
            'DT': 0.45,   # Defense/Team - high volatility
            'DP': 0.40,   # Individual defensive players - moderate-high volatility
        }
        
        expected_volatility = position_volatility.get(position_type, 0.35)
        
        # Normalize deviation by expected volatility for this position
        consistency_score = deviation / expected_volatility
        
        return consistency_score

    def _calculate_draft_value_score(self, league_id: str, player_id: str, season_year: int, 
                                   fantasy_points: float, position_rank: int) -> float:
        """
        Calculate draft value score by comparing actual performance to draft position.
        
        Positive scores indicate player outperformed their draft position.
        Negative scores indicate player underperformed their draft position.
        """
        try:
            # Get draft data for this player
            draft_info = None
            for draft_pick in self.data.get('draft_picks', []):
                if (draft_pick.get('league_id') == league_id and 
                    draft_pick.get('season_year') == season_year):
                    
                    # Handle Yahoo player ID format
                    draft_player_id = draft_pick.get('player_id', '')
                    if '.p.' in draft_player_id:
                        draft_numeric_id = draft_player_id.split('.p.')[-1]
                    else:
                        draft_numeric_id = draft_player_id
                    
                    if draft_numeric_id == player_id:
                        draft_info = draft_pick
                        break
            
            if not draft_info:
                return None  # Player wasn't drafted
            
            draft_position = draft_info.get('pick_number', 0)
            if draft_position <= 0:
                return None
            
            # Calculate expected points based on draft position
            # Use a logarithmic decay model: early picks should score much more
            # This is a simplified model - could be enhanced with historical ADP data
            max_expected_points = 400  # Top pick expected points
            min_expected_points = 50   # Last pick expected points
            total_picks = 200  # Assume ~200 total picks across all leagues
            
            # Logarithmic decay from max to min
            import math
            decay_factor = math.log(total_picks) / (max_expected_points - min_expected_points)
            expected_points = max_expected_points - (math.log(draft_position) / decay_factor)
            expected_points = max(expected_points, min_expected_points)
            
            # Calculate value score as percentage above/below expected
            if expected_points > 0:
                value_score = (fantasy_points - expected_points) / expected_points
            else:
                value_score = 0
            
            return value_score
            
        except Exception as e:
            logger.debug(f"Could not calculate draft value score for player {player_id}: {e}")
            return None
    
    def _calculate_playoff_probability(self, win_percentage: float, current_rank: int, 
                                     total_teams: int, current_week: int, weekly_points: float) -> float:
        """
        Calculate playoff probability based on multiple factors.
        
        Factors considered:
        1. Current standings position (most important)
        2. Win percentage vs. historical playoff cutoffs
        3. Weeks remaining in season
        4. Points scored relative to league average (tiebreaker factor)
        """
        try:
            # Default values if inputs are invalid
            if not all([win_percentage is not None, current_rank, total_teams]):
                return 0.0
            
            # Standard fantasy football assumptions
            playoff_teams = max(4, total_teams // 3)  # Typically 4-6 teams make playoffs
            
            # Determine playoff start week from actual data
            playoff_start_week = self._get_playoff_start_week()
            
            if current_week is None:
                # Better fallback: use a conservative mid-season estimate
                # This affects the "weeks remaining" calculation for probability
                logger.debug(f"⚠️ Using fallback week 10 for playoff probability calculation")
                current_week = 10  # Conservative mid-season estimate
            
            weeks_remaining = max(0, playoff_start_week - current_week)
            
            # Factor 1: Current standings position (40% weight)
            if current_rank <= playoff_teams:
                position_factor = 1.0 - (current_rank - 1) / playoff_teams  # 1.0 for 1st, decreasing
            else:
                # Teams outside playoff spots - exponentially decreasing probability
                spots_out = current_rank - playoff_teams
                position_factor = max(0.0, 0.5 * (0.7 ** spots_out))
            
            # Factor 2: Win percentage vs historical benchmarks (30% weight)
            # Historical data: ~0.600+ win% almost always makes playoffs, ~0.400- rarely does
            if win_percentage >= 0.700:
                record_factor = 1.0
            elif win_percentage >= 0.600:
                record_factor = 0.9
            elif win_percentage >= 0.500:
                record_factor = 0.7
            elif win_percentage >= 0.400:
                record_factor = 0.4
            else:
                record_factor = 0.1
            
            # Factor 3: Time remaining adjustment (20% weight)
            if weeks_remaining > 8:
                # Early season - lots of time to change position
                time_factor = 0.5 + (weeks_remaining / 20)  # More volatility early
            elif weeks_remaining > 4:
                # Mid season - moderate adjustments
                time_factor = 0.7 + (weeks_remaining / 40)
            elif weeks_remaining > 0:
                # Late season - position matters more
                time_factor = 0.9 + (weeks_remaining / 100)
            else:
                # Season over or playoffs started
                time_factor = 1.0 if current_rank <= playoff_teams else 0.0
            
            # Factor 4: Points differential (10% weight) - simplified
            # Assume league average is around 100-120 points
            league_avg_points = 110
            if weekly_points > league_avg_points * 1.1:  # 10% above average
                points_factor = 1.1
            elif weekly_points > league_avg_points * 0.9:  # Within 10% of average
                points_factor = 1.0
            else:  # Below average scoring
                points_factor = 0.9
            
            # Combine factors with weights
            playoff_probability = (
                position_factor * 0.40 +
                record_factor * 0.30 +
                time_factor * 0.20 +
                points_factor * 0.10
            )
            
            # Ensure probability is between 0 and 1
            playoff_probability = max(0.0, min(1.0, playoff_probability))
            
            return round(playoff_probability, 4)
            
        except Exception as e:
            logger.debug(f"Could not calculate playoff probability: {e}")
            return 0.0
    
    def _get_playoff_start_week(self) -> int:
        """
        Determine when playoffs start based on actual matchup data.
        
        Logic:
        1. Look for first week where matchup_type != 'regular'
        2. If no playoff data found, use current/future rule: week 15 (week 14 is last regular season)
        3. Playoffs are always 3 weeks long, so can work backwards from season end
        """
        try:
            # Look through matchup data to find playoff start
            playoff_weeks = set()
            season_end_weeks = {}  # Track the last week for each season
            
            for matchup in self.data.get('matchups', []):
                # Get season from league data
                season_year = None
                for league in self.data.get('leagues', []):
                    if league['league_id'] == matchup['league_id']:
                        season_year = int(league['season'])
                        break
                
                if not season_year:
                    continue
                
                week_num = matchup['week']
                
                # Track the highest week number for each season (season end)
                if season_year not in season_end_weeks:
                    season_end_weeks[season_year] = week_num
                else:
                    season_end_weeks[season_year] = max(season_end_weeks[season_year], week_num)
                
                # Look for non-regular matchups (playoffs)
                matchup_type = matchup.get('matchup_type', 'regular')
                is_playoffs = matchup.get('is_playoffs', False)
                
                if matchup_type != 'regular' or is_playoffs:
                    playoff_weeks.add(week_num)
            
            # Determine playoff start week
            if playoff_weeks:
                # Use the earliest playoff week found in data
                playoff_start_week = min(playoff_weeks)
                logger.debug(f"📊 Detected playoff start week: {playoff_start_week} from matchup data")
                return playoff_start_week
            elif season_end_weeks:
                # Work backwards: playoffs are 3 weeks, so playoff start = season_end - 2
                max_season_end = max(season_end_weeks.values())
                playoff_start_week = max_season_end - 2  # 3-week playoffs
                logger.debug(f"📊 Calculated playoff start week: {playoff_start_week} (working backwards from week {max_season_end})")
                return playoff_start_week
            else:
                # Current/future default: 17-week season with 3-week playoffs
                # Regular season ends week 14, playoffs start week 15
                playoff_start_week = 15
                logger.debug(f"📊 Using default playoff start week: {playoff_start_week} (current/future seasons)")
                return playoff_start_week
                
        except Exception as e:
            logger.debug(f"Could not determine playoff start week: {e}")
            # Safe default for current/future seasons
            return 15
    
    def transform_fact_team_performance(self) -> List[Dict]:
        """Transform team performance data from matchups and rosters"""
        facts = []
        
        if not self.data:
            logger.warning("⚠️ No data available for team performance fact transformation")
            return facts

        # Calculate team performance metrics from matchups and rosters
        team_performance = {}
        
        # Build league-to-season mapping and league of record set for efficient lookup
        league_to_season = {}
        league_of_record_ids = set()
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if self.is_league_of_record(league['league_id'], season_year):
                league_to_season[league['league_id']] = season_year
                league_of_record_ids.add(league['league_id'])
        
        # First pass: collect playoff team information (exclude consolation)
        playoff_teams_by_season = {}  # {(league_id, season_year): set of team_ids}
        
        for matchup in self.data.get('matchups', []):
            # Only include matchups from leagues of record
            if matchup['league_id'] not in league_of_record_ids:
                continue
                
            season_year = league_to_season.get(matchup['league_id'], 2024)
            
            # Check if this is a real playoff game (not consolation)
            is_playoffs = matchup.get('is_playoffs', False)
            is_consolation = matchup.get('is_consolation', False)
            matchup_type = matchup.get('matchup_type', 'regular')
            
            # Real playoffs: is_playoffs=True AND not consolation
            if is_playoffs and not is_consolation and matchup_type != 'consolation':
                league_season_key = (matchup['league_id'], season_year)
                if league_season_key not in playoff_teams_by_season:
                    playoff_teams_by_season[league_season_key] = set()
                
                # Both teams in this playoff matchup made the playoffs
                playoff_teams_by_season[league_season_key].add(matchup['team1_id'])
                playoff_teams_by_season[league_season_key].add(matchup['team2_id'])
        
        # Log playoff team detection results
        total_playoff_teams = sum(len(teams) for teams in playoff_teams_by_season.values())
        logger.info(f"📊 Detected {total_playoff_teams} playoff teams across {len(playoff_teams_by_season)} league-seasons (excluding consolation)")
        
        # Process matchups for wins/losses/points (only for leagues of record)
        for matchup in self.data.get('matchups', []):
            # Only include matchups from leagues of record
            if matchup['league_id'] not in league_of_record_ids:
                continue
                
            season_year = league_to_season.get(matchup['league_id'], 2024)
            week = matchup['week']
            
            for team_num in [1, 2]:
                team_id = matchup[f'team{team_num}_id']
                team_points = float(matchup.get(f'team{team_num}_score', 0))
                opponent_points = float(matchup.get(f'team{3-team_num}_score', 0))
                
                key = (team_id, season_year, week)
                if key not in team_performance:
                    team_performance[key] = {
                        'team_id': team_id,
                        'league_id': matchup['league_id'],
                        'season_year': season_year,
                        'week': week,
                        'wins': 0,
                        'losses': 0,
                        'ties': 0,
                        'weekly_points': team_points,
                        'points_against': opponent_points
                    }
                
                if team_points > opponent_points:
                    team_performance[key]['wins'] = 1
                elif team_points < opponent_points:
                    team_performance[key]['losses'] = 1
                else:
                    team_performance[key]['ties'] = 1
        
        # Calculate running averages for weekly_points before creating facts
        logger.info(f"📊 Calculating running averages for weekly points...")
        
        # Group team performance by team and season for running average calculation
        team_season_weeks = {}
        for perf in team_performance.values():
            team_season_key = (perf['team_id'], perf['league_id'], perf['season_year'])
            if team_season_key not in team_season_weeks:
                team_season_weeks[team_season_key] = []
            team_season_weeks[team_season_key].append({
                'week': perf['week'],
                'weekly_points': perf['weekly_points'],
                'points_against': perf['points_against']
            })
        
        # Calculate running averages and win percentages for each team-season
        running_averages = {}
        running_win_percentages = {}
        for team_season_key, weeks in team_season_weeks.items():
            # Sort weeks in ascending order
            weeks.sort(key=lambda x: x['week'])
            
            cumulative_points = 0
            cumulative_wins = 0
            cumulative_losses = 0
            cumulative_ties = 0
            
            for i, week_data in enumerate(weeks):
                # Calculate running average points
                cumulative_points += week_data['weekly_points']
                weeks_played = i + 1
                running_avg = cumulative_points / weeks_played
                
                # Calculate running win percentage
                # Need to get wins/losses/ties for this week from team_performance
                team_id, league_id, season_year = team_season_key
                week_num = week_data['week']
                perf_key = (team_id, season_year, week_num)
                
                if perf_key in team_performance:
                    cumulative_wins += team_performance[perf_key]['wins']
                    cumulative_losses += team_performance[perf_key]['losses']
                    cumulative_ties += team_performance[perf_key]['ties']
                
                # Calculate win percentage: (wins + 0.5 * ties) / (total games)
                total_games = cumulative_wins + cumulative_losses + cumulative_ties
                if total_games > 0:
                    win_percentage = (cumulative_wins + 0.5 * cumulative_ties) / total_games
                else:
                    win_percentage = 0.0
                
                # Store both metrics for this team-season-week
                lookup_key = (team_season_key[0], team_season_key[1], team_season_key[2], week_data['week'])
                running_averages[lookup_key] = running_avg
                running_win_percentages[lookup_key] = win_percentage
        
        logger.info(f"✅ Calculated running averages and win percentages for {len(running_averages)} team-week combinations")
        
        # Convert to fact format with dimension keys (use cached mappings)
        league_keys = self.dim_mappings.get('league_keys', {})
        team_keys = self.dim_mappings.get('team_keys', {})
        manager_keys = self.dim_mappings.get('manager_keys', {})
        week_keys = self.dim_mappings.get('week_keys', {})
        
        for perf in team_performance.values():
            team_key = team_keys.get(perf['team_id'])
            league_key = league_keys.get(perf['league_id'])
            week_key = week_keys.get((perf['season_year'], perf['week']))
            
            # Get manager_key from team's manager_name
            manager_key = None
            # Find team info to get manager_name
            for team in self.data.get('teams', []):
                if team['team_id'] == perf['team_id']:
                    raw_manager_name = team.get('manager_name')
                    if raw_manager_name:
                        consolidated_manager_name = self.get_manager_name_by_team_id(perf['team_id'], raw_manager_name)
                        manager_key = manager_keys.get(consolidated_manager_name)
                    break
            
            if not all([team_key, league_key, week_key, manager_key]):
                continue
            
            # Get the running average and win percentage for this team-week
            running_avg_key = (perf['team_id'], perf['league_id'], perf['season_year'], perf['week'])
            weekly_points_running_avg = running_averages.get(running_avg_key, perf['weekly_points'])
            running_win_pct = running_win_percentages.get(running_avg_key, 0.0)
            
            # Determine if this team made the playoffs (real playoffs, not consolation)
            league_season_key = (perf['league_id'], perf['season_year'])
            playoff_teams = playoff_teams_by_season.get(league_season_key, set())
            is_playoff_team = perf['team_id'] in playoff_teams
            

            
            facts.append({
                'team_key': team_key,
                'manager_key': manager_key,
                'league_key': league_key,
                'week_key': week_key,
                'season_year': perf['season_year'],
                'wins': perf['wins'],
                'losses': perf['losses'],
                'ties': perf['ties'],
                'points_for': perf['weekly_points'],  # This week's actual points
                'points_against': perf['points_against'],
                'weekly_points': weekly_points_running_avg,  # Running average from season start to this week
                'weekly_rank': None,  # Will be calculated below
                'season_rank': None,
                'win_percentage': running_win_pct,  # Running win percentage from season start to this week
                'point_differential': perf['weekly_points'] - perf['points_against'],
                'playoff_probability': None,
                'is_playoff_team': is_playoff_team  # True if team played in real playoffs (not consolation)
            })
        
        # Calculate weekly rankings and playoff probabilities by league and week
        logger.info(f"📊 Calculating weekly rankings and playoff probabilities for {len(facts)} team performance records...")
        
        # Group facts by league and week for ranking
        weekly_groups = {}
        for i, fact in enumerate(facts):
            group_key = (fact['league_key'], fact['week_key'])
            if group_key not in weekly_groups:
                weekly_groups[group_key] = []
            weekly_groups[group_key].append((i, fact))
        
        # Calculate weekly rank and playoff probability within each group
        for group_key, group_facts in weekly_groups.items():
            # Sort by weekly points (descending - highest points gets rank 1)
            sorted_facts = sorted(group_facts, key=lambda x: x[1]['weekly_points'], reverse=True)
            
            # Get league and week info for playoff probability calculation
            league_key, week_key = group_key
            total_teams = len(group_facts)
            
            # Get current week number for remaining weeks calculation
            current_week = None
            
            # Direct lookup from week_key mapping (more efficient)
            for (season_year, week_num), w_key in self.dim_mappings['week_keys'].items():
                if w_key == week_key:
                    current_week = week_num
                    break
            
            # If still None, log warning for debugging
            if current_week is None:
                logger.warning(f"⚠️ Could not determine week number for week_key {week_key}")
                logger.debug(f"Available week_keys: {list(self.dim_mappings['week_keys'].values())[:10]}...")
            
            # Assign ranks and calculate playoff probabilities
            current_rank = 1
            prev_points = None
            
            for rank_position, (fact_index, fact) in enumerate(sorted_facts):
                if prev_points is not None and fact['weekly_points'] != prev_points:
                    current_rank = rank_position + 1
                
                facts[fact_index]['weekly_rank'] = current_rank
                
                # Calculate playoff probability
                playoff_prob = self._calculate_playoff_probability(
                    fact['win_percentage'],
                    current_rank,
                    total_teams,
                    current_week,
                    fact['weekly_points']
                )
                facts[fact_index]['playoff_probability'] = playoff_prob
                
                prev_points = fact['weekly_points']
        
        logger.info(f"✅ Weekly rankings and playoff probabilities calculated for {len(weekly_groups)} league-week combinations")
        return facts
    
    def load_dimensions(self) -> bool:
        """Load all dimension tables"""
        try:
            logger.info("🏗️ Loading dimension tables...")
            
            # Load seasons
            seasons = self.extract_seasons()
            for season in seasons:
                result = self.session.execute(text("""
                    INSERT INTO edw.dim_season (season_year, season_start_date, season_end_date, 
                                          playoff_start_week, championship_week, total_weeks, 
                                          is_current_season, season_status)
                    VALUES (:season_year, :season_start_date, :season_end_date, 
                            :playoff_start_week, :championship_week, :total_weeks, 
                            :is_current_season, :season_status)
                    ON CONFLICT (season_year) DO UPDATE SET
                        is_current_season = EXCLUDED.is_current_season,
                        season_status = EXCLUDED.season_status
                    RETURNING season_key, season_year
                """), season)
                
                row = result.fetchone()
                self.dim_mappings['season_keys'][season['season_year']] = row[0]
            
            logger.info(f"  ✅ Seasons: {len(seasons)} processed")
            
            # Load weeks
            weeks = self.extract_weeks()
            for week in weeks:
                result = self.session.execute(text("""
                    INSERT INTO edw.dim_week (season_year, week_number, week_type, 
                                        week_start_date, week_end_date, is_current_week)
                    VALUES (:season_year, :week_number, :week_type, 
                            :week_start_date, :week_end_date, :is_current_week)
                    ON CONFLICT (season_year, week_number) DO UPDATE SET
                        week_type = EXCLUDED.week_type
                    RETURNING week_key, season_year, week_number
                """), week)
                
                row = result.fetchone()
                self.dim_mappings['week_keys'][(week['season_year'], week['week_number'])] = row[0]
            
            logger.info(f"  ✅ Weeks: {len(weeks)} processed")
            
            # Load leagues
            leagues = self.transform_leagues()
            for league in leagues:
                result = self.session.execute(text("""
                    INSERT INTO edw.dim_league (league_id, league_name, season_year, num_teams, 
                                          league_type, scoring_type, draft_type, 
                                          is_active, valid_from, valid_to)
                    VALUES (:league_id, :league_name, :season_year, :num_teams, 
                            :league_type, :scoring_type, :draft_type,
                            :is_active, :valid_from, :valid_to)
                    RETURNING league_key, league_id
                """), league)
                
                row = result.fetchone()
                self.dim_mappings['league_keys'][league['league_id']] = row[0]
            
            logger.info(f"  ✅ Leagues: {len(leagues)} processed")
            
            # Load players
            players = self.transform_players()
            for player in players:
                result = self.session.execute(text("""
                    INSERT INTO edw.dim_player (player_id, player_name, primary_position, 
                                          eligible_positions, nfl_team, jersey_number, 
                                          rookie_year, is_active, valid_from, valid_to)
                    VALUES (:player_id, :player_name, :primary_position, 
                            :eligible_positions, :nfl_team, :jersey_number, 
                            :rookie_year, :is_active, :valid_from, :valid_to)
                    ON CONFLICT (player_id) DO UPDATE SET
                        player_name = EXCLUDED.player_name,
                        primary_position = EXCLUDED.primary_position
                    RETURNING player_key, player_id
                """), player)
                
                row = result.fetchone()
                self.dim_mappings['player_keys'][player['player_id']] = row[0]
            
            logger.info(f"  ✅ Players: {len(players)} processed")
            
            # Load managers
            # Load managers (truncate first to eliminate duplicates from consolidation)
            managers = self.transform_managers()
            
            # Truncate manager table to ensure clean consolidation
            logger.info(f"🗑️ Truncating dim_manager to eliminate duplicates from consolidation...")
            self.session.execute(text("TRUNCATE TABLE edw.dim_manager RESTART IDENTITY CASCADE"))
            
            for manager in managers:
                result = self.session.execute(text("""
                    INSERT INTO edw.dim_manager (manager_name, manager_id, first_season_year, 
                                           last_season_year, total_seasons, total_leagues,
                                           is_current, include_in_analysis, email, 
                                           display_name, profile_image_url, is_active)
                    VALUES (:manager_name, :manager_id, :first_season_year, 
                            :last_season_year, :total_seasons, :total_leagues,
                            :is_current, :include_in_analysis, :email,
                            :display_name, :profile_image_url, :is_active)
                    RETURNING manager_key, manager_name
                """), manager)
                
                row = result.fetchone()
                self.dim_mappings['manager_keys'][manager['manager_name']] = row[0]
            
            logger.info(f"  ✅ Managers: {len(managers)} processed")
            
            # Load teams (after leagues are loaded for foreign key)
            teams = self.transform_teams()
            for team in teams:
                # Map league_id to league_key
                league_key = self.dim_mappings['league_keys'][team['league_id']]
                team['league_key'] = league_key
                
                result = self.session.execute(text("""
                    INSERT INTO edw.dim_team (team_id, league_key, team_name, manager_name, 
                                        manager_id, team_logo_url, is_active, valid_from, valid_to)
                    VALUES (:team_id, :league_key, :team_name, :manager_name, 
                            :manager_id, :team_logo_url, :is_active, :valid_from, :valid_to)
                    RETURNING team_key, team_id
                """), team)
                
                row = result.fetchone()
                self.dim_mappings['team_keys'][team['team_id']] = row[0]
            
            logger.info(f"  ✅ Teams: {len(teams)} processed")
            
            self.session.commit()
            logger.info("✅ All dimensions loaded successfully")
            
            # Track refreshed dimension tables for smart view management
            self.refreshed_tables.update(['dim_season', 'dim_week', 'dim_league', 'dim_player', 'dim_manager', 'dim_team'])
            
            # Cache dimension key mappings for efficient fact processing
            self.cache_dimension_mappings()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Dimension loading failed: {e}")
            self.session.rollback()
            return False
    
    def cache_dimension_mappings(self) -> None:
        """Cache dimension key mappings for efficient fact processing"""
        try:
            logger.info("🗂️ Caching dimension key mappings...")
            
            with self.engine.connect() as conn:
                # Load all dimension mappings once
                self.dim_mappings['league_keys'] = {
                    row[0]: row[1] for row in conn.execute(text("SELECT league_id, league_key FROM edw.dim_league")).fetchall()
                }
                self.dim_mappings['team_keys'] = {
                    row[0]: row[1] for row in conn.execute(text("SELECT team_id, team_key FROM edw.dim_team")).fetchall()
                }
                self.dim_mappings['player_keys'] = {
                    row[0]: row[1] for row in conn.execute(text("SELECT player_id, player_key FROM edw.dim_player")).fetchall()
                }
                self.dim_mappings['manager_keys'] = {
                    row[0]: row[1] for row in conn.execute(text("SELECT manager_name, manager_key FROM edw.dim_manager")).fetchall()
                }
                self.dim_mappings['week_keys'] = {
                    (row[0], row[1]): row[2] for row in conn.execute(text("SELECT season_year, week_number, week_key FROM edw.dim_week")).fetchall()
                }
                
                logger.info(f"✅ Cached {len(self.dim_mappings['league_keys'])} leagues, {len(self.dim_mappings['team_keys'])} teams, {len(self.dim_mappings['player_keys'])} players, {len(self.dim_mappings['manager_keys'])} managers, {len(self.dim_mappings['week_keys'])} weeks")
                
        except Exception as e:
            logger.error(f"❌ Failed to cache dimension mappings: {e}")
            raise
    
    def load_facts(self) -> bool:
        """Load all fact tables"""
        try:
            logger.info("📊 Loading fact tables...")
            
            # Load facts in dependency order
            fact_methods = [
                ("fact_roster", self.transform_fact_roster),
                ("fact_matchup", self.transform_fact_matchup),
                ("fact_transaction", self.transform_fact_transaction),
                ("fact_draft", self.transform_fact_draft),
                ("fact_player_statistics", self.transform_fact_player_statistics),
                ("fact_team_performance", self.transform_fact_team_performance)
            ]
            
            for table_name, transform_method in fact_methods:
                try:
                    logger.info(f"  📊 Processing {table_name}...")
                    data = transform_method()
                    if self.load_fact_table(table_name, data):
                        logger.info(f"  ✅ {table_name.title()}: {len(data)} processed")
                        self.refreshed_tables.add(table_name)  # Track refreshed table
                    else:
                        logger.error(f"  ❌ Failed to load {table_name}")
                        return False
                except Exception as e:
                    logger.error(f"  ❌ Error processing {table_name}: {e}")
                    return False
            
            logger.info(f"✅ All facts loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Fact loading failed: {e}")
            return False
    
    def load_marts(self) -> bool:
        """Load all data mart tables"""
        try:
            logger.info("🏪 Loading data mart tables...")
            
            # Load marts in dependency order (after facts)
            mart_methods = [
                ("mart_league_summary", self.transform_mart_league_summary),
                ("mart_manager_performance", self.transform_mart_manager_performance),
                ("mart_player_value", self.transform_mart_player_value),
                ("mart_weekly_power_rankings", self.transform_mart_weekly_power_rankings),
                ("mart_manager_h2h", self.transform_mart_manager_h2h)
            ]
            
            for table_name, transform_method in mart_methods:
                try:
                    logger.info(f"  🏪 Processing {table_name}...")
                    data = transform_method()
                    if self.load_mart_table(table_name, data):
                        logger.info(f"  ✅ {table_name.title()}: {len(data)} processed")
                        self.refreshed_tables.add(table_name)  # Track refreshed table
                    else:
                        logger.error(f"  ❌ Failed to load {table_name}")
                        return False
                except Exception as e:
                    logger.error(f"  ❌ Error processing {table_name}: {e}")
                    return False
            
            logger.info(f"✅ All marts loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Mart loading failed: {e}")
            return False
    
    def process_incremental_edw(self, operational_tables_changed: Set[str] = None) -> bool:
        """
        Process EDW updates based on operational table changes
        This method is called by the incremental loader after operational data is loaded
        """
        try:
            logger.info("🚀 Starting EDW incremental processing...")
            
            # Use provided changed tables or detect them
            if operational_tables_changed:
                self.changed_tables = operational_tables_changed
                logger.info(f"📊 Operational tables changed: {self.changed_tables}")
            else:
                if not self.detect_operational_changes():
                    return False
            
            if not self.changed_tables:
                logger.info("✅ No operational changes detected - EDW is up to date")
                return True
            
            # Determine which EDW tables need processing
            edw_tables_to_process = set()
            
            for op_table in self.changed_tables:
                if op_table in self.EDW_PROCESSING_STRATEGIES:
                    strategy = self.EDW_PROCESSING_STRATEGIES[op_table]
                    edw_tables_to_process.update(strategy['triggers_refresh'])
                    logger.info(f"📈 {op_table} → triggers: {strategy['triggers_refresh']}")
            
            logger.info(f"🎯 EDW tables to refresh: {edw_tables_to_process}")
            
            # Process dimensions first (they are dependencies for facts)
            # Order dimensions by dependency: season/week/player (no deps) -> league -> team (depends on league)
            all_dimension_tables = [t for t in edw_tables_to_process if t.startswith('dim_')]
            dimension_tables = []
            
            # First: Independent dimensions
            for table in ['dim_season', 'dim_week', 'dim_player']:
                if table in all_dimension_tables:
                    dimension_tables.append(table)
            
            # Second: Manager dimension (independent)
            if 'dim_manager' in all_dimension_tables:
                dimension_tables.append('dim_manager')
            
            # Third: League dimension
            if 'dim_league' in all_dimension_tables:
                dimension_tables.append('dim_league')
            
            # Fourth: Team dimension (depends on league and manager)
            if 'dim_team' in all_dimension_tables:
                dimension_tables.append('dim_team')
            
            # Add any remaining dimensions not covered above
            for table in all_dimension_tables:
                if table not in dimension_tables:
                    dimension_tables.append(table)
            
            fact_tables = [t for t in edw_tables_to_process if t.startswith('fact_')]
            mart_tables = [t for t in edw_tables_to_process if t.startswith('mart_')]
            view_tables = [t for t in edw_tables_to_process if t.startswith('vw_')]
            
            # Process in dependency order
            processing_order = [
                ("dimensions", dimension_tables),
                ("facts", fact_tables), 
                ("marts", mart_tables),
                ("views", view_tables)
            ]
            
            for category, tables in processing_order:
                if tables:
                    logger.info(f"🔄 Processing {category}: {tables}")
                    for table in tables:
                        if self.process_edw_table(table):
                            # Update the correct stats key 
                            if category == "dimensions":
                                self.load_stats["dimensions_processed"] += 1
                            elif category == "facts":
                                self.load_stats["facts_processed"] += 1
                            elif category == "marts":
                                self.load_stats["marts_processed"] += 1
                            else:
                                self.load_stats["tables_processed"] += 1
                        else:
                            logger.error(f"❌ Failed to process {table}")
                            return False
            
            # PHASE 2: Check for additional views that need refreshing based on what was actually processed
            logger.info("🔍 Checking for additional view dependencies...")
            views_to_refresh = self.determine_views_to_refresh()
            
            if views_to_refresh:
                additional_views = [view for view in views_to_refresh.keys() if view not in edw_tables_to_process]
                if additional_views:
                    logger.info(f"🔧 Found additional views to refresh: {additional_views}")
                    for view_name in additional_views:
                        logger.info(f"🔄 Processing additional view: {view_name}")
                        if self.process_edw_table(view_name):
                            logger.info(f"✅ Successfully refreshed additional view: {view_name}")
                        else:
                            logger.error(f"❌ Failed to refresh additional view: {view_name}")
                            return False
                else:
                    logger.info("✅ No additional views need refreshing")
            else:
                logger.info("✅ No views require refreshing based on dependencies")
            
            # Update metadata for processed operational tables
            for table in self.changed_tables:
                self.update_metadata(table)
            
            logger.info("✅ EDW incremental processing completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ EDW incremental processing failed: {e}")
            return False
    
    def process_edw_table(self, table_name: str) -> bool:
        """Process a specific EDW table based on its type and dependencies"""
        try:
            logger.info(f"🔄 Processing EDW table: {table_name}")
            
            if table_name.startswith('dim_'):
                return self.process_dimension_table(table_name)
            elif table_name.startswith('fact_'):
                return self.process_fact_table(table_name)
            elif table_name.startswith('mart_'):
                return self.process_mart_table(table_name)
            elif table_name.startswith('vw_'):
                return self.refresh_view(table_name)
            else:
                logger.warning(f"⚠️ Unknown table type: {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to process {table_name}: {e}")
            return False
    
    def process_dimension_table(self, table_name: str) -> bool:
        """Process dimension table with incremental strategy"""
        try:
            if table_name == 'dim_season':
                seasons = self.extract_seasons()
                if seasons:
                    self.load_dimension_table('dim_season', seasons)
                    self.refreshed_tables.add(table_name)  # Track refreshed table
                    
            elif table_name == 'dim_week':
                weeks = self.extract_weeks()
                if weeks:
                    self.load_dimension_table('dim_week', weeks)
                    self.refreshed_tables.add(table_name)  # Track refreshed table
                    
            elif table_name == 'dim_league':
                leagues = self.transform_leagues()
                if leagues:
                    self.load_dimension_table('dim_league', leagues)
                    self.refreshed_tables.add(table_name)  # Track refreshed table
                    
            elif table_name == 'dim_team':
                teams = self.transform_teams()
                if teams:
                    self.load_dimension_table('dim_team', teams)
                    self.refreshed_tables.add(table_name)  # Track refreshed table
                    
            elif table_name == 'dim_player':
                players = self.transform_players()
                if players:
                    self.load_dimension_table('dim_player', players)
                    self.refreshed_tables.add(table_name)  # Track refreshed table
                    
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to process dimension {table_name}: {e}")
            return False
    
    def load_dimension_table(self, table_name: str, data: List[Dict]) -> bool:
        """Load data into dimension table with proper upsert logic"""
        try:
            if not data:
                logger.info(f"📊 No data to load for {table_name}")
                return True
                
            logger.info(f"📊 Loading {len(data)} records into {table_name}")
            
            with self.engine.connect() as conn:
                # Only truncate if force_rebuild is enabled and not metadata table
                if self.force_rebuild and table_name != 'edw_metadata':
                    logger.info(f"🗑️ Force rebuild: Truncating {table_name}...")
                    conn.execute(text(f"TRUNCATE TABLE edw.{table_name} RESTART IDENTITY CASCADE"))
                    conn.commit()
                    
                    # Use bulk insert for clean rebuild
                    import pandas as pd
                    df = pd.DataFrame(data)
                    logger.info(f"⚡ Bulk inserting {len(df)} records...")
                    df.to_sql(table_name, conn, schema='edw', if_exists='append', index=False, method='multi')
                    conn.commit()
                    logger.info(f"✅ Successfully loaded {len(data)} records into {table_name}")
                    return True
                
                # Implement proper upsert logic for incremental updates
                logger.info(f"🔄 Using upsert strategy for {table_name}")
                
                if table_name == 'dim_season':
                    for record in data:
                        conn.execute(text("""
                            INSERT INTO edw.dim_season (season_year, season_start_date, season_end_date, 
                                                  playoff_start_week, championship_week, total_weeks, 
                                                  is_current_season, season_status)
                            VALUES (:season_year, :season_start_date, :season_end_date, 
                                   :playoff_start_week, :championship_week, :total_weeks,
                                   :is_current_season, :season_status)
                            ON CONFLICT (season_year) DO UPDATE SET
                                season_start_date = EXCLUDED.season_start_date,
                                season_end_date = EXCLUDED.season_end_date,
                                playoff_start_week = EXCLUDED.playoff_start_week,
                                championship_week = EXCLUDED.championship_week,
                                total_weeks = EXCLUDED.total_weeks,
                                is_current_season = EXCLUDED.is_current_season,
                                season_status = EXCLUDED.season_status
                        """), record)
                        
                elif table_name == 'dim_league':
                    for record in data:
                        # Use proper upsert with ON CONFLICT instead of manual check
                        conn.execute(text("""
                            INSERT INTO edw.dim_league (league_id, league_name, season_year, num_teams, 
                                                  league_type, scoring_type, draft_type, is_active, 
                                                  valid_from, valid_to)
                            VALUES (:league_id, :league_name, :season_year, :num_teams,
                                   :league_type, :scoring_type, :draft_type, :is_active,
                                   :valid_from, :valid_to)
                            ON CONFLICT (league_id, season_year) DO UPDATE SET
                                league_name = EXCLUDED.league_name,
                                num_teams = EXCLUDED.num_teams,
                                league_type = EXCLUDED.league_type,
                                scoring_type = EXCLUDED.scoring_type,
                                draft_type = EXCLUDED.draft_type,
                                is_active = EXCLUDED.is_active,
                                valid_to = EXCLUDED.valid_to
                        """), record)
                        
                elif table_name == 'dim_team':
                    for record in data:
                        # Get the league_key for this specific league_id
                        league_result = conn.execute(text("""
                            SELECT league_key FROM edw.dim_league WHERE league_id = :league_id LIMIT 1
                        """), {"league_id": record['league_id']})
                        league_row = league_result.fetchone()
                        
                        if not league_row:
                            logger.warning(f"⚠️ League {record['league_id']} not found in edw.dim_league, skipping team {record['team_id']}")
                            continue
                            
                        league_key = league_row[0]
                        team_data = {**record, 'league_key': league_key}
                        del team_data['league_id']  # Remove league_id, use league_key
                        
                        # Use proper upsert with ON CONFLICT
                        conn.execute(text("""
                            INSERT INTO edw.dim_team (team_id, league_key, team_name, manager_name, 
                                                manager_id, team_logo_url, is_active, valid_from, valid_to)
                            VALUES (:team_id, :league_key, :team_name, :manager_name,
                                   :manager_id, :team_logo_url, :is_active, :valid_from, :valid_to)
                            ON CONFLICT (team_id) DO UPDATE SET
                                league_key = EXCLUDED.league_key,
                                team_name = EXCLUDED.team_name,
                                manager_name = EXCLUDED.manager_name,
                                manager_id = EXCLUDED.manager_id,
                                team_logo_url = EXCLUDED.team_logo_url,
                                is_active = EXCLUDED.is_active,
                                valid_to = EXCLUDED.valid_to
                        """), team_data)
                        
                elif table_name == 'dim_player':
                    for record in data:
                        conn.execute(text("""
                            INSERT INTO edw.dim_player (player_id, player_name, primary_position, 
                                                  eligible_positions, nfl_team, jersey_number, 
                                                  rookie_year, is_active, valid_from, valid_to)
                            VALUES (:player_id, :player_name, :primary_position, :eligible_positions,
                                   :nfl_team, :jersey_number, :rookie_year, :is_active, 
                                   :valid_from, :valid_to)
                            ON CONFLICT (player_id) DO UPDATE SET
                                player_name = EXCLUDED.player_name,
                                primary_position = EXCLUDED.primary_position,
                                eligible_positions = EXCLUDED.eligible_positions,
                                nfl_team = EXCLUDED.nfl_team,
                                jersey_number = EXCLUDED.jersey_number,
                                rookie_year = EXCLUDED.rookie_year,
                                is_active = EXCLUDED.is_active,
                                valid_to = EXCLUDED.valid_to
                        """), record)
                        
                elif table_name == 'dim_week':
                    for record in data:
                        conn.execute(text("""
                            INSERT INTO edw.dim_week (season_year, week_number, week_type, 
                                                week_start_date, week_end_date, is_current_week)
                            VALUES (:season_year, :week_number, :week_type, 
                                   :week_start_date, :week_end_date, :is_current_week)
                            ON CONFLICT (season_year, week_number) DO UPDATE SET
                                week_type = EXCLUDED.week_type,
                                week_start_date = EXCLUDED.week_start_date,
                                week_end_date = EXCLUDED.week_end_date,
                                is_current_week = EXCLUDED.is_current_week
                        """), record)
                        
                elif table_name == 'dim_manager':
                    for record in data:
                        conn.execute(text("""
                            INSERT INTO edw.dim_manager (manager_name, first_active_season, 
                                                   last_active_season, total_seasons, is_active, 
                                                   valid_from, valid_to)
                            VALUES (:manager_name, :first_active_season, :last_active_season,
                                   :total_seasons, :is_active, :valid_from, :valid_to)
                            ON CONFLICT (manager_name) DO UPDATE SET
                                first_active_season = EXCLUDED.first_active_season,
                                last_active_season = EXCLUDED.last_active_season,
                                total_seasons = EXCLUDED.total_seasons,
                                is_active = EXCLUDED.is_active,
                                valid_to = EXCLUDED.valid_to
                        """), record)
                        
                else:
                    logger.warning(f"⚠️ No upsert logic defined for {table_name}")
                    # Default to bulk insert for unknown tables
                    import pandas as pd
                    df = pd.DataFrame(data)
                    df.to_sql(table_name, conn, schema='edw', if_exists='append', index=False, method='multi')
                
                conn.commit()
                logger.info(f"✅ Successfully upserted {len(data)} records into {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to load {table_name}: {e}")
            return False
    
    def process_fact_table(self, table_name: str) -> bool:
        """Process fact table with transformation and loading"""
        try:
            logger.info(f"📊 Processing fact table: {table_name}")
            
            # Get transformation function
            transform_method_name = f"transform_{table_name}"
            if hasattr(self, transform_method_name):
                transform_method = getattr(self, transform_method_name)
                logger.info(f"🔄 Transforming data for {table_name}...")
                data = transform_method()
                
                if data:
                    logger.info(f"✅ Transformed {len(data)} records for {table_name}")
                    # Load the data using load_fact_table
                    success = self.load_fact_table(table_name, data)
                    if success:
                        logger.info(f"✅ Successfully processed fact table: {table_name}")
                        self.refreshed_tables.add(table_name)  # Track refreshed table
                        return True
                    else:
                        logger.error(f"❌ Failed to load fact table: {table_name}")
                        return False
                else:
                    logger.info(f"ℹ️ No data generated for {table_name}")
                    return True
            else:
                logger.warning(f"⚠️ No transformation method found for {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to process fact table {table_name}: {e}")
            return False
    
    def process_mart_table(self, table_name: str) -> bool:
        """Process data mart table with transformation and loading"""
        try:
            logger.info(f"🏪 Processing data mart: {table_name}")
            
            # Get transformation function
            transform_method_name = f"transform_{table_name}"
            if hasattr(self, transform_method_name):
                transform_method = getattr(self, transform_method_name)
                logger.info(f"🔄 Transforming data for {table_name}...")
                data = transform_method()
                
                if data:
                    logger.info(f"✅ Transformed {len(data)} records for {table_name}")
                    # Load the data using load_mart_table
                    success = self.load_mart_table(table_name, data)
                    if success:
                        logger.info(f"✅ Successfully processed mart table: {table_name}")
                        self.refreshed_tables.add(table_name)  # Track refreshed table
                        return True
                    else:
                        logger.error(f"❌ Failed to load mart table: {table_name}")
                        return False
                else:
                    logger.info(f"ℹ️ No data generated for {table_name}")
                    return True
            else:
                logger.warning(f"⚠️ No transformation method found for {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to process mart table {table_name}: {e}")
            return False
    
    def refresh_view(self, view_name: str) -> bool:
        """Refresh a single analytical view using the complete view refresh system"""
        try:
            logger.info(f"🔧 Refreshing view: {view_name}")
            
            with self.engine.connect() as conn:
                success = self._refresh_single_view(conn, view_name)
                if success:
                    conn.commit()
                    logger.info(f"✅ Successfully refreshed {view_name}")
                else:
                    logger.error(f"❌ Failed to refresh {view_name}")
                return success
                
        except Exception as e:
            logger.error(f"❌ Error refreshing view {view_name}: {e}")
            return False
    
    def run_etl(self) -> bool:
        """Execute complete ETL process"""
        logger.info("🚀 Starting Fantasy Football EDW ETL Process")
        start_time = datetime.now()
        
        steps = [
            ("Connect to Database", self.connect),
            ("Load Operational Data", self.load_data),
            ("Detect Operational Changes", self.detect_operational_changes),
            ("Load Dimensions", self.load_dimensions),
            ("Load Facts", self.load_facts),
            ("Load Marts", self.load_marts),
            ("Refresh Analytical Views", self.refresh_analytical_views)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"📋 Step: {step_name}")
            if not step_func():
                logger.error(f"❌ ETL failed at step: {step_name}")
                return False
        
        runtime = datetime.now() - start_time
        logger.info(f"\n🎉 ETL COMPLETED SUCCESSFULLY!")
        logger.info(f"⏱️ Runtime: {runtime}")
        logger.info(f"🗄️ Enterprise Data Warehouse is ready for analytics!")
        
        self.log_league_filtering_config()
        self.log_view_refresh_summary()
        
        return True
    
    def load_fact_table(self, table_name: str, data: List[Dict]) -> bool:
        """Load data into fact table with proper incremental loading strategy"""
        try:
            if not data:
                logger.info(f"📊 No data to load for {table_name}")
                return True
                
            logger.info(f"📊 Loading {len(data)} records into {table_name}")
            
            # Convert to DataFrame for processing
            df = pd.DataFrame(data)
            
            with self.engine.connect() as conn:
                if self.force_rebuild:
                    # Force rebuild: truncate and reload all data
                    logger.info(f"🗑️ Force rebuild: Truncating {table_name}...")
                    conn.execute(text(f"TRUNCATE TABLE edw.{table_name} RESTART IDENTITY CASCADE"))
                    
                    if table_name == 'fact_roster':
                        # Use PostgreSQL COPY FROM STDIN for ultra-fast bulk insert
                        import io
                        logger.info(f"🚀 Using COPY FROM STDIN for {len(df)} roster records...")
                        
                        # Create StringIO buffer with CSV data
                        output = io.StringIO()
                        
                        # Write data to buffer (handle NaN values)
                        df_clean = df.copy()
                        df_clean = df_clean.fillna('\\N')  # PostgreSQL NULL representation in COPY
                        df_clean.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
                        output.seek(0)
                        
                        # Get column names for COPY command
                        columns = df.columns.tolist()
                        copy_sql = f"COPY edw.{table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
                        
                        # Execute COPY command using raw psycopg2 connection
                        raw_conn = conn.connection
                        with raw_conn.cursor() as cursor:
                            try:
                                cursor.copy_expert(copy_sql, output)
                                raw_conn.commit()
                                logger.info(f"✅ Successfully bulk loaded {len(df):,} roster records via COPY")
                            except Exception as e:
                                raw_conn.rollback()
                                logger.error(f"❌ COPY failed: {e}")
                                raise e
                    else:
                        # Use standard pandas to_sql for other fact tables
                        logger.info(f"⚡ Bulk inserting {len(df)} {table_name} records using pandas...")
                        df.to_sql(table_name, conn, schema='edw', if_exists='append', index=False)
                else:
                    # Incremental loading: implement proper upsert strategies
                    logger.info(f"🔄 Using incremental loading strategy for {table_name}")
                    
                    if table_name in ['fact_roster', 'fact_matchup', 'fact_team_performance']:
                        # Weekly refresh tables: delete current week data and insert new
                        if 'week_key' in df.columns or 'season_year' in df.columns:
                            if 'week_key' in df.columns:
                                week_keys = df['week_key'].unique()
                                delete_condition = f"week_key IN ({','.join(map(str, week_keys))})"
                            else:
                                # Use current season for deletion
                                current_season = df['season_year'].max()
                                delete_condition = f"season_year = {current_season}"
                            
                            logger.info(f"🗑️ Deleting existing records where {delete_condition}")
                            result = conn.execute(text(f"DELETE FROM edw.{table_name} WHERE {delete_condition}"))
                            deleted_count = result.rowcount
                            logger.info(f"  ✅ Deleted {deleted_count} existing records")
                        
                        # Insert new data
                        logger.info(f"⚡ Inserting {len(df)} new records...")
                        df.to_sql(table_name, conn, schema='edw', if_exists='append', index=False)
                        
                    elif table_name in ['fact_transaction', 'fact_draft', 'fact_player_statistics']:
                        # Append-only tables: use bulk insert (let database handle duplicates)
                        logger.info(f"🔄 Using bulk insert for append-only table {table_name}")
                        
                        # Remove transaction_id column if it exists (not in table schema)
                        if 'transaction_id' in df.columns:
                            df = df.drop('transaction_id', axis=1)
                            
                        # For statistics, use UPSERT strategy since we may update stats
                        if table_name == 'fact_player_statistics':
                            logger.info(f"🔄 Using UPSERT strategy for {table_name}")
                            # Convert DataFrame to records for individual upsert
                            for _, row in df.iterrows():
                                upsert_sql = text("""
                                    INSERT INTO edw.fact_player_statistics 
                                    (league_key, player_key, season_year, week_number, weekly_fantasy_points, position_type, 
                                     position_rank, league_rank, points_above_replacement, source_stat_id, game_code)
                                    VALUES (:league_key, :player_key, :season_year, :week_number, :weekly_fantasy_points, :position_type,
                                            :position_rank, :league_rank, :points_above_replacement, :source_stat_id, :game_code)
                                    ON CONFLICT (league_key, player_key, season_year, week_number) 
                                    DO UPDATE SET
                                        weekly_fantasy_points = EXCLUDED.weekly_fantasy_points,
                                        position_type = EXCLUDED.position_type,
                                        position_rank = EXCLUDED.position_rank,
                                        league_rank = EXCLUDED.league_rank,
                                        points_above_replacement = EXCLUDED.points_above_replacement,
                                        source_stat_id = EXCLUDED.source_stat_id,
                                        updated_at = CURRENT_TIMESTAMP
                                """)
                                conn.execute(upsert_sql, row.to_dict())
                            logger.info(f"✅ Upserted {len(df)} statistics records")
                        else:
                            # Use bulk insert for other tables
                            df.to_sql(table_name, conn, schema='edw', if_exists='append', index=False)
                            logger.info(f"✅ Bulk inserted {len(df)} records")
                        
                    else:
                        logger.warning(f"⚠️ No incremental loading strategy for {table_name}, using bulk insert")
                        df.to_sql(table_name, conn, schema='edw', if_exists='append', index=False)
                
                conn.commit()
                logger.info(f"✅ Successfully loaded {len(data)} records into {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to load {table_name}: {e}")
            return False
    
    def is_league_of_record(self, league_id: str, season_year: int) -> bool:
        """
        Determine if a league should be included in the EDW.
        
        Rules:
        1. Always include historical leagues (hard-coded)
        2. Automatically include future leagues (2025+)
        3. Exclude any manually specified leagues
        """
        # Never include manually excluded leagues
        if league_id in self.EXCLUDED_LEAGUE_IDS:
            logger.info(f"🚫 Excluding manually excluded league: {league_id}")
            return False
            
        # Always include historical leagues
        if league_id in self.HISTORICAL_LEAGUE_IDS:
            return True
            
        # Automatically include future leagues
        if season_year >= self.FUTURE_SEASON_THRESHOLD:
            logger.info(f"🔄 Auto-including future league: {league_id} ({season_year})")
            return True
            
        # Exclude everything else
        return False

    def log_league_filtering_config(self):
        """Log the current league filtering configuration"""
        logger.info("🏈 League of Record Filtering Configuration:")
        logger.info(f"  📜 Historical leagues (always included): {len(self.HISTORICAL_LEAGUE_IDS)} leagues (2005-2024)")
        logger.info(f"  🔄 Future league threshold: {self.FUTURE_SEASON_THRESHOLD}+ (auto-include)")
        if self.EXCLUDED_LEAGUE_IDS:
            logger.info(f"  🚫 Manually excluded leagues: {len(self.EXCLUDED_LEAGUE_IDS)} leagues")
            for excluded_id in self.EXCLUDED_LEAGUE_IDS:
                logger.info(f"    - {excluded_id}")
        else:
            logger.info("  🚫 No manually excluded leagues")
        logger.info("  ℹ️  To exclude a future league, add its ID to EXCLUDED_LEAGUE_IDS")

    def determine_views_to_refresh(self) -> Dict[str, Dict]:
        """
        Intelligently determine which views need refreshing based on changed tables.
        
        Returns:
            Dict mapping view names to their refresh metadata
        """
        views_to_refresh = {}
        
        if self.force_rebuild:
            # Force rebuild: refresh all views
            logger.info("🔄 Force rebuild mode: All views will be refreshed")
            for view_name, config in self.VIEW_DEPENDENCIES.items():
                views_to_refresh[view_name] = {
                    'reason': 'force_rebuild',
                    'priority': config['refresh_priority'],
                    'description': config['description']
                }
            return views_to_refresh
        
        # Check if any changed tables affect each view
        for view_name, config in self.VIEW_DEPENDENCIES.items():
            reasons = []
            
            # Check table dependencies
            for table in config['depends_on_tables']:
                if table in self.refreshed_tables:
                    reasons.append(f"table_{table}_changed")
            
            # Check mart dependencies
            for mart in config['depends_on_marts']:
                if mart in self.refreshed_tables:
                    reasons.append(f"mart_{mart}_changed")
            
            # If any dependencies changed, mark for refresh
            if reasons:
                views_to_refresh[view_name] = {
                    'reason': ', '.join(reasons),
                    'priority': config['refresh_priority'],
                    'description': config['description']
                }
        
        return views_to_refresh

    def refresh_analytical_views(self) -> bool:
        """
        Smart analytical view refresh - only refreshes views affected by data changes.
        
        Returns:
            bool: Success status
        """
        try:
            logger.info("🔍 Analyzing view refresh requirements...")
            
            # Determine which views need refreshing
            views_to_refresh = self.determine_views_to_refresh()
            
            if not views_to_refresh:
                logger.info("✅ No views require refreshing - all analytical views are current")
                self.view_refresh_stats['views_checked'] = len(self.VIEW_DEPENDENCIES)
                return True
            
            logger.info(f"🔧 Refreshing {len(views_to_refresh)} analytical views...")
            
            # Sort views by priority (HIGH -> MEDIUM -> LOW)
            priority_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
            sorted_views = sorted(
                views_to_refresh.items(),
                key=lambda x: priority_order.get(x[1]['priority'], 3)
            )
            
            success_count = 0
            
            with self.engine.connect() as conn:
                for view_name, metadata in sorted_views:
                    try:
                        logger.info(f"  📊 Refreshing {view_name} ({metadata['priority']} priority)...")
                        logger.info(f"      Reason: {metadata['reason']}")
                        logger.info(f"      Description: {metadata['description']}")
                        
                        if self._refresh_single_view(conn, view_name):
                            success_count += 1
                            self.view_refresh_stats['views_refreshed'] += 1
                        else:
                            self.view_refresh_stats['views_failed'] += 1
                            
                    except Exception as e:
                        logger.error(f"  ❌ Failed to refresh {view_name}: {e}")
                        self.view_refresh_stats['views_failed'] += 1
                
                # Update stats
                self.view_refresh_stats['views_checked'] = len(self.VIEW_DEPENDENCIES)
                self.view_refresh_stats['views_skipped'] = len(self.VIEW_DEPENDENCIES) - len(views_to_refresh)
                
                conn.commit()
            
            if success_count == len(views_to_refresh):
                logger.info(f"✅ Successfully refreshed {success_count} analytical views")
                return True
            else:
                logger.warning(f"⚠️ Refreshed {success_count}/{len(views_to_refresh)} views ({self.view_refresh_stats['views_failed']} failed)")
                return False
                    
        except Exception as e:
            logger.error(f"❌ View refresh process failed: {e}")
            return False

    def _refresh_single_view(self, conn, view_name: str) -> bool:
        """
        Refresh a single analytical view with proper error handling.
        
        Args:
            conn: Database connection
            view_name: Name of the view to refresh
            
        Returns:
            bool: Success status
        """
        try:
            # Get view definition based on view name
            view_sql = self._get_view_definition(view_name)
            
            if not view_sql:
                logger.error(f"  ❌ No definition found for view: {view_name}")
                return False
            
            # Drop existing view
            conn.execute(text(f'DROP VIEW IF EXISTS edw.{view_name}'))
            
            # Create new view
            conn.execute(text(view_sql))
            
            logger.info(f"  ✅ {view_name} refreshed successfully")
            return True
            
        except Exception as e:
            logger.error(f"  ❌ Failed to refresh {view_name}: {e}")
            return False

    def _get_view_definition(self, view_name: str) -> str:
        """
        Get the SQL definition for a specific view.
        
        Args:
            view_name: Name of the view
            
        Returns:
            str: SQL CREATE VIEW statement
        """
        view_definitions = {
            'vw_current_season_dashboard': """
                CREATE VIEW edw.vw_current_season_dashboard AS
                WITH season_totals AS (
                    SELECT 
                        ftp.team_key,
                        ftp.league_key,
                        SUM(ftp.wins) as total_wins,
                        SUM(ftp.losses) as total_losses,
                        SUM(ftp.ties) as total_ties,
                        SUM(ftp.points_for) as total_points_for,
                        SUM(ftp.points_against) as total_points_against,
                        SUM(ftp.point_differential) as total_point_differential,
                        CASE 
                            WHEN (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) > 0 
                            THEN ROUND((SUM(ftp.wins) + 0.5 * SUM(ftp.ties))::decimal / (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)), 4)
                            ELSE 0
                        END as season_win_percentage
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft)
                    GROUP BY ftp.team_key, ftp.league_key
                ),
                latest_week_rankings AS (
                    SELECT 
                        ftp.team_key,
                        ftp.league_key,
                        ftp.playoff_probability,
                        ftp.is_playoff_team,
                        ROW_NUMBER() OVER (
                            PARTITION BY ftp.team_key, ftp.league_key 
                            ORDER BY dw.week_number DESC
                        ) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft)
                ),
                ranked_teams AS (
                    SELECT 
                        st.*,
                        lwr.playoff_probability,
                        lwr.is_playoff_team,
                        ROW_NUMBER() OVER (
                            PARTITION BY st.league_key 
                            ORDER BY st.season_win_percentage DESC, st.total_points_for DESC
                        ) as calculated_season_rank
                    FROM season_totals st
                    JOIN latest_week_rankings lwr ON st.team_key = lwr.team_key 
                        AND st.league_key = lwr.league_key 
                        AND lwr.rn = 1
                )
                SELECT 
                    dl.league_name,
                    dl.season_year,
                    dt.team_name,
                    dt.manager_name,
                    rt.total_wins as wins,
                    rt.total_losses as losses,
                    rt.total_ties as ties,
                    rt.total_points_for as points_for,
                    rt.total_points_against as points_against,
                    rt.total_point_differential as point_differential,
                    rt.season_win_percentage as win_percentage,
                    rt.calculated_season_rank as season_rank,
                    rt.playoff_probability,
                    rt.is_playoff_team,
                    rt.calculated_season_rank as playoff_seed
                FROM ranked_teams rt
                JOIN edw.dim_team dt ON rt.team_key = dt.team_key
                JOIN edw.dim_league dl ON rt.league_key = dl.league_key
                WHERE dt.is_active = TRUE
                ORDER BY dl.league_name, rt.calculated_season_rank
            """,
            
            'vw_manager_hall_of_fame': """
                CREATE VIEW edw.vw_manager_hall_of_fame AS
                WITH manager_championships AS (
                    -- Count championships from mart_league_summary
                    SELECT 
                        champion_manager as manager_name,
                        COUNT(*) as championships_won
                    FROM edw.mart_league_summary
                    WHERE champion_manager IS NOT NULL
                    GROUP BY champion_manager
                ),
                manager_stats AS (
                    SELECT 
                        dt.manager_name,
                        COUNT(DISTINCT dl.season_year) as total_seasons,
                        SUM(ftp.wins) as career_wins,
                        SUM(ftp.losses) as career_losses,
                        SUM(ftp.ties) as career_ties,
                        ROUND(
                            CASE 
                                WHEN (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) > 0 
                                THEN (SUM(ftp.wins) + 0.5 * SUM(ftp.ties))::decimal / (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties))
                                ELSE 0
                            END, 3
                        ) as career_win_percentage,
                        SUM(ftp.points_for) as total_points_scored,
                        ROUND(AVG(ftp.points_for), 1) as avg_points_per_game,
                        COUNT(DISTINCT CASE WHEN ftp.is_playoff_team THEN dl.season_year END) as playoff_appearances,
                        ROUND(STDDEV(ftp.win_percentage), 3) as season_consistency_score
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    LEFT JOIN edw.dim_manager dm ON dt.manager_name = dm.manager_name
                    WHERE dt.manager_name IS NOT NULL
                      AND (
                          dm.include_in_analysis = 'Y'
                          OR EXISTS (
                              SELECT 1 FROM edw.mart_league_summary mls 
                              WHERE mls.champion_manager = dt.manager_name
                          )
                      )
                    GROUP BY dt.manager_name
                )
                SELECT 
                    ms.manager_name,
                    ms.total_seasons::INT as total_seasons,
                    COALESCE(mc.championships_won, 0)::INT as championships_won,
                    ms.career_wins::INT as career_wins,
                    ms.career_losses::INT as career_losses,
                    ms.career_ties::INT as career_ties,
                    ms.career_win_percentage::DECIMAL(6,3) as career_win_percentage,
                    ms.total_points_scored::DECIMAL(10,2) as total_points_scored,
                    ms.avg_points_per_game::DECIMAL(6,1) as avg_points_per_game,
                    ms.playoff_appearances::INT as playoff_appearances,
                    ms.season_consistency_score::DECIMAL(6,3) as season_consistency_score,
                    -- Hall of Fame Index: 60% Championships + 40% Win Percentage
                    ROUND(
                        (COALESCE(mc.championships_won, 0)::decimal / NULLIF((SELECT MAX(championships_won) FROM manager_championships), 0) * 0.6) + 
                        (ms.career_win_percentage * 0.4),
                        3
                    )::DECIMAL(6,3) as hall_of_fame_index,
                    RANK() OVER (ORDER BY 
                        (COALESCE(mc.championships_won, 0)::decimal / NULLIF((SELECT MAX(championships_won) FROM manager_championships), 0) * 0.6) + 
                        (ms.career_win_percentage * 0.4) DESC
                    )::INT as hall_of_fame_rank
                FROM manager_stats ms
                LEFT JOIN manager_championships mc ON ms.manager_name = mc.manager_name
                WHERE ms.total_seasons >= 3
                ORDER BY hall_of_fame_rank
            """,
            
            'vw_league_competitiveness': """
                CREATE VIEW edw.vw_league_competitiveness AS
                WITH season_team_totals AS (
                    -- Get season totals per team (aggregate weekly data)
                    SELECT 
                        ftp.league_key,
                        dl.season_year,
                        ftp.team_key,
                        SUM(ftp.wins) as season_wins,
                        SUM(ftp.losses) as season_losses,
                        SUM(ftp.ties) as season_ties,
                        SUM(ftp.points_for) as season_points,
                        CASE 
                            WHEN (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) > 0 
                            THEN ROUND((SUM(ftp.wins) + 0.5 * SUM(ftp.ties))::decimal / (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)), 4)
                            ELSE 0
                        END as season_win_pct
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    GROUP BY ftp.league_key, dl.season_year, ftp.team_key
                ),
                competitiveness_metrics AS (
                    SELECT 
                        league_key,
                        season_year,
                        
                        -- Component 1: Win Percentage Parity (lower std dev = more competitive)
                        GREATEST(0, 100 - (STDDEV(season_win_pct) * 300)) as win_parity_score,
                        
                        -- Component 2: Point Spread Tightness (smaller gap = more competitive)  
                        GREATEST(0, 100 - ((MAX(season_points) - MIN(season_points)) / AVG(season_points) * 100)) as point_spread_score,
                        
                        -- Component 3: Playoff Race Drama (teams within 1 game of 6th place)
                        (SELECT COUNT(*) * 10.0  -- Scale to 0-100 (10 teams max = 100)
                         FROM season_team_totals stt2 
                         WHERE stt2.league_key = stt.league_key 
                           AND stt2.season_year = stt.season_year
                           AND stt2.season_wins >= (
                               SELECT season_wins 
                               FROM season_team_totals stt3 
                               WHERE stt3.league_key = stt.league_key 
                                 AND stt3.season_year = stt.season_year 
                               ORDER BY season_wins DESC, season_points DESC 
                               LIMIT 1 OFFSET 5  -- 6th place
                           ) - 1  -- Within 1 game
                        ) as playoff_race_score
                        
                    FROM season_team_totals stt
                    GROUP BY league_key, season_year
                ),
                close_games_metrics AS (
                    SELECT 
                        fm.league_key,
                        fm.season_year,
                        
                        -- Component 4: Close Games Frequency (< 15 point margin)
                        ROUND(
                            COUNT(CASE WHEN fm.margin_of_victory <= 15 THEN 1 END) * 100.0 / COUNT(*),
                            1
                        ) as close_games_score
                        
                    FROM edw.fact_matchup fm
                    WHERE fm.margin_of_victory IS NOT NULL
                    GROUP BY fm.league_key, fm.season_year
                ),
                activity_scoring_percentiles AS (
                    SELECT 
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_transactions) as trans_q1,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_transactions) as trans_median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_transactions) as trans_q3,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY average_weekly_score) as score_q1,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY average_weekly_score) as score_median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY average_weekly_score) as score_q3
                    FROM edw.mart_league_summary
                )
                SELECT 
                    mls.league_name,
                    mls.season_year,
                    mls.total_teams,
                    mls.total_weeks,
                    mls.total_transactions,
                    mls.champion_manager,
                    mls.runner_up_manager,
                    mls.highest_scorer_manager,
                    mls.highest_single_week_score,
                    mls.average_weekly_score,
                    mls.most_active_trader,
                    
                    -- Improved Activity Tier (quartile-based)
                    CASE 
                        WHEN mls.total_transactions >= asp.trans_q3 THEN 'Elite Active'
                        WHEN mls.total_transactions >= asp.trans_median THEN 'Highly Active'
                        WHEN mls.total_transactions >= asp.trans_q1 THEN 'Active'
                        ELSE 'Low Activity'
                    END as activity_tier,
                    
                    -- Improved Scoring Tier (quartile-based)
                    CASE 
                        WHEN mls.average_weekly_score >= asp.score_q3 THEN 'Elite Scoring'
                        WHEN mls.average_weekly_score >= asp.score_median THEN 'High Scoring'
                        WHEN mls.average_weekly_score >= asp.score_q1 THEN 'Average Scoring'
                        ELSE 'Low Scoring'
                    END as scoring_tier,
                    
                    -- NEW: Competitiveness Index (0-100 scale)
                    ROUND(
                        COALESCE(
                            (cm.win_parity_score * 0.25) + 
                            (cm.point_spread_score * 0.25) + 
                            (cm.playoff_race_score * 0.25) + 
                            (cgm.close_games_score * 0.25),
                            0
                        ),
                        1
                    ) as competitiveness_index,
                    
                    -- NEW: Difficulty to Win Rating
                    CASE 
                        WHEN COALESCE(
                            (cm.win_parity_score * 0.25) + 
                            (cm.point_spread_score * 0.25) + 
                            (cm.playoff_race_score * 0.25) + 
                            (cgm.close_games_score * 0.25), 0
                        ) >= 70 THEN 'Brutal'
                        WHEN COALESCE(
                            (cm.win_parity_score * 0.25) + 
                            (cm.point_spread_score * 0.25) + 
                            (cm.playoff_race_score * 0.25) + 
                            (cgm.close_games_score * 0.25), 0
                        ) >= 60 THEN 'Competitive'
                        WHEN COALESCE(
                            (cm.win_parity_score * 0.25) + 
                            (cm.point_spread_score * 0.25) + 
                            (cm.playoff_race_score * 0.25) + 
                            (cgm.close_games_score * 0.25), 0
                        ) >= 50 THEN 'Moderate'
                        ELSE 'Easy'
                    END as difficulty_to_win,
                    
                    -- Component scores for analysis
                    ROUND(cm.win_parity_score, 1) as win_parity_score,
                    ROUND(cm.point_spread_score, 1) as point_spread_score,
                    ROUND(cm.playoff_race_score, 1) as playoff_race_score,
                    ROUND(cgm.close_games_score, 1) as close_games_score
                    
                FROM edw.mart_league_summary mls
                CROSS JOIN activity_scoring_percentiles asp
                LEFT JOIN competitiveness_metrics cm ON mls.league_key = cm.league_key 
                    AND mls.season_year = cm.season_year
                LEFT JOIN close_games_metrics cgm ON mls.league_key = cgm.league_key 
                    AND mls.season_year = cgm.season_year
                ORDER BY mls.season_year DESC, competitiveness_index DESC
            """,
            
            'vw_player_breakout_analysis': """
                CREATE VIEW edw.vw_player_breakout_analysis AS
                SELECT 
                    dp.player_name,
                    dp.primary_position,
                    mpv.season_year,
                    mpv.career_avg_draft_position,
                    mpv.total_fantasy_points,
                    mpv.draft_value_score,
                    mpv.waiver_pickup_value,
                    CASE 
                        WHEN mpv.career_avg_draft_position > 100 AND mpv.draft_value_score > 2.0 THEN 'Major Breakout'
                        WHEN mpv.career_avg_draft_position > 50 AND mpv.draft_value_score > 1.5 THEN 'Solid Breakout'
                        WHEN mpv.waiver_pickup_value > 1.5 THEN 'Waiver Wire Gem'
                        ELSE 'Standard Performance'
                    END as breakout_type
                FROM edw.mart_player_value mpv
                JOIN edw.dim_player dp ON mpv.player_key = dp.player_key
                WHERE mpv.draft_value_score > 1.3 OR mpv.waiver_pickup_value > 1.3
                ORDER BY mpv.draft_value_score DESC
            """,
            
            'vw_trade_analysis': """
                -- CALIBRATED COMPREHENSIVE TRADE ANALYSIS VIEW (v2.0)
                -- Fixes: Production scaling (0.15), Opportunity cost implementation, Refined thresholds (8-point)
                -- Components: Production (40%), Playoff Impact (30%), Opportunity Cost (20%), Context (10%)
                CREATE VIEW edw.vw_trade_analysis AS
                WITH trade_groups AS (
                    -- Create synthetic trade group IDs by grouping reciprocal trades
                    SELECT 
                        ft.*,
                        -- Generate consistent trade_group_id for reciprocal trades
                        CONCAT(
                            ft.league_key, '_', 
                            ft.transaction_date, '_',
                            CASE 
                                WHEN ft.from_team_key < ft.to_team_key 
                                THEN CONCAT(ft.from_team_key, '_', ft.to_team_key)
                                ELSE CONCAT(ft.to_team_key, '_', ft.from_team_key)
                            END
                        ) AS synthetic_trade_group_id
                    FROM edw.fact_transaction ft
                    WHERE ft.transaction_type = 'trade'
                ),
                trade_synthesis AS (
                    -- Synthesize trades into single rows with player aggregation
                    SELECT 
                        tg.synthetic_trade_group_id,
                        MIN(tg.league_key) as league_key,
                        MIN(tg.season_year) as season_year,
                        MIN(tg.transaction_date) as transaction_date,
                        MIN(tg.transaction_week) as transaction_week,
                        -- Determine the two teams involved (consistent ordering)
                        MIN(CASE WHEN tg.from_team_key < tg.to_team_key THEN tg.from_team_key ELSE tg.to_team_key END) as team_a_key,
                        MAX(CASE WHEN tg.from_team_key < tg.to_team_key THEN tg.to_team_key ELSE tg.from_team_key END) as team_b_key,
                        MIN(CASE WHEN tg.from_team_key < tg.to_team_key THEN tg.from_manager_key ELSE tg.to_manager_key END) as team_a_manager_key,
                        MAX(CASE WHEN tg.from_team_key < tg.to_team_key THEN tg.to_manager_key ELSE tg.from_manager_key END) as team_b_manager_key,
                        -- Count players on each side
                        COUNT(*) FILTER (WHERE tg.from_team_key = LEAST(tg.from_team_key, tg.to_team_key)) as team_a_player_count,
                        COUNT(*) FILTER (WHERE tg.from_team_key = GREATEST(tg.from_team_key, tg.to_team_key)) as team_b_player_count,
                        COUNT(*) as total_players
                    FROM trade_groups tg
                    GROUP BY tg.synthetic_trade_group_id
                ),
                trade_players AS (
                    -- Get player names for each side of trade
                    SELECT 
                        ts.synthetic_trade_group_id,
                        STRING_AGG(
                            CASE WHEN tg.from_team_key = ts.team_a_key 
                                 THEN dp.player_name ELSE NULL END, 
                            ', ' ORDER BY dp.player_name
                        ) FILTER (WHERE tg.from_team_key = ts.team_a_key) as team_a_gives,
                        STRING_AGG(
                            CASE WHEN tg.from_team_key = ts.team_b_key 
                                 THEN dp.player_name ELSE NULL END, 
                            ', ' ORDER BY dp.player_name
                        ) FILTER (WHERE tg.from_team_key = ts.team_b_key) as team_b_gives
                    FROM trade_synthesis ts
                    JOIN trade_groups tg ON ts.synthetic_trade_group_id = tg.synthetic_trade_group_id
                    JOIN edw.dim_player dp ON tg.player_key = dp.player_key
                    GROUP BY ts.synthetic_trade_group_id
                ),
                fantasy_production AS (
                    -- Calculate fantasy points for traded players after trade date (40% weight)
                    SELECT 
                        ts.synthetic_trade_group_id,
                        -- Team A production (players they received)
                        COALESCE(SUM(
                            CASE WHEN tg.to_team_key = ts.team_a_key 
                                 THEN fps.weekly_fantasy_points ELSE 0 END
                        ), 0) as team_a_production,
                        -- Team B production (players they received)  
                        COALESCE(SUM(
                            CASE WHEN tg.to_team_key = ts.team_b_key 
                                 THEN fps.weekly_fantasy_points ELSE 0 END
                        ), 0) as team_b_production
                    FROM trade_synthesis ts
                    JOIN trade_groups tg ON ts.synthetic_trade_group_id = tg.synthetic_trade_group_id
                    LEFT JOIN edw.fact_player_statistics fps ON (
                        fps.player_key = tg.player_key 
                        AND fps.league_key = tg.league_key
                        AND fps.season_year = tg.season_year
                        AND fps.week_number >= tg.transaction_week
                    )
                    GROUP BY ts.synthetic_trade_group_id
                ),
                opportunity_cost AS (
                    -- Evaluate pre-trade value differential (20% weight)
                    SELECT 
                        ts.synthetic_trade_group_id,
                        -- Average pre-trade weekly points of players traded away
                        AVG(CASE WHEN tg.from_team_key = ts.team_a_key 
                                 THEN fps.weekly_fantasy_points ELSE NULL END) as team_a_gave_avg_value,
                        AVG(CASE WHEN tg.from_team_key = ts.team_b_key 
                                 THEN fps.weekly_fantasy_points ELSE NULL END) as team_b_gave_avg_value
                    FROM trade_synthesis ts
                    JOIN trade_groups tg ON ts.synthetic_trade_group_id = tg.synthetic_trade_group_id
                    LEFT JOIN edw.fact_player_statistics fps ON (
                        fps.player_key = tg.player_key 
                        AND fps.league_key = tg.league_key
                        AND fps.season_year = tg.season_year
                        AND fps.week_number < tg.transaction_week
                        AND fps.week_number >= GREATEST(1, tg.transaction_week - 4)  -- Last 4 weeks before trade
                    )
                    GROUP BY ts.synthetic_trade_group_id
                ),
                playoff_impact AS (
                    -- Analyze playoff implications (30% weight)
                    SELECT 
                        ts.synthetic_trade_group_id,
                        -- Check if teams made playoffs
                        MAX(CASE WHEN ftp.team_key = ts.team_a_key AND ftp.is_playoff_team THEN 1 ELSE 0 END) as team_a_made_playoffs,
                        MAX(CASE WHEN ftp.team_key = ts.team_b_key AND ftp.is_playoff_team THEN 1 ELSE 0 END) as team_b_made_playoffs,
                         -- Check championship outcomes  
                        MAX(CASE WHEN fm.winner_team_key = ts.team_a_key THEN 1 ELSE 0 END) as team_a_champion,
                        MAX(CASE WHEN fm.winner_team_key = ts.team_b_key THEN 1 ELSE 0 END) as team_b_champion
                    FROM trade_synthesis ts
                    LEFT JOIN edw.fact_team_performance ftp ON (
                        ftp.league_key = ts.league_key 
                        AND ftp.season_year = ts.season_year
                        AND ftp.team_key IN (ts.team_a_key, ts.team_b_key))
					LEFT JOIN edw.fact_matchup fm ON (
						fm.league_key = ts.league_key
						AND fm.season_year = fm.season_year
						AND fm.winner_team_key IN (ts.team_a_key, ts.team_b_key)
						AND is_championship = TRUE
                    )
                    GROUP BY ts.synthetic_trade_group_id
                ),
                contextual_factors AS (
                    -- Trade timing context (10% weight)
                    SELECT 
                        ts.synthetic_trade_group_id,
                        ts.transaction_week,
                        -- Late season trades are more impactful
                        CASE 
                            WHEN ts.transaction_week >= 10 THEN 1.5
                            WHEN ts.transaction_week >= 6 THEN 1.2  
                            ELSE 1.0 
                        END as timing_multiplier,
                        -- Current season status for dynamic evaluation
                        CASE 
                            WHEN ts.season_year < EXTRACT(YEAR FROM CURRENT_DATE) THEN 'Complete'
                            WHEN ts.season_year = EXTRACT(YEAR FROM CURRENT_DATE) THEN 'In Progress'
                            ELSE 'Future'
                        END as evaluation_status
                    FROM trade_synthesis ts
                )
                SELECT 
                    dl.league_name,
                    ts.season_year,
                    ts.transaction_date,
                    ts.transaction_week,
                    dta.team_name as team_a_name,
                    dma.manager_name as team_a_manager,
                    tp.team_a_gives,
                    dtb.team_name as team_b_name, 
                    dmb.manager_name as team_b_manager,
                    tp.team_b_gives,
                    CONCAT(ts.team_a_player_count, '-for-', ts.team_b_player_count) as trade_type,
                    ts.total_players,
                    
                    -- Production metrics
                    ROUND(fp.team_a_production, 1) as team_a_production,
                    ROUND(fp.team_b_production, 1) as team_b_production,
                    ROUND(fp.team_a_production - fp.team_b_production, 1) as production_differential,
                    
                    -- Opportunity cost metrics (NEW)
                    ROUND(COALESCE(oc.team_a_gave_avg_value, 0), 1) as team_a_pre_trade_avg,
                    ROUND(COALESCE(oc.team_b_gave_avg_value, 0), 1) as team_b_pre_trade_avg,
                    ROUND(COALESCE(oc.team_b_gave_avg_value, 0) - COALESCE(oc.team_a_gave_avg_value, 0), 1) as opportunity_differential,
                    
                    -- Playoff outcomes
                    pi.team_a_made_playoffs,
                    pi.team_b_made_playoffs,
                    pi.team_a_champion,
                    pi.team_b_champion,
                    
                    -- Component Scores (0-100 scale) - CALIBRATED
                    ROUND(LEAST(100, GREATEST(0, 
                        50 + (fp.team_a_production - fp.team_b_production) * 0.15  -- Reduced from 0.3
                    )), 1) as team_a_production_score,
                    
                    ROUND(CASE 
                        WHEN pi.team_a_champion = 1 THEN 85  -- Reduced from 90
                        WHEN pi.team_a_made_playoffs = 1 AND pi.team_b_made_playoffs = 0 THEN 70  -- Reduced from 75
                        WHEN pi.team_a_made_playoffs = 0 AND pi.team_b_made_playoffs = 1 THEN 30  -- Increased from 25
                        ELSE 50
                    END, 1) as team_a_playoff_score,
                    
                    -- IMPLEMENTED Opportunity Cost Score (20% weight)
                    ROUND(LEAST(100, GREATEST(0,
                        50 + (COALESCE(oc.team_b_gave_avg_value, 0) - COALESCE(oc.team_a_gave_avg_value, 0)) * 2
                    )), 1) as team_a_opportunity_score,
                    
                    ROUND(50 + (cf.timing_multiplier - 1) * 25, 1) as team_a_context_score,
                    
                    -- Final Weighted Score (0-100) - REFINED FORMULA
                    ROUND(
                        LEAST(100, GREATEST(0, 
                            (50 + (fp.team_a_production - fp.team_b_production) * 0.15) * 0.40 +
                            CASE 
                                WHEN pi.team_a_champion = 1 THEN 85
                                WHEN pi.team_a_made_playoffs = 1 AND pi.team_b_made_playoffs = 0 THEN 70
                                WHEN pi.team_a_made_playoffs = 0 AND pi.team_b_made_playoffs = 1 THEN 30
                                ELSE 50
                            END * 0.30 +
                            (50 + (COALESCE(oc.team_b_gave_avg_value, 0) - COALESCE(oc.team_a_gave_avg_value, 0)) * 2) * 0.20 +
                            (50 + (cf.timing_multiplier - 1) * 25) * 0.10
                        )), 1
                    ) as team_a_final_score,
                    
                    ROUND(
                        LEAST(100, GREATEST(0, 
                            (50 - (fp.team_a_production - fp.team_b_production) * 0.15) * 0.40 +
                            CASE 
                                WHEN pi.team_b_champion = 1 THEN 85
                                WHEN pi.team_b_made_playoffs = 1 AND pi.team_a_made_playoffs = 0 THEN 70
                                WHEN pi.team_b_made_playoffs = 0 AND pi.team_a_made_playoffs = 1 THEN 30
                                ELSE 50
                            END * 0.30 +
                            (50 + (COALESCE(oc.team_a_gave_avg_value, 0) - COALESCE(oc.team_b_gave_avg_value, 0)) * 2) * 0.20 +
                            (50 + (cf.timing_multiplier - 1) * 25) * 0.10
                        )), 1
                    ) as team_b_final_score,
                    
                    -- Winner determination - REFINED THRESHOLDS
                    CASE 
                        WHEN ABS((50 + (fp.team_a_production - fp.team_b_production) * 0.15) * 0.40 +
                            CASE 
                                WHEN pi.team_a_champion = 1 THEN 85
                                WHEN pi.team_a_made_playoffs = 1 AND pi.team_b_made_playoffs = 0 THEN 70
                                WHEN pi.team_a_made_playoffs = 0 AND pi.team_b_made_playoffs = 1 THEN 30
                                ELSE 50
                            END * 0.30 +
                            (50 + (COALESCE(oc.team_b_gave_avg_value, 0) - COALESCE(oc.team_a_gave_avg_value, 0)) * 2) * 0.20 +
                            (50 + (cf.timing_multiplier - 1) * 25) * 0.10 -
                            
                            (50 - (fp.team_a_production - fp.team_b_production) * 0.15) * 0.40 -
                            CASE 
                                WHEN pi.team_b_champion = 1 THEN 85
                                WHEN pi.team_b_made_playoffs = 1 AND pi.team_a_made_playoffs = 0 THEN 70
                                WHEN pi.team_b_made_playoffs = 0 AND pi.team_a_made_playoffs = 1 THEN 30
                                ELSE 50
                            END * 0.30 -
                            (50 + (COALESCE(oc.team_a_gave_avg_value, 0) - COALESCE(oc.team_b_gave_avg_value, 0)) * 2) * 0.20 -
                            (50 + (cf.timing_multiplier - 1) * 25) * 0.10) >= 8 
                        THEN 
                            CASE WHEN (50 + (fp.team_a_production - fp.team_b_production) * 0.15) * 0.40 +
                                CASE 
                                    WHEN pi.team_a_champion = 1 THEN 85
                                    WHEN pi.team_a_made_playoffs = 1 AND pi.team_b_made_playoffs = 0 THEN 70
                                    WHEN pi.team_a_made_playoffs = 0 AND pi.team_b_made_playoffs = 1 THEN 30
                                    ELSE 50
                                END * 0.30 +
                                (50 + (COALESCE(oc.team_b_gave_avg_value, 0) - COALESCE(oc.team_a_gave_avg_value, 0)) * 2) * 0.20 +
                                (50 + (cf.timing_multiplier - 1) * 25) * 0.10 > 
                                (50 - (fp.team_a_production - fp.team_b_production) * 0.15) * 0.40 +
                                CASE 
                                    WHEN pi.team_b_champion = 1 THEN 85
                                    WHEN pi.team_b_made_playoffs = 1 AND pi.team_a_made_playoffs = 0 THEN 70
                                    WHEN pi.team_b_made_playoffs = 0 AND pi.team_a_made_playoffs = 1 THEN 30
                                    ELSE 50
                                END * 0.30 +
                                (50 + (COALESCE(oc.team_a_gave_avg_value, 0) - COALESCE(oc.team_b_gave_avg_value, 0)) * 2) * 0.20 +
                                (50 + (cf.timing_multiplier - 1) * 25) * 0.10
                            THEN dma.manager_name 
                            ELSE dmb.manager_name 
                            END
                        ELSE 'Even Trade'
                    END as trade_winner,
                    
                    -- Trade analysis summary
                    CASE 
                        WHEN ABS(fp.team_a_production - fp.team_b_production) >= 50.0 THEN 
                            CONCAT('Production-driven: ', 
                                CASE WHEN fp.team_a_production > fp.team_b_production THEN dma.manager_name ELSE dmb.manager_name END,
                                ' gained ', ROUND(ABS(fp.team_a_production - fp.team_b_production), 1), ' more points')
                        WHEN (pi.team_a_champion = 1 OR pi.team_b_champion = 1) THEN 
                            CONCAT('Championship impact: ', 
                                CASE WHEN pi.team_a_champion = 1 THEN dma.manager_name ELSE dmb.manager_name END,
                                ' won title')
                        WHEN (pi.team_a_made_playoffs != pi.team_b_made_playoffs) THEN
                            CONCAT('Playoff impact: ',
                                CASE WHEN pi.team_a_made_playoffs = 1 THEN dma.manager_name ELSE dmb.manager_name END,
                                ' made playoffs')
                        ELSE 'Balanced trade with minimal impact'
                    END as trade_analysis,
                    
                    -- Evaluation status
                    cf.evaluation_status,
                    
                    ts.synthetic_trade_group_id as trade_group_id
                    
                FROM trade_synthesis ts
                JOIN edw.dim_league dl ON ts.league_key = dl.league_key
                LEFT JOIN edw.dim_team dta ON ts.team_a_key = dta.team_key
                LEFT JOIN edw.dim_manager dma ON ts.team_a_manager_key = dma.manager_key
                LEFT JOIN edw.dim_team dtb ON ts.team_b_key = dtb.team_key
                LEFT JOIN edw.dim_manager dmb ON ts.team_b_manager_key = dmb.manager_key
                LEFT JOIN trade_players tp ON ts.synthetic_trade_group_id = tp.synthetic_trade_group_id
                LEFT JOIN fantasy_production fp ON ts.synthetic_trade_group_id = fp.synthetic_trade_group_id
                LEFT JOIN opportunity_cost oc ON ts.synthetic_trade_group_id = oc.synthetic_trade_group_id
                LEFT JOIN playoff_impact pi ON ts.synthetic_trade_group_id = pi.synthetic_trade_group_id
                LEFT JOIN contextual_factors cf ON ts.synthetic_trade_group_id = cf.synthetic_trade_group_id
                ORDER BY dl.league_name, ts.transaction_date DESC
            """,
            
            'vw_league_record_book': """
                CREATE VIEW edw.vw_league_record_book AS
                
                WITH all_records AS (
                    -- Most points scored in a season
                    SELECT 
                        'Most Points Scored in a Season' as record_name,
                        CONCAT(ROUND(SUM(ftp.points_for), 1), ' points') as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY SUM(ftp.points_for) DESC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    GROUP BY ftp.team_key, dt.team_name, dt.manager_name, dl.season_year
                    
                    UNION ALL
                    
                    -- Fewest points scored in a season
                    SELECT 
                        'Fewest Points Scored in a Season' as record_name,
                        CONCAT(ROUND(SUM(ftp.points_for), 1), ' points') as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY SUM(ftp.points_for) ASC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    GROUP BY ftp.team_key, dt.team_name, dt.manager_name, dl.season_year
                    
                    UNION ALL
                    
                    -- Most points allowed in a season (unluckiest team)
                    SELECT 
                        'Most Points Allowed in a Season (Unluckiest Team)' as record_name,
                        CONCAT(ROUND(SUM(ftp.points_against), 1), ' points allowed') as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY SUM(ftp.points_against) DESC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    GROUP BY ftp.team_key, dt.team_name, dt.manager_name, dl.season_year
                    
                    UNION ALL
                    
                    -- Fewest points allowed in a season (luckiest team)
                    SELECT 
                        'Fewest Points Allowed in a Season (Luckiest Team)' as record_name,
                        CONCAT(ROUND(SUM(ftp.points_against), 1), ' points allowed') as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY SUM(ftp.points_against) ASC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    GROUP BY ftp.team_key, dt.team_name, dt.manager_name, dl.season_year
                    
                    UNION ALL
                    
                    -- Best regular-season record (win percentage)
                    SELECT 
                        'Best Regular-Season Record' as record_name,
                        CONCAT(
                            SUM(ftp.wins), '-', SUM(ftp.losses), 
                            CASE WHEN SUM(ftp.ties) > 0 THEN CONCAT('-', SUM(ftp.ties)) ELSE '' END,
                            ' (', ROUND(
                                CASE 
                                    WHEN (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) > 0 
                                    THEN (SUM(ftp.wins) + 0.5 * SUM(ftp.ties))::decimal / (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) * 100
                                    ELSE 0
                                END, 1
                            ), '%)'
                        ) as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY 
                            CASE 
                                WHEN (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) > 0 
                                THEN (SUM(ftp.wins) + 0.5 * SUM(ftp.ties))::decimal / (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties))
                                ELSE 0
                            END DESC,
                            SUM(ftp.wins) DESC
                        ) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    GROUP BY ftp.team_key, dt.team_name, dt.manager_name, dl.season_year
                    
                    UNION ALL
                    
                    -- Worst regular-season record (win percentage)
                    SELECT 
                        'Worst Regular-Season Record' as record_name,
                        CONCAT(
                            SUM(ftp.wins), '-', SUM(ftp.losses), 
                            CASE WHEN SUM(ftp.ties) > 0 THEN CONCAT('-', SUM(ftp.ties)) ELSE '' END,
                            ' (', ROUND(
                                CASE 
                                    WHEN (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) > 0 
                                    THEN (SUM(ftp.wins) + 0.5 * SUM(ftp.ties))::decimal / (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) * 100
                                    ELSE 0
                                END, 1
                            ), '%)'
                        ) as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY 
                            CASE 
                                WHEN (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) > 0 
                                THEN (SUM(ftp.wins) + 0.5 * SUM(ftp.ties))::decimal / (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties))
                                ELSE 0
                            END ASC,
                            SUM(ftp.losses) DESC
                        ) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    GROUP BY ftp.team_key, dt.team_name, dt.manager_name, dl.season_year
                    
                    UNION ALL
                    
                    -- Largest point differential in a season
                    SELECT 
                        'Largest Point Differential in a Season' as record_name,
                        CONCAT('+', ROUND(SUM(ftp.point_differential), 1)) as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY SUM(ftp.point_differential) DESC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    GROUP BY ftp.team_key, dt.team_name, dt.manager_name, dl.season_year
                    
                    UNION ALL
                    
                    -- Worst point differential in a season
                    SELECT 
                        'Worst Point Differential in a Season' as record_name,
                        CONCAT(ROUND(SUM(ftp.point_differential), 1)) as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY SUM(ftp.point_differential) ASC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    GROUP BY ftp.team_key, dt.team_name, dt.manager_name, dl.season_year
                    
                    UNION ALL
                    
                    -- Most trades in a season
                    SELECT 
                        'Most Trades in a Season' as record_name,
                        CONCAT(COUNT(*), ' trades') as record_value,
                        NULL as team_name,
                        dm.manager_name,
                        ft.season_year,
                        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
                    FROM edw.fact_transaction ft
                    JOIN edw.dim_manager dm ON (ft.from_manager_key = dm.manager_key OR ft.to_manager_key = dm.manager_key)
                    WHERE ft.transaction_type = 'trade'
                    GROUP BY dm.manager_name, ft.season_year
                    
                    UNION ALL
                    
                    -- Most waiver pickups in a season
                    SELECT 
                        'Most Waiver Pickups in a Season' as record_name,
                        CONCAT(COUNT(*), ' pickups') as record_value,
                        NULL as team_name,
                        dm.manager_name,
                        ft.season_year,
                        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
                    FROM edw.fact_transaction ft
                    JOIN edw.dim_manager dm ON ft.to_manager_key = dm.manager_key
                    WHERE ft.transaction_type IN ('waiver', 'free_agent')
                    GROUP BY dm.manager_name, ft.season_year
                    
                    UNION ALL
                    
                    -- Most FAAB spent in a season
                    SELECT 
                        'Most FAAB Spent in a Season' as record_name,
                        CONCAT('$', ROUND(SUM(ft.faab_bid), 0)) as record_value,
                        NULL as team_name,
                        dm.manager_name,
                        ft.season_year,
                        ROW_NUMBER() OVER (ORDER BY SUM(ft.faab_bid) DESC) as rn
                    FROM edw.fact_transaction ft
                    JOIN edw.dim_manager dm ON ft.to_manager_key = dm.manager_key
                    WHERE ft.transaction_type IN ('waiver', 'free_agent') 
                        AND ft.faab_bid IS NOT NULL 
                        AND ft.faab_bid > 0
                    GROUP BY dm.manager_name, ft.season_year
                    
                    UNION ALL
                    
                    -- Most weeks as top scorer in a season
                    SELECT 
                        'Most Weeks as Top Scorer in a Season' as record_name,
                        CONCAT(COUNT(*), ' weeks') as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    WHERE ftp.weekly_rank = 1
                    GROUP BY ftp.team_key, dt.team_name, dt.manager_name, dl.season_year
                    
                    UNION ALL
                    
                    -- Highest single game score
                    SELECT 
                        'Highest Single Game Score' as record_name,
                        CONCAT(ROUND(ftp.points_for, 1), ' points (Week ', dw.week_number, ')') as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY ftp.points_for DESC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
                    
                    UNION ALL
                    
                    -- Lowest single game score
                    SELECT 
                        'Lowest Single Game Score' as record_name,
                        CONCAT(ROUND(ftp.points_for, 1), ' points (Week ', dw.week_number, ')') as record_value,
                        dt.team_name,
                        dt.manager_name,
                        dl.season_year,
                        ROW_NUMBER() OVER (ORDER BY ftp.points_for ASC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
                    WHERE ftp.points_for > 0
                    
                    UNION ALL
                    
                    -- Biggest blowout (single game)
                    SELECT 
                        'Biggest Blowout (Single Game)' as record_name,
                        CONCAT(ROUND(fm.margin_of_victory, 1), ' point margin (Week ', dw.week_number, ')') as record_value,
                        dt.team_name,
                        dt.manager_name,
                        fm.season_year,
                        ROW_NUMBER() OVER (ORDER BY fm.margin_of_victory DESC) as rn
                    FROM edw.fact_matchup fm
                    JOIN edw.dim_team dt ON fm.winner_team_key = dt.team_key
                    JOIN edw.dim_week dw ON fm.week_key = dw.week_key
                    WHERE fm.margin_of_victory > 0
                    
                    UNION ALL
                    
                    -- Closest win (single game)
                    SELECT 
                        'Closest Win (Single Game)' as record_name,
                        CONCAT(ROUND(fm.margin_of_victory, 1), ' point margin (Week ', dw.week_number, ')') as record_value,
                        dt.team_name,
                        dt.manager_name,
                        fm.season_year,
                        ROW_NUMBER() OVER (ORDER BY fm.margin_of_victory ASC) as rn
                    FROM edw.fact_matchup fm
                    JOIN edw.dim_team dt ON fm.winner_team_key = dt.team_key
                    JOIN edw.dim_week dw ON fm.week_key = dw.week_key
                    WHERE fm.margin_of_victory > 0
                    
                    UNION ALL
                    
                    -- Highest scoring game (combined points)
                    SELECT 
                        'Highest Scoring Game (Combined Points)' as record_name,
                        CONCAT(ROUND(fm.total_points, 1), ' total points (Week ', dw.week_number, ')') as record_value,
                        CONCAT(dt1.team_name, ' vs ', dt2.team_name) as team_name,
                        CONCAT(dt1.manager_name, ' vs ', dt2.manager_name) as manager_name,
                        fm.season_year,
                        ROW_NUMBER() OVER (ORDER BY fm.total_points DESC) as rn
                    FROM edw.fact_matchup fm
                    JOIN edw.dim_team dt1 ON fm.team1_key = dt1.team_key
                    JOIN edw.dim_team dt2 ON fm.team2_key = dt2.team_key
                    JOIN edw.dim_week dw ON fm.week_key = dw.week_key
                    
                    UNION ALL
                    
                    -- Lowest scoring game (combined points)
                    SELECT 
                        'Lowest Scoring Game (Combined Points)' as record_name,
                        CONCAT(ROUND(fm.total_points, 1), ' total points (Week ', dw.week_number, ')') as record_value,
                        CONCAT(dt1.team_name, ' vs ', dt2.team_name) as team_name,
                        CONCAT(dt1.manager_name, ' vs ', dt2.manager_name) as manager_name,
                        fm.season_year,
                        ROW_NUMBER() OVER (ORDER BY fm.total_points ASC) as rn
                    FROM edw.fact_matchup fm
                    JOIN edw.dim_team dt1 ON fm.team1_key = dt1.team_key
                    JOIN edw.dim_team dt2 ON fm.team2_key = dt2.team_key
                    JOIN edw.dim_week dw ON fm.week_key = dw.week_key
                    WHERE fm.total_points > 0
                    
                    UNION ALL
                    
                    -- Most championships (career)
                    SELECT 
                        'Most Championships (Career)' as record_name,
                        CONCAT(championships_won, ' championships') as record_value,
                        NULL as team_name,
                        manager_name,
                        NULL as season_year,
                        ROW_NUMBER() OVER (ORDER BY championships_won DESC, career_win_percentage DESC) as rn
                    FROM edw.mart_manager_performance
                    
                    UNION ALL
                    
                    -- Most championship appearances (career)
                    SELECT 
                        'Most Championship Appearances (Career)' as record_name,
                        CONCAT(championship_appearances, ' appearances') as record_value,
                        NULL as team_name,
                        manager_name,
                        NULL as season_year,
                        ROW_NUMBER() OVER (ORDER BY championship_appearances DESC, championships_won DESC) as rn
                    FROM edw.mart_manager_performance
                    
                    UNION ALL
                    
                    -- Most playoff appearances (career)
                    SELECT 
                        'Most Playoff Appearances (Career)' as record_name,
                        CONCAT(playoff_appearances, ' appearances') as record_value,
                        NULL as team_name,
                        manager_name,
                        NULL as season_year,
                        ROW_NUMBER() OVER (ORDER BY playoff_appearances DESC, championships_won DESC) as rn
                    FROM edw.mart_manager_performance
                    
                    UNION ALL
                    
                    -- Best career win percentage (min 3 seasons)
                    SELECT 
                        'Best Career Win Percentage (Min 3 Seasons)' as record_name,
                        CONCAT(ROUND(career_win_percentage * 100, 1), '%') as record_value,
                        NULL as team_name,
                        manager_name,
                        NULL as season_year,
                        ROW_NUMBER() OVER (ORDER BY career_win_percentage DESC, total_wins DESC) as rn
                    FROM edw.mart_manager_performance
                    WHERE total_seasons >= 3
                    
                    UNION ALL
                    
                    -- Most career points
                    SELECT 
                        'Most Career Points' as record_name,
                        CONCAT(ROUND(total_points_scored, 1), ' points') as record_value,
                        NULL as team_name,
                        manager_name,
                        NULL as season_year,
                        ROW_NUMBER() OVER (ORDER BY total_points_scored DESC) as rn
                    FROM edw.mart_manager_performance
                    
                    UNION ALL
                    
                    -- Most career points allowed (unluckiest manager)
                    SELECT 
                        'Most Career Points Allowed (Unluckiest Manager)' as record_name,
                        CONCAT(ROUND(SUM(ftp.points_against), 1), ' points allowed') as record_value,
                        NULL as team_name,
                        dt.manager_name,
                        NULL as season_year,
                        ROW_NUMBER() OVER (ORDER BY SUM(ftp.points_against) DESC) as rn
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    GROUP BY dt.manager_name
                    
                    UNION ALL
                    
                    -- Most career transactions
                    SELECT 
                        'Most Career Transactions' as record_name,
                        CONCAT(total_transactions, ' transactions') as record_value,
                        NULL as team_name,
                        manager_name,
                        NULL as season_year,
                        ROW_NUMBER() OVER (ORDER BY total_transactions DESC) as rn
                    FROM edw.mart_manager_performance
                )
                SELECT 
                    record_name,
                    record_value,
                    team_name,
                    manager_name,
                    season_year
                FROM all_records
                WHERE rn = 1
                ORDER BY 
                    CASE 
                        WHEN record_name LIKE '%Season%' OR record_name LIKE '%Weeks%' THEN 1
                        WHEN record_name LIKE '%Single%' OR record_name LIKE '%Game%' OR record_name LIKE '%Blowout%' OR record_name LIKE '%Combined%' OR record_name LIKE '%Closest%' THEN 2
                        ELSE 3
                    END,
                    record_name
            """
        }
        
        return view_definitions.get(view_name, "")

    def log_view_refresh_summary(self):
        """Log summary of view refresh operations"""
        logger.info("\n🔧 VIEW REFRESH SUMMARY:")
        logger.info(f"  📊 Views Checked: {self.view_refresh_stats['views_checked']}")
        logger.info(f"  ✅ Views Refreshed: {self.view_refresh_stats['views_refreshed']}")
        logger.info(f"  ⏭️ Views Skipped: {self.view_refresh_stats['views_skipped']}")
        if self.view_refresh_stats['views_failed'] > 0:
            logger.warning(f"  ❌ Views Failed: {self.view_refresh_stats['views_failed']}")
        
        if self.view_refresh_stats['views_refreshed'] > 0:
            logger.info("  🎯 Smart refresh successful - only affected views were updated")
        elif self.view_refresh_stats['views_skipped'] > 0:
            logger.info("  ⚡ No refresh needed - all views are current with latest data")

    def consolidate_manager_name(self, manager_name: str) -> str:
        """Consolidate duplicate manager names to canonical names"""
        # Manager name consolidation mapping
        name_mapping = {
            # Cody variations
            'Cody': 'Cody Benbow',
            'Cody Benbow': 'Cody Benbow',
            
            # Craig/Crow
            'Crow': 'Craig',
            'Craig': 'Craig',
            
            # Erik variations  
            'Erik': 'Erik Snow',
            'Erik Snow': 'Erik Snow',
            
            # Israel variations
            'IsraelF': 'Israel',
            'Israel': 'Israel',
            
            # Jesse variations
            'Jesse': 'Jesse Brown',
            'Jesse Brown': 'Jesse Brown',
            
            # Trevor variations
            'Trevor': 'Trevor',
            'Trevor Cramer': 'Trevor',
            
            # Gabriel/Gabe variations
            'gabriel': 'Gabe the Younger',
            'Gabe the Younger': 'Gabe the Younger'
        }
        
        return name_mapping.get(manager_name, manager_name)
    
    def get_manager_name_by_team_id(self, team_id: str, original_manager_name: str) -> str:
        """Get correct manager name based on team_id overrides, then apply consolidation"""
        # Bobby's team IDs (override hidden manager names)
        bobby_team_ids = {
            "124.l.109785.t.3",
            "273.l.107980.t.9", 
            "314.l.319572.t.5",
            "348.l.655822.t.2",
            "359.l.696366.t.8",
            "380.l.1143665.t.8",
            "199.l.42364.t.3",
            "222.l.222935.t.6",
            "257.l.89145.t.4",
            "199.l.42364.t.9"
        }
        
        # Override manager name for Bobby's teams
        if team_id in bobby_team_ids:
            return 'Bobby'
        
        # Apply normal consolidation for other teams
        return self.consolidate_manager_name(original_manager_name)

    def transform_managers(self) -> List[Dict]:
        """Transform manager data for dim_manager table - only from leagues of record with name consolidation"""
        managers = []
        
        if not self.data:
            logger.warning("⚠️ No data available for manager transformation")
            return managers
        
        # Build set of league of record IDs for efficient lookup (same pattern as other transforms)
        league_of_record_ids = set()
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if self.is_league_of_record(league['league_id'], season_year):
                league_of_record_ids.add(league['league_id'])
        
        logger.info(f"🔍 Filtering teams to {len(league_of_record_ids)} leagues of record")
        
        # Build league lookup for efficient season retrieval
        league_lookup = {}
        for league in self.data.get('leagues', []):
            league_lookup[league['league_id']] = league
        
        # Extract unique canonical managers from teams data - only from leagues of record with name consolidation
        raw_managers = set()
        canonical_managers = set()
        total_teams = 0
        filtered_teams = 0
        
        for team in self.data.get('teams', []):
            total_teams += 1
            # Only include teams from leagues of record (same pattern as transform_teams)
            if team['league_id'] not in league_of_record_ids:
                continue
            
            filtered_teams += 1    
            manager_name = team.get('manager_name')
            team_id = team['team_id']
            if manager_name and manager_name.strip():
                raw_name = manager_name.strip()
                canonical_name = self.get_manager_name_by_team_id(team_id, raw_name)
                raw_managers.add(raw_name)
                canonical_managers.add(canonical_name)  # Only canonical names in final set
        
        logger.info(f"👤 Processed {total_teams} total teams, kept {filtered_teams} from leagues of record")
        logger.info(f"👤 Found {len(raw_managers)} raw manager names, consolidated to {len(canonical_managers)} unique managers")
        
        # Create manager records - aggregate across all name variations
        for canonical_name in sorted(canonical_managers):
            # Calculate season ranges for this manager across all name variations - only from leagues of record
            manager_seasons = []
            manager_leagues = set()
            name_variations_found = []
            manager_data = {}  # Store the best manager data found
            
            for team in self.data.get('teams', []):
                # Only include teams from leagues of record (same filtering as above)
                if team['league_id'] not in league_of_record_ids:
                    continue
                    
                raw_team_name = team.get('manager_name', '').strip()
                team_id = team['team_id']
                if raw_team_name and self.get_manager_name_by_team_id(team_id, raw_team_name) == canonical_name:
                    # Track which name variations we found for this canonical manager
                    if raw_team_name not in name_variations_found:
                        name_variations_found.append(raw_team_name)
                    
                    # Collect manager data (prefer most recent or most complete)
                    if team.get('managers'):
                        # We have manager detail data - extract it
                        managers_list = team.get('managers', [])
                        if managers_list and len(managers_list) > 0:
                            manager_info = managers_list[0].get('manager', {})
                            if manager_info.get('manager_id') and not manager_data.get('manager_id'):
                                manager_data['manager_id'] = manager_info.get('manager_id')
                            if manager_info.get('email') and not manager_data.get('email'):
                                manager_data['email'] = manager_info.get('email')
                            if manager_info.get('nickname') and not manager_data.get('display_name'):
                                manager_data['display_name'] = manager_info.get('nickname')
                            if manager_info.get('image_url') and not manager_data.get('profile_image_url'):
                                manager_data['profile_image_url'] = manager_info.get('image_url')
                    
                    # Get season year from league lookup (teams don't have season_year field)
                    league_id = team.get('league_id')
                    league = league_lookup.get(league_id)
                    if league:
                        season_str = league.get('season')
                        if season_str:
                            try:
                                season_year = int(season_str)
                                manager_seasons.append(season_year)
                                manager_leagues.add(league_id)
                            except (ValueError, TypeError):
                                logger.warning(f"⚠️ Invalid season value '{season_str}' for league {league_id}")
                        else:
                            logger.warning(f"⚠️ No season field for league {league_id}")
                    else:
                        logger.warning(f"⚠️ League {league_id} not found in lookup")
            
            # Calculate stats
            first_season = min(manager_seasons) if manager_seasons else None
            last_season = max(manager_seasons) if manager_seasons else None
            total_seasons = len(set(manager_seasons)) if manager_seasons else 0
            total_leagues = len(manager_leagues)
            
            # Log consolidation details if multiple name variations found
            if len(name_variations_found) > 1:
                logger.info(f"🔄 Consolidated '{canonical_name}' from variations: {name_variations_found}")
            
            # Generate consistent manager_id from canonical name for usability
            # Convert canonical name to a stable, URL-friendly manager ID
            manager_id = canonical_name.lower().replace(' ', '_').replace("'", '').replace('.', '').replace('-', '_')
            
            # Set is_current and include_in_analysis based on specified criteria
            current_manager_ids = {
                'craig', 'erik_snow', 'gabe_flores', 'gabe_the_younger', 
                'israel', 'luke_s', 'nick', 'omar', 'trevor', 'troy_colvin'
            }
            
            analysis_manager_ids = current_manager_ids | {'bobby'}  # All current managers plus Bobby
            
            is_current = manager_id in current_manager_ids
            include_in_analysis = manager_id in analysis_manager_ids
            
            # Use collected manager data or reasonable defaults
            manager_record = {
                'manager_name': canonical_name,  # Use canonical name
                'manager_id': manager_id,  # Generated stable manager ID
                'first_season_year': first_season,
                'last_season_year': last_season,
                'total_seasons': total_seasons,
                'total_leagues': total_leagues,
                'is_current': is_current,  # Based on specified current managers
                'include_in_analysis': include_in_analysis,  # Current managers + Bobby
                'email': manager_data.get('email'),  # Email if available
                'display_name': manager_data.get('display_name', canonical_name),  # Prefer nickname, fallback to canonical name
                'profile_image_url': manager_data.get('profile_image_url'),  # Profile image if available
                'is_active': True
            }
            
            managers.append(manager_record)
        
        return managers

    def transform_mart_league_summary(self) -> List[Dict]:
        """Transform data for mart_league_summary - matches current table schema"""
        marts = []
        
        with self.engine.connect() as conn:
            logger.info("🏪 Calculating league summary statistics...")
            
            # Query matching the actual mart_league_summary schema
            sql = """
            WITH league_basics AS (
                SELECT 
                    dl.league_key,
                    dl.league_name,
                    dl.season_year,
                    dl.num_teams as total_teams,
                    COUNT(DISTINCT dw.week_number) as total_weeks
                FROM edw.dim_league dl
                LEFT JOIN edw.fact_matchup fm ON dl.league_key = fm.league_key 
                    AND dl.season_year = fm.season_year
                LEFT JOIN edw.dim_week dw ON fm.week_key = dw.week_key
                WHERE dl.is_active = TRUE
                GROUP BY dl.league_key, dl.league_name, dl.season_year, dl.num_teams
            ),
            championship_results AS (
                SELECT DISTINCT ON (dl.league_key)
                    dl.league_key,
                    -- Champion info (winner of championship game)
                    champ_team.team_name as champion_team_name,
                    champ_mgr.manager_name as champion_manager,
                    -- Runner-up info (loser of championship game)
                    runner_team.team_name as runner_up_team_name,
                    runner_mgr.manager_name as runner_up_manager
                FROM edw.dim_league dl
                LEFT JOIN edw.fact_matchup fm_champ ON dl.league_key = fm_champ.league_key 
                    AND dl.season_year = fm_champ.season_year 
                    AND fm_champ.is_championship = TRUE
                -- Champion team and manager
                LEFT JOIN edw.dim_team champ_team ON fm_champ.winner_team_key = champ_team.team_key
                LEFT JOIN edw.dim_manager champ_mgr ON champ_team.manager_name = champ_mgr.manager_name
                -- Runner-up team and manager (loser of championship)
                LEFT JOIN edw.dim_team runner_team ON (
                    CASE WHEN fm_champ.winner_team_key = fm_champ.team1_key 
                         THEN fm_champ.team2_key 
                         ELSE fm_champ.team1_key END = runner_team.team_key
                )
                LEFT JOIN edw.dim_manager runner_mgr ON runner_team.manager_name = runner_mgr.manager_name
                WHERE dl.is_active = TRUE
                ORDER BY dl.league_key, fm_champ.week_key DESC
            ),
            scoring_leaders AS (
                SELECT 
                    dl.league_key,
                    -- Highest single week score
                    MAX(ftp.points_for) as highest_single_week_score,
                    -- Average weekly score across all teams/weeks
                    ROUND(AVG(ftp.points_for), 2) as average_weekly_score
                FROM edw.dim_league dl
                JOIN edw.fact_team_performance ftp ON dl.league_key = ftp.league_key 
                    AND dl.season_year = ftp.season_year
                JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                JOIN edw.dim_manager dm ON dt.manager_name = dm.manager_name
                WHERE dl.is_active = TRUE AND dm.include_in_analysis = TRUE
                GROUP BY dl.league_key
            ),
            highest_scorer_info AS (
                SELECT DISTINCT
                    dl.league_key,
                    FIRST_VALUE(dt.team_name) OVER (
                        PARTITION BY dl.league_key 
                        ORDER BY ftp.points_for DESC
                    ) as highest_scorer_team,
                    FIRST_VALUE(dm.manager_name) OVER (
                        PARTITION BY dl.league_key 
                        ORDER BY ftp.points_for DESC
                    ) as highest_scorer_manager
                FROM edw.dim_league dl
                JOIN edw.fact_team_performance ftp ON dl.league_key = ftp.league_key 
                    AND dl.season_year = ftp.season_year
                JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                JOIN edw.dim_manager dm ON dt.manager_name = dm.manager_name
                WHERE dl.is_active = TRUE AND dm.include_in_analysis = TRUE
            ),
            transaction_activity AS (
                SELECT 
                    dl.league_key,
                    COUNT(ft.transaction_key) as total_transactions,
                    -- Most active trader (manager with most transactions)
                    (SELECT dm.manager_name 
                     FROM edw.fact_transaction ft2
                     JOIN edw.dim_manager dm ON ft2.to_manager_key = dm.manager_key
                     WHERE ft2.league_key = dl.league_key AND ft2.season_year = dl.season_year
                       AND dm.include_in_analysis = TRUE
                     GROUP BY dm.manager_name
                     ORDER BY COUNT(ft2.transaction_key) DESC
                     LIMIT 1) as most_active_trader
                FROM edw.dim_league dl
                LEFT JOIN edw.fact_transaction ft ON dl.league_key = ft.league_key 
                    AND dl.season_year = ft.season_year
                WHERE dl.is_active = TRUE
                GROUP BY dl.league_key, dl.season_year
            )
            SELECT 
                lb.league_key,
                lb.league_name,
                lb.season_year,
                lb.total_teams,
                lb.total_weeks,
                cr.champion_team_name,
                cr.champion_manager,
                cr.runner_up_team_name,
                cr.runner_up_manager,
                hsi.highest_scorer_team,
                hsi.highest_scorer_manager,
                sl.highest_single_week_score,
                sl.average_weekly_score,
                COALESCE(ta.total_transactions, 0) as total_transactions,
                ta.most_active_trader
            FROM league_basics lb
            LEFT JOIN championship_results cr ON lb.league_key = cr.league_key
            LEFT JOIN scoring_leaders sl ON lb.league_key = sl.league_key
            LEFT JOIN highest_scorer_info hsi ON lb.league_key = hsi.league_key
            LEFT JOIN transaction_activity ta ON lb.league_key = ta.league_key
            ORDER BY lb.season_year DESC, lb.league_name
            """
            
            result = conn.execute(text(sql))
            rows = result.fetchall()
            
            for row in rows:
                marts.append({
                    'league_key': row[0],
                    'league_name': row[1],
                    'season_year': row[2],
                    'total_teams': row[3],
                    'total_weeks': row[4],
                    'champion_team_name': row[5],
                    'champion_manager': row[6],
                    'runner_up_team_name': row[7],
                    'runner_up_manager': row[8],
                    'highest_scorer_team': row[9],
                    'highest_scorer_manager': row[10],
                    'highest_single_week_score': row[11],
                    'average_weekly_score': row[12],
                    'total_transactions': row[13],
                    'most_active_trader': row[14]
                })
        
        logger.info(f"🏪 Generated {len(marts)} league summary records")
        return marts

    def transform_mart_manager_performance(self) -> List[Dict]:
        """Transform data for mart_manager_performance"""
        marts = []
        
        with self.engine.connect() as conn:
            logger.info("🏪 Calculating manager performance metrics...")
            
            # Comprehensive manager performance query with all metrics
            sql = """
            WITH manager_stats AS (
                SELECT 
                    dt.manager_name,
                    dl.season_year,
                    SUM(CASE WHEN fm.winner_team_key = dt.team_key THEN 1 ELSE 0 END) as season_wins,
                    SUM(CASE WHEN fm.winner_team_key != dt.team_key AND fm.is_tie = FALSE THEN 1 ELSE 0 END) as season_losses,
                    SUM(CASE WHEN fm.is_tie = TRUE THEN 1 ELSE 0 END) as season_ties,
                    SUM(CASE WHEN fm.team1_key = dt.team_key THEN fm.team1_points 
                             WHEN fm.team2_key = dt.team_key THEN fm.team2_points 
                             ELSE 0 END) as season_points,
                    COUNT(DISTINCT fm.matchup_key) as games_played,
                    SUM(CASE WHEN fm.matchup_type = 'championship' AND fm.winner_team_key = dt.team_key THEN 1 ELSE 0 END) as championships,
                    SUM(CASE WHEN fm.matchup_type = 'championship' THEN 1 ELSE 0 END) as championship_games,
                    SUM(CASE WHEN fm.matchup_type IN ('playoffs', 'championship') AND fm.winner_team_key = dt.team_key THEN 1 ELSE 0 END) as playoff_wins,
                    SUM(CASE WHEN fm.matchup_type IN ('playoffs', 'championship') AND fm.winner_team_key != dt.team_key AND fm.is_tie = FALSE THEN 1 ELSE 0 END) as playoff_losses
                FROM edw.dim_team dt
                JOIN edw.dim_league dl ON dt.league_key = dl.league_key
                JOIN edw.dim_manager dm ON dt.manager_name = dm.manager_name
                LEFT JOIN edw.fact_matchup fm ON (fm.team1_key = dt.team_key OR fm.team2_key = dt.team_key)
                    AND fm.season_year = dl.season_year
                WHERE dt.is_active = TRUE AND dm.include_in_analysis = TRUE
                GROUP BY dt.manager_name, dl.season_year
            ),
            playoff_stats AS (
                SELECT 
                    dt.manager_name,
                    COUNT(DISTINCT CASE WHEN ftp.is_playoff_team = TRUE THEN dt.league_key END) as playoff_seasons
                FROM edw.dim_team dt
                JOIN edw.dim_league dl ON dt.league_key = dl.league_key
                JOIN edw.dim_manager dm ON dt.manager_name = dm.manager_name
                JOIN edw.fact_team_performance ftp ON dt.team_key = ftp.team_key 
                    AND dl.season_year = ftp.season_year
                WHERE dt.is_active = TRUE AND dm.include_in_analysis = TRUE
                GROUP BY dt.manager_name
            ),
            transaction_stats AS (
                SELECT 
                    dm.manager_name,
                    COUNT(ft.transaction_key) as total_transactions,
                    AVG(ft.faab_bid) as avg_faab_bid,
                    SUM(ft.faab_bid) as total_faab_spent
                FROM edw.dim_manager dm
                JOIN edw.fact_transaction ft ON dm.manager_key = ft.to_manager_key
                WHERE dm.is_active = TRUE AND dm.include_in_analysis = TRUE
                GROUP BY dm.manager_name
            ),
            draft_stats AS (
                SELECT 
                    dm.manager_name,
                    COUNT(fd.draft_key) as total_drafts,
                    AVG(fd.overall_pick) as avg_draft_position
                FROM edw.dim_manager dm
                JOIN edw.fact_draft fd ON dm.manager_key = fd.manager_key
                WHERE dm.is_active = TRUE AND dm.include_in_analysis = TRUE
                GROUP BY dm.manager_name
            ),
            manager_aggregates AS (
                SELECT 
                    ms.manager_name,
                    COUNT(DISTINCT ms.season_year) as total_seasons,
                    (SELECT COUNT(DISTINCT dt2.league_key) 
                     FROM edw.dim_team dt2 
                     JOIN edw.dim_manager dm2 ON dt2.manager_name = dm2.manager_name
                     WHERE dt2.manager_name = ms.manager_name 
                       AND dt2.is_active = TRUE 
                       AND dm2.include_in_analysis = TRUE) as total_leagues,
                    MIN(ms.season_year) as first_season,
                    MAX(ms.season_year) as last_season,
                    SUM(ms.season_wins) as total_wins,
                    SUM(ms.season_losses) as total_losses,
                    SUM(ms.season_ties) as total_ties,
                    SUM(ms.season_points) as total_points_scored,
                    SUM(ms.games_played) as total_games,
                    AVG(ms.season_points) as avg_points_per_season,
                    SUM(ms.championships) as championships_won,
                    SUM(ms.championship_games) as championship_appearances,
                    COALESCE(ps.playoff_seasons, 0) as playoff_appearances,
                    SUM(ms.playoff_wins) as total_playoff_wins,
                    SUM(ms.playoff_losses) as total_playoff_losses,
                    STDDEV(ms.season_wins::decimal / NULLIF(ms.games_played, 0)) as win_consistency,
                    (SELECT fd.season_year 
                     FROM edw.fact_draft fd 
                     JOIN edw.dim_manager dm_sub ON fd.manager_key = dm_sub.manager_key
                     WHERE dm_sub.manager_name = ms.manager_name
                     ORDER BY fd.season_points DESC 
                     LIMIT 1) as best_draft_year,
                    (SELECT fd.season_year 
                     FROM edw.fact_draft fd 
                     JOIN edw.dim_manager dm_sub ON fd.manager_key = dm_sub.manager_key
                     WHERE dm_sub.manager_name = ms.manager_name
                     ORDER BY fd.season_points ASC 
                     LIMIT 1) as worst_draft_year,
                    (SELECT CONCAT(ms2.season_wins, '-', ms2.season_losses, '-', ms2.season_ties) 
                     FROM manager_stats ms2 
                     WHERE ms2.manager_name = ms.manager_name 
                     ORDER BY (ms2.season_wins::decimal / NULLIF(ms2.games_played, 0)) DESC 
                     LIMIT 1) as best_season_record,
                    (SELECT CONCAT(ms2.season_wins, '-', ms2.season_losses, '-', ms2.season_ties) 
                     FROM manager_stats ms2 
                     WHERE ms2.manager_name = ms.manager_name 
                     ORDER BY (ms2.season_wins::decimal / NULLIF(ms2.games_played, 0)) ASC 
                     LIMIT 1) as worst_season_record
                FROM manager_stats ms
                LEFT JOIN playoff_stats ps ON ms.manager_name = ps.manager_name
                WHERE ms.games_played > 0
                GROUP BY ms.manager_name, ps.playoff_seasons
            )
            SELECT 
                ma.manager_name,
                ma.first_season,
                ma.last_season,
                ma.total_seasons,
                ma.total_leagues,
                ma.total_wins,
                ma.total_losses,
                ma.total_ties,
                CASE 
                    WHEN (ma.total_wins + ma.total_losses + ma.total_ties) > 0 
                    THEN ROUND(ma.total_wins::decimal / (ma.total_wins + ma.total_losses + ma.total_ties), 4)
                    ELSE 0
                END as career_win_percentage,
                ROUND(ma.total_points_scored, 2) as total_points_scored,
                CASE 
                    WHEN ma.total_games > 0 
                    THEN ROUND(ma.total_points_scored / ma.total_games, 2)
                    ELSE 0
                END as avg_points_per_game,
                ROUND(ma.avg_points_per_season, 2) as avg_points_per_season,
                ma.championships_won,
                ma.championship_appearances,
                ma.playoff_appearances,
                CASE 
                    WHEN (ma.total_playoff_wins + ma.total_playoff_losses) > 0 
                    THEN ROUND(ma.total_playoff_wins::decimal / (ma.total_playoff_wins + ma.total_playoff_losses), 4)
                    ELSE 0
                END as playoff_win_percentage,
                COALESCE(ds.avg_draft_position, 0) as avg_draft_grade,
                ma.best_draft_year,
                ma.worst_draft_year,
                COALESCE(ts.total_transactions, 0) as total_transactions,
                CASE 
                    WHEN ma.total_seasons > 0 
                    THEN ROUND(COALESCE(ts.total_transactions, 0)::decimal / ma.total_seasons, 2)
                    ELSE 0
                END as avg_transactions_per_season,
                CASE 
                    WHEN COALESCE(ts.total_faab_spent, 0) > 0 
                    THEN ROUND(ma.total_points_scored / ts.total_faab_spent, 4)
                    ELSE 0
                END as faab_efficiency_rating,
                COALESCE(ROUND(ma.win_consistency, 4), 0) as season_consistency_score,
                ma.best_season_record,
                ma.worst_season_record
            FROM manager_aggregates ma
            LEFT JOIN transaction_stats ts ON ma.manager_name = ts.manager_name
            LEFT JOIN draft_stats ds ON ma.manager_name = ds.manager_name
            WHERE ma.total_seasons >= 1
            ORDER BY ma.total_seasons DESC, career_win_percentage DESC
            """
            
            result = conn.execute(text(sql))
            rows = result.fetchall()
            
            for row in rows:
                # Generate a unique manager_id from manager_name
                manager_id = row[0].lower().replace(' ', '_').replace("'", '').replace('.', '')
                
                # Map all columns from the comprehensive query
                marts.append({
                    'manager_id': manager_id,
                    'manager_name': row[0],
                    'first_season': row[1],
                    'last_season': row[2], 
                    'total_seasons': row[3],
                    'total_leagues': row[4],
                    'total_wins': row[5],
                    'total_losses': row[6],
                    'total_ties': row[7],
                    'career_win_percentage': row[8],
                    'total_points_scored': row[9],
                    'avg_points_per_game': row[10],
                    'avg_points_per_season': row[11],
                    'championships_won': row[12],
                    'championship_appearances': row[13],
                    'playoff_appearances': row[14],
                    'playoff_win_percentage': row[15],
                    'avg_draft_grade': row[16],
                    'best_draft_year': row[17],
                    'worst_draft_year': row[18],
                    'total_transactions': row[19],
                    'avg_transactions_per_season': row[20],
                    'faab_efficiency_rating': row[21],
                    'season_consistency_score': row[22],
                    'best_season_record': row[23],
                    'worst_season_record': row[24]
                })
        
        logger.info(f"🏪 Generated {len(marts)} manager performance records")
        return marts

    def transform_mart_player_value(self) -> List[Dict]:
        """Transform data for mart_player_value"""
        marts = []
        
        with self.engine.connect() as conn:
            logger.info("🏪 Calculating player value metrics...")
            
            # Comprehensive player value analysis with fixed PostgreSQL syntax
            sql = """
            WITH             player_draft_stats AS (
                SELECT 
                    fd.player_key,
                    fd.season_year,
                    COUNT(*) as times_drafted_season,
                    AVG(fd.overall_pick) as avg_draft_position_season,
                    MIN(fd.overall_pick) as earliest_draft_pick_season,
                    MAX(fd.overall_pick) as latest_draft_pick_season,
                    AVG(fd.draft_cost) as avg_auction_value_season,
                    COUNT(DISTINCT fd.team_key) as teams_drafted_by_season
                FROM edw.fact_draft fd
                WHERE fd.overall_pick > 0
                GROUP BY fd.player_key, fd.season_year
            ),
            player_transaction_stats AS (
                SELECT 
                    ft.player_key,
                    ft.season_year,
                    COUNT(CASE WHEN ft.transaction_type = 'add' THEN 1 END) as waiver_pickups_season,
                    COUNT(CASE WHEN ft.transaction_type = 'trade' THEN 1 END) as trades_season,
                    AVG(CASE WHEN ft.faab_bid > 0 THEN ft.faab_bid END) as avg_waiver_cost_season,
                    COUNT(DISTINCT ft.to_team_key) as teams_acquired_by_season
                FROM edw.fact_transaction ft
                WHERE ft.transaction_type IN ('add', 'trade', 'drop')
                GROUP BY ft.player_key, ft.season_year
            ),
            player_career_stats AS (
                SELECT 
                    player_key,
                    COUNT(DISTINCT season_year) as total_seasons_drafted,
                    SUM(times_drafted_season) as career_times_drafted,
                    -- Career-wide draft position metrics across ALL seasons
                    AVG(avg_draft_position_season) as career_avg_draft_position,
                    MIN(earliest_draft_pick_season) as career_earliest_pick,
                    MAX(latest_draft_pick_season) as career_latest_pick,
                    AVG(avg_auction_value_season) as career_avg_auction_value
                FROM player_draft_stats
                GROUP BY player_key
            ),
            player_career_transactions AS (
                SELECT 
                    player_key,
                    COUNT(DISTINCT season_year) as total_seasons_transacted,
                    SUM(waiver_pickups_season) as career_waiver_pickups,
                    SUM(trades_season) as career_trades,
                    AVG(avg_waiver_cost_season) as career_avg_waiver_cost
                FROM player_transaction_stats
                GROUP BY player_key
            ),
            player_fantasy_performance AS (
                SELECT 
                    fps.player_key,
                    fps.season_year,
                    SUM(fps.weekly_fantasy_points) as total_fantasy_points,
                    COUNT(CASE WHEN fps.weekly_fantasy_points > 0 THEN 1 END) as games_played,
                    AVG(CASE WHEN fps.weekly_fantasy_points > 0 THEN fps.weekly_fantasy_points END) as avg_points_per_game,
                    MAX(fps.weekly_fantasy_points) as best_weekly_score,
                    MIN(CASE WHEN fps.weekly_fantasy_points > 0 THEN fps.weekly_fantasy_points END) as worst_weekly_score,
                    COALESCE(STDDEV(CASE WHEN fps.weekly_fantasy_points > 0 THEN fps.weekly_fantasy_points END), 0) as consistency_rating
                FROM edw.fact_player_statistics fps
                GROUP BY fps.player_key, fps.season_year
            ),
            player_roster_stats AS (
                SELECT 
                    fr.player_key,
                    fr.season_year,
                    COUNT(DISTINCT fr.team_key) as teams_rostered,
                    COUNT(DISTINCT fr.league_key) as leagues_played,
                    COUNT(CASE WHEN fr.is_starter = TRUE THEN 1 END) as weeks_as_starter,
                    COUNT(*) as total_roster_weeks,
                    COUNT(CASE WHEN fr.is_starter = TRUE THEN 1 END)::decimal / NULLIF(COUNT(*), 0) as starter_percentage,
                    -- Calculate ownership percentage within each league, then average across leagues
                    AVG(team_count_in_league::decimal / total_teams_in_league) as avg_ownership_percentage
                FROM edw.fact_roster fr
                JOIN (
                    -- Get team counts per league for ownership calculation
                    SELECT 
                        fr2.player_key,
                        fr2.season_year,
                        fr2.league_key,
                        COUNT(DISTINCT fr2.team_key) as team_count_in_league,
                        (SELECT COUNT(DISTINCT dt.team_key) 
                         FROM edw.dim_team dt 
                         WHERE dt.league_key = fr2.league_key 
                           AND dt.is_active = TRUE) as total_teams_in_league
                    FROM edw.fact_roster fr2
                    GROUP BY fr2.player_key, fr2.season_year, fr2.league_key
                ) league_ownership ON fr.player_key = league_ownership.player_key 
                                   AND fr.season_year = league_ownership.season_year 
                                   AND fr.league_key = league_ownership.league_key
                GROUP BY fr.player_key, fr.season_year
            ),
            player_season_combined AS (
                SELECT 
                    COALESCE(pds.player_key, pts.player_key, prs.player_key) as player_key,
                    COALESCE(pds.season_year, pts.season_year, prs.season_year) as season_year,
                    COALESCE(pds.times_drafted_season, 0) as times_drafted,
                    pds.avg_draft_position_season as avg_draft_position,
                    pds.earliest_draft_pick_season as earliest_draft_pick,
                    pds.latest_draft_pick_season as latest_draft_pick,
                    COALESCE(pds.avg_auction_value_season, 0) as avg_auction_value,
                    COALESCE(prs.teams_rostered, pds.teams_drafted_by_season, pts.teams_acquired_by_season, 0) as total_teams_rostered,
                    COALESCE(pts.waiver_pickups_season, 0) as waiver_pickups,
                    COALESCE(pts.trades_season, 0) as trades,
                    COALESCE(pts.avg_waiver_cost_season, 0) as avg_waiver_cost,
                    COALESCE(prs.leagues_played, 1) as total_leagues_played,
                    COALESCE(prs.weeks_as_starter, 0) as weeks_as_starter,
                    COALESCE(prs.starter_percentage, 0) as starter_percentage,
                    COALESCE(prs.avg_ownership_percentage, 0) as avg_ownership_percentage
                FROM player_draft_stats pds
                FULL OUTER JOIN player_transaction_stats pts ON pds.player_key = pts.player_key AND pds.season_year = pts.season_year
                FULL OUTER JOIN player_roster_stats prs ON COALESCE(pds.player_key, pts.player_key) = prs.player_key 
                                                        AND COALESCE(pds.season_year, pts.season_year) = prs.season_year
                WHERE COALESCE(pds.times_drafted_season, 0) > 0 
                   OR COALESCE(pts.waiver_pickups_season, 0) > 0 
                   OR COALESCE(prs.teams_rostered, 0) > 0
            )
            SELECT 
                psc.player_key,
                psc.season_year,
                COALESCE(pcs.total_seasons_drafted, pct.total_seasons_transacted, 1) as total_seasons_rostered,
                psc.total_teams_rostered,
                psc.total_leagues_played,
                psc.times_drafted,
                COALESCE(pcs.career_avg_draft_position, psc.avg_draft_position) as career_avg_draft_position,
                COALESCE(pcs.career_earliest_pick, psc.earliest_draft_pick) as career_earliest_draft_pick,
                COALESCE(pcs.career_latest_pick, psc.latest_draft_pick) as career_latest_draft_pick,
                psc.avg_auction_value,
                COALESCE(fps.total_fantasy_points, 0.0) as total_fantasy_points,
                COALESCE(fps.avg_points_per_game, 0.0) as avg_points_per_game,
                COALESCE(fps.best_weekly_score, 0.0) as best_weekly_score,
                COALESCE(fps.worst_weekly_score, 0.0) as worst_weekly_score,
                COALESCE(fps.consistency_rating, 0.0) as consistency_rating,
                psc.avg_ownership_percentage,
                psc.weeks_as_starter,
                psc.starter_percentage,
                -- Calculate points above replacement using actual performance vs career draft position
                CASE 
                    WHEN COALESCE(pcs.career_avg_draft_position, psc.avg_draft_position) > 0 AND fps.total_fantasy_points > 0
                    THEN ROUND(fps.total_fantasy_points - (200 - COALESCE(pcs.career_avg_draft_position, psc.avg_draft_position)) * 2.0, 4)
                    ELSE 0
                END as points_above_replacement,
                -- Calculate draft value based on actual points vs expected points for career draft position
                CASE 
                    WHEN COALESCE(pcs.career_avg_draft_position, psc.avg_draft_position) > 0 AND fps.total_fantasy_points > 0
                    THEN ROUND(fps.total_fantasy_points / GREATEST(COALESCE(pcs.career_avg_draft_position, psc.avg_draft_position), 1), 4)
                    ELSE 0
                END as draft_value_score,
                -- Calculate waiver value based on actual fantasy points earned
                CASE 
                    WHEN psc.waiver_pickups > 0 AND fps.total_fantasy_points > 0
                    THEN ROUND(fps.total_fantasy_points / psc.waiver_pickups, 4)
                    ELSE 0
                END as waiver_pickup_value,
                COALESCE(fps.games_played, 0) as games_played
            FROM player_season_combined psc
            LEFT JOIN player_career_stats pcs ON psc.player_key = pcs.player_key
            LEFT JOIN player_career_transactions pct ON psc.player_key = pct.player_key
            LEFT JOIN player_fantasy_performance fps ON psc.player_key = fps.player_key AND psc.season_year = fps.season_year
            WHERE psc.times_drafted > 0 OR psc.waiver_pickups > 0
            ORDER BY psc.season_year DESC, psc.times_drafted DESC, COALESCE(pcs.career_avg_draft_position, psc.avg_draft_position) ASC
            """
            
            result = conn.execute(text(sql))
            rows = result.fetchall()
            
            for row in rows:
                # Map all columns from comprehensive query with fixed logic
                marts.append({
                    'player_key': row[0],
                    'season_year': row[1],
                    'total_seasons_rostered': row[2],
                    'total_teams_rostered': row[3],
                    'total_leagues_played': row[4],
                    'times_drafted': row[5] or 0,
                    'career_avg_draft_position': row[6],
                    'career_earliest_draft_pick': row[7],
                    'career_latest_draft_pick': row[8],
                    'avg_auction_value': row[9] or 0.0,
                    'total_fantasy_points': row[10],
                    'avg_points_per_game': row[11],
                    'best_weekly_score': row[12],
                    'worst_weekly_score': row[13],
                    'consistency_rating': row[14],
                    'avg_ownership_percentage': row[15],
                    'weeks_as_starter': row[16],
                    'starter_percentage': row[17],
                    'points_above_replacement': row[18],
                    'draft_value_score': row[19],
                    'waiver_pickup_value': row[20],
                    'games_played': row[21] if len(row) > 21 else 0  # Add games_played from fantasy performance
                })
        
        logger.info(f"🏪 Generated {len(marts)} player value records")
        return marts

    def transform_mart_weekly_power_rankings(self) -> List[Dict]:
        """Transform data for mart_weekly_power_rankings"""
        marts = []
        
        with self.engine.connect() as conn:
            logger.info("🏪 Calculating weekly power rankings...")
            
            # Fixed comprehensive power rankings with corrected calculations
            sql = """
            WITH all_weeks AS (
                -- Get all league/week combinations for cumulative calculation
                SELECT DISTINCT 
                    ftp.league_key,
                    ftp.season_year,
                    ftp.week_key,
                    dw.week_number
                FROM edw.fact_team_performance ftp
                JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
                WHERE dw.week_type = 'regular'
            ),
            actual_matchups AS (
                -- Get actual head-to-head matchups to calculate correct SOS and pythagorean wins
                SELECT 
                    fm.league_key,
                    fm.week_key,
                    fm.season_year,
                    fm.team1_key as team_key,
                    fm.team2_key as opponent_key,
                    fm.team1_points as team_points,
                    fm.team2_points as opponent_points,
                    CASE WHEN fm.winner_team_key = fm.team1_key THEN 1 ELSE 0 END as team_won,
                    dw.week_number,
                    dw.week_number as matchup_week
                FROM edw.fact_matchup fm
                JOIN edw.dim_week dw ON fm.week_key = dw.week_key
                WHERE dw.week_type = 'regular'
                
                UNION ALL
                
                SELECT 
                    fm.league_key,
                    fm.week_key,
                    fm.season_year,
                    fm.team2_key as team_key,
                    fm.team1_key as opponent_key,
                    fm.team2_points as team_points,
                    fm.team1_points as opponent_points,
                    CASE WHEN fm.winner_team_key = fm.team2_key THEN 1 ELSE 0 END as team_won,
                    dw.week_number,
                    dw.week_number as matchup_week
                FROM edw.fact_matchup fm
                JOIN edw.dim_week dw ON fm.week_key = dw.week_key
                WHERE dw.week_type = 'regular'
            ),
            opponent_records AS (
                -- Calculate win percentages for strength of schedule
                SELECT 
                    ftp.team_key,
                    ftp.league_key,
                    ftp.week_key,
                    ftp.win_percentage as opponent_win_pct
                FROM edw.fact_team_performance ftp
            ),
            team_sos_and_pyth AS (
                -- Calculate cumulative strength of schedule and actual pythagorean wins
                SELECT 
                    aw.team_key,
                    aw.league_key,
                    aw.week_key,
                    aw.season_year,
                    AVG(opr.opponent_win_pct) as strength_of_schedule,
                    -- Calculate actual pythagorean wins based on point scoring
                    SUM(am.team_points) as total_points_for,
                    SUM(am.opponent_points) as total_points_against,
                    COUNT(*) as games_played,
                    -- Pythagorean expectation formula: (PF^2) / (PF^2 + PA^2) * Games
                    CASE 
                        WHEN SUM(am.team_points) > 0 AND SUM(am.opponent_points) > 0 THEN
                            (POWER(SUM(am.team_points), 2) / 
                             (POWER(SUM(am.team_points), 2) + POWER(SUM(am.opponent_points), 2))) * COUNT(*)
                        ELSE SUM(am.team_won)  -- Fallback to actual wins if no points
                    END as pythagorean_wins
                FROM (
                    SELECT DISTINCT 
                        ftp.team_key,
                        aw.league_key,
                        aw.week_key,
                        aw.season_year,
                        aw.week_number
                    FROM all_weeks aw
                    CROSS JOIN edw.fact_team_performance ftp
                    WHERE ftp.league_key = aw.league_key 
                      AND ftp.season_year = aw.season_year
                ) aw
                JOIN actual_matchups am ON aw.team_key = am.team_key 
                    AND aw.league_key = am.league_key 
                    AND aw.season_year = am.season_year
                    AND am.matchup_week <= aw.week_number  -- Cumulative through this week
                JOIN opponent_records opr ON am.opponent_key = opr.team_key 
                    AND am.league_key = opr.league_key 
                    AND am.week_key = opr.week_key
                GROUP BY aw.team_key, aw.league_key, aw.week_key, aw.season_year
            ),
            recent_performance AS (
                -- Calculate recent form (last 3 weeks of actual scoring, cumulative view)
                SELECT 
                    aw.team_key,
                    aw.league_key,
                    aw.week_key,
                    AVG(am.team_points) as recent_form_score
                FROM (
                    SELECT DISTINCT 
                        ftp.team_key,
                        aw.league_key,
                        aw.week_key,
                        aw.season_year,
                        aw.week_number
                    FROM all_weeks aw
                    CROSS JOIN edw.fact_team_performance ftp
                    WHERE ftp.league_key = aw.league_key 
                      AND ftp.season_year = aw.season_year
                ) aw
                JOIN actual_matchups am ON aw.team_key = am.team_key 
                    AND aw.league_key = am.league_key 
                    AND aw.season_year = am.season_year
                    AND am.matchup_week >= GREATEST(1, aw.week_number - 2)  -- Last 3 weeks
                    AND am.matchup_week <= aw.week_number
                GROUP BY aw.team_key, aw.league_key, aw.week_key
            ),
            league_settings AS (
                -- League settings for playoff odds calculation (10-team league, 6 playoff spots)
                SELECT 
                    dl.league_key,
                    dl.season_year,
                    dl.num_teams,
                    -- Your league structure: 10 teams, 6 playoff spots
                    6 as playoff_spots
                FROM edw.dim_league dl
            ),
            cumulative_records AS (
                -- Calculate cumulative wins/losses/ties through each week
                SELECT 
                    aw.team_key,
                    aw.league_key,
                    aw.week_key,
                    aw.season_year,
                    SUM(am.team_won) as cumulative_wins,
                    SUM(CASE WHEN am.team_won = 0 THEN 1 ELSE 0 END) as cumulative_losses,
                    0 as cumulative_ties,  -- No ties in current data
                    COUNT(*) as games_played
                FROM (
                    SELECT DISTINCT 
                        ftp.team_key,
                        aw.league_key,
                        aw.week_key,
                        aw.season_year,
                        aw.week_number
                    FROM all_weeks aw
                    CROSS JOIN edw.fact_team_performance ftp
                    WHERE ftp.league_key = aw.league_key 
                      AND ftp.season_year = aw.season_year
                ) aw
                JOIN actual_matchups am ON aw.team_key = am.team_key 
                    AND aw.league_key = am.league_key 
                    AND aw.season_year = am.season_year
                    AND am.matchup_week <= aw.week_number  -- Cumulative through this week
                GROUP BY aw.team_key, aw.league_key, aw.week_key, aw.season_year
            ),
            playoff_cutoff_teams AS (
                -- Get 6th place team's win percentage for games back calculation
                SELECT 
                    ws.league_key,
                    ws.week_key,
                    -- Use win percentage of 6th place team (handles ties better than raw wins)
                    MAX(CASE WHEN ws.season_rank = 6 THEN ws.win_percentage ELSE NULL END) as sixth_place_win_pct,
                    -- If no exact 6th place team, use the team closest to 6th place
                    COALESCE(
                        MAX(CASE WHEN ws.season_rank = 6 THEN ws.win_percentage ELSE NULL END),
                        MAX(CASE WHEN ws.season_rank <= 6 THEN ws.win_percentage ELSE NULL END)
                    ) as playoff_cutoff_win_pct
                FROM (
                    SELECT 
                        ftp.league_key,
                        ftp.week_key,
                        ftp.season_rank,
                        -- Calculate win percentage from cumulative records
                        CASE 
                            WHEN COALESCE(cr.games_played, 0) > 0 THEN
                                (COALESCE(cr.cumulative_wins, 0) + 0.5 * COALESCE(cr.cumulative_ties, 0)) / cr.games_played
                            ELSE 0.0
                        END as win_percentage
                    FROM edw.fact_team_performance ftp
                    LEFT JOIN cumulative_records cr ON ftp.team_key = cr.team_key 
                        AND ftp.league_key = cr.league_key 
                        AND ftp.week_key = cr.week_key
                    WHERE ftp.season_rank <= 8  -- Only look at teams reasonably close to playoffs
                ) ws
                GROUP BY ws.league_key, ws.week_key
            ),
            weekly_stats AS (
                SELECT 
                    ftp.league_key,
                    ftp.week_key,
                    ftp.team_key,
                    ftp.season_year,
                    -- Use cumulative record instead of weekly record
                    COALESCE(cr.cumulative_wins, 0) as wins,
                    COALESCE(cr.cumulative_losses, 0) as losses,
                    COALESCE(cr.cumulative_ties, 0) as ties,
                    -- Calculate cumulative win percentage
                    CASE 
                        WHEN COALESCE(cr.games_played, 0) > 0 THEN
                            (COALESCE(cr.cumulative_wins, 0) + 0.5 * COALESCE(cr.cumulative_ties, 0)) / cr.games_played
                        ELSE 0.0
                    END as win_percentage,
                    ftp.points_for,
                    ftp.points_against,
                    ftp.season_rank,
                    ftp.playoff_probability,
                    dw.week_number,
                    -- Use actual SOS and pythagorean wins from corrected calculations
                    COALESCE(tsap.strength_of_schedule, 0.5) as strength_of_schedule,
                    COALESCE(rp.recent_form_score, ftp.points_for / NULLIF(COALESCE(cr.games_played, 1), 0)) as recent_form_score,
                    COALESCE(tsap.pythagorean_wins, COALESCE(cr.cumulative_wins, 0)) as pythagorean_wins,
                    -- Previous rank for rank change calculation
                    LAG(ftp.season_rank) OVER (PARTITION BY ftp.team_key, ftp.league_key ORDER BY dw.week_number) as prev_rank,
                    ls.playoff_spots,
                    ls.num_teams,
                    -- Calculate games back using win percentage (more accurate with ties)
                    GREATEST(0, 
                        ROUND((COALESCE(pct.playoff_cutoff_win_pct, 0.6) - 
                               CASE 
                                   WHEN COALESCE(cr.games_played, 0) > 0 THEN
                                       (COALESCE(cr.cumulative_wins, 0) + 0.5 * COALESCE(cr.cumulative_ties, 0)) / cr.games_played
                                   ELSE 0.0
                               END) * COALESCE(cr.games_played, 0), 1)
                    ) as games_back_from_playoffs,
                    -- Calculate weeks remaining using actual season data
                    GREATEST(0, 
                        COALESCE(ds.championship_week, 16) - 1 - dw.week_number
                    ) as weeks_remaining
                FROM edw.fact_team_performance ftp
                JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
                JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                JOIN edw.dim_manager dm ON dt.manager_name = dm.manager_name
                JOIN edw.dim_season ds ON ftp.season_year = ds.season_year
                JOIN league_settings ls ON ftp.league_key = ls.league_key AND ftp.season_year = ls.season_year
                LEFT JOIN cumulative_records cr ON ftp.team_key = cr.team_key 
                    AND ftp.league_key = cr.league_key 
                    AND ftp.week_key = cr.week_key
                LEFT JOIN team_sos_and_pyth tsap ON ftp.team_key = tsap.team_key 
                    AND ftp.league_key = tsap.league_key 
                    AND ftp.week_key = tsap.week_key
                LEFT JOIN recent_performance rp ON ftp.team_key = rp.team_key 
                    AND ftp.league_key = rp.league_key 
                    AND ftp.week_key = rp.week_key
                LEFT JOIN playoff_cutoff_teams pct ON ftp.league_key = pct.league_key 
                    AND ftp.week_key = pct.week_key
                WHERE dw.week_type = 'regular' 
                  AND dm.include_in_analysis = TRUE
            ),
            power_calculations AS (
                SELECT 
                    ws.*,
                    -- Improved power score calculation (weighted: 40% record, 30% points, 20% recent form, 10% SOS)
                    (ws.win_percentage * 0.4 + 
                     (ws.points_for / NULLIF(MAX(ws.points_for) OVER (PARTITION BY ws.league_key, ws.week_key), 0)) * 0.3 +
                     (ws.recent_form_score / NULLIF(MAX(ws.recent_form_score) OVER (PARTITION BY ws.league_key, ws.week_key), 0)) * 0.2 +
                     (1.0 - COALESCE(ws.strength_of_schedule, 0.5)) * 0.1) as power_score,
                    -- Luck factor (actual cumulative wins vs expected wins based on matchups)
                    ws.wins - ws.pythagorean_wins as luck_factor,
                    -- Rank change from previous week
                    COALESCE(ws.prev_rank - ws.season_rank, 0) as rank_change,
                    -- Improved playoff probability based on rank, games back, and realistic scenarios
                    CASE 
                        WHEN ws.season_rank <= ws.playoff_spots THEN 
                            -- Teams in playoff position: high probability, increases with week and position
                            LEAST(0.98, 0.65 + (ws.week_number * 0.025) + 
                                  ((ws.playoff_spots - ws.season_rank + 1.0) / ws.playoff_spots * 0.20))
                        WHEN ws.weeks_remaining <= 0 THEN
                            -- Season over: 100% if in playoffs, 0% if not
                            CASE WHEN ws.season_rank <= ws.playoff_spots THEN 1.0 ELSE 0.0 END
                        WHEN ws.games_back_from_playoffs > (ws.weeks_remaining * 1.5) THEN
                            -- Mathematically very difficult (would need other teams to lose most games)
                            GREATEST(0.01, 0.05 - (ws.week_number * 0.002))
                        ELSE
                            -- Teams that can realistically catch up: factor in games back vs time remaining
                            CASE 
                                WHEN ws.games_back_from_playoffs <= 0.5 THEN 
                                    -- Tied or virtually tied for last playoff spot
                                    GREATEST(0.40, 0.75 - (ws.week_number * 0.02))
                                WHEN ws.games_back_from_playoffs <= 1.0 THEN
                                    -- 1 game back: good odds early, declining late
                                    GREATEST(0.15, 0.60 - (ws.week_number * 0.035) - (ws.games_back_from_playoffs * 0.10))
                                WHEN ws.games_back_from_playoffs <= 2.0 THEN
                                    -- 2 games back: moderate odds early, low odds late
                                    GREATEST(0.08, 0.40 - (ws.week_number * 0.030) - (ws.games_back_from_playoffs * 0.08))
                                WHEN ws.games_back_from_playoffs <= 3.0 THEN
                                    -- 3 games back: low odds, very time sensitive
                                    GREATEST(0.03, 0.25 - (ws.week_number * 0.025) - (ws.games_back_from_playoffs * 0.06))
                                ELSE
                                    -- 4+ games back: minimal odds, need early season + help
                                    GREATEST(0.01, 0.15 - (ws.week_number * 0.020) - (ws.games_back_from_playoffs * 0.04))
                            END
                    END as calculated_playoff_odds
                FROM weekly_stats ws
            ),
            matchup_margins AS (
                -- Calculate cumulative biggest win/loss margins from all matchups through each week
                SELECT 
                    aw.team_key,
                    aw.league_key,
                    aw.week_key,
                    MAX(CASE WHEN am.team_won = 1 THEN ABS(am.team_points - am.opponent_points) ELSE 0 END) as biggest_win_margin,
                    MAX(CASE WHEN am.team_won = 0 THEN ABS(am.team_points - am.opponent_points) ELSE 0 END) as biggest_loss_margin
                FROM (
                    SELECT DISTINCT 
                        ftp.team_key,
                        aw.league_key,
                        aw.week_key,
                        aw.season_year,
                        aw.week_number
                    FROM all_weeks aw
                    CROSS JOIN edw.fact_team_performance ftp
                    WHERE ftp.league_key = aw.league_key 
                      AND ftp.season_year = aw.season_year
                ) aw
                JOIN actual_matchups am ON aw.team_key = am.team_key 
                    AND aw.league_key = am.league_key 
                    AND aw.season_year = am.season_year
                    AND am.matchup_week <= aw.week_number  -- Cumulative through this week
                GROUP BY aw.team_key, aw.league_key, aw.week_key
            ),
            ranked_power AS (
                SELECT 
                    pc.*,
                    COALESCE(mm.biggest_win_margin, 0) as biggest_win_margin,
                    COALESCE(mm.biggest_loss_margin, 0) as biggest_loss_margin,
                    ROW_NUMBER() OVER (PARTITION BY pc.league_key, pc.week_key ORDER BY pc.power_score DESC) as power_rank,
                    ROW_NUMBER() OVER (PARTITION BY pc.league_key, pc.week_key ORDER BY pc.win_percentage DESC, pc.points_for DESC) as record_rank,
                    ROW_NUMBER() OVER (PARTITION BY pc.league_key, pc.week_key ORDER BY pc.points_for DESC) as points_rank
                FROM power_calculations pc
                LEFT JOIN matchup_margins mm ON pc.team_key = mm.team_key 
                    AND pc.league_key = mm.league_key 
                    AND pc.week_key = mm.week_key
            )
            SELECT 
                rp.league_key,
                rp.week_key,
                rp.team_key,
                rp.power_rank,
                rp.record_rank,
                rp.points_rank,
                rp.wins,
                rp.losses,
                COALESCE(rp.ties, 0) as ties,
                ROUND(rp.win_percentage, 4) as win_percentage,
                ROUND(rp.power_score, 4) as power_score,
                ROUND(rp.strength_of_schedule, 4) as strength_of_schedule,
                ROUND(rp.recent_form_score, 2) as recent_form_score,
                ROUND(rp.power_score * 0.85 + rp.win_percentage * 0.15, 4) as projection_score,
                rp.rank_change,
                ROUND(rp.biggest_win_margin, 2) as biggest_win_margin,
                ROUND(rp.biggest_loss_margin, 2) as biggest_loss_margin,
                ROUND(rp.pythagorean_wins, 2) as pythagorean_wins,
                ROUND(rp.luck_factor, 2) as luck_factor,
                ROUND(rp.calculated_playoff_odds, 4) as playoff_odds
            FROM ranked_power rp
            ORDER BY rp.league_key, rp.week_key, rp.power_rank
            """
            
            result = conn.execute(text(sql))
            rows = result.fetchall()
            
            for row in rows:
                # Map all corrected power ranking metrics including season record
                marts.append({
                    'league_key': row[0],
                    'week_key': row[1],
                    'team_key': row[2],
                    'power_rank': row[3],
                    'record_rank': row[4],
                    'points_rank': row[5],
                    'wins': row[6],
                    'losses': row[7],
                    'ties': row[8],
                    'win_percentage': row[9],
                    'power_score': row[10],
                    'strength_of_schedule': row[11],
                    'recent_form_score': row[12],
                    'projection_score': row[13],
                    'rank_change': row[14],
                    'biggest_win_margin': row[15],
                    'biggest_loss_margin': row[16],
                    'pythagorean_wins': row[17],
                    'luck_factor': row[18],
                    'playoff_odds': row[19]
                })
        
        logger.info(f"🏪 Generated {len(marts)} weekly power ranking records with FIXED pythagorean wins and playoff odds")
        return marts

    def transform_mart_manager_h2h(self) -> List[Dict]:
        """Transform data for mart_manager_h2h"""
        marts = []
        
        with self.engine.connect() as conn:
            logger.info("🏪 Calculating manager head-to-head statistics...")
            
            # Comprehensive H2H analysis with all schema columns
            sql = """
            WITH manager_matchups AS (
                SELECT 
                    dt1.manager_name as manager1,
                    dt2.manager_name as manager2,
                    dt1.manager_id as manager1_id,
                    dt2.manager_id as manager2_id,
                    fm.season_year,
                    fm.week_key,
                    ds.season_year || '-W' || dw.week_number as matchup_identifier,
                    fm.team1_points,
                    fm.team2_points,
                    fm.point_difference,
                    fm.matchup_type,
                    dl.league_name,
                    dw.week_number,
                    CASE 
                        WHEN fm.winner_team_key = dt1.team_key THEN 'manager1_win'
                        WHEN fm.winner_team_key = dt2.team_key THEN 'manager2_win'
                        ELSE 'tie'
                    END as outcome,
                    CASE WHEN fm.is_playoffs = TRUE THEN 1 ELSE 0 END as is_playoff,
                    CASE WHEN fm.is_championship = TRUE THEN 1 ELSE 0 END as is_championship,
                    CASE WHEN fm.is_semifinal = TRUE THEN 1 ELSE 0 END as is_semifinal,
                    -- Add row number for streak calculation (ordered by season, week)
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            CASE WHEN dt1.manager_name < dt2.manager_name THEN dt1.manager_name ELSE dt2.manager_name END,
                            CASE WHEN dt1.manager_name < dt2.manager_name THEN dt2.manager_name ELSE dt1.manager_name END
                        ORDER BY fm.season_year DESC, dw.week_number DESC
                    ) as game_recency_rank
                FROM edw.fact_matchup fm
                JOIN edw.dim_team dt1 ON fm.team1_key = dt1.team_key
                JOIN edw.dim_team dt2 ON fm.team2_key = dt2.team_key
                JOIN edw.dim_manager dm1 ON dt1.manager_name = dm1.manager_name
                JOIN edw.dim_manager dm2 ON dt2.manager_name = dm2.manager_name
                JOIN edw.dim_league dl ON fm.league_key = dl.league_key
                JOIN edw.dim_week dw ON fm.week_key = dw.week_key
                JOIN edw.dim_season ds ON fm.season_year = ds.season_year
                WHERE dt1.manager_name != dt2.manager_name
                    AND dt1.is_active = TRUE 
                    AND dt2.is_active = TRUE
                    AND dm1.include_in_analysis = TRUE
                    AND dm2.include_in_analysis = TRUE
            ),
            manager_pairs AS (
                SELECT 
                    CASE WHEN manager1 < manager2 THEN manager1 ELSE manager2 END as manager_a_name,
                    CASE WHEN manager1 < manager2 THEN manager2 ELSE manager1 END as manager_b_name,
                    CASE WHEN manager1 < manager2 THEN manager1_id ELSE manager2_id END as manager_a_id,
                    CASE WHEN manager1 < manager2 THEN manager2_id ELSE manager1_id END as manager_b_id,
                    matchup_identifier,
                    season_year,
                    league_name,
                    team1_points,
                    team2_points,
                    point_difference,
                    matchup_type,
                    outcome,
                    is_playoff,
                    is_championship,
                    is_semifinal,
                    manager1,
                    manager2,
                    game_recency_rank,
                    -- Calculate manager-specific points and outcomes
                    CASE WHEN manager1 < manager2 THEN team1_points ELSE team2_points END as manager_a_points,
                    CASE WHEN manager1 < manager2 THEN team2_points ELSE team1_points END as manager_b_points,
                    CASE 
                        WHEN (manager1 < manager2 AND outcome = 'manager1_win') 
                             OR (manager1 > manager2 AND outcome = 'manager2_win') 
                        THEN 'manager_a_win'
                        WHEN (manager1 < manager2 AND outcome = 'manager2_win') 
                             OR (manager1 > manager2 AND outcome = 'manager1_win') 
                        THEN 'manager_b_win'
                        ELSE 'tie'
                    END as normalized_outcome,
                    team1_points + team2_points as total_game_points
                FROM manager_matchups
            ),
            h2h_scoring_stats AS (
                SELECT 
                    manager_a_name,
                    manager_b_name,
                    -- Percentile calculations for high/low scoring games
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_game_points) as high_score_threshold,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_game_points) as low_score_threshold,
                    AVG(total_game_points) as avg_total_points
                FROM manager_pairs
                GROUP BY manager_a_name, manager_b_name
            ),
            h2h_streaks AS (
                SELECT 
                    manager_a_name,
                    manager_b_name,
                    -- Calculate current streak for manager A
                    CASE 
                        WHEN COUNT(CASE WHEN game_recency_rank = 1 THEN normalized_outcome END) > 0 THEN
                            CASE 
                                WHEN MAX(CASE WHEN game_recency_rank = 1 THEN normalized_outcome END) = 'manager_a_win' THEN
                                    COUNT(CASE WHEN normalized_outcome = 'manager_a_win' AND 
                                               game_recency_rank <= (
                                                   SELECT MIN(sub.game_recency_rank) 
                                                   FROM manager_pairs sub 
                                                   WHERE sub.manager_a_name = mp.manager_a_name 
                                                     AND sub.manager_b_name = mp.manager_b_name 
                                                     AND sub.normalized_outcome != 'manager_a_win'
                                                     AND sub.game_recency_rank >= 1
                                               ) THEN 1 END)
                                WHEN MAX(CASE WHEN game_recency_rank = 1 THEN normalized_outcome END) = 'manager_b_win' THEN
                                    -COUNT(CASE WHEN normalized_outcome = 'manager_b_win' AND 
                                                game_recency_rank <= (
                                                    SELECT MIN(sub.game_recency_rank) 
                                                    FROM manager_pairs sub 
                                                    WHERE sub.manager_a_name = mp.manager_a_name 
                                                      AND sub.manager_b_name = mp.manager_b_name 
                                                      AND sub.normalized_outcome != 'manager_b_win'
                                                      AND sub.game_recency_rank >= 1
                                                ) THEN 1 END)
                                ELSE 0
                            END
                        ELSE 0
                    END as manager_a_current_streak,
                    -- Calculate current streak for manager B (opposite of A)
                    CASE 
                        WHEN COUNT(CASE WHEN game_recency_rank = 1 THEN normalized_outcome END) > 0 THEN
                            CASE 
                                WHEN MAX(CASE WHEN game_recency_rank = 1 THEN normalized_outcome END) = 'manager_b_win' THEN
                                    COUNT(CASE WHEN normalized_outcome = 'manager_b_win' AND 
                                               game_recency_rank <= (
                                                   SELECT MIN(sub.game_recency_rank) 
                                                   FROM manager_pairs sub 
                                                   WHERE sub.manager_a_name = mp.manager_a_name 
                                                     AND sub.manager_b_name = mp.manager_b_name 
                                                     AND sub.normalized_outcome != 'manager_b_win'
                                                     AND sub.game_recency_rank >= 1
                                               ) THEN 1 END)
                                WHEN MAX(CASE WHEN game_recency_rank = 1 THEN normalized_outcome END) = 'manager_a_win' THEN
                                    -COUNT(CASE WHEN normalized_outcome = 'manager_a_win' AND 
                                                game_recency_rank <= (
                                                    SELECT MIN(sub.game_recency_rank) 
                                                    FROM manager_pairs sub 
                                                    WHERE sub.manager_a_name = mp.manager_a_name 
                                                      AND sub.manager_b_name = mp.manager_b_name 
                                                      AND sub.normalized_outcome != 'manager_a_win'
                                                      AND sub.game_recency_rank >= 1
                                                ) THEN 1 END)
                                ELSE 0
                            END
                        ELSE 0
                    END as manager_b_current_streak
                FROM manager_pairs mp
                GROUP BY manager_a_name, manager_b_name
            ),
            h2h_detailed AS (
                SELECT 
                    mp.manager_a_name,
                    mp.manager_b_name,
                    MIN(mp.manager_a_id) as manager_a_id,
                    MIN(mp.manager_b_id) as manager_b_id,
                    COUNT(*) as total_matchups,
                    MIN(mp.matchup_identifier) as first_matchup_identifier,
                    MAX(mp.matchup_identifier) as last_matchup_identifier,
                    COUNT(DISTINCT mp.season_year) as seasons_played_together,
                    COUNT(DISTINCT mp.league_name) as leagues_played_together,
                    
                    -- Manager A stats
                    SUM(CASE WHEN mp.normalized_outcome = 'manager_a_win' THEN 1 ELSE 0 END) as manager_a_wins,
                    SUM(CASE WHEN mp.normalized_outcome = 'manager_b_win' THEN 1 ELSE 0 END) as manager_a_losses,
                    SUM(CASE WHEN mp.normalized_outcome = 'tie' THEN 1 ELSE 0 END) as manager_a_ties,
                    SUM(mp.manager_a_points) as manager_a_total_points,
                    MAX(mp.manager_a_points) as manager_a_highest_score,
                    MIN(mp.manager_a_points) as manager_a_lowest_score,
                    MAX(CASE WHEN mp.normalized_outcome = 'manager_a_win' THEN ABS(mp.point_difference) ELSE 0 END) as manager_a_biggest_win_margin,
                    
                    -- Manager B stats  
                    SUM(CASE WHEN mp.normalized_outcome = 'manager_b_win' THEN 1 ELSE 0 END) as manager_b_wins,
                    SUM(CASE WHEN mp.normalized_outcome = 'manager_a_win' THEN 1 ELSE 0 END) as manager_b_losses,
                    SUM(mp.manager_b_points) as manager_b_total_points,
                    MAX(mp.manager_b_points) as manager_b_highest_score,
                    MIN(mp.manager_b_points) as manager_b_lowest_score,
                    MAX(CASE WHEN mp.normalized_outcome = 'manager_b_win' THEN ABS(mp.point_difference) ELSE 0 END) as manager_b_biggest_win_margin,
                    
                    -- Game analysis
                    SUM(mp.total_game_points) as total_points_in_series,
                    MAX(ABS(mp.point_difference)) as most_lopsided_game,
                    MIN(ABS(mp.point_difference)) as closest_game,
                    SUM(mp.is_playoff) as playoff_matchups,
                    SUM(mp.is_championship) as championship_matchups,
                    SUM(mp.is_semifinal) as semifinal_matchups,
                    
                    -- Pythagorean wins calculation for Manager A
                    CASE 
                        WHEN (SUM(mp.manager_a_points) + SUM(mp.manager_b_points)) > 0 THEN
                            ROUND(
                                (POWER(SUM(mp.manager_a_points), 2) / 
                                 (POWER(SUM(mp.manager_a_points), 2) + POWER(SUM(mp.manager_b_points), 2))) * COUNT(*), 
                                2
                            )
                        ELSE 0
                    END as manager_a_pythagorean_wins,
                    
                    -- Pythagorean wins calculation for Manager B
                    CASE 
                        WHEN (SUM(mp.manager_a_points) + SUM(mp.manager_b_points)) > 0 THEN
                            ROUND(
                                (POWER(SUM(mp.manager_b_points), 2) / 
                                 (POWER(SUM(mp.manager_a_points), 2) + POWER(SUM(mp.manager_b_points), 2))) * COUNT(*), 
                                2
                            )
                        ELSE 0
                    END as manager_b_pythagorean_wins,
                    
                    -- High/Low scoring games (top/bottom 25%)
                    SUM(CASE WHEN mp.total_game_points >= hss.high_score_threshold THEN 1 ELSE 0 END) as high_scoring_games,
                    SUM(CASE WHEN mp.total_game_points <= hss.low_score_threshold THEN 1 ELSE 0 END) as low_scoring_games,
                    
                    -- Playoff wins
                    SUM(CASE WHEN mp.is_playoff = 1 AND mp.normalized_outcome = 'manager_a_win' THEN 1 ELSE 0 END) as manager_a_playoff_wins,
                    SUM(CASE WHEN mp.is_championship = 1 AND mp.normalized_outcome = 'manager_a_win' THEN 1 ELSE 0 END) as manager_a_championship_wins,
                    SUM(CASE WHEN mp.is_semifinal = 1 AND mp.normalized_outcome = 'manager_a_win' THEN 1 ELSE 0 END) as manager_a_semifinal_wins,
                    SUM(CASE WHEN mp.is_playoff = 1 AND mp.normalized_outcome = 'manager_b_win' THEN 1 ELSE 0 END) as manager_b_playoff_wins,
                    SUM(CASE WHEN mp.is_championship = 1 AND mp.normalized_outcome = 'manager_b_win' THEN 1 ELSE 0 END) as manager_b_championship_wins,
                    SUM(CASE WHEN mp.is_semifinal = 1 AND mp.normalized_outcome = 'manager_b_win' THEN 1 ELSE 0 END) as manager_b_semifinal_wins
                FROM manager_pairs mp
                LEFT JOIN h2h_scoring_stats hss ON mp.manager_a_name = hss.manager_a_name 
                    AND mp.manager_b_name = hss.manager_b_name
                GROUP BY 
                    mp.manager_a_name,
                    mp.manager_b_name
            ),
            most_important_single AS (
                SELECT 
                    manager_a_name,
                    manager_b_name,
                    MIN(CASE 
                        WHEN matchup_type = 'championship' THEN '1_' || matchup_identifier
                        WHEN matchup_type = 'semifinal' THEN '2_' || matchup_identifier
                        WHEN is_playoff = 1 THEN '3_' || matchup_identifier
                        ELSE '4_' || matchup_identifier
                    END) as ranked_game,
                    SUBSTRING(MIN(CASE 
                        WHEN matchup_type = 'championship' THEN '1_' || matchup_type
                        WHEN matchup_type = 'semifinal' THEN '2_' || matchup_type
                        WHEN is_playoff = 1 THEN '3_' || matchup_type
                        ELSE '4_' || matchup_type
                    END) FROM 3) as most_important_game_type,
                    SUBSTRING(MIN(CASE 
                        WHEN matchup_type = 'championship' THEN '1_' || matchup_identifier
                        WHEN matchup_type = 'semifinal' THEN '2_' || matchup_identifier
                        WHEN is_playoff = 1 THEN '3_' || matchup_identifier
                        ELSE '4_' || matchup_identifier
                    END) FROM 3) as most_important_game_identifier
                FROM manager_pairs
                GROUP BY manager_a_name, manager_b_name
            ),
            most_important_details AS (
                SELECT 
                    mis.manager_a_name,
                    mis.manager_b_name,
                    mis.most_important_game_type,
                    mis.most_important_game_identifier,
                    -- Get the actual game details by joining back to manager_pairs
                    mp.season_year as most_important_game_season,
                    CASE 
                        WHEN mp.matchup_identifier LIKE '%W%' THEN 
                            CAST(SUBSTRING(mp.matchup_identifier FROM 'W(\\d+)') AS INTEGER)
                        ELSE NULL 
                    END as most_important_game_week,
                    mp.league_name as most_important_game_league,
                    CASE 
                        WHEN mp.normalized_outcome = 'manager_a_win' THEN mp.manager_a_name
                        WHEN mp.normalized_outcome = 'manager_b_win' THEN mp.manager_b_name
                        ELSE 'Tie'
                    END as most_important_game_winner,
                    CONCAT(
                        ROUND(mp.manager_a_points, 2), 
                        ' vs ', 
                        ROUND(mp.manager_b_points, 2)
                    ) as most_important_game_score,
                    ROUND(ABS(mp.point_difference), 2) as most_important_game_margin
                FROM most_important_single mis
                LEFT JOIN manager_pairs mp ON mis.manager_a_name = mp.manager_a_name 
                    AND mis.manager_b_name = mp.manager_b_name
                    AND mis.most_important_game_identifier = mp.matchup_identifier
            ),
            h2h_with_most_important AS (
                SELECT 
                    h2h.*,
                    mid.most_important_game_type,
                    mid.most_important_game_identifier as most_important_game_date,
                    mid.most_important_game_winner,
                    mid.most_important_game_score,
                    mid.most_important_game_margin,
                    mid.most_important_game_season,
                    mid.most_important_game_week,
                    mid.most_important_game_league,
                    -- Add streaks
                    COALESCE(hs.manager_a_current_streak, 0) as manager_a_current_streak,
                    COALESCE(hs.manager_b_current_streak, 0) as manager_b_current_streak
                FROM h2h_detailed h2h
                LEFT JOIN most_important_details mid ON h2h.manager_a_name = mid.manager_a_name 
                    AND h2h.manager_b_name = mid.manager_b_name
                LEFT JOIN h2h_streaks hs ON h2h.manager_a_name = hs.manager_a_name 
                    AND h2h.manager_b_name = hs.manager_b_name
            )
            SELECT 
                manager_a_name,
                manager_b_name,
                manager_a_id,
                manager_b_id,
                total_matchups,
                first_matchup_identifier as first_matchup_date,
                last_matchup_identifier as last_matchup_date,
                seasons_played_together,
                leagues_played_together,
                manager_a_wins,
                manager_a_losses,
                manager_a_ties,
                CASE 
                    WHEN (manager_a_wins + manager_a_losses + manager_a_ties) > 0 
                    THEN ROUND(manager_a_wins::decimal / (manager_a_wins + manager_a_losses + manager_a_ties), 4)
                    ELSE 0
                END as manager_a_win_percentage,
                manager_a_total_points,
                CASE 
                    WHEN total_matchups > 0 
                    THEN ROUND(manager_a_total_points / total_matchups, 2)
                    ELSE 0
                END as manager_a_avg_points,
                manager_a_highest_score,
                manager_a_lowest_score,
                manager_a_pythagorean_wins,
                -- Luck factor = actual wins - expected wins
                ROUND(manager_a_wins - manager_a_pythagorean_wins, 4) as manager_a_luck_factor,
                manager_a_biggest_win_margin,
                manager_a_current_streak,
                manager_b_wins,
                manager_b_losses,
                manager_a_ties as manager_b_ties,
                CASE 
                    WHEN (manager_b_wins + manager_b_losses + manager_a_ties) > 0 
                    THEN ROUND(manager_b_wins::decimal / (manager_b_wins + manager_b_losses + manager_a_ties), 4)
                    ELSE 0
                END as manager_b_win_percentage,
                manager_b_total_points,
                CASE 
                    WHEN total_matchups > 0 
                    THEN ROUND(manager_b_total_points / total_matchups, 2)
                    ELSE 0
                END as manager_b_avg_points,
                manager_b_highest_score,
                manager_b_lowest_score,
                manager_b_pythagorean_wins,
                -- Luck factor = actual wins - expected wins
                ROUND(manager_b_wins - manager_b_pythagorean_wins, 4) as manager_b_luck_factor,
                manager_b_biggest_win_margin,
                manager_b_current_streak,
                CASE 
                    WHEN manager_a_wins > manager_b_wins THEN manager_a_name
                    WHEN manager_b_wins > manager_a_wins THEN manager_b_name
                    ELSE 'Tied'
                END as series_leader,
                CONCAT(manager_a_wins, '-', manager_b_wins, '-', manager_a_ties) as series_record,
                ROUND(manager_a_total_points - manager_b_total_points, 2) as point_differential,
                ROUND((manager_a_total_points - manager_b_total_points) / total_matchups, 2) as avg_point_differential,
                most_lopsided_game,
                closest_game,
                total_points_in_series,
                ROUND(total_points_in_series / total_matchups, 2) as avg_total_points_per_game,
                high_scoring_games,
                low_scoring_games,
                playoff_matchups,
                championship_matchups,
                semifinal_matchups,
                manager_a_playoff_wins,
                manager_a_championship_wins,
                manager_a_semifinal_wins,
                manager_b_playoff_wins,
                manager_b_championship_wins,
                manager_b_semifinal_wins,
                most_important_game_type,
                most_important_game_date,
                -- Additional columns that still need proper implementation
                most_important_game_winner,
                most_important_game_score,
                most_important_game_margin,
                most_important_game_season,
                most_important_game_week,
                most_important_game_league
            FROM h2h_with_most_important
            WHERE total_matchups >= 2
            ORDER BY total_matchups DESC, point_differential DESC
            """
            
            result = conn.execute(text(sql))
            rows = result.fetchall()
            
            for row in rows:
                # Map all comprehensive H2H columns with properly calculated values
                marts.append({
                    'manager_a_name': row[0],
                    'manager_b_name': row[1],
                    'manager_a_id': row[2],
                    'manager_b_id': row[3],
                    'total_matchups': row[4],
                    'first_matchup_date': row[5],
                    'last_matchup_date': row[6],
                    'seasons_played_together': row[7],
                    'leagues_played_together': row[8],
                    'manager_a_wins': row[9],
                    'manager_a_losses': row[10],
                    'manager_a_ties': row[11],
                    'manager_a_win_percentage': row[12],
                    'manager_a_total_points': row[13],
                    'manager_a_avg_points': row[14],
                    'manager_a_highest_score': row[15],  # Now calculated
                    'manager_a_lowest_score': row[16],   # Now calculated
                    'manager_a_pythagorean_wins': row[17],  # Now calculated
                    'manager_a_luck_factor': row[18],    # Now calculated
                    'manager_a_biggest_win_margin': row[19],
                    'manager_a_current_streak': row[20], # Now calculated
                    'manager_b_wins': row[21],
                    'manager_b_losses': row[22],
                    'manager_b_ties': row[23],
                    'manager_b_win_percentage': row[24],
                    'manager_b_total_points': row[25],
                    'manager_b_avg_points': row[26],
                    'manager_b_highest_score': row[27],  # Now calculated
                    'manager_b_lowest_score': row[28],   # Now calculated
                    'manager_b_pythagorean_wins': row[29],  # Now calculated
                    'manager_b_luck_factor': row[30],    # Now calculated
                    'manager_b_biggest_win_margin': row[31],
                    'manager_b_current_streak': row[32], # Now calculated
                    'series_leader': row[33],
                    'series_record': row[34],
                    'point_differential': row[35],
                    'avg_point_differential': row[36],
                    'most_lopsided_game': row[37],
                    'closest_game': row[38],
                    'total_points_in_series': row[39],
                    'avg_total_points_per_game': row[40],
                    'high_scoring_games': row[41],      # Now calculated (top 25%)
                    'low_scoring_games': row[42],       # Now calculated (bottom 25%)
                    'playoff_matchups': row[43],
                    'championship_matchups': row[44],
                    'semifinal_matchups': row[45],
                    'manager_a_playoff_wins': row[46],  # Now calculated
                    'manager_a_championship_wins': row[47],  # Now calculated
                    'manager_a_semifinal_wins': row[48],     # Now calculated
                    'manager_b_playoff_wins': row[49],  # Now calculated
                    'manager_b_championship_wins': row[50],  # Now calculated
                    'manager_b_semifinal_wins': row[51],     # Now calculated
                    'most_important_game_type': row[52],
                    'most_important_game_date': row[53],
                    'most_important_game_winner': row[54],
                    'most_important_game_score': row[55],
                    'most_important_game_margin': row[56],
                    'most_important_game_season': row[57],
                    'most_important_game_week': row[58],
                    'most_important_game_league': row[59]
                })
        
        logger.info(f"🏪 Generated {len(marts)} head-to-head rivalry records")
        return marts

    def load_mart_table(self, table_name: str, data: List[Dict]) -> bool:
        """Load data into mart table using appropriate strategy"""
        try:
            if not data:
                logger.info(f"📊 No data to load for {table_name}")
                return True
                
            logger.info(f"📊 Loading {len(data)} records into {table_name}")
            
            # Convert to DataFrame for processing
            df = pd.DataFrame(data)
            
            with self.engine.connect() as conn:
                if self.force_rebuild:
                    # Force rebuild: truncate and reload all data
                    logger.info(f"🗑️ Force rebuild: Truncating {table_name}...")
                    conn.execute(text(f"TRUNCATE TABLE edw.{table_name} RESTART IDENTITY CASCADE"))
                    
                    # Use bulk insert for clean rebuild
                    logger.info(f"⚡ Bulk inserting {len(df)} records...")
                    df.to_sql(table_name, conn, schema='edw', if_exists='append', index=False)
                    
                    # Force rebuild: skip incremental logic entirely
                    conn.commit()
                    logger.info(f"✅ Successfully loaded {len(data)} records into {table_name}")
                    return True
                else:
                    # Incremental loading: truncate and reload for marts (they are aggregations)
                    logger.info(f"🔄 Refreshing mart table {table_name}...")
                    conn.execute(text(f"TRUNCATE TABLE edw.{table_name} RESTART IDENTITY CASCADE"))
                    
                    # Insert new data
                    logger.info(f"⚡ Loading {len(df)} refreshed records...")
                    df.to_sql(table_name, conn, schema='edw', if_exists='append', index=False)
                
                conn.commit()
                logger.info(f"✅ Successfully loaded {len(data)} records into {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to load {table_name}: {e}")
            return False

def main():
    """Main ETL entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fantasy Football EDW ETL Processor')
    parser.add_argument('--data-file', 
                       default='data/current/yahoo_fantasy_COMPLETE_*.json',
                       help='Operational data file path (supports wildcards)')
    parser.add_argument('--database-url', 
                       help='EDW Database URL (or set DATABASE_URL env var)')
    parser.add_argument('--create-schema', action='store_true',
                       help='Create EDW schema before ETL')
    
    args = parser.parse_args()
    
    # Get database URL
    database_url = args.database_url or os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("❌ DATABASE_URL required: set as environment variable or pass directly")
        sys.exit(1)
    
    # Auto-detect data file if wildcard, or use database-only mode
    import glob
    data_file = None
    
    if args.data_file and '*' in args.data_file:
        files = glob.glob(args.data_file)
        if not files:
            logger.warning(f"⚠️ No data files found matching: {args.data_file}")
            logger.info("📊 Switching to database-only mode")
            data_file = None
        else:
            data_file = sorted(files)[-1]  # Use most recent
            logger.info(f"📂 Auto-detected data file: {data_file}")
    elif args.data_file and args.data_file != 'data/current/yahoo_fantasy_COMPLETE_*.json':
        # User specified a specific file
        data_file = args.data_file
    else:
        # Default wildcard didn't match or no file specified - use database-only mode
        logger.info("📊 Using database-only mode (reading from operational tables)")
        data_file = None
    
    # Create schema if requested
    if args.create_schema:
        logger.info("🏗️ Creating EDW schema...")
        engine = create_engine(database_url.replace('postgres://', 'postgresql://', 1))
        with open('fantasy_edw_schema.sql', 'r') as f:
            schema_sql = f.read()
        
        with engine.connect() as conn:
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            for stmt in statements:
                if stmt.upper().startswith(('CREATE', 'ALTER', 'COMMENT')):
                    try:
                        conn.execute(text(stmt))
                    except Exception as e:
                        if "already exists" not in str(e).lower():
                            logger.warning(f"Schema warning: {e}")
            conn.commit()
        logger.info("✅ EDW schema created")
    
    # Run ETL
    try:
        etl = EdwEtlProcessor(database_url, data_file, args.force_rebuild)
        
        if etl.run_etl():
            logger.info("🎊 Fantasy Football EDW is ready for your web app!")
        else:
            logger.error("❌ ETL process failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ ETL error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 