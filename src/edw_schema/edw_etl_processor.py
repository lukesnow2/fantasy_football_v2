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
            
            self.data = {}
            operational_tables = ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks', 'statistics']
            
            with self.engine.connect() as conn:
                for table in operational_tables:
                    try:
                        # Load all records from operational table
                        result = conn.execute(text(f"SELECT * FROM {table}"))
                        records = []
                        for row in result:
                            record = dict(row._mapping)
                            # Convert any datetime fields to strings for consistency
                            for key, value in record.items():
                                if isinstance(value, (datetime, date)):
                                    record[key] = value.isoformat()
                            records.append(record)
                        
                        self.data[table] = records
                        if records:
                            logger.info(f"  📊 {table}: {len(records):,} records")
                            self.changed_tables.add(table)
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Could not load {table}: {e}")
                        self.data[table] = []
            
            total_records = sum(len(records) for records in self.data.values())
            logger.info(f"✅ Database data loaded: {total_records:,} total records")
            return True
            
        except Exception as e:
            logger.error(f"❌ Database data loading failed: {e}")
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
            
            # Consolidate manager name and lookup manager_key
            raw_manager_name = team.get('manager_name')
            if raw_manager_name:
                consolidated_manager_name = self.consolidate_manager_name(raw_manager_name)
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
        """Transform roster data into fact_roster format"""
        facts = []
        
        if not self.data:
            logger.warning("⚠️ No data available for roster fact transformation")
            return facts

        # Check if roster data exists
        rosters_data = self.data.get('rosters', [])
        if not rosters_data:
            logger.info("🏈 No roster data available - skipping fact_roster")
            return facts

        # Build set of league of record IDs for efficient lookup
        league_of_record_ids = set()
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if self.is_league_of_record(league['league_id'], season_year):
                league_of_record_ids.add(league['league_id'])

        # Use cached dimension mappings (no database queries in loop)
        league_keys = self.dim_mappings.get('league_keys', {})
        team_keys = self.dim_mappings.get('team_keys', {})
        player_keys = self.dim_mappings.get('player_keys', {})
        manager_keys = self.dim_mappings.get('manager_keys', {})
        week_keys = self.dim_mappings.get('week_keys', {})
        
        for roster in rosters_data:
            # Only include rosters from leagues of record
            if roster['league_id'] not in league_of_record_ids:
                continue
                
            # Extract league info to get season
            season_year = None
            for league in self.data.get('leagues', []):
                if league['league_id'] == roster['league_id']:
                    season_year = int(league['season'])
                    break
            
            if not season_year:
                continue
                
            league_key = league_keys.get(roster['league_id'])
            team_key = team_keys.get(roster['team_id'])
            
            # Extract numeric player ID from Yahoo format
            raw_player_id = roster['player_id']
            if '.p.' in raw_player_id:
                numeric_player_id = raw_player_id.split('.p.')[-1]
            else:
                numeric_player_id = raw_player_id
            
            player_key = player_keys.get(numeric_player_id)
            week_key = week_keys.get((season_year, roster['week']))
            
            # Get manager_key from team's manager_name
            manager_key = None
            raw_manager_name = roster.get('manager_name')
            if raw_manager_name:
                consolidated_manager_name = self.consolidate_manager_name(raw_manager_name)
                manager_key = manager_keys.get(consolidated_manager_name)
            
            if not all([league_key, team_key, player_key, week_key, manager_key]):
                continue
            
            facts.append({
                'league_key': league_key,
                'team_key': team_key,
                'manager_key': manager_key,
                'player_key': player_key,
                'week_key': week_key,
                'season_year': season_year,
                'is_starter': roster.get('is_starter', False),
                'roster_position': roster.get('selected_position', 'BN'),
                'weekly_points': float(roster.get('player_points', 0)),
                'projected_points': float(roster.get('projected_points', 0)) if roster.get('projected_points') else None,
                'roster_slot_type': roster.get('roster_position'),
                'ownership_percentage': None,  # Not available in current data
                'acquisition_type': roster.get('acquisition_type'),
                'acquisition_date': None  # Not available in current data
            })
        
        logger.info(f"📋 Transformed {len(facts)} roster facts from {len(rosters_data)} roster records")
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
                        consolidated_manager_name = self.consolidate_manager_name(raw_manager_name)
                        manager1_key = manager_keys.get(consolidated_manager_name)
                elif team['team_id'] == matchup['team2_id']:
                    raw_manager_name = team.get('manager_name')
                    if raw_manager_name:
                        consolidated_manager_name = self.consolidate_manager_name(raw_manager_name)
                        manager2_key = manager_keys.get(consolidated_manager_name)
            
            if not all([league_key, team1_key, team2_key, week_key, manager1_key, manager2_key]):
                continue
            
            team1_points = float(matchup.get('team1_score', 0))
            team2_points = float(matchup.get('team2_score', 0))
            
            # Determine matchup type based on flags
            if matchup.get('is_championship', False):
                matchup_type = 'championship'
            elif matchup.get('is_consolation', False):
                matchup_type = 'consolation'
            elif matchup.get('is_playoffs', False):
                matchup_type = 'playoffs'
            else:
                matchup_type = 'regular'
            
            # Determine winner manager key
            winner_manager_key = None
            if team1_points > team2_points:
                winner_manager_key = manager1_key
            elif team2_points > team1_points:
                winner_manager_key = manager2_key
            
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
                'is_consolation': bool(matchup_type == 'consolation')
            })
        
        return facts
    
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
            
            # Get manager keys for from/to teams
            from_manager_key = None
            to_manager_key = None
            
            if transaction.get('source_team_id'):
                for team in self.data.get('teams', []):
                    if team['team_id'] == transaction['source_team_id']:
                        raw_manager_name = team.get('manager_name')
                        if raw_manager_name:
                            consolidated_manager_name = self.consolidate_manager_name(raw_manager_name)
                            from_manager_key = manager_keys.get(consolidated_manager_name)
                        break
                        
            if transaction.get('destination_team_id'):
                for team in self.data.get('teams', []):
                    if team['team_id'] == transaction['destination_team_id']:
                        raw_manager_name = team.get('manager_name')
                        if raw_manager_name:
                            consolidated_manager_name = self.consolidate_manager_name(raw_manager_name)
                            to_manager_key = manager_keys.get(consolidated_manager_name)
                        break
            
            facts.append({
                'league_key': int(league_key),
                'player_key': int(player_key),
                'season_year': int(season_year),
                'transaction_date': transaction_date,
                'transaction_week': None,  # Not available in data
                'transaction_type': str(transaction['type']),
                'from_team_key': int(team_keys.get(transaction.get('source_team_id'))) if team_keys.get(transaction.get('source_team_id')) else None,
                'to_team_key': int(team_keys.get(transaction.get('destination_team_id'))) if team_keys.get(transaction.get('destination_team_id')) else None,
                'from_manager_key': int(from_manager_key) if from_manager_key else None,
                'to_manager_key': int(to_manager_key) if to_manager_key else None,
                'faab_bid': float(transaction.get('faab_bid', 0)) if transaction.get('faab_bid') else None,
                'waiver_priority': None,  # Not available in data
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
        # - games_played: count of non-bench weeks from rosters 
        # - points_per_game: season_points / total_weeks_in_season
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
                # Get season_points from public.statistics
                league_ids_str = "', '".join(league_of_record_ids)
                sql = f"""
                    SELECT 
                        s.league_id,
                        s.player_id,
                        s.season_year,
                        s.total_fantasy_points
                    FROM public.statistics s
                    WHERE s.league_id IN ('{league_ids_str}')
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
        
        # Calculate games_played from roster data (non-bench positions only)
        logger.info("📊 Calculating games_played from roster data...")
        games_played_counts = {}
        
        for roster in self.data.get('rosters', []):
            if roster['league_id'] not in league_of_record_ids:
                continue
            if roster.get('status') != 'active':
                continue
            if roster.get('position') == 'BN':  # Skip bench players
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
            if perf_key not in games_played_counts:
                games_played_counts[perf_key] = 0
            games_played_counts[perf_key] += 1
        
        # Apply games_played counts to performance data
        for perf_key, count in games_played_counts.items():
            if perf_key in player_performance:
                player_performance[perf_key]['games_played'] = count
        
        # Calculate points_per_game = season_points / total_weeks_in_season
        for perf_key, perf_data in player_performance.items():
            season_year = perf_key[1]
            total_weeks = season_weeks.get(season_year, 17)  # Default to 17 weeks
            season_points = perf_data['season_points']
            
            if total_weeks > 0:
                perf_data['points_per_game'] = season_points / total_weeks
            else:
                perf_data['points_per_game'] = 0.0
        
        logger.info(f"📊 Built performance data for {len(player_performance)} player-season combinations")
        logger.info(f"📊 Games played calculated from {len(games_played_counts)} roster entries")

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
                        consolidated_manager_name = self.consolidate_manager_name(raw_manager_name)
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
                'points_per_game': 0.0
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
                'draft_cost': float(draft_pick.get('cost', 0)) if draft_pick.get('cost') else None,
                'is_keeper_pick': bool(draft_pick.get('is_keeper', False)),
                'season_points': float(perf_data['season_points']),
                'games_played': int(perf_data['games_played']),
                'points_per_game': float(perf_data['points_per_game'])
            })
        
        return facts
    
    def transform_fact_player_statistics(self) -> List[Dict]:
        """Transform player statistics data into fact_player_statistics format"""
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

        logger.info(f"📊 Processing {len(statistics_data)} player statistics records...")

        # Group stats by league-season for ranking calculations
        league_season_stats = {}
        position_stats = {}

        # First pass: collect all stats for ranking calculations
        for stat in statistics_data:
            league_id = stat['league_id']
            if league_id not in league_of_record_ids:
                continue

            season_year = league_to_season.get(league_id)
            if not season_year:
                continue

            fantasy_points = float(stat.get('total_fantasy_points', 0))
            position_type = stat.get('position_type', 'Unknown')

            # Group for league rankings
            league_season_key = (league_id, season_year)
            if league_season_key not in league_season_stats:
                league_season_stats[league_season_key] = []
            league_season_stats[league_season_key].append((stat, fantasy_points))

            # Group for position rankings
            position_key = (league_id, season_year, position_type)
            if position_key not in position_stats:
                position_stats[position_key] = []
            position_stats[position_key].append((stat, fantasy_points))

        # Calculate rankings
        league_rankings = {}
        position_rankings = {}

        for league_season_key, stats_list in league_season_stats.items():
            # Sort by fantasy points descending
            sorted_stats = sorted(stats_list, key=lambda x: x[1], reverse=True)
            for rank, (stat, points) in enumerate(sorted_stats, 1):
                stat_key = (stat['league_id'], stat['player_id'], stat['season_year'])
                league_rankings[stat_key] = rank

        for position_key, stats_list in position_stats.items():
            # Sort by fantasy points descending
            sorted_stats = sorted(stats_list, key=lambda x: x[1], reverse=True)
            for rank, (stat, points) in enumerate(sorted_stats, 1):
                stat_key = (stat['league_id'], stat['player_id'], stat['season_year'])
                position_rankings[stat_key] = rank

        # Second pass: create fact records with rankings
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

            # Basic stats
            fantasy_points = float(stat.get('total_fantasy_points', 0))
            position_type = stat.get('position_type', 'Unknown')

            # Get rankings
            stat_key = (league_id, stat['player_id'], int(stat['season_year']))
            league_rank = league_rankings.get(stat_key)
            position_rank = position_rankings.get(stat_key)

            # Calculate estimated performance metrics
            # For now, use simplified calculations - can be enhanced later
            estimated_games = 17 if fantasy_points > 0 else 0
            points_per_game = fantasy_points / estimated_games if estimated_games > 0 else 0

            # Calculate points above replacement (simplified)
            # Use bottom 25% of position as replacement level
            position_key = (league_id, season_year, position_type)
            if position_key in position_stats:
                position_players = position_stats[position_key]
                if len(position_players) > 3:  # Need enough players for replacement calculation
                    replacement_threshold = int(len(position_players) * 0.75)  # Bottom 25%
                    sorted_position = sorted(position_players, key=lambda x: x[1], reverse=True)
                    replacement_points = sorted_position[replacement_threshold][1] if replacement_threshold < len(sorted_position) else 0
                    points_above_replacement = fantasy_points - replacement_points
                else:
                    points_above_replacement = fantasy_points
            else:
                points_above_replacement = fantasy_points

            # Create fact record
            fact = {
                'league_key': league_key,
                'player_key': player_key,
                'season_year': season_year,
                'total_fantasy_points': fantasy_points,
                'position_type': position_type,
                'games_played': estimated_games,
                'points_per_game': round(points_per_game, 2),
                'consistency_score': None,  # Would need weekly data to calculate
                'position_rank': position_rank,
                'league_rank': league_rank,
                'points_above_replacement': round(points_above_replacement, 2),
                'draft_value_score': None,  # Would need draft data correlation
                'source_stat_id': stat.get('stat_id'),
                'game_code': stat.get('game_code', 'nfl')
            }

            facts.append(fact)

        logger.info(f"✅ Player statistics facts transformed: {len(facts)} records")
        return facts
    
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
                        consolidated_manager_name = self.consolidate_manager_name(raw_manager_name)
                        manager_key = manager_keys.get(consolidated_manager_name)
                    break
            
            if not all([team_key, league_key, week_key, manager_key]):
                continue
            
            facts.append({
                'team_key': team_key,
                'manager_key': manager_key,
                'league_key': league_key,
                'week_key': week_key,
                'season_year': perf['season_year'],
                'wins': perf['wins'],
                'losses': perf['losses'],
                'ties': perf['ties'],
                'points_for': perf['weekly_points'],  # Fixed: Set to actual weekly points
                'points_against': perf['points_against'],
                'weekly_points': perf['weekly_points'],
                'weekly_rank': None,
                'season_rank': None,
                'win_percentage': None,
                'point_differential': perf['weekly_points'] - perf['points_against'],
                'avg_points_per_game': None,
                'playoff_probability': None,
                'is_playoff_team': False,
                'playoff_seed': None,
                'waiver_priority': None,
                'faab_balance': None
            })
        
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
                    
            elif table_name == 'dim_week':
                weeks = self.extract_weeks()
                if weeks:
                    self.load_dimension_table('dim_week', weeks)
                    
            elif table_name == 'dim_league':
                leagues = self.transform_leagues()
                if leagues:
                    self.load_dimension_table('dim_league', leagues)
                    
            elif table_name == 'dim_team':
                teams = self.transform_teams()
                if teams:
                    self.load_dimension_table('dim_team', teams)
                    
            elif table_name == 'dim_player':
                players = self.transform_players()
                if players:
                    self.load_dimension_table('dim_player', players)
                    
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
        """Process fact table (placeholder for future implementation)"""
        logger.info(f"📊 Fact table processing for {table_name} - to be implemented")
        return True
    
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
        """Refresh materialized view (placeholder for future implementation)"""
        logger.info(f"👁️ View refresh for {view_name} - to be implemented")
        return True
    
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
            ("Load Marts", self.load_marts)
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
                                    (league_key, player_key, season_year, total_fantasy_points, position_type, 
                                     games_played, points_per_game, consistency_score, position_rank, league_rank, 
                                     points_above_replacement, draft_value_score, source_stat_id, game_code)
                                    VALUES (:league_key, :player_key, :season_year, :total_fantasy_points, :position_type,
                                            :games_played, :points_per_game, :consistency_score, :position_rank, :league_rank,
                                            :points_above_replacement, :draft_value_score, :source_stat_id, :game_code)
                                    ON CONFLICT (league_key, player_key, season_year) 
                                    DO UPDATE SET
                                        total_fantasy_points = EXCLUDED.total_fantasy_points,
                                        position_type = EXCLUDED.position_type,
                                        games_played = EXCLUDED.games_played,
                                        points_per_game = EXCLUDED.points_per_game,
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
            if manager_name and manager_name.strip():
                raw_name = manager_name.strip()
                canonical_name = self.consolidate_manager_name(raw_name)
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
                if raw_team_name and self.consolidate_manager_name(raw_team_name) == canonical_name:
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
            
            # Use collected manager data or reasonable defaults
            manager_record = {
                'manager_name': canonical_name,  # Use canonical name
                'manager_id': manager_id,  # Generated stable manager ID
                'first_season_year': first_season,
                'last_season_year': last_season,
                'total_seasons': total_seasons,
                'total_leagues': total_leagues,
                'is_current': True,  # Default - will be manually updated
                'include_in_analysis': True,  # Default - will be manually updated
                'email': manager_data.get('email'),  # Email if available
                'display_name': manager_data.get('display_name', canonical_name),  # Prefer nickname, fallback to canonical name
                'profile_image_url': manager_data.get('profile_image_url'),  # Profile image if available
                'is_active': True
            }
            
            managers.append(manager_record)
        
        return managers

    def transform_mart_league_summary(self) -> List[Dict]:
        """Transform data for mart_league_summary"""
        marts = []
        
        # This would aggregate data from fact tables
        # For now, return empty list as placeholder
        logger.info("🏪 League summary mart - placeholder implementation")
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
                    dt.league_key,
                    SUM(CASE WHEN fm.winner_team_key = dt.team_key THEN 1 ELSE 0 END) as season_wins,
                    SUM(CASE WHEN fm.winner_team_key != dt.team_key AND fm.is_tie = FALSE THEN 1 ELSE 0 END) as season_losses,
                    SUM(CASE WHEN fm.is_tie = TRUE THEN 1 ELSE 0 END) as season_ties,
                    SUM(CASE WHEN fm.team1_key = dt.team_key THEN fm.team1_points 
                             WHEN fm.team2_key = dt.team_key THEN fm.team2_points 
                             ELSE 0 END) as season_points,
                    COUNT(fm.matchup_key) as games_played,
                    0 as championships,
                    0 as championship_games,
                    COUNT(CASE WHEN ftp.is_playoff_team = TRUE THEN 1 END) as playoff_seasons,
                    0 as playoff_wins,
                    0 as playoff_losses
                FROM edw.dim_team dt
                JOIN edw.dim_league dl ON dt.league_key = dl.league_key
                LEFT JOIN edw.fact_matchup fm ON (fm.team1_key = dt.team_key OR fm.team2_key = dt.team_key)
                    AND fm.season_year = dl.season_year
                LEFT JOIN edw.fact_team_performance ftp ON dt.team_key = ftp.team_key 
                    AND dl.season_year = ftp.season_year
                WHERE dt.is_active = TRUE
                GROUP BY dt.manager_name, dl.season_year, dt.league_key
            ),
            transaction_stats AS (
                SELECT 
                    dm.manager_name,
                    COUNT(ft.transaction_key) as total_transactions,
                    AVG(ft.faab_bid) as avg_faab_bid,
                    SUM(ft.faab_bid) as total_faab_spent
                FROM edw.dim_manager dm
                JOIN edw.fact_transaction ft ON dm.manager_key = ft.to_manager_key
                WHERE dm.is_active = TRUE AND ft.faab_bid > 0
                GROUP BY dm.manager_name
            ),
            draft_stats AS (
                SELECT 
                    dm.manager_name,
                    COUNT(fd.draft_key) as total_drafts,
                    AVG(fd.overall_pick) as avg_draft_position
                FROM edw.dim_manager dm
                JOIN edw.fact_draft fd ON dm.manager_key = fd.manager_key
                WHERE dm.is_active = TRUE
                GROUP BY dm.manager_name
            ),
            manager_aggregates AS (
                SELECT 
                    ms.manager_name,
                    COUNT(DISTINCT ms.season_year) as total_seasons,
                    COUNT(DISTINCT ms.league_key) as total_leagues,
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
                    SUM(ms.playoff_seasons) as playoff_appearances,
                    SUM(ms.playoff_wins) as total_playoff_wins,
                    SUM(ms.playoff_losses) as total_playoff_losses,
                    STDDEV(ms.season_wins::decimal / NULLIF(ms.games_played, 0)) as win_consistency
                FROM manager_stats ms
                WHERE ms.games_played > 0
                GROUP BY ms.manager_name
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
                NULL as best_draft_year,
                NULL as worst_draft_year,
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
                NULL as best_season_record,
                NULL as worst_season_record
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
            WITH player_draft_stats AS (
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
            player_season_combined AS (
                SELECT 
                    COALESCE(pds.player_key, pts.player_key) as player_key,
                    COALESCE(pds.season_year, pts.season_year) as season_year,
                    COALESCE(pds.times_drafted_season, 0) as times_drafted,
                    pds.avg_draft_position_season as avg_draft_position,
                    pds.earliest_draft_pick_season as earliest_draft_pick,
                    pds.latest_draft_pick_season as latest_draft_pick,
                    COALESCE(pds.avg_auction_value_season, 0) as avg_auction_value,
                    COALESCE(pds.teams_drafted_by_season, pts.teams_acquired_by_season, 0) as total_teams_rostered,
                    COALESCE(pts.waiver_pickups_season, 0) as waiver_pickups,
                    COALESCE(pts.trades_season, 0) as trades,
                    COALESCE(pts.avg_waiver_cost_season, 0) as avg_waiver_cost,
                    1 as total_leagues_played  -- Simplified for now
                FROM player_draft_stats pds
                FULL OUTER JOIN player_transaction_stats pts ON pds.player_key = pts.player_key AND pds.season_year = pts.season_year
                WHERE COALESCE(pds.times_drafted_season, 0) > 0 OR COALESCE(pts.waiver_pickups_season, 0) > 0
            )
            SELECT 
                psc.player_key,
                psc.season_year,
                COALESCE(pcs.total_seasons_drafted, pct.total_seasons_transacted, 1) as total_seasons_rostered,
                psc.total_teams_rostered,
                psc.total_leagues_played,
                psc.times_drafted,
                psc.avg_draft_position,
                psc.earliest_draft_pick,
                psc.latest_draft_pick,
                psc.avg_auction_value,
                0.0 as total_fantasy_points,  -- No roster data available
                0.0 as avg_points_per_game,
                0.0 as best_weekly_score,
                0.0 as worst_weekly_score,
                0.0 as consistency_rating,
                CASE 
                    WHEN psc.times_drafted > 0 THEN 0.8  -- Assume high ownership if drafted
                    WHEN psc.waiver_pickups > 0 THEN 0.3  -- Lower ownership for waiver pickups
                    ELSE 0.0
                END as avg_ownership_percentage,
                0 as weeks_as_starter,  -- No roster data available
                CASE 
                    WHEN psc.times_drafted > 0 THEN 0.6  -- Assume decent starter rate if drafted
                    ELSE 0.0
                END as starter_percentage,
                CASE 
                    WHEN psc.avg_draft_position > 0 
                    THEN ROUND((200 - psc.avg_draft_position) * 1.5, 4)
                    ELSE 0
                END as points_above_replacement,
                CASE 
                    WHEN psc.avg_draft_position > 0 
                    THEN ROUND(150 / psc.avg_draft_position, 4)
                    ELSE 0
                END as draft_value_score,
                CASE 
                    WHEN psc.waiver_pickups > 0 
                    THEN ROUND(psc.waiver_pickups * 10, 4)
                    ELSE 0
                END as waiver_pickup_value
            FROM player_season_combined psc
            LEFT JOIN player_career_stats pcs ON psc.player_key = pcs.player_key
            LEFT JOIN player_career_transactions pct ON psc.player_key = pct.player_key
            WHERE psc.times_drafted > 0 OR psc.waiver_pickups > 0
            ORDER BY psc.season_year DESC, psc.times_drafted DESC, psc.avg_draft_position ASC
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
                    'avg_draft_position': row[6],
                    'earliest_draft_pick': row[7],
                    'latest_draft_pick': row[8],
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
                    'waiver_pickup_value': row[20]
                })
        
        logger.info(f"🏪 Generated {len(marts)} player value records")
        return marts

    def transform_mart_weekly_power_rankings(self) -> List[Dict]:
        """Transform data for mart_weekly_power_rankings"""
        marts = []
        
        with self.engine.connect() as conn:
            logger.info("🏪 Calculating weekly power rankings...")
            
            # Comprehensive power rankings with advanced metrics
            sql = """
            WITH weekly_stats AS (
                SELECT 
                    ftp.league_key,
                    ftp.week_key,
                    ftp.team_key,
                    ftp.season_year,
                    ftp.wins,
                    ftp.losses,
                    ftp.points_for,
                    ftp.points_against,
                    ftp.win_percentage,
                    ftp.season_rank,
                    ftp.weekly_rank,
                    dw.week_number,
                    -- Calculate strength of schedule (avg opponent win %)
                    AVG(opp_ftp.win_percentage) OVER (PARTITION BY ftp.team_key ORDER BY dw.week_number ROWS UNBOUNDED PRECEDING) as strength_of_schedule,
                    -- Recent form (last 3 weeks performance)
                    AVG(ftp.weekly_points) OVER (PARTITION BY ftp.team_key ORDER BY dw.week_number ROWS 2 PRECEDING) as recent_form_score,
                    -- Expected wins based on points
                    SUM(CASE WHEN ftp.weekly_points > opp_ftp.weekly_points THEN 1.0 ELSE 0.0 END) OVER (PARTITION BY ftp.team_key ORDER BY dw.week_number ROWS UNBOUNDED PRECEDING) as pythagorean_wins,
                    -- Rank change calculation  
                    LAG(ftp.season_rank) OVER (PARTITION BY ftp.team_key, ftp.league_key ORDER BY dw.week_number) as prev_rank,
                    -- Biggest margins
                    -- Biggest margins (using point_differential since weekly_points_against doesn't exist)
                    MAX(ftp.point_differential) OVER (PARTITION BY ftp.team_key ORDER BY dw.week_number ROWS UNBOUNDED PRECEDING) as biggest_win_margin,
                    MIN(ftp.point_differential) OVER (PARTITION BY ftp.team_key ORDER BY dw.week_number ROWS UNBOUNDED PRECEDING) as biggest_loss_margin
                FROM edw.fact_team_performance ftp
                JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
                LEFT JOIN edw.fact_team_performance opp_ftp ON ftp.league_key = opp_ftp.league_key 
                    AND ftp.week_key = opp_ftp.week_key 
                    AND ftp.team_key != opp_ftp.team_key
                WHERE dw.week_type = 'regular'
            ),
            power_calculations AS (
                SELECT 
                    ws.*,
                    -- Power score calculation (weighted: 40% record, 30% points, 20% recent form, 10% SOS)
                    (ws.win_percentage * 0.4 + 
                     (ws.points_for / NULLIF(MAX(ws.points_for) OVER (PARTITION BY ws.league_key, ws.week_key), 0)) * 0.3 +
                     (ws.recent_form_score / NULLIF(MAX(ws.recent_form_score) OVER (PARTITION BY ws.league_key, ws.week_key), 0)) * 0.2 +
                     (1 - ws.strength_of_schedule) * 0.1) as power_score,
                    -- Luck factor (actual wins vs expected wins)  
                    ws.wins - ws.pythagorean_wins as luck_factor,
                    -- Rank change
                    COALESCE(ws.prev_rank - ws.season_rank, 0) as rank_change,
                    -- Playoff probability (simplified)
                    CASE 
                        WHEN ws.season_rank <= 6 THEN 0.9
                        WHEN ws.season_rank <= 8 THEN 0.6  
                        WHEN ws.season_rank <= 10 THEN 0.3
                        ELSE 0.1
                    END as playoff_odds
                FROM weekly_stats ws
            ),
            ranked_power AS (
                SELECT 
                    pc.*,
                    ROW_NUMBER() OVER (PARTITION BY pc.league_key, pc.week_key ORDER BY pc.power_score DESC) as power_rank,
                    ROW_NUMBER() OVER (PARTITION BY pc.league_key, pc.week_key ORDER BY pc.win_percentage DESC, pc.points_for DESC) as record_rank,
                    ROW_NUMBER() OVER (PARTITION BY pc.league_key, pc.week_key ORDER BY pc.points_for DESC) as points_rank
                FROM power_calculations pc
            )
            SELECT 
                rp.league_key,
                rp.week_key,
                rp.team_key,
                rp.power_rank,
                rp.record_rank,
                rp.points_rank,
                ROUND(rp.power_score, 4) as power_score,
                ROUND(rp.strength_of_schedule, 4) as strength_of_schedule,
                ROUND(rp.recent_form_score, 4) as recent_form_score,
                ROUND(rp.power_score * 0.8, 4) as projection_score,  -- Simplified projection
                rp.rank_change,
                ROUND(rp.biggest_win_margin, 2) as biggest_win_margin,
                ROUND(ABS(rp.biggest_loss_margin), 2) as biggest_loss_margin,
                ROUND(rp.pythagorean_wins, 2) as pythagorean_wins,
                ROUND(rp.luck_factor, 4) as luck_factor,
                ROUND(rp.playoff_odds, 4) as playoff_odds
            FROM ranked_power rp
            ORDER BY rp.league_key, rp.week_key, rp.power_rank
            """
            
            result = conn.execute(text(sql))
            rows = result.fetchall()
            
            for row in rows:
                # Map all comprehensive power ranking metrics
                marts.append({
                    'league_key': row[0],
                    'week_key': row[1],
                    'team_key': row[2],
                    'power_rank': row[3],
                    'record_rank': row[4],
                    'points_rank': row[5],
                    'power_score': row[6],
                    'strength_of_schedule': row[7],
                    'recent_form_score': row[8],
                    'projection_score': row[9],
                    'rank_change': row[10],
                    'biggest_win_margin': row[11],
                    'biggest_loss_margin': row[12],
                    'pythagorean_wins': row[13],
                    'luck_factor': row[14],
                    'playoff_odds': row[15]
                })
        
        logger.info(f"🏪 Generated {len(marts)} weekly power ranking records")
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
                    CASE WHEN fm.matchup_type = 'semifinal' THEN 1 ELSE 0 END as is_semifinal
                FROM edw.fact_matchup fm
                JOIN edw.dim_team dt1 ON fm.team1_key = dt1.team_key
                JOIN edw.dim_team dt2 ON fm.team2_key = dt2.team_key
                JOIN edw.dim_league dl ON fm.league_key = dl.league_key
                JOIN edw.dim_week dw ON fm.week_key = dw.week_key
                JOIN edw.dim_season ds ON fm.season_year = ds.season_year
                WHERE dt1.manager_name != dt2.manager_name
                    AND dt1.is_active = TRUE 
                    AND dt2.is_active = TRUE
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
                    manager2
                FROM manager_matchups
            ),
            most_important_games AS (
                SELECT 
                    manager_a_name,
                    manager_b_name,
                    FIRST_VALUE(matchup_type) OVER (
                        PARTITION BY manager_a_name, manager_b_name 
                        ORDER BY 
                            CASE WHEN matchup_type = 'championship' THEN 1
                                 WHEN matchup_type = 'semifinal' THEN 2  
                                 WHEN is_playoff = 1 THEN 3
                                 ELSE 4 END,
                            point_difference DESC
                    ) as most_important_game_type,
                    FIRST_VALUE(matchup_identifier) OVER (
                        PARTITION BY manager_a_name, manager_b_name 
                        ORDER BY 
                            CASE WHEN matchup_type = 'championship' THEN 1
                                 WHEN matchup_type = 'semifinal' THEN 2  
                                 WHEN is_playoff = 1 THEN 3
                                 ELSE 4 END,
                            point_difference DESC
                    ) as most_important_game_identifier
                FROM manager_pairs
            ),
            h2h_detailed AS (
                SELECT 
                    mp.manager_a_name,
                    mp.manager_b_name,
                    -- Use consistent manager IDs (take the first one alphabetically)
                    MIN(mp.manager_a_id) as manager_a_id,
                    MIN(mp.manager_b_id) as manager_b_id,
                    COUNT(*) as total_matchups,
                    MIN(mp.matchup_identifier) as first_matchup_identifier,
                    MAX(mp.matchup_identifier) as last_matchup_identifier,
                    COUNT(DISTINCT mp.season_year) as seasons_played_together,
                    COUNT(DISTINCT mp.league_name) as leagues_played_together,
                    
                    -- Manager A stats (alphabetically first)
                    SUM(CASE 
                        WHEN (mp.manager1 < mp.manager2 AND mp.outcome = 'manager1_win') 
                             OR (mp.manager1 > mp.manager2 AND mp.outcome = 'manager2_win') 
                        THEN 1 ELSE 0 
                    END) as manager_a_wins,
                    SUM(CASE 
                        WHEN (mp.manager1 < mp.manager2 AND mp.outcome = 'manager2_win') 
                             OR (mp.manager1 > mp.manager2 AND mp.outcome = 'manager1_win') 
                        THEN 1 ELSE 0 
                    END) as manager_a_losses,
                    SUM(CASE WHEN mp.outcome = 'tie' THEN 1 ELSE 0 END) as manager_a_ties,
                    SUM(CASE 
                        WHEN mp.manager1 < mp.manager2 THEN mp.team1_points 
                        ELSE mp.team2_points 
                    END) as manager_a_total_points,
                    MAX(CASE 
                        WHEN (mp.manager1 < mp.manager2 AND mp.outcome = 'manager1_win') 
                             OR (mp.manager1 > mp.manager2 AND mp.outcome = 'manager2_win') 
                        THEN mp.point_difference ELSE 0 
                    END) as manager_a_biggest_win_margin,
                    
                    -- Manager B stats  
                    SUM(CASE 
                        WHEN (mp.manager1 < mp.manager2 AND mp.outcome = 'manager2_win') 
                             OR (mp.manager1 > mp.manager2 AND mp.outcome = 'manager1_win') 
                        THEN 1 ELSE 0 
                    END) as manager_b_wins,
                    SUM(CASE 
                        WHEN (mp.manager1 < mp.manager2 AND mp.outcome = 'manager1_win') 
                             OR (mp.manager1 > mp.manager2 AND mp.outcome = 'manager2_win') 
                        THEN 1 ELSE 0 
                    END) as manager_b_losses,
                    SUM(CASE 
                        WHEN mp.manager1 < mp.manager2 THEN mp.team2_points 
                        ELSE mp.team1_points 
                    END) as manager_b_total_points,
                    MAX(CASE 
                        WHEN (mp.manager1 < mp.manager2 AND mp.outcome = 'manager2_win') 
                             OR (mp.manager1 > mp.manager2 AND mp.outcome = 'manager1_win') 
                        THEN mp.point_difference ELSE 0 
                    END) as manager_b_biggest_win_margin,
                    
                    -- Game analysis
                    SUM(mp.team1_points + mp.team2_points) as total_points_in_series,
                    MAX(mp.point_difference) as most_lopsided_game,
                    MIN(mp.point_difference) as closest_game,
                    SUM(mp.is_playoff) as playoff_matchups,
                    SUM(mp.is_championship) as championship_matchups,
                    SUM(mp.is_semifinal) as semifinal_matchups
                FROM manager_pairs mp
                GROUP BY 
                    mp.manager_a_name,
                    mp.manager_b_name
                    -- Removed manager IDs from GROUP BY since we're aggregating them
            ),
            most_important_single AS (
                SELECT 
                    manager_a_name,
                    manager_b_name,
                    -- Get the single most important game per pair
                    MIN(CASE 
                        WHEN matchup_type = 'championship' THEN '1_' || matchup_identifier
                        WHEN matchup_type = 'semifinal' THEN '2_' || matchup_identifier
                        WHEN is_playoff = 1 THEN '3_' || matchup_identifier
                        ELSE '4_' || matchup_identifier
                    END) as ranked_game,
                    -- Extract the actual values
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
            h2h_with_most_important AS (
                SELECT 
                    h2h.*,
                    mis.most_important_game_type,
                    mis.most_important_game_identifier
                FROM h2h_detailed h2h
                LEFT JOIN most_important_single mis ON h2h.manager_a_name = mis.manager_a_name 
                    AND h2h.manager_b_name = mis.manager_b_name
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
                manager_a_biggest_win_margin,
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
                manager_b_biggest_win_margin,
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
                playoff_matchups,
                championship_matchups,
                semifinal_matchups,
                most_important_game_type,
                most_important_game_identifier as most_important_game_date
            FROM h2h_with_most_important
            WHERE total_matchups >= 2
            ORDER BY total_matchups DESC, point_differential DESC
            """
            
            result = conn.execute(text(sql))
            rows = result.fetchall()
            
            for row in rows:
                # Map all comprehensive H2H columns
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
                    'manager_a_biggest_win_margin': row[15],
                    'manager_b_wins': row[16],
                    'manager_b_losses': row[17],
                    'manager_b_ties': row[18],
                    'manager_b_win_percentage': row[19],
                    'manager_b_total_points': row[20],
                    'manager_b_avg_points': row[21],
                    'manager_b_biggest_win_margin': row[22],
                    'series_leader': row[23],
                    'series_record': row[24],
                    'point_differential': row[25],
                    'avg_point_differential': row[26],
                    'most_lopsided_game': row[27],
                    'closest_game': row[28],
                    'total_points_in_series': row[29],
                    'avg_total_points_per_game': row[30],
                    'playoff_matchups': row[31],
                    'championship_matchups': row[32],
                    'semifinal_matchups': row[33],
                    'most_important_game_type': row[34],
                    'most_important_game_date': row[35],
                    # Additional required columns with defaults
                    'manager_a_highest_score': 0.0,
                    'manager_a_lowest_score': 0.0,
                    'manager_a_pythagorean_wins': 0.0,
                    'manager_a_luck_factor': 0.0,
                    'manager_a_current_streak': 0,
                    'manager_b_highest_score': 0.0,
                    'manager_b_lowest_score': 0.0,
                    'manager_b_pythagorean_wins': 0.0,
                    'manager_b_luck_factor': 0.0,
                    'manager_b_current_streak': 0,
                    'high_scoring_games': 0,
                    'low_scoring_games': 0,
                    'manager_a_playoff_wins': 0,
                    'manager_a_championship_wins': 0,
                    'manager_a_semifinal_wins': 0,
                    'manager_b_playoff_wins': 0,
                    'manager_b_championship_wins': 0,
                    'manager_b_semifinal_wins': 0,
                    'most_important_game_winner': None,
                    'most_important_game_score': None,
                    'most_important_game_margin': 0.0,
                    'most_important_game_season': None,
                    'most_important_game_week': None,
                    'most_important_game_league': None
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