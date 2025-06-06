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
    """EDW ETL processor that integrates with incremental loading workflow"""
    
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
        'rosters': {
            'triggers_refresh': ['fact_roster', 'mart_weekly_power_rankings', 'vw_current_season_dashboard'],
            'refresh_type': 'WEEKLY',  # Full refresh for current week
            'depends_on': 'operational_rosters'
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
        }
    }
    
    def __init__(self, database_url: str = None, data_file: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        self.data_file = data_file
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
        """Load data from JSON file if provided"""
        if not self.data_file:
            logger.info("📊 No data file provided - using database-only mode")
            return True
            
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
        """Extract unique seasons from leagues data"""
        seasons = {}
        
        if not self.data:
            logger.warning("⚠️ No data available for season extraction")
            return list(seasons.values())
        
        for league in self.data.get('leagues', []):
            season_year = int(league['season'])
            if season_year not in seasons:
                seasons[season_year] = {
                    'season_year': season_year,
                    'season_start_date': date(season_year, 9, 1),  # Approximate NFL season start
                    'season_end_date': date(season_year + 1, 1, 31),  # Approximate end
                    'playoff_start_week': 15,  # Standard fantasy playoffs
                    'championship_week': 17,
                    'total_weeks': 17,
                    'is_current_season': season_year == datetime.now().year,
                    'season_status': 'completed' if season_year < datetime.now().year else 'active'
                }
        
        return list(seasons.values())
    
    def extract_weeks(self) -> List[Dict]:
        """Extract week dimension from available data"""
        weeks = {}
        
        # Extract from roster data (has week information)
        for roster in self.data.get('rosters', []):
            # Try to get season_year from roster data or use a default
            season_year = roster.get('season', 2024)  # Default to 2024 if not present
            week_number = roster['week']
            
            key = (season_year, week_number)
            if key not in weeks:
                weeks[key] = {
                    'season_year': season_year,
                    'week_number': week_number,
                    'week_type': self.classify_week_type(week_number),
                    'week_start_date': None,  # Could calculate based on season
                    'week_end_date': None,
                    'is_current_week': False  # Will be updated based on current logic
                }
        
        return list(weeks.values())
    
    def classify_week_type(self, week_number: int) -> str:
        """Classify week type based on week number"""
        if week_number <= 14:
            return 'regular'
        elif week_number <= 16:
            return 'playoffs'
        else:
            return 'championship'
    
    def transform_leagues(self) -> List[Dict]:
        """Transform leagues into dimension format"""
        transformed = []
        
        if not self.data:
            logger.warning("⚠️ No data available for league transformation")
            return transformed
        
        for league in self.data.get('leagues', []):
            transformed.append({
                'league_id': league['league_id'],
                'league_name': league['name'],
                'season_year': int(league['season']),
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
        
        for team in self.data.get('teams', []):
            # Generate manager_id from manager_name
            manager_id = hashlib.md5(team.get('manager_name', 'unknown').encode()).hexdigest()[:10]
            
            transformed.append({
                'team_id': team['team_id'],
                'league_id': team['league_id'],  # Will be mapped to league_key
                'team_name': team['name'],
                'manager_name': team.get('manager_name'),
                'manager_id': manager_id,
                'team_logo_url': team.get('team_logo_url'),
                'is_active': True,
                'valid_from': datetime.now(),
                'valid_to': datetime(9999, 12, 31)
            })
        
        return transformed
    
    def transform_players(self) -> List[Dict]:
        """Transform players from roster data into dimension format"""
        players = {}
        
        # Extract unique players from roster data
        for roster in self.data.get('rosters', []):
            player_id = roster['player_id']
            if player_id not in players:
                players[player_id] = {
                    'player_id': player_id,
                    'player_name': roster['player_name'],
                    'primary_position': roster.get('position'),
                    'eligible_positions': roster.get('eligible_positions'),
                    'nfl_team': None,  # Not available in operational data
                    'jersey_number': None,
                    'rookie_year': None,
                    'is_active': True,
                    'valid_from': datetime.now(),
                    'valid_to': datetime(9999, 12, 31)
                }
        
        return list(players.values())
    
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
                                          league_type, scoring_type, draft_type, is_active, 
                                          valid_from, valid_to)
                    VALUES (:league_id, :league_name, :season_year, :num_teams, 
                            :league_type, :scoring_type, :draft_type, :is_active, 
                            :valid_from, :valid_to)
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
            return True
            
        except Exception as e:
            logger.error(f"❌ Dimension loading failed: {e}")
            self.session.rollback()
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
            
            # Second: League dimension
            if 'dim_league' in all_dimension_tables:
                dimension_tables.append('dim_league')
            
            # Third: Team dimension (depends on league)
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
        """Load data into dimension table with upsert logic"""
        try:
            if not data:
                logger.info(f"📊 No data to load for {table_name}")
                return True
                
            logger.info(f"📊 Loading {len(data)} records into {table_name}")
            
            with self.engine.connect() as conn:
                # Define upsert logic for each dimension table
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
                        # Check if league exists
                        existing = conn.execute(text("""
                            SELECT league_key FROM edw.dim_league WHERE league_id = :league_id AND season_year = :season_year
                        """), {"league_id": record['league_id'], "season_year": record['season_year']}).fetchone()
                        
                        if existing:
                            # Update existing
                            conn.execute(text("""
                                UPDATE edw.dim_league SET 
                                    league_name = :league_name,
                                    num_teams = :num_teams,
                                    league_type = :league_type,
                                    scoring_type = :scoring_type,
                                    draft_type = :draft_type,
                                    is_active = :is_active,
                                    valid_to = :valid_to
                                WHERE league_id = :league_id AND season_year = :season_year
                            """), record)
                        else:
                            # Insert new
                            conn.execute(text("""
                                INSERT INTO edw.dim_league (league_id, league_name, season_year, num_teams, 
                                                      league_type, scoring_type, draft_type, is_active, 
                                                      valid_from, valid_to)
                                VALUES (:league_id, :league_name, :season_year, :num_teams,
                                       :league_type, :scoring_type, :draft_type, :is_active,
                                       :valid_from, :valid_to)
                            """), record)
                        
                elif table_name == 'dim_team':
                    for record in data:
                        # First get the league_key for this league_id
                        league_result = conn.execute(text("""
                            SELECT league_key FROM edw.dim_league WHERE league_id = :league_id
                        """), {"league_id": record['league_id']})
                        league_row = league_result.fetchone()
                        
                        if not league_row:
                            logger.warning(f"⚠️ League {record['league_id']} not found in edw.dim_league, skipping team {record['team_id']}")
                            continue
                            
                        league_key = league_row[0]
                        
                        # Check if team exists
                        existing = conn.execute(text("""
                            SELECT team_key FROM edw.dim_team WHERE team_id = :team_id
                        """), {"team_id": record['team_id']}).fetchone()
                        
                        team_data = {**record, 'league_key': league_key}
                        del team_data['league_id']  # Remove league_id, use league_key
                        
                        if existing:
                            # Update existing
                            conn.execute(text("""
                                UPDATE edw.dim_team SET 
                                    league_key = :league_key,
                                    team_name = :team_name,
                                    manager_name = :manager_name,
                                    manager_id = :manager_id,
                                    team_logo_url = :team_logo_url,
                                    is_active = :is_active,
                                    valid_to = :valid_to
                                WHERE team_id = :team_id
                            """), team_data)
                        else:
                            # Insert new
                            conn.execute(text("""
                                INSERT INTO edw.dim_team (team_id, league_key, team_name, manager_name, 
                                                    manager_id, team_logo_url, is_active, valid_from, valid_to)
                                VALUES (:team_id, :league_key, :team_name, :manager_name,
                                       :manager_id, :team_logo_url, :is_active, :valid_from, :valid_to)
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
                        
                else:
                    logger.warning(f"⚠️ No loading logic for {table_name}")
                    return True
                
                conn.commit()
                logger.info(f"✅ Successfully loaded {len(data)} records into {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to load {table_name}: {e}")
            return False
    
    def process_fact_table(self, table_name: str) -> bool:
        """Process fact table (placeholder for future implementation)"""
        logger.info(f"📊 Fact table processing for {table_name} - to be implemented")
        return True
    
    def process_mart_table(self, table_name: str) -> bool:
        """Process data mart table (placeholder for future implementation)"""
        logger.info(f"🏪 Data mart processing for {table_name} - to be implemented") 
        return True
    
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
            ("Load Dimensions", self.load_dimensions)
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
        
        return True

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
    
    # Auto-detect data file if wildcard
    import glob
    if '*' in args.data_file:
        files = glob.glob(args.data_file)
        if not files:
            logger.error(f"❌ No data files found matching: {args.data_file}")
            sys.exit(1)
        data_file = sorted(files)[-1]  # Use most recent
        logger.info(f"📂 Auto-detected data file: {data_file}")
    else:
        data_file = args.data_file
    
    # Create schema if requested
    if args.create_schema:
        logger.info("🏗️ Creating EDW schema...")
        engine = create_engine(database_url.replace('postgres://', 'postgresql://', 1))
        with open('src/edw_schema/fantasy_edw_schema.sql', 'r') as f:
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
        etl = EdwEtlProcessor(database_url, data_file)
        
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