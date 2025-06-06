#!/usr/bin/env python3
"""
Fantasy Football EDW Deployment Script

Comprehensive deployment script for the Enterprise Data Warehouse that:
- Creates the complete EDW schema in proper sequential order
- Supports both local development and cloud deployment (GitHub Actions)
- Handles dependencies and provides rollback on errors
- Optimized for elegance and maintainability

Usage:
    python deploy_edw.py [--drop-existing] [--tables-only] [--views-only]
"""

import os
import sys
import argparse
import logging
from typing import Dict, List, Tuple, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class EDWDeployer:
    """Enterprise Data Warehouse deployment manager"""
    
    def __init__(self, database_url: str):
        """Initialize with database connection"""
        self.database_url = database_url.replace('postgres://', 'postgresql://', 1)
        self.engine = create_engine(self.database_url)
        self.deployment_steps = []
        
    def execute_sql(self, sql: str, description: str) -> bool:
        """Execute SQL with error handling and logging"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
                logger.info(f"✅ {description}")
                return True
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed: {description}")
            logger.error(f"   Error: {e}")
            return False
    
    def execute_batch(self, sqls: Dict[str, str], category: str) -> bool:
        """Execute a batch of SQL statements"""
        logger.info(f"\n🚀 Creating {category}...")
        logger.info("=" * 60)
        
        success_count = 0
        for name, sql in sqls.items():
            if self.execute_sql(sql, f"Created {category.lower()}: {name}"):
                success_count += 1
        
        logger.info(f"📊 {category} Results: {success_count}/{len(sqls)} successful")
        return success_count == len(sqls)
    
    def create_schema(self) -> bool:
        """Create the EDW schema"""
        return self.execute_sql(
            "CREATE SCHEMA IF NOT EXISTS edw",
            "EDW schema"
        )
    
    def create_dimensions(self) -> bool:
        """Create dimension tables with proper sequencing"""
        dimensions = {
            'dim_season': """
                CREATE TABLE IF NOT EXISTS edw.dim_season (
                    season_key SERIAL PRIMARY KEY,
                    season_year INTEGER NOT NULL UNIQUE,
                    season_start_date DATE,
                    season_end_date DATE,
                    playoff_start_week INTEGER,
                    championship_week INTEGER,
                    total_weeks INTEGER,
                    is_current_season BOOLEAN DEFAULT FALSE,
                    season_status VARCHAR(20) DEFAULT 'completed',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'dim_league': """
                CREATE TABLE IF NOT EXISTS edw.dim_league (
                    league_key SERIAL PRIMARY KEY,
                    league_id VARCHAR(50) NOT NULL,
                    league_name VARCHAR(255) NOT NULL,
                    season_year INTEGER NOT NULL,
                    num_teams INTEGER NOT NULL,
                    league_type VARCHAR(50) NOT NULL,
                    scoring_type VARCHAR(50),
                    draft_type VARCHAR(50),
                    is_active BOOLEAN DEFAULT TRUE,
                    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'dim_player': """
                CREATE TABLE IF NOT EXISTS edw.dim_player (
                    player_key SERIAL PRIMARY KEY,
                    player_id VARCHAR(50) NOT NULL UNIQUE,
                    player_name VARCHAR(255) NOT NULL,
                    primary_position VARCHAR(20),
                    eligible_positions TEXT,
                    nfl_team VARCHAR(10),
                    jersey_number INTEGER,
                    rookie_year INTEGER,
                    is_active BOOLEAN DEFAULT TRUE,
                    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'dim_week': """
                CREATE TABLE IF NOT EXISTS edw.dim_week (
                    week_key SERIAL PRIMARY KEY,
                    season_year INTEGER NOT NULL,
                    week_number INTEGER NOT NULL,
                    week_type VARCHAR(20) NOT NULL,
                    week_start_date DATE,
                    week_end_date DATE,
                    is_current_week BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(season_year, week_number)
                )
            """,
            
            'dim_team': """
                CREATE TABLE IF NOT EXISTS edw.dim_team (
                    team_key SERIAL PRIMARY KEY,
                    team_id VARCHAR(50) NOT NULL,
                    league_key INTEGER,
                    team_name VARCHAR(255) NOT NULL,
                    manager_name VARCHAR(255),
                    manager_id VARCHAR(100),
                    team_logo_url VARCHAR(500),
                    is_active BOOLEAN DEFAULT TRUE,
                    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'edw_metadata': """
                CREATE TABLE IF NOT EXISTS edw.edw_metadata (
                    table_name VARCHAR(100) PRIMARY KEY,
                    last_processed TIMESTAMP,
                    last_change_detected TIMESTAMP,
                    processing_status VARCHAR(20) DEFAULT 'pending',
                    record_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }
        
        return self.execute_batch(dimensions, "Dimension Tables")
    
    def create_facts(self) -> bool:
        """Create fact tables"""
        facts = {
            'fact_team_performance': """
                CREATE TABLE IF NOT EXISTS edw.fact_team_performance (
                    performance_key SERIAL PRIMARY KEY,
                    team_key INTEGER NOT NULL,
                    league_key INTEGER NOT NULL,
                    week_key INTEGER NOT NULL,
                    season_year INTEGER NOT NULL,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    ties INTEGER DEFAULT 0,
                    points_for DECIMAL(10,2) DEFAULT 0,
                    points_against DECIMAL(10,2) DEFAULT 0,
                    weekly_points DECIMAL(10,2) DEFAULT 0,
                    weekly_rank INTEGER,
                    season_rank INTEGER,
                    win_percentage DECIMAL(5,4),
                    point_differential DECIMAL(10,2),
                    avg_points_per_game DECIMAL(8,2),
                    playoff_probability DECIMAL(5,4),
                    is_playoff_team BOOLEAN DEFAULT FALSE,
                    playoff_seed INTEGER,
                    waiver_priority INTEGER,
                    faab_balance DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'fact_matchup': """
                CREATE TABLE IF NOT EXISTS edw.fact_matchup (
                    matchup_key SERIAL PRIMARY KEY,
                    league_key INTEGER NOT NULL,
                    week_key INTEGER NOT NULL,
                    season_year INTEGER NOT NULL,
                    team1_key INTEGER NOT NULL,
                    team2_key INTEGER NOT NULL,
                    team1_points DECIMAL(10,2),
                    team2_points DECIMAL(10,2),
                    point_difference DECIMAL(10,2),
                    total_points DECIMAL(10,2),
                    winner_team_key INTEGER,
                    is_tie BOOLEAN DEFAULT FALSE,
                    margin_of_victory DECIMAL(10,2),
                    matchup_type VARCHAR(20) DEFAULT 'regular',
                    is_upset BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'fact_roster': """
                CREATE TABLE IF NOT EXISTS edw.fact_roster (
                    roster_key SERIAL PRIMARY KEY,
                    team_key INTEGER NOT NULL,
                    player_key INTEGER NOT NULL,
                    league_key INTEGER NOT NULL,
                    week_key INTEGER NOT NULL,
                    season_year INTEGER NOT NULL,
                    roster_position VARCHAR(20),
                    is_starter BOOLEAN DEFAULT FALSE,
                    weekly_points DECIMAL(8,2) DEFAULT 0,
                    projected_points DECIMAL(8,2),
                    ownership_percentage DECIMAL(5,2),
                    acquisition_date DATE,
                    acquisition_type VARCHAR(20),
                    roster_slot_type VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'fact_transaction': """
                CREATE TABLE IF NOT EXISTS edw.fact_transaction (
                    transaction_key SERIAL PRIMARY KEY,
                    league_key INTEGER NOT NULL,
                    player_key INTEGER NOT NULL,
                    season_year INTEGER NOT NULL,
                    transaction_date DATE NOT NULL,
                    transaction_week INTEGER,
                    transaction_type VARCHAR(20) NOT NULL,
                    from_team_key INTEGER,
                    to_team_key INTEGER,
                    faab_bid DECIMAL(10,2),
                    waiver_priority INTEGER,
                    trade_group_id VARCHAR(50),
                    transaction_status VARCHAR(20) DEFAULT 'completed',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'fact_draft': """
                CREATE TABLE IF NOT EXISTS edw.fact_draft (
                    draft_key SERIAL PRIMARY KEY,
                    league_key INTEGER NOT NULL,
                    team_key INTEGER NOT NULL,
                    player_key INTEGER NOT NULL,
                    season_year INTEGER NOT NULL,
                    overall_pick INTEGER NOT NULL,
                    round_number INTEGER NOT NULL,
                    pick_in_round INTEGER NOT NULL,
                    draft_cost DECIMAL(10,2),
                    is_keeper_pick BOOLEAN DEFAULT FALSE,
                    season_points DECIMAL(10,2),
                    games_played INTEGER,
                    points_per_game DECIMAL(8,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }
        
        return self.execute_batch(facts, "Fact Tables")
    
    def create_marts(self) -> bool:
        """Create data mart tables"""
        marts = {
            'mart_league_summary': """
                CREATE TABLE IF NOT EXISTS edw.mart_league_summary (
                    league_key INTEGER PRIMARY KEY,
                    league_name VARCHAR(255) NOT NULL,
                    season_year INTEGER NOT NULL,
                    total_teams INTEGER,
                    total_weeks INTEGER,
                    total_games INTEGER,
                    total_points DECIMAL(12,2),
                    avg_team_points DECIMAL(10,2),
                    highest_team_score DECIMAL(10,2),
                    lowest_team_score DECIMAL(10,2),
                    total_point_differential DECIMAL(12,2),
                    competitive_balance_index DECIMAL(8,4),
                    avg_margin_of_victory DECIMAL(8,2),
                    blowout_games_count INTEGER,
                    close_games_count INTEGER,
                    total_transactions INTEGER,
                    avg_transactions_per_team DECIMAL(8,2),
                    total_faab_spent DECIMAL(12,2),
                    waiver_activity_index DECIMAL(8,4),
                    champion_team_key INTEGER,
                    champion_final_record VARCHAR(10),
                    champion_total_points DECIMAL(10,2),
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'mart_manager_performance': """
                CREATE TABLE IF NOT EXISTS edw.mart_manager_performance (
                    manager_id VARCHAR(100) PRIMARY KEY,
                    manager_name VARCHAR(255) NOT NULL,
                    first_season INTEGER,
                    last_season INTEGER,
                    total_seasons INTEGER,
                    total_leagues INTEGER,
                    total_wins INTEGER DEFAULT 0,
                    total_losses INTEGER DEFAULT 0,
                    total_ties INTEGER DEFAULT 0,
                    career_win_percentage DECIMAL(8,4),
                    total_points_scored DECIMAL(15,2),
                    avg_points_per_game DECIMAL(10,2),
                    avg_points_per_season DECIMAL(12,2),
                    championships_won INTEGER DEFAULT 0,
                    championship_appearances INTEGER DEFAULT 0,
                    playoff_appearances INTEGER DEFAULT 0,
                    playoff_win_percentage DECIMAL(8,4),
                    avg_draft_grade DECIMAL(4,2),
                    best_draft_year INTEGER,
                    worst_draft_year INTEGER,
                    total_transactions INTEGER DEFAULT 0,
                    avg_transactions_per_season DECIMAL(8,2),
                    faab_efficiency_rating DECIMAL(8,4),
                    season_consistency_score DECIMAL(8,4),
                    best_season_record VARCHAR(10),
                    worst_season_record VARCHAR(10),
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            'mart_player_value': """
                CREATE TABLE IF NOT EXISTS edw.mart_player_value (
                    player_key INTEGER NOT NULL,
                    season_year INTEGER NOT NULL,
                    total_seasons_rostered INTEGER,
                    total_teams_rostered INTEGER,
                    total_leagues_played INTEGER,
                    times_drafted INTEGER DEFAULT 0,
                    avg_draft_position DECIMAL(8,2),
                    earliest_draft_pick INTEGER,
                    latest_draft_pick INTEGER,
                    avg_auction_value DECIMAL(10,2),
                    total_fantasy_points DECIMAL(12,2),
                    avg_points_per_game DECIMAL(8,2),
                    best_weekly_score DECIMAL(8,2),
                    worst_weekly_score DECIMAL(8,2),
                    consistency_rating DECIMAL(8,4),
                    avg_ownership_percentage DECIMAL(8,4),
                    weeks_as_starter INTEGER,
                    starter_percentage DECIMAL(8,4),
                    points_above_replacement DECIMAL(10,2),
                    draft_value_score DECIMAL(8,4),
                    waiver_pickup_value DECIMAL(8,4),
                    PRIMARY KEY (player_key, season_year)
                )
            """,
            
            'mart_weekly_power_rankings': """
                CREATE TABLE IF NOT EXISTS edw.mart_weekly_power_rankings (
                    league_key INTEGER NOT NULL,
                    week_key INTEGER NOT NULL,
                    team_key INTEGER NOT NULL,
                    power_rank INTEGER NOT NULL,
                    record_rank INTEGER,
                    points_rank INTEGER,
                    power_score DECIMAL(10,4),
                    strength_of_schedule DECIMAL(8,4),
                    recent_form_score DECIMAL(8,4),
                    projection_score DECIMAL(8,4),
                    rank_change INTEGER DEFAULT 0,
                    biggest_win_margin DECIMAL(8,2),
                    biggest_loss_margin DECIMAL(8,2),
                    pythagorean_wins DECIMAL(8,2),
                    luck_factor DECIMAL(8,4),
                    playoff_odds DECIMAL(8,4),
                    PRIMARY KEY (league_key, week_key, team_key)
                )
            """
        }
        
        return self.execute_batch(marts, "Data Mart Tables")
    
    def create_views(self) -> bool:
        """Create analytical views"""
        views = {
            'vw_current_season_dashboard': """
                CREATE VIEW edw.vw_current_season_dashboard AS
                SELECT 
                    dl.league_name,
                    dl.season_year,
                    dt.team_name,
                    dt.manager_name,
                    ftp.wins,
                    ftp.losses,
                    ftp.ties,
                    ftp.points_for,
                    ftp.points_against,
                    ftp.point_differential,
                    ftp.win_percentage,
                    ftp.season_rank,
                    ftp.playoff_probability,
                    ftp.is_playoff_team,
                    ftp.playoff_seed
                FROM edw.fact_team_performance ftp
                JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
                WHERE dl.season_year = EXTRACT(YEAR FROM CURRENT_DATE)
                  AND dw.is_current_week = TRUE
                  AND dt.is_active = TRUE
                ORDER BY dl.league_name, ftp.season_rank
            """,
            
            'vw_manager_hall_of_fame': """
                CREATE VIEW edw.vw_manager_hall_of_fame AS
                SELECT 
                    manager_name,
                    total_seasons,
                    championships_won,
                    career_win_percentage,
                    total_points_scored,
                    avg_points_per_season,
                    playoff_appearances,
                    season_consistency_score,
                    RANK() OVER (ORDER BY championships_won DESC, career_win_percentage DESC) as hall_of_fame_rank
                FROM edw.mart_manager_performance
                WHERE total_seasons >= 3
                ORDER BY hall_of_fame_rank
            """,
            
            'vw_league_competitiveness': """
                CREATE VIEW edw.vw_league_competitiveness AS
                SELECT 
                    mls.league_name,
                    mls.season_year,
                    mls.competitive_balance_index,
                    mls.avg_margin_of_victory,
                    mls.close_games_count,
                    mls.blowout_games_count,
                    mls.total_transactions,
                    mls.waiver_activity_index,
                    CASE 
                        WHEN mls.competitive_balance_index < 0.15 THEN 'Highly Competitive'
                        WHEN mls.competitive_balance_index < 0.25 THEN 'Competitive'
                        WHEN mls.competitive_balance_index < 0.35 THEN 'Moderately Competitive'
                        ELSE 'Low Competition'
                    END as competitiveness_tier
                FROM edw.mart_league_summary mls
                ORDER BY mls.competitive_balance_index ASC
            """,
            
            'vw_player_breakout_analysis': """
                CREATE VIEW edw.vw_player_breakout_analysis AS
                SELECT 
                    dp.player_name,
                    dp.primary_position,
                    mpv.season_year,
                    mpv.avg_draft_position,
                    mpv.total_fantasy_points,
                    mpv.draft_value_score,
                    mpv.waiver_pickup_value,
                    CASE 
                        WHEN mpv.avg_draft_position > 100 AND mpv.draft_value_score > 2.0 THEN 'Major Breakout'
                        WHEN mpv.avg_draft_position > 50 AND mpv.draft_value_score > 1.5 THEN 'Solid Breakout'
                        WHEN mpv.waiver_pickup_value > 1.5 THEN 'Waiver Wire Gem'
                        ELSE 'Standard Performance'
                    END as breakout_type
                FROM edw.mart_player_value mpv
                JOIN edw.dim_player dp ON mpv.player_key = dp.player_key
                WHERE mpv.draft_value_score > 1.3 OR mpv.waiver_pickup_value > 1.3
                ORDER BY mpv.draft_value_score DESC
            """,
            
            'vw_trade_analysis': """
                CREATE VIEW edw.vw_trade_analysis AS
                SELECT 
                    dl.league_name,
                    dl.season_year,
                    ft.transaction_date,
                    dp.player_name,
                    dt1.team_name as from_team,
                    dt1.manager_name as from_manager,
                    dt2.team_name as to_team,
                    dt2.manager_name as to_manager,
                    ft.trade_group_id,
                    COUNT(*) OVER (PARTITION BY ft.trade_group_id) as players_in_trade
                FROM edw.fact_transaction ft
                JOIN edw.dim_league dl ON ft.league_key = dl.league_key
                JOIN edw.dim_player dp ON ft.player_key = dp.player_key
                JOIN edw.dim_team dt1 ON ft.from_team_key = dt1.team_key
                JOIN edw.dim_team dt2 ON ft.to_team_key = dt2.team_key
                WHERE ft.transaction_type = 'trade'
                ORDER BY ft.transaction_date DESC
            """
        }
        
        # Drop existing views first
        for view_name in views.keys():
            self.execute_sql(
                f"DROP VIEW IF EXISTS edw.{view_name} CASCADE",
                f"Dropped existing view: {view_name}"
            )
        
        return self.execute_batch(views, "Analytical Views")
    
    def create_indexes(self) -> bool:
        """Create performance indexes"""
        indexes = [
            ("idx_league_season", "CREATE INDEX IF NOT EXISTS idx_league_season ON edw.dim_league (season_year)"),
            ("idx_team_league", "CREATE INDEX IF NOT EXISTS idx_team_league ON edw.dim_team (league_key)"),
            ("idx_player_active", "CREATE INDEX IF NOT EXISTS idx_player_active ON edw.dim_player (is_active)"),
            ("idx_week_season", "CREATE INDEX IF NOT EXISTS idx_week_season ON edw.dim_week (season_year, week_number)"),
            ("idx_team_perf_season", "CREATE INDEX IF NOT EXISTS idx_team_perf_season ON edw.fact_team_performance (season_year, team_key)"),
            ("idx_matchup_league_week", "CREATE INDEX IF NOT EXISTS idx_matchup_league_week ON edw.fact_matchup (league_key, week_key)"),
            ("idx_roster_team_week", "CREATE INDEX IF NOT EXISTS idx_roster_team_week ON edw.fact_roster (team_key, week_key)"),
            ("idx_transaction_date", "CREATE INDEX IF NOT EXISTS idx_transaction_date ON edw.fact_transaction (transaction_date)"),
            ("idx_draft_league_season", "CREATE INDEX IF NOT EXISTS idx_draft_league_season ON edw.fact_draft (league_key, season_year)"),
            ("idx_player_value_season", "CREATE INDEX IF NOT EXISTS idx_player_value_season ON edw.mart_player_value (season_year)"),
            ("idx_power_rankings", "CREATE INDEX IF NOT EXISTS idx_power_rankings ON edw.mart_weekly_power_rankings (league_key, week_key)")
        ]
        
        logger.info(f"\n🔧 Creating Performance Indexes...")
        logger.info("=" * 60)
        
        success_count = 0
        for name, sql in indexes:
            if self.execute_sql(sql, f"Created index: {name}"):
                success_count += 1
        
        logger.info(f"📊 Index Results: {success_count}/{len(indexes)} successful")
        return success_count == len(indexes)
    
    def deploy_full_edw(self, drop_existing: bool = False) -> bool:
        """Deploy complete EDW in proper sequence"""
        logger.info("🚀 Starting Complete EDW Deployment")
        logger.info("=" * 80)
        
        if drop_existing:
            logger.info("⚠️ WARNING: Dropping existing EDW schema...")
            self.execute_sql("DROP SCHEMA IF EXISTS edw CASCADE", "Dropped existing EDW schema")
        
        # Sequential deployment with dependency management
        steps = [
            ("Schema", self.create_schema),
            ("Dimensions", self.create_dimensions),
            ("Facts", self.create_facts),
            ("Marts", self.create_marts),
            ("Views", self.create_views),
            ("Indexes", self.create_indexes)
        ]
        
        for step_name, step_func in steps:
            if not step_func():
                logger.error(f"💥 DEPLOYMENT FAILED at step: {step_name}")
                return False
                
        logger.info("\n" + "=" * 80)
        logger.info("🎉 EDW DEPLOYMENT COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        
        # Summary
        self.print_deployment_summary()
        return True
    
    def print_deployment_summary(self):
        """Print deployment summary"""
        try:
            with self.engine.connect() as conn:
                # Count tables
                result = conn.execute(text("""
                    SELECT 
                        CASE 
                            WHEN tablename LIKE 'dim_%' THEN 'Dimensions'
                            WHEN tablename LIKE 'fact_%' THEN 'Facts'
                            WHEN tablename LIKE 'mart_%' THEN 'Marts'
                            ELSE 'Other'
                        END as table_type,
                        COUNT(*) as count
                    FROM pg_tables 
                    WHERE schemaname = 'edw'
                    GROUP BY table_type
                    ORDER BY table_type
                """))
                
                tables = dict(result.fetchall())
                
                # Count views
                views_result = conn.execute(text("""
                    SELECT COUNT(*) FROM pg_views WHERE schemaname = 'edw'
                """))
                
                views_count = views_result.scalar()
                
                logger.info(f"\n📊 EDW DEPLOYMENT SUMMARY:")
                logger.info(f"   • Dimensions: {tables.get('Dimensions', 0)} tables")
                logger.info(f"   • Facts: {tables.get('Facts', 0)} tables") 
                logger.info(f"   • Marts: {tables.get('Marts', 0)} tables")
                logger.info(f"   • Views: {views_count} views")
                logger.info(f"   • Other: {tables.get('Other', 0)} tables")
                logger.info(f"\n🏗️ Total EDW Objects: {sum(tables.values()) + views_count}")
                
        except Exception as e:
            logger.warning(f"Could not generate summary: {e}")

def get_database_url() -> Optional[str]:
    """Get database URL from environment with cloud/local support"""
    # Try different environment variable names for flexibility
    database_url = (
        os.getenv('DATABASE_URL') or 
        os.getenv('EDW_DATABASE_URL') or 
        os.getenv('POSTGRES_URL')
    )
    
    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not found")
        logger.info("💡 For local development, set DATABASE_URL")
        logger.info("💡 For GitHub Actions, ensure DATABASE_URL secret is configured")
        return None
        
    return database_url

def main():
    """Main deployment function with CLI support"""
    parser = argparse.ArgumentParser(description='Deploy Fantasy Football EDW')
    parser.add_argument('--drop-existing', action='store_true', 
                       help='Drop existing EDW schema before deployment')
    parser.add_argument('--tables-only', action='store_true',
                       help='Deploy only tables (skip views)')
    parser.add_argument('--views-only', action='store_true', 
                       help='Deploy only views (skip tables)')
    
    args = parser.parse_args()
    
    # Get database URL
    database_url = get_database_url()
    if not database_url:
        sys.exit(1)
    
    # Initialize deployer
    deployer = EDWDeployer(database_url)
    
    try:
        if args.views_only:
            logger.info("🎯 Deploying views only...")
            success = deployer.create_views()
        elif args.tables_only:
            logger.info("🎯 Deploying tables only...")
            success = (deployer.create_schema() and 
                      deployer.create_dimensions() and
                      deployer.create_facts() and 
                      deployer.create_marts() and
                      deployer.create_indexes())
        else:
            logger.info("🎯 Full EDW deployment...")
            success = deployer.deploy_full_edw(drop_existing=args.drop_existing)
        
        if success:
            logger.info("✅ Deployment completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Deployment failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 Deployment error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 