#!/usr/bin/env python3
"""
Complete EDW Deployment Script
Enhanced with better verification and automatic view fixes

This script handles the complete deployment of the Fantasy Football EDW:
1. Deploy/update schema objects
2. Run ETL to populate tables
3. Fix analytical views automatically
4. Verify data quality and completeness with enhanced checks

Usage:
    python deploy_complete_edw.py [--database-url URL] [--force-rebuild] [--verify-only]

Environment Variables:
    DATABASE_URL: PostgreSQL connection string
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from sqlalchemy import create_engine, text
from edw_etl_processor import EdwEtlProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class EdwDeployment:
    """
    Enhanced EDW deployment with improved verification and automatic fixes
    """
    
    def __init__(self, database_url: str, force_rebuild: bool = False):
        self.database_url = database_url
        self.force_rebuild = force_rebuild
        self.engine = None
        self.deployment_stats = {
            'start_time': datetime.now(),
            'schema_objects': 0,
            'dimension_records': 0,
            'fact_records': 0,
            'verification_passed': False,
            'views_fixed': 0
        }
        
        # Expected counts for verification
        self.expected_counts = {
            'leagues': 20,
            'seasons': 20,
            'weeks': 324,
            'matchups': 1499,
            'transactions': 9691,
            'draft_picks': 3192,
            'teams': 196
        }
    
    def connect_database(self) -> bool:
        """Connect to the database"""
        try:
            logger.info("🔌 Connecting to database...")
            
            url = self.database_url
            if url.startswith('postgres://'):
                url = url.replace('postgres://', 'postgresql://', 1)
            
            self.engine = create_engine(url)
            
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"✅ Database Connected: {version.split()[0:2]}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def deploy_schema(self) -> bool:
        """Deploy or update EDW schema using reliable direct SQL approach"""
        try:
            logger.info("🏗️ Deploying EDW schema...")
            
            # Import psycopg2 for direct connection (more reliable than SQLAlchemy for schema work)
            import psycopg2
            
            # Connect using psycopg2 directly for better transaction control
            conn = psycopg2.connect(self.database_url)
            conn.autocommit = True
            cur = conn.cursor()
            
            # 1. Ensure EDW schema exists
            logger.info("📋 Ensuring EDW schema exists...")
            cur.execute("CREATE SCHEMA IF NOT EXISTS edw")
            logger.info("✅ EDW schema ready")
            
            # 2. Check which tables exist
            expected_tables = ['dim_season', 'dim_league', 'dim_team', 'dim_player', 'dim_manager', 'dim_week',
                             'fact_roster', 'fact_team_performance', 'fact_matchup', 'fact_transaction', 'fact_draft', 
                             'fact_player_statistics', 'mart_league_summary', 'mart_manager_performance', 
                             'mart_player_value', 'mart_weekly_power_rankings']
            
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'edw'
            """)
            existing_tables = {row[0] for row in cur.fetchall()}
            missing_tables = set(expected_tables) - existing_tables
            
            logger.info(f"📋 Found {len(existing_tables)} existing tables: {sorted(existing_tables)}")
            if missing_tables:
                logger.info(f"📋 Missing {len(missing_tables)} tables: {sorted(missing_tables)}")
            
            if not missing_tables:
                logger.info("✅ All expected tables exist")
                cur.close()
                conn.close()
                return True
            
            # 3. Read and parse schema file for complete definitions with constraints
            schema_file = 'src/edw_schema/fantasy_edw_schema.sql'
            if not os.path.exists(schema_file):
                schema_file = 'fantasy_edw_schema.sql'  # Fallback
            
            if not os.path.exists(schema_file):
                logger.error(f"❌ Schema file not found: {schema_file}")
                cur.close()
                conn.close()
                return False
            
            logger.info(f"📋 Reading schema from: {schema_file}")
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            # 4. Parse SQL statements and organize by type
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            
            create_table_stmts = []
            create_index_stmts = []
            create_view_stmts = []
            alter_stmts = []
            
            for stmt in statements:
                stmt_upper = stmt.upper().strip()
                if 'CREATE TABLE' in stmt_upper:
                    create_table_stmts.append(stmt)
                elif 'CREATE INDEX' in stmt_upper or 'CREATE UNIQUE INDEX' in stmt_upper:
                    create_index_stmts.append(stmt)
                elif 'CREATE VIEW' in stmt_upper:
                    create_view_stmts.append(stmt)
                elif 'ALTER TABLE' in stmt_upper:
                    alter_stmts.append(stmt)
            
            logger.info(f"📋 Found {len(create_table_stmts)} tables, {len(create_index_stmts)} indexes, {len(create_view_stmts)} views, {len(alter_stmts)} constraints")
            
            # 5. Create missing tables only (with foreign keys and constraints)
            logger.info("🏗️ Creating missing tables...")
            tables_created = 0
            
            for stmt in create_table_stmts:
                # Extract table name for logging
                import re
                match = re.search(r'CREATE\s+TABLE\s+(\w+)', stmt, re.IGNORECASE)
                table_name = match.group(1) if match else "unknown"
                
                if table_name in missing_tables:
                    try:
                        logger.info(f"  📋 Creating {table_name}...")
                        cur.execute(stmt)
                        logger.info(f"  ✅ {table_name} created successfully")
                        tables_created += 1
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            logger.info(f"  ✓ {table_name} already exists")
                        else:
                            logger.error(f"  ❌ Failed to create {table_name}: {e}")
                            # Continue with next table
                else:
                    logger.info(f"  ✓ {table_name} already exists")
            
            # 6. Create indexes (performance optimization)
            logger.info("📋 Creating indexes...")
            indexes_created = 0
            for stmt in create_index_stmts:
                try:
                    cur.execute(stmt)
                    indexes_created += 1
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.debug(f"  ⚠️ Index warning: {str(e)[:100]}...")
            
            # 7. Create views (analytics)
            logger.info("👁️ Creating views...")
            views_created = 0
            for stmt in create_view_stmts:
                try:
                    cur.execute(stmt)
                    views_created += 1
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.debug(f"  ⚠️ View warning: {str(e)[:100]}...")
            
            # 8. Add additional constraints
            logger.info("🔗 Adding constraints...")
            constraints_added = 0
            for stmt in alter_stmts:
                try:
                    cur.execute(stmt)
                    constraints_added += 1
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.debug(f"  ⚠️ Constraint warning: {str(e)[:100]}...")
            
            total_objects = tables_created + indexes_created + views_created + constraints_added
            self.deployment_stats['schema_objects'] = total_objects
            
            logger.info(f"✅ Schema deployment complete:")
            logger.info(f"  📊 Tables: {tables_created} created")
            logger.info(f"  📋 Indexes: {indexes_created} created") 
            logger.info(f"  👁️ Views: {views_created} created")
            logger.info(f"  🔗 Constraints: {constraints_added} added")
            logger.info(f"  🎯 Total: {total_objects} schema objects processed")
            
            cur.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema deployment failed: {e}")
            return False
    
    def truncate_edw_tables(self) -> bool:
        """Truncate EDW tables for clean rebuild if requested"""
        if not self.force_rebuild:
            logger.info("ℹ️ Skipping table truncation (use --force-rebuild for clean rebuild)")
            return True
        
        try:
            logger.info("🗑️ Truncating EDW tables for clean rebuild...")
            
            edw_tables = [
                'fact_team_performance', 'fact_matchup', 'fact_transaction', 
                'fact_draft', 'fact_roster',
                'dim_team', 'dim_player', 'dim_league', 'dim_week', 'dim_season', 'dim_manager'
            ]
            
            with self.engine.connect() as conn:
                for table in edw_tables:
                    try:
                        conn.execute(text(f"TRUNCATE TABLE edw.{table} RESTART IDENTITY CASCADE"))
                        logger.info(f"  ✅ Truncated {table}")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Could not truncate {table}: {e}")
                
                conn.commit()
                logger.info("✅ Table truncation completed")
            
            return True
        except Exception as e:
            logger.error(f"❌ Table truncation failed: {e}")
            return False
    
    def run_etl(self) -> bool:
        """Run the ETL process"""
        try:
            logger.info("🚀 Running ETL process...")
            etl = EdwEtlProcessor(self.database_url, force_rebuild=self.force_rebuild)
            
            if etl.run_etl():
                logger.info("✅ ETL process completed successfully")
                return True
            else:
                logger.error("❌ ETL process failed")
                return False
        except Exception as e:
            logger.error(f"❌ ETL process error: {e}")
            return False
    
    def fix_analytical_views(self) -> bool:
        """Fix analytical views that reference empty mart tables"""
        try:
            logger.info("🔧 Fixing analytical views...")
            
            with self.engine.connect() as conn:
                views_fixed = 0
                
                # Fix vw_current_season_dashboard with dynamic season rollover
                logger.info("  📊 Fixing vw_current_season_dashboard...")
                conn.execute(text('DROP VIEW IF EXISTS edw.vw_current_season_dashboard'))
                current_season_view = """
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
                WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft)
                  AND dt.is_active = TRUE
                ORDER BY dl.league_name, ftp.season_rank
                """
                conn.execute(text(current_season_view))
                views_fixed += 1
                
                # Fix vw_manager_hall_of_fame
                logger.info("  📊 Fixing vw_manager_hall_of_fame...")
                conn.execute(text('DROP VIEW IF EXISTS edw.vw_manager_hall_of_fame'))
                hall_of_fame_view = """
                CREATE VIEW edw.vw_manager_hall_of_fame AS
                WITH manager_stats AS (
                    SELECT 
                        dt.manager_name,
                        COUNT(DISTINCT dl.season_year) as total_seasons,
                        SUM(CASE WHEN ftp.season_rank = 1 THEN 1 ELSE 0 END) as championships_won,
                        AVG(ftp.win_percentage) as career_win_percentage,
                        SUM(ftp.points_for) as total_points_scored,
                        AVG(ftp.points_for) as avg_points_per_season,
                        SUM(CASE WHEN ftp.is_playoff_team THEN 1 ELSE 0 END) as playoff_appearances,
                        STDDEV(ftp.win_percentage) as season_consistency_score
                    FROM edw.fact_team_performance ftp
                    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
                    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
                    WHERE dt.manager_name IS NOT NULL
                    GROUP BY dt.manager_name
                )
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
                FROM manager_stats
                WHERE total_seasons >= 3
                ORDER BY hall_of_fame_rank
                """
                conn.execute(text(hall_of_fame_view))
                views_fixed += 1
                
                conn.commit()
                self.deployment_stats['views_fixed'] = views_fixed
                logger.info(f"✅ Fixed {views_fixed} analytical views")
            
            return True
        except Exception as e:
            logger.error(f"❌ Failed to fix analytical views: {e}")
            return False
    
    def verify_deployment(self) -> bool:
        """Enhanced verification with better view checking"""
        try:
            logger.info("🔍 Verifying EDW deployment...")
            
            verification_passed = True
            
            with self.engine.connect() as conn:
                # 1. Check dimension table counts
                logger.info("📊 Verifying dimension tables...")
                dimension_tables = {
                    'dim_season': self.expected_counts['seasons'],
                    'dim_league': self.expected_counts['leagues'],
                    'dim_team': self.expected_counts['teams'],
                    'dim_week': None
                }
                
                total_dimension_records = 0
                for table, expected in dimension_tables.items():
                    result = conn.execute(text(f'SELECT COUNT(*) FROM edw.{table}'))
                    actual = result.scalar()
                    total_dimension_records += actual
                    
                    if expected and actual != expected:
                        logger.warning(f"  ⚠️ {table}: {actual} records (expected: {expected})")
                        if table in ['dim_season', 'dim_league']:
                            verification_passed = False
                    else:
                        logger.info(f"  ✅ {table}: {actual} records")
                
                # 2. Check fact table counts
                logger.info("📊 Verifying fact tables...")
                fact_tables = {
                    'fact_roster': None,  # Variable based on roster data availability
                    'fact_matchup': self.expected_counts['matchups'],
                    'fact_transaction': self.expected_counts['transactions'],
                    'fact_draft': self.expected_counts['draft_picks']
                }
                
                total_fact_records = 0
                for table, expected in fact_tables.items():
                    result = conn.execute(text(f'SELECT COUNT(*) FROM edw.{table}'))
                    actual = result.scalar()
                    total_fact_records += actual
                    
                    if expected and actual < expected * 0.9:
                        logger.warning(f"  ⚠️ {table}: {actual} records (expected: ~{expected})")
                    else:
                        logger.info(f"  ✅ {table}: {actual} records")
                
                # 3. Enhanced analytical views verification
                logger.info("👁️ Verifying analytical views...")
                views_with_expectations = [
                    ('vw_current_season_dashboard', 10, 'current season team data'),
                    ('vw_manager_hall_of_fame', 5, 'manager career statistics'),
                    ('vw_league_competitiveness', 1, 'league analysis'),
                    ('vw_player_breakout_analysis', 1, 'player performance'),
                    ('vw_trade_analysis', 100, 'trade transactions')
                ]
                
                view_issues = 0
                for view, min_expected, description in views_with_expectations:
                    try:
                        result = conn.execute(text(f'SELECT COUNT(*) FROM edw.{view}'))
                        count = result.scalar()
                        if count >= min_expected:
                            logger.info(f"  ✅ {view}: {count} records ({description})")
                        else:
                            logger.warning(f"  ⚠️ {view}: {count} records (expected >= {min_expected} for {description})")
                            view_issues += 1
                    except Exception as e:
                        logger.error(f"  ❌ {view}: Error - {e}")
                        view_issues += 1
                
                # 4. Critical checks that should fail verification
                if view_issues > 3:  # If most views are broken
                    logger.error(f"❌ {view_issues} views have critical issues")
                    verification_passed = False
                elif view_issues > 0:
                    logger.warning(f"⚠️ {view_issues} views have minor issues")
                
                # 5. League of record verification
                logger.info("🏈 Verifying league of record filtering...")
                result = conn.execute(text('SELECT COUNT(DISTINCT league_id) FROM edw.dim_league'))
                unique_leagues = result.scalar()
                
                if unique_leagues != self.expected_counts['leagues']:
                    logger.error(f"  ❌ Found {unique_leagues} leagues (expected: {self.expected_counts['leagues']})")
                    verification_passed = False
                else:
                    logger.info(f"  ✅ Found {unique_leagues} leagues (correct)")
                
                self.deployment_stats['dimension_records'] = total_dimension_records
                self.deployment_stats['fact_records'] = total_fact_records
                self.deployment_stats['verification_passed'] = verification_passed
                
                if verification_passed:
                    logger.info("✅ Deployment verification PASSED")
                else:
                    logger.error("❌ Deployment verification FAILED")
                
                return verification_passed
                
        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return False
    
    def print_deployment_summary(self):
        """Print comprehensive deployment summary"""
        end_time = datetime.now()
        runtime = end_time - self.deployment_stats['start_time']
        
        print("\n" + "="*70)
        print("🎉 ENHANCED EDW DEPLOYMENT SUMMARY")
        print("="*70)
        print(f"📅 Deployment Date: {self.deployment_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ Total Runtime: {runtime}")
        print(f"🏗️ Schema Objects: {self.deployment_stats['schema_objects']} processed")
        print(f"📊 Dimension Records: {self.deployment_stats['dimension_records']:,}")
        print(f"📈 Fact Records: {self.deployment_stats['fact_records']:,}")
        print(f"🔧 Views Fixed: {self.deployment_stats['views_fixed']}")
        print(f"✅ Verification: {'PASSED' if self.deployment_stats['verification_passed'] else 'FAILED'}")
        
        print("\n🔧 DEPLOYMENT IMPROVEMENTS:")
        print("  📊 Enhanced view verification with data expectations")
        print("  🛠️ Automatic analytical view fixes")
        print("  🔍 Better error detection and reporting")
        print("  📈 Data quality thresholds")
        print("  🔄 Dynamic season rollover for current season dashboard")
        
        print("\n🚀 EDW IS READY FOR ANALYTICS!")
        print("="*70)
    
    def deploy(self) -> bool:
        """Execute enhanced deployment workflow"""
        logger.info("🚀 Starting Enhanced EDW Deployment")
        logger.info("="*70)
        
        steps = [
            ("Connect to Database", self.connect_database),
            ("Deploy Schema", self.deploy_schema),
            ("Truncate Tables (if requested)", self.truncate_edw_tables),
            ("Run ETL Process", self.run_etl),
            ("Fix Analytical Views", self.fix_analytical_views),
            ("Verify Deployment", self.verify_deployment)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"📋 Step: {step_name}")
            if not step_func():
                logger.error(f"❌ Deployment failed at step: {step_name}")
                return False
        
        self.print_deployment_summary()
        return True

def main():
    """Main deployment entry point"""
    parser = argparse.ArgumentParser(description='Enhanced Fantasy Football EDW Deployment')
    parser.add_argument('--database-url', 
                       help='Database URL (or set DATABASE_URL env var)')
    parser.add_argument('--force-rebuild', action='store_true',
                       help='Force complete rebuild (truncate all tables)')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only run verification (skip deployment)')
    
    args = parser.parse_args()
    
    database_url = args.database_url or os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("❌ DATABASE_URL required: set as environment variable or pass directly")
        sys.exit(1)
    
    try:
        deployment = EdwDeployment(database_url, args.force_rebuild)
        
        if args.verify_only:
            logger.info("🔍 Running enhanced verification only...")
            if (deployment.connect_database() and 
                deployment.fix_analytical_views() and 
                deployment.verify_deployment()):
                deployment.print_deployment_summary()
                logger.info("✅ Enhanced verification completed successfully")
            else:
                logger.error("❌ Enhanced verification failed")
                sys.exit(1)
        else:
            if deployment.deploy():
                logger.info("🎊 Enhanced EDW deployment successful!")
            else:
                logger.error("❌ Enhanced EDW deployment failed")
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"❌ Deployment error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 