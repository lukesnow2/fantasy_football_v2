#!/usr/bin/env python3
"""
EDW Deployment Test Suite

Comprehensive test suite to validate the Enterprise Data Warehouse deployment.
This should run after EDW deployment to ensure everything is working correctly.

Tests include:
- Schema existence and structure
- Table creation and constraints
- View functionality
- Index performance
- Data integrity checks

Usage:
    python test_edw_deployment.py [--verbose] [--fast]
"""

import os
import sys
import argparse
import logging
from typing import Dict, List, Tuple, Optional, Any
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class EDWTester:
    """Enterprise Data Warehouse test suite"""
    
    def __init__(self, database_url: str, verbose: bool = False):
        """Initialize with database connection"""
        self.database_url = database_url.replace('postgres://', 'postgresql://', 1)
        self.engine = create_engine(self.database_url)
        self.verbose = verbose
        self.test_results = []
        
    def run_test(self, test_name: str, test_func) -> bool:
        """Run a single test with error handling"""
        try:
            result = test_func()
            if result:
                logger.info(f"✅ {test_name}")
                self.test_results.append((test_name, True, None))
            else:
                logger.error(f"❌ {test_name}")
                self.test_results.append((test_name, False, "Test returned False"))
            return result
        except Exception as e:
            logger.error(f"❌ {test_name} - ERROR: {e}")
            self.test_results.append((test_name, False, str(e)))
            return False
    
    def test_schema_exists(self) -> bool:
        """Test that EDW schema exists"""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = 'edw'
            """))
            return result.fetchone() is not None
    
    def test_dimension_tables(self) -> bool:
        """Test that all dimension tables exist with correct structure"""
        expected_dims = [
            'dim_season', 'dim_league', 'dim_team', 'dim_player', 'dim_week', 'edw_metadata'
        ]
        
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'edw' 
                  AND tablename LIKE 'dim_%' OR tablename = 'edw_metadata'
            """))
            
            actual_dims = [row[0] for row in result.fetchall()]
            missing_dims = set(expected_dims) - set(actual_dims)
            
            if missing_dims:
                logger.error(f"Missing dimension tables: {missing_dims}")
                return False
                
            if self.verbose:
                logger.info(f"Found dimension tables: {sorted(actual_dims)}")
            return True
    
    def test_fact_tables(self) -> bool:
        """Test that all fact tables exist"""
        expected_facts = [
            'fact_team_performance', 'fact_matchup', 'fact_roster', 
            'fact_transaction', 'fact_draft'
        ]
        
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'edw' 
                  AND tablename LIKE 'fact_%'
            """))
            
            actual_facts = [row[0] for row in result.fetchall()]
            missing_facts = set(expected_facts) - set(actual_facts)
            
            if missing_facts:
                logger.error(f"Missing fact tables: {missing_facts}")
                return False
                
            if self.verbose:
                logger.info(f"Found fact tables: {sorted(actual_facts)}")
            return True
    
    def test_mart_tables(self) -> bool:
        """Test that all mart tables exist"""
        expected_marts = [
            'mart_league_summary', 'mart_manager_performance', 
            'mart_player_value', 'mart_weekly_power_rankings'
        ]
        
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'edw' 
                  AND tablename LIKE 'mart_%'
            """))
            
            actual_marts = [row[0] for row in result.fetchall()]
            missing_marts = set(expected_marts) - set(actual_marts)
            
            if missing_marts:
                logger.error(f"Missing mart tables: {missing_marts}")
                return False
                
            if self.verbose:
                logger.info(f"Found mart tables: {sorted(actual_marts)}")
            return True
    
    def test_views_exist(self) -> bool:
        """Test that all analytical views exist"""
        expected_views = [
            'vw_current_season_dashboard', 'vw_manager_hall_of_fame',
            'vw_league_competitiveness', 'vw_player_breakout_analysis',
            'vw_trade_analysis'
        ]
        
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT viewname 
                FROM pg_views 
                WHERE schemaname = 'edw'
            """))
            
            actual_views = [row[0] for row in result.fetchall()]
            missing_views = set(expected_views) - set(actual_views)
            
            if missing_views:
                logger.error(f"Missing views: {missing_views}")
                return False
                
            if self.verbose:
                logger.info(f"Found views: {sorted(actual_views)}")
            return True
    
    def test_views_functional(self) -> bool:
        """Test that views can be queried without errors"""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT viewname 
                FROM pg_views 
                WHERE schemaname = 'edw'
            """))
            
            views = [row[0] for row in result.fetchall()]
            
            for view in views:
                try:
                    # Test that view can be queried
                    conn.execute(text(f"SELECT COUNT(*) FROM edw.{view}"))
                    if self.verbose:
                        logger.info(f"View {view} is functional")
                except Exception as e:
                    logger.error(f"View {view} query failed: {e}")
                    return False
                    
        return True
    
    def test_primary_keys(self) -> bool:
        """Test that primary keys exist on critical tables"""
        critical_tables = [
            'dim_season', 'dim_league', 'dim_team', 'dim_player', 'dim_week',
            'fact_team_performance', 'fact_matchup', 'fact_roster', 
            'fact_transaction', 'fact_draft'
        ]
        
        with self.engine.connect() as conn:
            for table in critical_tables:
                result = conn.execute(text(f"""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_schema = 'edw' 
                      AND table_name = '{table}' 
                      AND constraint_type = 'PRIMARY KEY'
                """))
                
                if not result.fetchone():
                    logger.error(f"Table {table} missing primary key")
                    return False
                    
        if self.verbose:
            logger.info("All critical tables have primary keys")
        return True
    
    def test_indexes_exist(self) -> bool:
        """Test that performance indexes exist"""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as index_count
                FROM pg_indexes 
                WHERE schemaname = 'edw'
            """))
            
            index_count = result.scalar()
            
            # Should have at least 10 indexes for performance
            if index_count < 10:
                logger.error(f"Only {index_count} indexes found, expected at least 10")
                return False
                
            if self.verbose:
                logger.info(f"Found {index_count} indexes")
            return True
    
    def test_table_counts(self) -> bool:
        """Test table record counts and report status"""
        with self.engine.connect() as conn:
            # Get all EDW tables
            result = conn.execute(text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'edw' 
                  AND tablename NOT LIKE 'vw_%'
                ORDER BY tablename
            """))
            
            tables = [row[0] for row in result.fetchall()]
            empty_tables = []
            populated_tables = []
            
            for table in tables:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM edw.{table}"))
                count = count_result.scalar()
                
                if count == 0:
                    empty_tables.append(table)
                else:
                    populated_tables.append((table, count))
                    
            if self.verbose:
                if populated_tables:
                    logger.info("Populated tables:")
                    for table, count in populated_tables:
                        logger.info(f"  • {table}: {count:,} records")
                        
                if empty_tables:
                    logger.info(f"Empty tables (ready for data): {len(empty_tables)}")
                    if self.verbose:
                        for table in empty_tables:
                            logger.info(f"  • {table}")
                            
            return True  # Empty tables are OK, they're ready for data
    
    def test_metadata_table(self) -> bool:
        """Test that metadata table exists and has structure"""
        with self.engine.connect() as conn:
            # Check table exists
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'edw' 
                  AND table_name = 'edw_metadata'
            """))
            
            if result.scalar() == 0:
                logger.error("edw_metadata table does not exist")
                return False
                
            # Check that table has some basic columns (flexible check)
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'edw' 
                  AND table_name = 'edw_metadata'
            """))
            
            actual_columns = [row[0] for row in result.fetchall()]
            
            # Must have at least table_name column
            if 'table_name' not in actual_columns:
                logger.error("edw_metadata missing required 'table_name' column")
                return False
                
            if len(actual_columns) < 3:
                logger.error(f"edw_metadata has only {len(actual_columns)} columns, expected at least 3")
                return False
                
            if self.verbose:
                logger.info("edw_metadata table structure is correct")
            return True
    
    def run_full_test_suite(self, fast_mode: bool = False) -> bool:
        """Run complete test suite"""
        logger.info("🧪 Starting EDW Deployment Test Suite")
        logger.info("=" * 80)
        
        # Define test suite
        tests = [
            ("Schema Exists", self.test_schema_exists),
            ("Dimension Tables", self.test_dimension_tables),
            ("Fact Tables", self.test_fact_tables),
            ("Mart Tables", self.test_mart_tables),
            ("Views Exist", self.test_views_exist),
            ("Views Functional", self.test_views_functional),
            ("Primary Keys", self.test_primary_keys),
            ("Metadata Table", self.test_metadata_table),
        ]
        
        # Add comprehensive tests if not in fast mode
        if not fast_mode:
            tests.extend([
                ("Indexes Exist", self.test_indexes_exist),
                ("Table Counts", self.test_table_counts),
            ])
        
        # Run all tests
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            if self.run_test(test_name, test_func):
                passed += 1
        
        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("🏁 TEST SUITE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Tests Passed: {passed}/{total}")
        logger.info(f"Success Rate: {(passed/total)*100:.1f}%")
        
        if passed == total:
            logger.info("🎉 ALL TESTS PASSED! EDW deployment is successful!")
            return True
        else:
            logger.error("❌ Some tests failed. Check deployment.")
            
            # Show failed tests
            failed_tests = [name for name, passed, error in self.test_results if not passed]
            if failed_tests:
                logger.error(f"Failed tests: {', '.join(failed_tests)}")
                
            return False

def get_database_url() -> Optional[str]:
    """Get database URL from environment"""
    database_url = (
        os.getenv('DATABASE_URL') or 
        os.getenv('EDW_DATABASE_URL') or 
        os.getenv('POSTGRES_URL')
    )
    
    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not found")
        return None
        
    return database_url

def main():
    """Main test function with CLI support"""
    parser = argparse.ArgumentParser(description='Test Fantasy Football EDW Deployment')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Verbose output with detailed information')
    parser.add_argument('--fast', action='store_true',
                       help='Run faster test suite (skip comprehensive checks)')
    
    args = parser.parse_args()
    
    # Get database URL
    database_url = get_database_url()
    if not database_url:
        sys.exit(1)
    
    # Initialize tester
    tester = EDWTester(database_url, verbose=args.verbose)
    
    try:
        success = tester.run_full_test_suite(fast_mode=args.fast)
        
        if success:
            logger.info("✅ EDW deployment test suite completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ EDW deployment test suite failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 Test suite error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 