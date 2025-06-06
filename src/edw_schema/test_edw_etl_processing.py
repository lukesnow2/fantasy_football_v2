#!/usr/bin/env python3
"""
EDW ETL Processing Test Suite

Tests the actual dimension/fact building logic in the EDW ETL processor.
This validates data transformation, loading, and processing functionality.

Usage:
    python test_edw_etl_processing.py [--verbose] [--integration]
"""

import os
import sys
import json
import argparse
import logging
import tempfile
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Add parent directory to path
script_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(script_dir)
sys.path.insert(0, parent_dir)

from edw_schema.edw_etl_processor import EdwEtlProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class EDWETLTester:
    """Test suite for EDW ETL processing functionality"""
    
    def __init__(self, database_url: str, verbose: bool = False):
        """Initialize with database connection"""
        self.database_url = database_url.replace('postgres://', 'postgresql://', 1)
        self.engine = create_engine(self.database_url)
        self.verbose = verbose
        self.test_results = []
        
        # Sample test data
        self.sample_data = self.create_sample_data()
    
    def create_sample_data(self) -> Dict[str, List[Dict]]:
        """Create sample data for testing ETL processing"""
        return {
            'leagues': [
                {
                    'league_id': 'test_league_1',
                    'name': 'Test League 1',
                    'season': 2024,
                    'num_teams': 10,
                    'league_type': 'private',
                    'scoring_type': 'standard',
                    'draft_type': 'snake'
                }
            ],
            'teams': [
                {
                    'team_id': 'test_team_1',
                    'league_id': 'test_league_1',
                    'name': 'Test Team 1',
                    'manager_name': 'Test Manager 1',
                    'manager_id': 'mgr_001',
                    'wins': 8,
                    'losses': 5,
                    'ties': 0,
                    'points_for': 1234.56,
                    'points_against': 1123.45
                },
                {
                    'team_id': 'test_team_2',
                    'league_id': 'test_league_1',
                    'name': 'Test Team 2',
                    'manager_name': 'Test Manager 2',
                    'manager_id': 'mgr_002',
                    'wins': 6,
                    'losses': 7,
                    'ties': 0,
                    'points_for': 1156.78,
                    'points_against': 1201.23
                }
            ],
            'rosters': [
                {
                    'league_id': 'test_league_1',
                    'team_id': 'test_team_1',
                    'week': 14,
                    'season': 2024,
                    'player_id': 'player_001',
                    'player_name': 'Test Player QB',
                    'is_starter': True,
                    'position': 'QB',
                    'eligible_positions': ['QB'],
                    'points': 18.5
                }
            ],
            'transactions': [
                {
                    'league_id': 'test_league_1',
                    'transaction_id': 'txn_001',
                    'type': 'add',
                    'player_id': 'player_001',
                    'from_team_id': None,
                    'to_team_id': 'test_team_1',
                    'timestamp': '2024-12-01T10:30:00Z',
                    'faab_bid': 15
                }
            ]
        }
    
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
    
    def test_etl_processor_initialization(self) -> bool:
        """Test EDW ETL processor can be initialized"""
        try:
            processor = EdwEtlProcessor(database_url=self.database_url)
            connected = processor.connect()
            
            if not connected:
                logger.error("ETL processor failed to connect to database")
                return False
                
            if self.verbose:
                logger.info("ETL processor initialized and connected successfully")
            return True
        except Exception as e:
            logger.error(f"ETL processor initialization failed: {e}")
            return False
    
    def test_sample_data_loading(self) -> bool:
        """Test loading sample data into ETL processor"""
        try:
            # Create temporary file with sample data
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                json.dump(self.sample_data, temp_file, indent=2)
                temp_filename = temp_file.name
            
            try:
                processor = EdwEtlProcessor(
                    database_url=self.database_url,
                    data_file=temp_filename
                )
                
                connected = processor.connect()
                loaded = processor.load_data()
                
                if not (connected and loaded):
                    return False
                    
                # Verify data was loaded
                if processor.data != self.sample_data:
                    logger.error("Sample data not loaded correctly")
                    return False
                    
                if self.verbose:
                    logger.info(f"Sample data loaded: {len(processor.data)} tables")
                    
                return True
            finally:
                # Clean up temp file
                os.unlink(temp_filename)
                
        except Exception as e:
            logger.error(f"Sample data loading failed: {e}")
            return False
    
    def test_change_detection(self) -> bool:
        """Test operational change detection logic"""
        try:
            processor = EdwEtlProcessor(database_url=self.database_url)
            if not processor.connect():
                return False
                
            # Test change detection
            detected = processor.detect_operational_changes()
            
            if not detected:
                logger.error("Change detection failed")
                return False
                
            # Should detect some changes (or mark as first run)
            if len(processor.changed_tables) == 0:
                logger.warning("No changes detected - this might be expected for first run")
                
            if self.verbose:
                logger.info(f"Change detection found: {processor.changed_tables}")
                
            return True
        except Exception as e:
            logger.error(f"Change detection failed: {e}")
            return False
    
    def test_dimension_transformation(self) -> bool:
        """Test dimension data transformation logic"""
        try:
            processor = EdwEtlProcessor(database_url=self.database_url)
            if not processor.connect():
                return False
                
            # Set sample data
            processor.data = self.sample_data
            
            # Test league transformation
            leagues = processor.transform_leagues()
            if not leagues or len(leagues) == 0:
                logger.error("League transformation failed")
                return False
                
            # Check required fields
            league = leagues[0]
            required_fields = ['league_id', 'league_name', 'season_year', 'num_teams']
            for field in required_fields:
                if field not in league:
                    logger.error(f"Missing field in league transformation: {field}")
                    return False
            
            # Test team transformation
            teams = processor.transform_teams()
            if not teams or len(teams) == 0:
                logger.error("Team transformation failed")
                return False
                
            # Check required fields (league_key is added during loading, not transformation)
            team = teams[0]
            required_fields = ['team_id', 'league_id', 'team_name', 'manager_name']
            for field in required_fields:
                if field not in team:
                    logger.error(f"Missing field in team transformation: {field}")
                    return False
                    
            if self.verbose:
                logger.info(f"Transformed {len(leagues)} leagues, {len(teams)} teams")
                
            return True
        except Exception as e:
            logger.error(f"Dimension transformation failed: {e}")
            return False
    
    def test_dimension_loading(self) -> bool:
        """Test dimension table loading"""
        try:
            processor = EdwEtlProcessor(database_url=self.database_url)
            if not processor.connect():
                return False
                
            # Set sample data
            processor.data = self.sample_data
            
            # Load dimensions (this will test actual database operations)
            loaded = processor.load_dimensions()
            
            if not loaded:
                logger.error("Dimension loading failed")
                return False
                
            # Verify data was actually inserted
            with self.engine.connect() as conn:
                # Check dim_league
                league_count = conn.execute(text("""
                    SELECT COUNT(*) FROM edw.dim_league 
                    WHERE league_id = 'test_league_1'
                """)).scalar()
                
                if league_count == 0:
                    logger.error("Test league not found in dim_league")
                    return False
                    
                # Check dim_team
                team_count = conn.execute(text("""
                    SELECT COUNT(*) FROM edw.dim_team 
                    WHERE team_id IN ('test_team_1', 'test_team_2')
                """)).scalar()
                
                if team_count < 2:
                    logger.error("Test teams not found in dim_team")
                    return False
                    
            if self.verbose:
                logger.info("Dimension loading verified - data found in EDW tables")
                
            return True
        except Exception as e:
            logger.error(f"Dimension loading failed: {e}")
            return False
    
    def test_incremental_processing(self) -> bool:
        """Test incremental EDW processing logic"""
        try:
            processor = EdwEtlProcessor(database_url=self.database_url)
            if not processor.connect():
                return False
                
            # Set sample data for incremental processing
            processor.data = self.sample_data
                
            # Test incremental processing with sample changed tables
            changed_tables = {'leagues', 'teams'}
            result = processor.process_incremental_edw(changed_tables)
            
            if not result:
                logger.error("Incremental processing failed")
                return False
                
            # Check that processing stats were updated
            if processor.load_stats['tables_processed'] == 0:
                logger.warning("No tables were processed during incremental update")
                
            if self.verbose:
                logger.info(f"Incremental processing stats: {processor.load_stats}")
                
            return True
        except Exception as e:
            logger.error(f"Incremental processing failed: {e}")
            return False
    
    def test_edw_processing_strategies(self) -> bool:
        """Test EDW processing strategy mapping"""
        try:
            processor = EdwEtlProcessor(database_url=self.database_url)
            
            # Check that processing strategies are defined
            strategies = processor.EDW_PROCESSING_STRATEGIES
            
            if not strategies:
                logger.error("No EDW processing strategies defined")
                return False
                
            # Check required operational tables
            required_tables = ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks']
            for table in required_tables:
                if table not in strategies:
                    logger.error(f"Missing strategy for table: {table}")
                    return False
                    
                strategy = strategies[table]
                required_fields = ['triggers_refresh', 'refresh_type', 'depends_on']
                for field in required_fields:
                    if field not in strategy:
                        logger.error(f"Missing field in strategy for {table}: {field}")
                        return False
                        
            if self.verbose:
                logger.info(f"Processing strategies validated for {len(strategies)} tables")
                
            return True
        except Exception as e:
            logger.error(f"Processing strategy validation failed: {e}")
            return False
    
    def test_metadata_tracking(self) -> bool:
        """Test EDW metadata tracking functionality"""
        try:
            processor = EdwEtlProcessor(database_url=self.database_url)
            if not processor.connect():
                return False
                
            # Test metadata update with an actual table that exists
            test_table = 'edw.dim_league'
            processor.update_metadata(test_table)
            
            # Check that metadata was recorded
            with self.engine.connect() as conn:
                metadata_count = conn.execute(text("""
                    SELECT COUNT(*) FROM edw.edw_metadata 
                    WHERE table_name = :table_name
                """), {'table_name': test_table}).scalar()
                
                if metadata_count == 0:
                    logger.error("Metadata tracking not working")
                    return False
                    
            if self.verbose:
                logger.info("Metadata tracking working correctly")
                
            return True
        except Exception as e:
            logger.error(f"Metadata tracking test failed: {e}")
            return False
    
    def cleanup_test_data(self) -> bool:
        """Clean up test data from EDW tables"""
        try:
            with self.engine.connect() as conn:
                # Clean up test data
                cleanup_queries = [
                    "DELETE FROM edw.dim_team WHERE team_id LIKE 'test_team_%'",
                    "DELETE FROM edw.dim_league WHERE league_id LIKE 'test_league_%'",
                    "DELETE FROM edw.edw_metadata WHERE table_name = 'test_metadata_table'"
                ]
                
                for query in cleanup_queries:
                    try:
                        conn.execute(text(query))
                    except Exception as e:
                        if self.verbose:
                            logger.warning(f"Cleanup warning: {e}")
                
                conn.commit()
                
            if self.verbose:
                logger.info("Test data cleanup completed")
                
            return True
        except Exception as e:
            logger.warning(f"Test data cleanup failed: {e}")
            return False  # Don't fail tests for cleanup issues
    
    def run_full_etl_test_suite(self, integration_mode: bool = False) -> bool:
        """Run complete ETL test suite"""
        logger.info("🧪 Starting EDW ETL Processing Test Suite")
        logger.info("=" * 80)
        
        # Define test suite
        tests = [
            ("ETL Processor Initialization", self.test_etl_processor_initialization),
            ("Sample Data Loading", self.test_sample_data_loading),
            ("Change Detection Logic", self.test_change_detection),
            ("Dimension Transformation", self.test_dimension_transformation),
            ("Processing Strategies", self.test_edw_processing_strategies),
            ("Metadata Tracking", self.test_metadata_tracking),
        ]
        
        # Add integration tests if requested
        if integration_mode:
            tests.extend([
                ("Dimension Loading (Integration)", self.test_dimension_loading),
                ("Incremental Processing (Integration)", self.test_incremental_processing),
            ])
        
        # Run all tests
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            if self.run_test(test_name, test_func):
                passed += 1
        
        # Cleanup (run regardless of test results)
        if integration_mode:
            self.cleanup_test_data()
        
        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("🏁 ETL TEST SUITE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Tests Passed: {passed}/{total}")
        logger.info(f"Success Rate: {(passed/total)*100:.1f}%")
        
        if passed == total:
            logger.info("🎉 ALL ETL TESTS PASSED! EDW processing logic is working correctly!")
            return True
        else:
            logger.error("❌ Some ETL tests failed. Check EDW processing logic.")
            
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
    parser = argparse.ArgumentParser(description='Test Fantasy Football EDW ETL Processing')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Verbose output with detailed information')
    parser.add_argument('--integration', action='store_true',
                       help='Run integration tests (includes database writes)')
    
    args = parser.parse_args()
    
    # Get database URL
    database_url = get_database_url()
    if not database_url:
        sys.exit(1)
    
    # Initialize tester
    tester = EDWETLTester(database_url, verbose=args.verbose)
    
    try:
        success = tester.run_full_etl_test_suite(integration_mode=args.integration)
        
        if success:
            logger.info("✅ EDW ETL processing test suite completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ EDW ETL processing test suite failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 ETL test suite error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 