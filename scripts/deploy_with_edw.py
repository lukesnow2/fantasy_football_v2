#!/usr/bin/env python3
"""
Integrated Fantasy Football Data & EDW Deployment

This script provides a streamlined deployment process that:
1. Deploys operational data to the database
2. Deploys the complete EDW schema
3. Tests the EDW deployment
4. Provides comprehensive reporting

Usage:
    python scripts/deploy_with_edw.py --data-file data/current/latest.json
    python scripts/deploy_with_edw.py --data-file "data/current/yahoo_fantasy_*.json"
"""

import sys
import os
import logging
from datetime import datetime
from pathlib import Path

# Add src directory to Python path
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

from src.deployment.heroku_deployer import HerokuPostgresDeployer, auto_detect_data_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class IntegratedDeployer:
    """Integrated deployer for operational data and EDW"""
    
    def __init__(self, data_file: str, database_url: str = None):
        self.data_file = data_file
        self.database_url = database_url or os.getenv('DATABASE_URL')
        
        if not self.database_url:
            raise ValueError("DATABASE_URL required: set as environment variable or pass directly")
    
    def deploy_operational_data(self) -> bool:
        """Deploy operational data using the Heroku deployer"""
        logger.info("🗄️ PHASE 1: Deploying Operational Data")
        logger.info("=" * 60)
        
        try:
            deployer = HerokuPostgresDeployer(self.data_file, self.database_url)
            return deployer.deploy()
        except Exception as e:
            logger.error(f"❌ Operational data deployment failed: {e}")
            return False
    
    def deploy_edw_schema(self) -> bool:
        """Deploy EDW schema"""
        logger.info("\n🏗️ PHASE 2: Deploying EDW Schema")
        logger.info("=" * 60)
        
        try:
            # Import and run EDW deployer
            from src.edw_schema.deploy_edw import EDWDeployer
            
            edw_deployer = EDWDeployer(self.database_url)
            return edw_deployer.deploy_full_edw(drop_existing=False)
        except Exception as e:
            logger.error(f"❌ EDW schema deployment failed: {e}")
            return False
    
    def test_edw_deployment(self) -> bool:
        """Test EDW deployment"""
        logger.info("\n🧪 PHASE 3: Testing EDW Deployment")
        logger.info("=" * 60)
        
        try:
            # Import and run EDW tester
            from src.edw_schema.test_edw_deployment import EDWTester
            
            tester = EDWTester(self.database_url, verbose=False)
            return tester.run_full_test_suite(fast_mode=True)
        except Exception as e:
            logger.error(f"❌ EDW testing failed: {e}")
            return False
    
    def deploy_complete_system(self) -> bool:
        """Deploy complete system: operational data + EDW"""
        start_time = datetime.now()
        
        logger.info("🚀 INTEGRATED FANTASY FOOTBALL DEPLOYMENT")
        logger.info("=" * 80)
        logger.info(f"📊 Data file: {self.data_file}")
        logger.info(f"🗄️ Database: {self.database_url.split('@')[1] if '@' in self.database_url else 'Local'}")
        logger.info(f"⏰ Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Phase 1: Operational Data
        if not self.deploy_operational_data():
            logger.error("💥 DEPLOYMENT FAILED: Operational data deployment failed")
            return False
        
        # Phase 2: EDW Schema
        if not self.deploy_edw_schema():
            logger.error("💥 DEPLOYMENT FAILED: EDW schema deployment failed")
            return False
        
        # Phase 3: EDW Testing
        if not self.test_edw_deployment():
            logger.error("💥 DEPLOYMENT FAILED: EDW testing failed")
            return False
        
        # Success summary
        runtime = datetime.now() - start_time
        logger.info("\n" + "=" * 80)
        logger.info("🎉 COMPLETE DEPLOYMENT SUCCESSFUL!")
        logger.info("=" * 80)
        logger.info(f"⏱️ Total Runtime: {runtime}")
        logger.info(f"✅ Operational Data: Deployed and verified")
        logger.info(f"✅ EDW Schema: Complete with dimensions, facts, marts, views")
        logger.info(f"✅ EDW Testing: All validation tests passed")
        logger.info(f"🚀 System is ready for analytics and web applications!")
        
        return True

def main():
    """Main deployment entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy complete Fantasy Football system (Data + EDW)')
    parser.add_argument('--data-file', 
                       default='data/current/yahoo_fantasy_COMPLETE_with_drafts_*.json',
                       help='Data file path (supports wildcards)')
    parser.add_argument('--database-url', 
                       help='DATABASE_URL (or set DATABASE_URL env var)')
    parser.add_argument('--operational-only', action='store_true',
                       help='Deploy only operational data (skip EDW)')
    parser.add_argument('--edw-only', action='store_true',
                       help='Deploy only EDW (skip operational data)')
    
    args = parser.parse_args()
    
    # Auto-detect data file
    data_file = auto_detect_data_file(args.data_file)
    
    try:
        deployer = IntegratedDeployer(data_file, args.database_url)
        
        if args.operational_only:
            logger.info("🎯 Mode: Operational Data Only")
            success = deployer.deploy_operational_data()
        elif args.edw_only:
            logger.info("🎯 Mode: EDW Only")
            success = deployer.deploy_edw_schema() and deployer.test_edw_deployment()
        else:
            logger.info("🎯 Mode: Complete System Deployment")
            success = deployer.deploy_complete_system()
        
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