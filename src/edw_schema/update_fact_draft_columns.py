#!/usr/bin/env python3
"""
Update fact_draft Column Names and Force Rebuild EDW

This script:
1. Renames columns in the existing fact_draft table:
   - games_played -> fantasy_games_played
   - points_per_game -> points_per_week
2. Updates the related index
3. Runs a force rebuild of the complete EDW

Usage:
    python update_fact_draft_columns.py [--database-url URL]

Environment Variables:
    DATABASE_URL: PostgreSQL connection string
"""

import os
import sys
import logging
import argparse
from datetime import datetime
import psycopg2
from deploy_complete_edw import EdwDeployment

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def update_fact_draft_columns(database_url: str) -> bool:
    """Update column names in fact_draft table"""
    try:
        logger.info("🔧 Starting fact_draft column updates...")
        
        # Connect using psycopg2 for better transaction control
        conn = psycopg2.connect(database_url)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'edw' 
                AND table_name = 'fact_draft'
            )
        """)
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            logger.warning("⚠️ fact_draft table does not exist. Skipping column updates.")
            cur.close()
            conn.close()
            return True
        
        # Check if columns need to be renamed
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'edw' 
            AND table_name = 'fact_draft'
            AND column_name IN ('games_played', 'points_per_game', 'fantasy_games_played', 'points_per_week')
        """)
        columns = {row[0] for row in cur.fetchall()}
        
        logger.info(f"📋 Found columns: {sorted(columns)}")
        
        # 1. Rename games_played -> fantasy_games_played
        if 'games_played' in columns and 'fantasy_games_played' not in columns:
            logger.info("🔧 Renaming games_played -> fantasy_games_played...")
            cur.execute("""
                ALTER TABLE edw.fact_draft 
                RENAME COLUMN games_played TO fantasy_games_played
            """)
            logger.info("✅ Renamed games_played -> fantasy_games_played")
        elif 'fantasy_games_played' in columns:
            logger.info("✓ fantasy_games_played column already exists")
        
        # 2. Rename points_per_game -> points_per_week
        if 'points_per_game' in columns and 'points_per_week' not in columns:
            logger.info("🔧 Renaming points_per_game -> points_per_week...")
            cur.execute("""
                ALTER TABLE edw.fact_draft 
                RENAME COLUMN points_per_game TO points_per_week
            """)
            logger.info("✅ Renamed points_per_game -> points_per_week")
        elif 'points_per_week' in columns:
            logger.info("✓ points_per_week column already exists")
        
        # 3. Update the performance index if it exists
        logger.info("🔧 Updating performance index...")
        
        # Drop old index if it exists
        try:
            cur.execute("DROP INDEX IF EXISTS edw.idx_draft_performance")
            logger.info("✅ Dropped old idx_draft_performance index")
        except Exception as e:
            logger.debug(f"Index drop warning: {e}")
        
        # Create new index with correct column name
        try:
            cur.execute("""
                CREATE INDEX idx_draft_performance 
                ON edw.fact_draft (season_points, points_per_week)
            """)
            logger.info("✅ Created new idx_draft_performance index")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info("✓ idx_draft_performance index already exists")
            else:
                logger.warning(f"⚠️ Could not create index: {e}")
        
        cur.close()
        conn.close()
        
        logger.info("✅ fact_draft column updates completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to update fact_draft columns: {e}")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Update fact_draft columns and rebuild EDW')
    parser.add_argument('--database-url', 
                       default=os.getenv('DATABASE_URL'),
                       help='PostgreSQL connection string')
    
    args = parser.parse_args()
    
    if not args.database_url:
        logger.error("❌ DATABASE_URL not provided. Use --database-url or set DATABASE_URL environment variable")
        sys.exit(1)
    
    logger.info("🚀 Starting fact_draft column update and EDW rebuild")
    logger.info(f"🔗 Database: {args.database_url.split('@')[-1] if '@' in args.database_url else 'localhost'}")
    
    # Step 1: Update column names
    logger.info("\n" + "="*60)
    logger.info("STEP 1: UPDATE FACT_DRAFT COLUMNS")
    logger.info("="*60)
    
    if not update_fact_draft_columns(args.database_url):
        logger.error("❌ Column update failed. Aborting.")
        sys.exit(1)
    
    # Step 2: Force rebuild EDW
    logger.info("\n" + "="*60)
    logger.info("STEP 2: FORCE REBUILD EDW")
    logger.info("="*60)
    
    try:
        deployer = EdwDeployment(args.database_url, force_rebuild=True)
        
        if deployer.deploy():
            logger.info("✅ EDW force rebuild completed successfully")
            deployer.print_deployment_summary()
        else:
            logger.error("❌ EDW force rebuild failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ EDW deployment error: {e}")
        sys.exit(1)
    
    logger.info("\n🎉 All operations completed successfully!")
    logger.info("📊 The fact_draft table now has correct column names:")
    logger.info("   - fantasy_games_played (count of weeks in starting lineup)")
    logger.info("   - points_per_week (average weekly impact)")

if __name__ == "__main__":
    main() 