#!/usr/bin/env python3
"""
Check dim_player data for test records and provide cleanup options
"""

import os
import sys
import logging
from sqlalchemy import create_engine, text
from typing import List, Dict

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_database_url() -> str:
    """Get database URL from environment"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")
    return database_url

def check_dim_player_data(database_url: str) -> Dict:
    """Check current data in dim_player table"""
    engine = create_engine(database_url)
    
    with engine.connect() as conn:
        # Check if EDW schema exists
        schema_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = 'edw'
            )
        """)).scalar()
        
        if not schema_exists:
            logger.info("❌ EDW schema does not exist")
            return {"schema_exists": False}
        
        # Check if dim_player table exists
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'edw' AND table_name = 'dim_player'
            )
        """)).scalar()
        
        if not table_exists:
            logger.info("❌ dim_player table does not exist")
            return {"schema_exists": True, "table_exists": False}
        
        # Get total count
        total_count = conn.execute(text("""
            SELECT COUNT(*) FROM edw.dim_player
        """)).scalar()
        
        logger.info(f"📊 Total players in dim_player: {total_count}")
        
        # Look for test data patterns
        test_patterns = [
            "Test Player",
            "test_player",
            "TEST",
            "Sample Player",
            "Mock Player"
        ]
        
        test_players = []
        for pattern in test_patterns:
            result = conn.execute(text("""
                SELECT player_key, player_id, player_name, primary_position
                FROM edw.dim_player 
                WHERE player_name ILIKE :pattern
                ORDER BY player_name
            """), {"pattern": f"%{pattern}%"})
            
            for row in result.fetchall():
                test_players.append({
                    "player_key": row[0],
                    "player_id": row[1], 
                    "player_name": row[2],
                    "primary_position": row[3]
                })
        
        # Get sample of real data
        real_players = []
        result = conn.execute(text("""
            SELECT player_key, player_id, player_name, primary_position
            FROM edw.dim_player 
            WHERE player_name NOT ILIKE '%test%' 
              AND player_name NOT ILIKE '%sample%'
              AND player_name NOT ILIKE '%mock%'
            ORDER BY player_name
            LIMIT 10
        """))
        
        for row in result.fetchall():
            real_players.append({
                "player_key": row[0],
                "player_id": row[1],
                "player_name": row[2], 
                "primary_position": row[3]
            })
        
        return {
            "schema_exists": True,
            "table_exists": True,
            "total_count": total_count,
            "test_players": test_players,
            "real_players": real_players
        }

def clean_test_data(database_url: str, test_player_keys: List[int]) -> bool:
    """Remove test players from dim_player and related tables"""
    if not test_player_keys:
        logger.info("✅ No test data to clean")
        return True
    
    engine = create_engine(database_url)
    
    try:
        with engine.begin() as conn:  # Use transaction
            # Check for dependent data first
            dependencies = []
            
            # Check fact_roster
            roster_count = conn.execute(text("""
                SELECT COUNT(*) FROM edw.fact_roster 
                WHERE player_key = ANY(:keys)
            """), {"keys": test_player_keys}).scalar()
            
            if roster_count > 0:
                dependencies.append(f"fact_roster ({roster_count} records)")
            
            # Check fact_transaction  
            transaction_count = conn.execute(text("""
                SELECT COUNT(*) FROM edw.fact_transaction 
                WHERE player_key = ANY(:keys)
            """), {"keys": test_player_keys}).scalar()
            
            if transaction_count > 0:
                dependencies.append(f"fact_transaction ({transaction_count} records)")
            
            # Check fact_draft
            draft_count = conn.execute(text("""
                SELECT COUNT(*) FROM edw.fact_draft 
                WHERE player_key = ANY(:keys)
            """), {"keys": test_player_keys}).scalar()
            
            if draft_count > 0:
                dependencies.append(f"fact_draft ({draft_count} records)")
            
            if dependencies:
                logger.info(f"🔗 Found dependent data: {', '.join(dependencies)}")
                logger.info("🧹 Cleaning dependent data first...")
                
                # Clean fact tables first (in reverse dependency order)
                if roster_count > 0:
                    conn.execute(text("""
                        DELETE FROM edw.fact_roster WHERE player_key = ANY(:keys)
                    """), {"keys": test_player_keys})
                    logger.info(f"  ✅ Cleaned {roster_count} roster records")
                
                if transaction_count > 0:
                    conn.execute(text("""
                        DELETE FROM edw.fact_transaction WHERE player_key = ANY(:keys)
                    """), {"keys": test_player_keys})
                    logger.info(f"  ✅ Cleaned {transaction_count} transaction records")
                
                if draft_count > 0:
                    conn.execute(text("""
                        DELETE FROM edw.fact_draft WHERE player_key = ANY(:keys)
                    """), {"keys": test_player_keys})
                    logger.info(f"  ✅ Cleaned {draft_count} draft records")
            
            # Now clean the dim_player records
            result = conn.execute(text("""
                DELETE FROM edw.dim_player WHERE player_key = ANY(:keys)
            """), {"keys": test_player_keys})
            
            logger.info(f"✅ Cleaned {len(test_player_keys)} test players from dim_player")
            logger.info("🎯 Transaction committed successfully")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to clean test data: {e}")
        return False

def main():
    """Main function"""
    try:
        database_url = get_database_url()
        
        logger.info("🔍 Checking dim_player data...")
        result = check_dim_player_data(database_url)
        
        if not result.get("schema_exists"):
            logger.info("❌ EDW schema doesn't exist - nothing to check")
            return
        
        if not result.get("table_exists"):
            logger.info("❌ dim_player table doesn't exist - nothing to check")
            return
        
        test_players = result.get("test_players", [])
        real_players = result.get("real_players", [])
        
        logger.info(f"\n📊 RESULTS:")
        logger.info(f"Total players: {result['total_count']}")
        logger.info(f"Test players found: {len(test_players)}")
        logger.info(f"Real players sample: {len(real_players)}")
        
        if test_players:
            logger.info(f"\n🚨 TEST DATA FOUND:")
            for player in test_players:
                logger.info(f"  - {player['player_name']} (ID: {player['player_id']}, Key: {player['player_key']})")
            
            # Ask for confirmation to clean
            while True:
                response = input(f"\n🧹 Clean {len(test_players)} test players? (y/n): ").lower().strip()
                if response in ['y', 'yes']:
                    test_keys = [p['player_key'] for p in test_players]
                    if clean_test_data(database_url, test_keys):
                        logger.info("✅ Test data cleanup completed successfully!")
                    else:
                        logger.error("❌ Test data cleanup failed!")
                    break
                elif response in ['n', 'no']:
                    logger.info("⏭️  Skipping cleanup")
                    break
                else:
                    print("Please enter 'y' or 'n'")
        else:
            logger.info("✅ No test data found in dim_player!")
        
        if real_players:
            logger.info(f"\n📋 SAMPLE REAL DATA:")
            for player in real_players[:5]:
                logger.info(f"  - {player['player_name']} ({player['primary_position']}) - ID: {player['player_id']}")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 