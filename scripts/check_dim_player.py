#!/usr/bin/env python3
"""Check dim_player data for test records"""

import os
import logging
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        return
    
    engine = create_engine(database_url)
    
    with engine.connect() as conn:
        # Check if EDW schema and table exist
        schema_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = 'edw'
            )
        """)).scalar()
        
        if not schema_exists:
            logger.info("❌ EDW schema does not exist")
            return
        
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'edw' AND table_name = 'dim_player'
            )
        """)).scalar()
        
        if not table_exists:
            logger.info("❌ dim_player table does not exist")
            return
        
        # Get total count
        total_count = conn.execute(text("""
            SELECT COUNT(*) FROM edw.dim_player
        """)).scalar()
        
        logger.info(f"📊 Total players in dim_player: {total_count}")
        
        # Look for test data
        test_result = conn.execute(text("""
            SELECT player_key, player_id, player_name, primary_position
            FROM edw.dim_player 
            WHERE player_name ILIKE '%test%' 
               OR player_name ILIKE '%sample%'
               OR player_name ILIKE '%mock%'
            ORDER BY player_name
        """))
        
        test_players = test_result.fetchall()
        
        if test_players:
            logger.info(f"🚨 Found {len(test_players)} test players:")
            for row in test_players:
                logger.info(f"  - {row[2]} (ID: {row[1]}, Key: {row[0]})")
        else:
            logger.info("✅ No test data found!")
        
        # Show sample real data
        real_result = conn.execute(text("""
            SELECT player_name, primary_position
            FROM edw.dim_player 
            WHERE player_name NOT ILIKE '%test%' 
              AND player_name NOT ILIKE '%sample%'
              AND player_name NOT ILIKE '%mock%'
            ORDER BY player_name
            LIMIT 5
        """))
        
        real_players = real_result.fetchall()
        if real_players:
            logger.info("📋 Sample real players:")
            for row in real_players:
                logger.info(f"  - {row[0]} ({row[1]})")

if __name__ == "__main__":
    main() 