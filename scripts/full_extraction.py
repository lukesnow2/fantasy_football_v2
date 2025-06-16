#!/usr/bin/env python3
"""
Full Fantasy Football Data Extraction
Optimized entry point for complete historical data extraction with rate limiting
"""

import sys
import os
import argparse

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.extractors.comprehensive_data_extractor import YahooFantasyExtractor
from src.extractors.draft_extractor import main as extract_drafts
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run complete data extraction with BULK SEASON optimization for weekly fantasy points"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract Yahoo Fantasy Sports data with selective data extraction')
    parser.add_argument('--all-sports', action='store_true', 
                       help='Extract all sports (default: NFL only)')
    parser.add_argument('--include-public', action='store_true',
                       help='Include public leagues (default: private only)')
    
    # Selective data extraction flags
    parser.add_argument('--leagues-only', action='store_true',
                       help='Extract only league data')
    parser.add_argument('--teams-only', action='store_true',
                       help='Extract only team data')
    parser.add_argument('--rosters-only', action='store_true',
                       help='Extract only roster data')
    parser.add_argument('--matchups-only', action='store_true',
                       help='Extract only matchup data')
    parser.add_argument('--transactions-only', action='store_true',
                       help='Extract only transaction data')
    parser.add_argument('--drafts-only', action='store_true',
                       help='Extract only draft data')
    
    # Include flags (for combining data types)
    parser.add_argument('--include-rosters', action='store_true',
                       help='Include roster data in extraction')
    parser.add_argument('--exclude-leagues', action='store_true',
                       help='Exclude league data from extraction')
    parser.add_argument('--exclude-teams', action='store_true',
                       help='Exclude team data from extraction')
    parser.add_argument('--exclude-matchups', action='store_true',
                       help='Exclude matchup data from extraction')
    parser.add_argument('--exclude-transactions', action='store_true',
                       help='Exclude transaction data from extraction')
    parser.add_argument('--exclude-drafts', action='store_true',
                       help='Exclude draft data from extraction')
    
    # Roster-specific options
    parser.add_argument('--roster-weeks', type=str,
                       help='Comma-separated list of weeks for roster extraction (e.g., "1,2,3" or "current")')
    
    # NEW: Database streaming options
    parser.add_argument('--direct-to-db', action='store_true',
                       help='Stream data directly to database instead of JSON file')
    parser.add_argument('--db-url', type=str,
                       help='Database URL for direct streaming (if not provided, uses .env DATABASE_URL)')
    
    # NEW: Resume functionality
    parser.add_argument('--resume-from', type=str,
                       help='Resume extraction from specific resume file (default: resume_extraction.txt)')
    parser.add_argument('--clear-resume', action='store_true',
                       help='Clear any existing resume point and start fresh')
    
    # NEW: Sleep/workflow options
    parser.add_argument('--sleep-between', type=int, default=0,
                       help='Seconds to sleep between leagues (0 = no sleep, 420 = 7 minutes)')
    
    # Statistics options
    parser.add_argument('--statistics-weeks', type=str,
                       help='Comma-separated list of weeks for statistics extraction (e.g., "1,2,3")')
                       
    # Enhanced output options
    parser.add_argument('--output-file', type=str,
                       help='Custom output filename (JSON mode only)')
    parser.add_argument('--truncate-tables', action='store_true',
                       help='Truncate database tables before insertion (database mode only)')
    
    args = parser.parse_args()
    
    start_time = datetime.now()
    logger.info("🚀 Starting enhanced fantasy sports data extraction")
    logger.info("🆕 NEW FEATURES: Database streaming, resume capability, sleep intervals")
    
    # Handle resume/clear options
    resume_file = args.resume_from if args.resume_from else "resume_extraction.txt"
    if args.clear_resume:
        if os.path.exists(resume_file):
            os.remove(resume_file)
            logger.info(f"🗑️ Cleared resume point: {resume_file}")
    
    # Determine output mode
    output_mode = 'database' if args.direct_to_db else 'json'
    if output_mode == 'database':
        logger.info("📦 DATABASE MODE: Streaming data directly to database")
        if args.truncate_tables:
            logger.info("🔄 TRUNCATE MODE: Will clear tables before insertion")
    else:
        logger.info("📄 JSON MODE: Saving data to JSON file")
    
    # Sleep configuration
    if args.sleep_between > 0:
        sleep_minutes = args.sleep_between // 60
        logger.info(f"😴 SLEEP MODE: {sleep_minutes} minute delay between leagues")
    
    if args.all_sports:
        logger.info("🏟️ ALL SPORTS: Extracting all available sports")
        sport_filter = None
    else:
        logger.info("🏈 NFL ONLY: Extracting only football leagues (use --all-sports for others)")
        sport_filter = 'nfl'
    
    if args.include_public:
        logger.info("🌐 ALL LEAGUES: Including both private and public leagues")
        private_only = False
    else:
        logger.info("🔒 PRIVATE ONLY: Extracting only private leagues (use --include-public for all)")
        private_only = True
    
    try:
        logger.info("🔧 Initializing extractor with BULK SEASON optimization...")
        logger.info("📊 Weekly fantasy points will be extracted with maximum efficiency")
        extractor = YahooFantasyExtractor()
        
        # Determine what data to extract based on flags
        extract_leagues = True
        extract_teams = True
        extract_rosters = True  # Always include roster data by default for proper analytics
        extract_matchups = True
        extract_transactions = True
        extract_drafts = True
        
        # Handle "only" flags (exclusive extraction)
        only_flags = [args.leagues_only, args.teams_only, args.rosters_only, 
                     args.matchups_only, args.transactions_only, args.drafts_only]
        
        if any(only_flags):
            # If any "only" flag is set, disable all others
            extract_leagues = args.leagues_only
            extract_teams = args.teams_only
            extract_rosters = args.rosters_only
            extract_matchups = args.matchups_only
            extract_transactions = args.transactions_only
            extract_drafts = args.drafts_only
            
            logger.info("🎯 SELECTIVE EXTRACTION: Only extracting specified data types")
        else:
            # Handle include/exclude flags
            if args.include_rosters:
                extract_rosters = True
                logger.info("📋 INCLUDING: Roster data")
            
            if args.exclude_leagues:
                extract_leagues = False
                logger.info("❌ EXCLUDING: League data")
            if args.exclude_teams:
                extract_teams = False
                logger.info("❌ EXCLUDING: Team data")
            if args.exclude_matchups:
                extract_matchups = False
                logger.info("❌ EXCLUDING: Matchup data")
            if args.exclude_transactions:
                extract_transactions = False
                logger.info("❌ EXCLUDING: Transaction data")
            if args.exclude_drafts:
                extract_drafts = False
                logger.info("❌ EXCLUDING: Draft data")
        
        # Parse roster weeks
        roster_weeks = None
        if args.roster_weeks:
            if args.roster_weeks.lower() == 'current':
                roster_weeks = None  # Will use current week
                logger.info("📋 ROSTER WEEKS: Current week only")
            else:
                try:
                    roster_weeks = [int(w.strip()) for w in args.roster_weeks.split(',')]
                    logger.info(f"📋 ROSTER WEEKS: {roster_weeks}")
                except ValueError:
                    logger.error("❌ Invalid roster weeks format. Use comma-separated numbers or 'current'")
                    return
        
        # Parse statistics weeks
        statistics_weeks = None
        if args.statistics_weeks:
            try:
                statistics_weeks = [int(w.strip()) for w in args.statistics_weeks.split(',')]
                logger.info(f"📊 STATISTICS WEEKS: {statistics_weeks}")
            except ValueError:
                logger.error("❌ Invalid statistics weeks format. Use comma-separated numbers")
                return
        
        # Log extraction plan
        extraction_plan = []
        if extract_leagues: extraction_plan.append("Leagues")
        if extract_teams: extraction_plan.append("Teams")
        if extract_rosters: extraction_plan.append("Rosters")
        if extract_matchups: extraction_plan.append("Matchups")
        if extract_transactions: extraction_plan.append("Transactions")
        if extract_drafts: extraction_plan.append("Drafts")
        
        logger.info(f"📋 EXTRACTION PLAN: {', '.join(extraction_plan)}")
        
        # Handle database table truncation
        if output_mode == 'database' and args.truncate_tables:
            logger.info("🔄 Truncating database tables...")
            from src.extractors.comprehensive_data_extractor import DATABASE_SUPPORT
            if DATABASE_SUPPORT:
                try:
                    import psycopg2
                    from dotenv import load_dotenv
                    load_dotenv()
                    
                    # Get database URL
                    db_url = args.db_url
                    if not db_url:
                        db_url = os.getenv("DATABASE_URL")
                        if not db_url:
                            logger.error("❌ DATABASE_URL not found. Provide --db-url or set in .env file")
                            return
                    
                    # Truncate relevant tables
                    tables_to_truncate = []
                    if extract_rosters: tables_to_truncate.append("public.rosters")
                    if extract_leagues: tables_to_truncate.append("public.leagues")
                    if extract_teams: tables_to_truncate.append("public.teams")
                    if extract_matchups: tables_to_truncate.append("public.matchups")
                    if extract_transactions: tables_to_truncate.append("public.transactions")
                    if extract_drafts: tables_to_truncate.append("public.draft_picks")
                    
                    if tables_to_truncate:
                        with psycopg2.connect(db_url) as conn:
                            with conn.cursor() as cur:
                                for table in tables_to_truncate:
                                    try:
                                        cur.execute(f"TRUNCATE TABLE {table}")
                                        logger.info(f"🗑️ Truncated {table}")
                                    except Exception as e:
                                        logger.warning(f"⚠️ Could not truncate {table}: {e}")
                                conn.commit()
                        
                except Exception as e:
                    logger.error(f"❌ Failed to truncate tables: {e}")
                    return
            else:
                logger.error("❌ Database support not available for truncation")
                return
        
        # Extract data with enhanced method
        all_data = extractor.extract_all_data_with_resume(
            output_mode=output_mode,
            db_url=args.db_url,
            resume_file=resume_file,
            sleep_between_leagues=args.sleep_between,
            initial_batch_size=10,
            initial_batch_delay=10,
            sport_filter=sport_filter,
            private_only=private_only,
            extract_leagues=extract_leagues,
            extract_teams=extract_teams,
            extract_rosters=extract_rosters,
            extract_matchups=extract_matchups,
            extract_transactions=extract_transactions,
            extract_drafts=extract_drafts,
            roster_weeks=roster_weeks,
            statistics_weeks=statistics_weeks
        )
        
        # Handle output based on mode
        end_time = datetime.now()
        duration = end_time - start_time
        
        if output_mode == 'database':
            # Database streaming mode - data already saved during extraction
            logger.info("🎉 Data extraction completed successfully!")
            logger.info(f"⏱️ Total extraction time: {duration}")
            logger.info(f"📦 Data streamed directly to database")
            
            # Print statistics from extractor's tracking
            stats = extractor.extraction_stats
            logger.info(f"📊 Final extraction statistics:")
            logger.info(f"  📋 Leagues processed: {stats['leagues_processed']:,}")
            logger.info(f"  👥 Teams processed: {stats['teams_processed']:,}")
            logger.info(f"  🔢 Rosters processed: {stats['rosters_processed']:,}")
            logger.info(f"  ⚔️ Matchups processed: {stats['matchups_processed']:,}")
            logger.info(f"  💸 Transactions processed: {stats['transactions_processed']:,}")
            logger.info(f"  📋 Drafts processed: {stats['drafts_processed']:,}")
            logger.info(f"  ❌ Errors encountered: {stats['errors_encountered']:,}")
            logger.info(f"  📊 Total items: {extractor.total_items_processed:,}")
            
        else:
            # JSON mode - save to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if args.output_file:
                filename = args.output_file
            else:
                # Create descriptive filename
                sport_suffix = "all_sports" if args.all_sports else "nfl"
                privacy_suffix = "all_leagues" if args.include_public else "private"
                
                # Ensure data/current directory exists
                os.makedirs('data/current', exist_ok=True)
                filename = f'data/current/yahoo_fantasy_{sport_suffix}_{privacy_suffix}_{timestamp}.json'
            
            logger.info(f"💾 Saving complete dataset to {filename}")
            extractor.save_to_json(filename)
            
            logger.info("🎉 Data extraction completed successfully!")
            logger.info(f"⏱️ Total extraction time: {duration}")
            logger.info(f"📄 Data saved to: {filename}")
            
            # Print final statistics
            total_items = sum(len(v) for v in all_data.values())
            logger.info(f"📊 Total data points extracted: {total_items:,}")
            
            for data_type, items in all_data.items():
                if items:
                    logger.info(f"  📋 {data_type.title()}: {len(items):,}")
        
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        raise

if __name__ == "__main__":
    main() 