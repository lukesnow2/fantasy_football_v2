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
    """Run complete data extraction for NFL private leagues with bulk optimizations and rate limiting"""
    
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
    
    args = parser.parse_args()
    
    start_time = datetime.now()
    logger.info("🚀 Starting OPTIMIZED fantasy sports data extraction")
    
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
        logger.info("🔧 Initializing extractor with bulk optimizations...")
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
        
        # Log extraction plan
        extraction_plan = []
        if extract_leagues: extraction_plan.append("Leagues")
        if extract_teams: extraction_plan.append("Teams")
        if extract_rosters: extraction_plan.append("Rosters")
        if extract_matchups: extraction_plan.append("Matchups")
        if extract_transactions: extraction_plan.append("Transactions")
        if extract_drafts: extraction_plan.append("Drafts")
        
        logger.info(f"📋 EXTRACTION PLAN: {', '.join(extraction_plan)}")
        
        # Extract data with selective flags
        all_data = extractor.extract_all_data(
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
            roster_weeks=roster_weeks
        )
        
        # Save the extracted data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create descriptive filename and ensure directory exists
        sport_suffix = "all_sports" if args.all_sports else "nfl"
        privacy_suffix = "all_leagues" if args.include_public else "private"
        
        # Ensure data/current directory exists
        os.makedirs('data/current', exist_ok=True)
        
        filename = f'data/current/yahoo_fantasy_{sport_suffix}_{privacy_suffix}_{timestamp}.json'
        
        logger.info(f"💾 Saving complete dataset to {filename}")
        extractor.save_to_json(filename)
        
        # Print summary
        end_time = datetime.now()
        duration = end_time - start_time
        
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