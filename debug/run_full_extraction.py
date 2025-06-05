#!/usr/bin/env python3
"""
Full Yahoo Fantasy Data Extraction Script
Runs comprehensive extraction on all leagues with progress tracking
"""

import json
import logging
import time
from datetime import datetime
from comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'full_extraction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_full_extraction():
    """Run comprehensive data extraction on all leagues"""
    
    logger.info("🚀 Starting FULL comprehensive Yahoo Fantasy data extraction...")
    start_time = datetime.now()
    
    # Create extractor
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    if not extractor.authenticate():
        logger.error("❌ Authentication failed")
        return False
    
    # Get all leagues
    all_leagues = extractor.get_all_leagues()
    
    if not all_leagues:
        logger.error("❌ No leagues found")
        return False
    
    logger.info(f"📋 Found {len(all_leagues)} total leagues for full extraction")
    
    # Initialize extraction statistics
    stats = {
        'total_leagues': len(all_leagues),
        'processed_leagues': 0,
        'failed_leagues': 0,
        'total_teams': 0,
        'total_matchups': 0,
        'total_transactions': 0,
        'errors': []
    }
    
    # Process all leagues
    for i, league_info in enumerate(all_leagues, 1):
        league_id = league_info['league_id']
        league_name = league_info['name']
        season = league_info['season']
        
        logger.info(f"📋 Processing league {i}/{len(all_leagues)}: {league_name} ({season})")
        logger.info(f"    🔍 League ID: {league_id}")
        
        try:
            # Extract league data
            league_data = extractor.extract_league_data(league_info)
            extractor.extracted_data['leagues'].append(league_data.__dict__)
            
            # Extract teams
            teams = extractor.extract_teams_for_league(league_id)
            stats['total_teams'] += len(teams)
            
            for team in teams:
                extractor.extracted_data['teams'].append(team.__dict__)
            
            # Skip rosters for now (can be added later)
            # rosters = extractor.extract_rosters_for_league(league_id, teams)
            
            # Extract matchups (skip for now due to API limitations)
            # matchups = extractor.extract_matchups_for_league(league_id)
            
            # Extract transactions (skip for now due to API limitations)  
            # transactions = extractor.extract_transactions_for_league(league_id)
            
            stats['processed_leagues'] += 1
            
            # Progress update every 5 leagues
            if i % 5 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = i / elapsed * 60  # leagues per minute
                eta_minutes = (len(all_leagues) - i) / rate if rate > 0 else 0
                
                logger.info(f"📊 Progress: {i}/{len(all_leagues)} leagues ({i/len(all_leagues)*100:.1f}%)")
                logger.info(f"    ⏱️  Processing rate: {rate:.1f} leagues/min")
                logger.info(f"    🕐 ETA: {eta_minutes:.1f} minutes")
                logger.info(f"    📈 Teams extracted: {stats['total_teams']}")
            
            # Rate limiting between leagues
            time.sleep(0.2)
            
        except Exception as e:
            error_msg = f"Error processing league {league_name} ({season}): {e}"
            logger.error(f"❌ {error_msg}")
            stats['errors'].append(error_msg)
            stats['failed_leagues'] += 1
            continue
    
    # Final statistics
    end_time = datetime.now()
    total_time = (end_time - start_time).total_seconds()
    
    logger.info("✅ Full extraction completed!")
    logger.info(f"📊 Final Statistics:")
    logger.info(f"  - Total time: {total_time/60:.1f} minutes")
    logger.info(f"  - Leagues processed: {stats['processed_leagues']}/{stats['total_leagues']}")
    logger.info(f"  - Success rate: {stats['processed_leagues']/stats['total_leagues']*100:.1f}%")
    logger.info(f"  - Failed leagues: {stats['failed_leagues']}")
    logger.info(f"  - Total teams: {stats['total_teams']}")
    logger.info(f"  - Total rosters: {len(extractor.extracted_data['rosters'])}")
    logger.info(f"  - Total matchups: {len(extractor.extracted_data['matchups'])}")
    logger.info(f"  - Total transactions: {len(extractor.extracted_data['transactions'])}")
    
    # Save comprehensive data
    output_filename = f'yahoo_fantasy_complete_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    extractor.save_to_json(output_filename)
    
    # Save statistics
    stats_filename = f'extraction_stats_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(stats_filename, 'w') as f:
        json.dump({
            'extraction_stats': stats,
            'extraction_time': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat(),
                'duration_seconds': total_time
            },
            'data_summary': {
                'leagues': len(extractor.extracted_data['leagues']),
                'teams': len(extractor.extracted_data['teams']),
                'rosters': len(extractor.extracted_data['rosters']),
                'matchups': len(extractor.extracted_data['matchups']),
                'transactions': len(extractor.extracted_data['transactions'])
            }
        }, f, indent=2)
    
    logger.info(f"💾 Complete data saved to: {output_filename}")
    logger.info(f"📈 Statistics saved to: {stats_filename}")
    
    if stats['errors']:
        logger.warning(f"⚠️  {len(stats['errors'])} errors occurred during extraction:")
        for error in stats['errors'][:5]:  # Show first 5 errors
            logger.warning(f"    - {error}")
        if len(stats['errors']) > 5:
            logger.warning(f"    ... and {len(stats['errors']) - 5} more errors")
    
    logger.info("🎯 Next steps:")
    logger.info("  1. Review the extracted data file")
    logger.info("  2. Set up your database using yahoo_fantasy_schema.sql")
    logger.info("  3. Provide database endpoint for data loading")
    logger.info("  4. Consider adding roster/matchup/transaction extraction for more detailed data")
    
    return True

def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("Yahoo Fantasy Football - Complete Data Extraction")
    logger.info("Extracting 20 years of fantasy football history")
    logger.info("=" * 80)
    
    success = run_full_extraction()
    
    if success:
        logger.info("✅ Full extraction completed successfully!")
    else:
        logger.error("❌ Full extraction failed!")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 