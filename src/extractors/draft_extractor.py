#!/usr/bin/env python3
"""
Extract draft data for all leagues and merge with existing final dataset
"""

import json
import logging
from datetime import datetime
from .comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Extract draft data and merge with existing final dataset"""
    
    start_time = datetime.now()
    logger.info(f"🎯 Starting draft data extraction at {start_time}")
    
    # Load existing final dataset
    final_data_file = 'data/current/yahoo_fantasy_FINAL_complete_data_20250605_101225.json'
    logger.info(f"📂 Loading existing dataset: {final_data_file}")
    
    try:
        with open(final_data_file, 'r') as f:
            final_data = json.load(f)
        logger.info("✅ Successfully loaded existing dataset")
    except Exception as e:
        logger.error(f"❌ Error loading existing dataset: {e}")
        return
    
    # Create extractor
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    logger.info("🔑 Authenticating...")
    if not extractor.authenticate():
        logger.error("❌ Authentication failed")
        return
    
    # Get all leagues from existing data
    leagues = final_data.get('leagues', [])
    logger.info(f"📋 Found {len(leagues)} leagues to extract draft data from")
    
    # Extract draft data for each league
    all_draft_picks = []
    successful_extractions = 0
    
    for i, league in enumerate(leagues, 1):
        league_id = league['league_id']
        league_name = league['name']
        season = league['season']
        
        logger.info(f"🎯 Processing league {i}/{len(leagues)}: {league_name} ({season})")
        
        try:
            # Extract draft data for this league
            draft_picks = extractor.extract_draft_for_league(league_id)
            
            if draft_picks:
                successful_extractions += 1
                for pick in draft_picks:
                    # Convert to dict with datetime serialization
                    pick_dict = {}
                    for k, v in pick.__dict__.items():
                        if isinstance(v, datetime):
                            pick_dict[k] = v.isoformat()
                        else:
                            pick_dict[k] = v
                    all_draft_picks.append(pick_dict)
                
                logger.info(f"  ✅ Extracted {len(draft_picks)} draft picks")
            else:
                logger.info(f"  ⚠️ No draft picks found (likely older league)")
            
            # Rate limiting
            import time
            time.sleep(0.3)
            
        except Exception as e:
            logger.error(f"  ❌ Error extracting draft data for {league_name}: {e}")
            continue
    
    # Add draft_picks to final dataset
    final_data['draft_picks'] = all_draft_picks
    
    # Save updated dataset
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f'data/current/yahoo_fantasy_COMPLETE_with_drafts_{timestamp}.json'
    
    logger.info(f"💾 Saving updated dataset to {output_file}...")
    try:
        with open(output_file, 'w') as f:
            json.dump(final_data, f, indent=2, default=str)
        logger.info("✅ Successfully saved updated dataset")
    except Exception as e:
        logger.error(f"❌ Error saving updated dataset: {e}")
        return
    
    # Calculate runtime and show summary
    end_time = datetime.now()
    runtime = end_time - start_time
    
    logger.info("\n" + "="*80)
    logger.info("🎯 DRAFT DATA EXTRACTION COMPLETE!")
    logger.info("="*80)
    logger.info(f"⏱️ Total Runtime: {runtime}")
    logger.info(f"🏆 Leagues processed: {len(leagues)}")
    logger.info(f"✅ Successful extractions: {successful_extractions}")
    logger.info(f"🎯 Total draft picks extracted: {len(all_draft_picks):,}")
    
    # Draft analysis
    if all_draft_picks:
        auction_picks = sum(1 for pick in all_draft_picks if pick.get('is_auction_draft', False))
        snake_picks = len(all_draft_picks) - auction_picks
        unique_players = len(set(pick.get('player_id') for pick in all_draft_picks if pick.get('player_id')))
        
        logger.info(f"🎯 Snake draft picks: {snake_picks:,}")
        logger.info(f"🎯 Auction draft picks: {auction_picks:,}")
        logger.info(f"🎯 Unique players drafted: {unique_players:,}")
        
        # Position breakdown
        positions = {}
        for pick in all_draft_picks:
            pos = pick.get('position', 'Unknown')
            positions[pos] = positions.get(pos, 0) + 1
        
        logger.info(f"\n📊 Position breakdown:")
        for pos, count in sorted(positions.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {pos}: {count:,} picks")
        
        # Round breakdown
        rounds = {}
        for pick in all_draft_picks:
            round_num = pick.get('round_number', 0)
            rounds[round_num] = rounds.get(round_num, 0) + 1
        
        logger.info(f"\n📊 Top 5 rounds by pick count:")
        for round_num, count in sorted(rounds.items(), key=lambda x: x[1], reverse=True)[:5]:
            logger.info(f"  Round {round_num}: {count:,} picks")
    
    # Final dataset summary
    total_records = sum(len(final_data.get(table, [])) for table in ['leagues', 'teams', 'rosters', 'matchups', 'transactions', 'draft_picks'])
    logger.info(f"\n📈 UPDATED DATASET SUMMARY:")
    logger.info(f"  - Leagues: {len(final_data.get('leagues', [])):,}")
    logger.info(f"  - Teams: {len(final_data.get('teams', [])):,}")
    logger.info(f"  - Rosters: {len(final_data.get('rosters', [])):,}")
    logger.info(f"  - Matchups: {len(final_data.get('matchups', [])):,}")
    logger.info(f"  - Transactions: {len(final_data.get('transactions', [])):,}")
    logger.info(f"  - Draft Picks: {len(final_data.get('draft_picks', [])):,}")
    logger.info(f"📈 TOTAL RECORDS: {total_records:,}")
    
    # File size
    file_size_mb = len(json.dumps(final_data, default=str)) / (1024 * 1024)
    logger.info(f"\n💾 Updated dataset saved to: {output_file}")
    logger.info(f"💾 File size: {file_size_mb:.1f} MB")
    logger.info(f"🚀 Ready for Heroku deployment with draft data!")

if __name__ == "__main__":
    main() 