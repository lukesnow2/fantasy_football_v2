#!/usr/bin/env python3
"""
Run full extraction with fixed team data logic
"""

import json
import logging
from datetime import datetime
from comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run the complete fixed extraction"""
    
    start_time = datetime.now()
    logger.info(f"🚀 Starting fixed full extraction at {start_time}")
    
    # Create extractor
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    logger.info("🔑 Authenticating...")
    if not extractor.authenticate():
        logger.error("❌ Authentication failed")
        return
    
    # Run full extraction
    logger.info("📊 Starting comprehensive data extraction...")
    extracted_data = extractor.extract_all_data()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'yahoo_fantasy_fixed_complete_data_{timestamp}.json'
    
    logger.info(f"💾 Saving results to {filename}...")
    extractor.save_to_json(filename)
    
    # Calculate runtime
    end_time = datetime.now()
    runtime = end_time - start_time
    
    # Print final statistics
    logger.info("\n" + "="*70)
    logger.info("🎯 EXTRACTION COMPLETE!")
    logger.info("="*70)
    
    leagues = extracted_data.get('leagues', [])
    teams = extracted_data.get('teams', [])
    
    logger.info(f"📅 Total Runtime: {runtime}")
    logger.info(f"🏆 Total Leagues: {len(leagues)}")
    logger.info(f"👥 Total Teams: {len(teams)}")
    
    if teams:
        teams_with_managers = sum(1 for t in teams if t.manager_name)
        teams_with_records = sum(1 for t in teams if t.wins > 0 or t.losses > 0)
        teams_with_points = sum(1 for t in teams if t.points_for > 0)
        
        logger.info(f"✅ Teams with managers: {teams_with_managers}/{len(teams)} ({teams_with_managers/len(teams)*100:.1f}%)")
        logger.info(f"✅ Teams with records: {teams_with_records}/{len(teams)} ({teams_with_records/len(teams)*100:.1f}%)")
        logger.info(f"✅ Teams with points: {teams_with_points}/{len(teams)} ({teams_with_points/len(teams)*100:.1f}%)")
        
        # Year breakdown
        year_stats = {}
        for team in teams:
            league_id = team.league_id
            # Extract year from league_id pattern (e.g., "449.l.674707" -> get year from leagues data)
            matching_leagues = [l for l in leagues if l.league_id == league_id]
            if matching_leagues:
                year = matching_leagues[0].season
                if year not in year_stats:
                    year_stats[year] = {'total': 0, 'with_managers': 0}
                year_stats[year]['total'] += 1
                if team.manager_name:
                    year_stats[year]['with_managers'] += 1
        
        logger.info(f"\n📈 DATA QUALITY BY YEAR:")
        for year in sorted(year_stats.keys(), reverse=True):
            stats = year_stats[year]
            pct = stats['with_managers']/stats['total']*100 if stats['total'] > 0 else 0
            logger.info(f"  {year}: {stats['with_managers']}/{stats['total']} teams with managers ({pct:.1f}%)")
    
    logger.info(f"\n💾 Complete data saved to: {filename}")
    logger.info(f"📊 File ready for database import!")

if __name__ == "__main__":
    main() 