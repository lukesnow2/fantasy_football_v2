#!/usr/bin/env python3
"""
Run extraction with predraft leagues filtered out
"""

import json
import logging
from datetime import datetime
from comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run the filtered extraction (excluding predraft leagues)"""
    
    start_time = datetime.now()
    logger.info(f"🚀 Starting filtered extraction (excluding predraft leagues) at {start_time}")
    
    # Create extractor
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    logger.info("🔑 Authenticating...")
    if not extractor.authenticate():
        logger.error("❌ Authentication failed")
        return
    
    # Run full extraction
    logger.info("📊 Starting comprehensive data extraction (postdraft leagues only)...")
    extracted_data = extractor.extract_all_data()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'yahoo_fantasy_filtered_data_{timestamp}.json'
    
    logger.info(f"💾 Saving results to {filename}...")
    extractor.save_to_json(filename)
    
    # Calculate runtime
    end_time = datetime.now()
    runtime = end_time - start_time
    
    # Print final statistics
    logger.info("\n" + "="*70)
    logger.info("🎯 FILTERED EXTRACTION COMPLETE!")
    logger.info("="*70)
    
    leagues = extracted_data.get('leagues', [])
    teams = extracted_data.get('teams', [])
    
    logger.info(f"📅 Total Runtime: {runtime}")
    logger.info(f"🏆 Total Leagues (postdraft only): {len(leagues)}")
    logger.info(f"👥 Total Teams: {len(teams)}")
    
    if teams:
        # Analyze data quality
        teams_with_managers = 0
        teams_with_records = 0
        teams_with_points = 0
        total_wins = 0
        total_losses = 0
        total_points_for = 0.0
        
        for team in teams:
            # Manager check
            if team.get('manager_name') and team['manager_name'].strip() and team['manager_name'] != '--hidden--':
                teams_with_managers += 1
                
            # Records check
            wins = team.get('wins', 0)
            losses = team.get('losses', 0)
            if wins > 0 or losses > 0:
                teams_with_records += 1
                total_wins += wins
                total_losses += losses
                
            # Points check
            points_for = team.get('points_for', 0)
            if points_for > 0:
                teams_with_points += 1
                total_points_for += points_for
        
        logger.info(f"✅ Teams with managers: {teams_with_managers}/{len(teams)} ({teams_with_managers/len(teams)*100:.1f}%)")
        logger.info(f"✅ Teams with records: {teams_with_records}/{len(teams)} ({teams_with_records/len(teams)*100:.1f}%)")
        logger.info(f"✅ Teams with points: {teams_with_points}/{len(teams)} ({teams_with_points/len(teams)*100:.1f}%)")
        
        # Summary stats
        if teams_with_records > 0:
            avg_wins = total_wins / teams_with_records
            avg_losses = total_losses / teams_with_records
            logger.info(f"📈 Average record per team: {avg_wins:.1f}-{avg_losses:.1f}")
            
        if teams_with_points > 0:
            avg_points = total_points_for / teams_with_points
            logger.info(f"📈 Average points per team: {avg_points:.1f}")
        
        # Year breakdown
        year_stats = {}
        for team in teams:
            league_id = team.get('league_id', '')
            # Find corresponding league
            matching_leagues = [l for l in leagues if l['league_id'] == league_id]
            if matching_leagues:
                year = matching_leagues[0]['season']
                if year not in year_stats:
                    year_stats[year] = {'total': 0, 'complete': 0}
                year_stats[year]['total'] += 1
                
                # Check if team is complete (has manager, records, and points)
                has_manager = team.get('manager_name') and team['manager_name'].strip() and team['manager_name'] != '--hidden--'
                has_records = team.get('wins', 0) > 0 or team.get('losses', 0) > 0
                has_points = team.get('points_for', 0) > 0
                
                if has_manager and has_records and has_points:
                    year_stats[year]['complete'] += 1
        
        logger.info(f"\n📈 DATA QUALITY BY YEAR (POSTDRAFT LEAGUES ONLY):")
        for year in sorted(year_stats.keys(), reverse=True):
            stats = year_stats[year]
            pct = stats['complete']/stats['total']*100 if stats['total'] > 0 else 0
            logger.info(f"  {year}: {stats['complete']}/{stats['total']} teams complete ({pct:.1f}%)")
    
    logger.info(f"\n💾 Filtered data saved to: {filename}")
    logger.info(f"📊 File ready for database import!")
    
    # Compare with previous extraction
    logger.info(f"\n🔄 COMPARISON WITH PREVIOUS EXTRACTION:")
    try:
        with open('yahoo_fantasy_fixed_complete_data_20250605_094453.json', 'r') as f:
            old_data = json.load(f)
        
        old_leagues = len(old_data.get('leagues', []))
        old_teams = len(old_data.get('teams', []))
        
        logger.info(f"  Previous (all leagues): {old_leagues} leagues, {old_teams} teams")
        logger.info(f"  Current (postdraft only): {len(leagues)} leagues, {len(teams)} teams")
        logger.info(f"  Filtered out: {old_leagues - len(leagues)} predraft leagues, {old_teams - len(teams)} teams")
        
    except Exception as e:
        logger.warning(f"Could not load previous extraction for comparison: {e}")

if __name__ == "__main__":
    main() 