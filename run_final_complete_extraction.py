#!/usr/bin/env python3
"""
Final complete extraction with all data types on all postdraft leagues
"""

import json
import logging
from datetime import datetime
from comprehensive_data_extractor import YahooFantasyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run the final complete extraction on all postdraft leagues"""
    
    start_time = datetime.now()
    logger.info(f"🚀 Starting FINAL COMPLETE extraction (all data types, all postdraft leagues) at {start_time}")
    
    # Create extractor
    extractor = YahooFantasyExtractor()
    
    # Authenticate
    logger.info("🔑 Authenticating...")
    if not extractor.authenticate():
        logger.error("❌ Authentication failed")
        return
    
    # Run full extraction with all data types
    logger.info("📊 Starting comprehensive data extraction...")
    logger.info("🔥 This will extract: Leagues, Teams, Rosters, Matchups, Transactions, Draft Picks")
    logger.info("🔥 From all 26 postdraft leagues spanning 2005-2024 (20 years)")
    
    extracted_data = extractor.extract_all_data()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'yahoo_fantasy_FINAL_complete_data_{timestamp}.json'
    
    logger.info(f"💾 Saving final results to {filename}...")
    extractor.save_to_json(filename)
    
    # Calculate runtime
    end_time = datetime.now()
    runtime = end_time - start_time
    
    # Print final statistics
    logger.info("\n" + "="*80)
    logger.info("🎯 FINAL COMPLETE EXTRACTION RESULTS!")
    logger.info("="*80)
    
    leagues = extracted_data.get('leagues', [])
    teams = extracted_data.get('teams', [])
    rosters = extracted_data.get('rosters', [])
    matchups = extracted_data.get('matchups', [])
    transactions = extracted_data.get('transactions', [])
    draft_picks = extracted_data.get('draft_picks', [])
    
    logger.info(f"📅 Total Runtime: {runtime}")
    logger.info(f"📅 Time Period: 2005-2024 (20 years)")
    logger.info(f"🏆 Total Leagues (postdraft only): {len(leagues)}")
    logger.info(f"👥 Total Teams: {len(teams)}")
    logger.info(f"📋 Total Roster Entries: {len(rosters)}")
    logger.info(f"🏆 Total Matchups: {len(matchups)}")
    logger.info(f"💰 Total Transactions: {len(transactions)}")
    logger.info(f"🎯 Total Draft Picks: {len(draft_picks)}")
    
    # Calculate totals
    total_records = len(leagues) + len(teams) + len(rosters) + len(matchups) + len(transactions) + len(draft_picks)
    logger.info(f"📈 TOTAL DATABASE RECORDS: {total_records:,}")
    
    if teams:
        # Data quality analysis
        teams_with_managers = sum(1 for t in teams if t.get('manager_name') and t['manager_name'].strip() and t['manager_name'] != '--hidden--')
        teams_with_records = sum(1 for t in teams if t.get('wins', 0) > 0 or t.get('losses', 0) > 0)
        teams_with_points = sum(1 for t in teams if t.get('points_for', 0) > 0)
        
        total_wins = sum(t.get('wins', 0) for t in teams)
        total_losses = sum(t.get('losses', 0) for t in teams)
        total_points_for = sum(t.get('points_for', 0) for t in teams)
        
        logger.info(f"\n📈 DATA QUALITY SUMMARY:")
        logger.info(f"✅ Teams with managers: {teams_with_managers}/{len(teams)} ({teams_with_managers/len(teams)*100:.1f}%)")
        logger.info(f"✅ Teams with records: {teams_with_records}/{len(teams)} ({teams_with_records/len(teams)*100:.1f}%)")
        logger.info(f"✅ Teams with points: {teams_with_points}/{len(teams)} ({teams_with_points/len(teams)*100:.1f}%)")
        
        if teams_with_records > 0:
            avg_wins = total_wins / teams_with_records
            avg_losses = total_losses / teams_with_records
            logger.info(f"📊 Average record per team: {avg_wins:.1f}-{avg_losses:.1f}")
            
        if teams_with_points > 0:
            avg_points = total_points_for / teams_with_points
            logger.info(f"📊 Average points per team: {avg_points:.1f}")
        
        logger.info(f"📊 Total games played: {(total_wins + total_losses):,}")
        logger.info(f"📊 Total points scored: {total_points_for:,.1f}")
    
    # Roster analysis
    if rosters:
        unique_players = len(set(r.get('player_id') for r in rosters if r.get('player_id')))
        logger.info(f"📋 Unique players rostered: {unique_players:,}")
    
    # Transaction analysis
    if transactions:
        trade_count = sum(1 for t in transactions if t.get('type') == 'trade')
        add_count = sum(1 for t in transactions if t.get('type') == 'add')
        drop_count = sum(1 for t in transactions if t.get('type') == 'drop')
        
        logger.info(f"💰 Trades: {trade_count:,}")
        logger.info(f"💰 Adds: {add_count:,}")
        logger.info(f"💰 Drops: {drop_count:,}")
    
    # Draft analysis
    if draft_picks:
        auction_drafts = sum(1 for d in draft_picks if d.get('is_auction_draft', False))
        snake_drafts = len(draft_picks) - auction_drafts
        unique_players_drafted = len(set(d.get('player_id') for d in draft_picks if d.get('player_id')))
        
        logger.info(f"🎯 Snake draft picks: {snake_drafts:,}")
        logger.info(f"🎯 Auction draft picks: {auction_drafts:,}")
        logger.info(f"🎯 Unique players drafted: {unique_players_drafted:,}")
        
        if auction_drafts > 0:
            auction_picks = [d for d in draft_picks if d.get('is_auction_draft', False) and d.get('cost')]
            if auction_picks:
                total_spent = sum(d.get('cost', 0) for d in auction_picks)
                avg_cost = total_spent / len(auction_picks)
                logger.info(f"🎯 Total auction spend: ${total_spent:,.0f}")
                logger.info(f"🎯 Average auction cost: ${avg_cost:.1f}")
    
    # Year breakdown
    year_stats = {}
    for team in teams:
        # Find corresponding league
        matching_leagues = [l for l in leagues if l['league_id'] == team.get('league_id')]
        if matching_leagues:
            year = matching_leagues[0]['season']
            if year not in year_stats:
                year_stats[year] = {'leagues': 0, 'teams': 0, 'complete': 0}
            year_stats[year]['teams'] += 1
            
            # Check if team is complete (has manager, records, and points)
            has_manager = team.get('manager_name') and team['manager_name'].strip() and team['manager_name'] != '--hidden--'
            has_records = team.get('wins', 0) > 0 or team.get('losses', 0) > 0
            has_points = team.get('points_for', 0) > 0
            
            if has_manager and has_records and has_points:
                year_stats[year]['complete'] += 1
    
    # Count leagues per year
    for league in leagues:
        year = league['season']
        if year in year_stats:
            year_stats[year]['leagues'] += 1
    
    logger.info(f"\n📈 20-YEAR BREAKDOWN:")
    total_complete_teams = 0
    total_teams_count = 0
    
    for year in sorted(year_stats.keys(), reverse=True):
        stats = year_stats[year]
        pct = stats['complete']/stats['teams']*100 if stats['teams'] > 0 else 0
        logger.info(f"  {year}: {stats['leagues']} leagues, {stats['complete']}/{stats['teams']} teams complete ({pct:.1f}%)")
        total_complete_teams += stats['complete']
        total_teams_count += stats['teams']
    
    overall_pct = total_complete_teams/total_teams_count*100 if total_teams_count > 0 else 0
    logger.info(f"\n🎯 OVERALL DATA QUALITY: {total_complete_teams}/{total_teams_count} teams complete ({overall_pct:.1f}%)")
    
    # File info
    file_size_mb = len(json.dumps(extracted_data, default=str)) / (1024 * 1024)
    logger.info(f"\n💾 Final dataset saved to: {filename}")
    logger.info(f"💾 File size: {file_size_mb:.1f} MB")
    logger.info(f"📊 Ready for database import!")
    logger.info(f"🚀 20-year Yahoo Fantasy Football dataset extraction COMPLETE!")

if __name__ == "__main__":
    main() 