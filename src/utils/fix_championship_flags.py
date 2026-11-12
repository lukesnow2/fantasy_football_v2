#!/usr/bin/env python3
"""
Fix Championship Flags in public.matchups

This script corrects the is_championship flags based on proper playoff progression logic:
1. Playoffs are always 3 weeks long every season
2. is_championship = TRUE only for the matchup between the two teams who won 
   playoff games (is_playoff=TRUE, is_consolation=FALSE) for 3 consecutive weeks
"""

import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from typing import Dict, List, Tuple, Set

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChampionshipFixer:
    """Fix championship flags in public.matchups based on playoff progression"""
    
    def __init__(self, database_url: str = None, dry_run: bool = False):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        self.engine = None
        self.dry_run = dry_run
        
        if not self.database_url:
            raise ValueError("DATABASE_URL required")
    
    def connect(self) -> bool:
        """Connect to database"""
        try:
            logger.info("🔌 Connecting to database...")
            url = self.database_url.replace('postgres://', 'postgresql://', 1)
            self.engine = create_engine(url)
            
            with self.engine.connect() as conn:
                version = conn.execute(text("SELECT version()")).fetchone()[0]
                logger.info(f"✅ Connected: {version.split()[0:2]}")
            return True
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            return False
    
    def fix_all_championships(self) -> bool:
        """Fix championship flags for leagues of record only"""
        # Historical league of record IDs (2005-2024) - same as EDW processor
        HISTORICAL_LEAGUE_IDS = {
            "449.l.674707",    # Idaho's DEI Quota (2024)
            "423.l.841006",    # Move the Raiders to PDX (2023)
            "414.l.1194955",   # Wet Hot Tahoe Summer (2022)
            "406.l.1065326",   # Rocky Mountain High (2021)
            "399.l.837311",    # The Lost Year (2020)
            "390.l.777720",    # Women & Women First (2019)
            "380.l.1143665",   # Sleepless In Seattle (2018)
            "371.l.1025465",   # Go Fuck Yourself San Diego (2017)
            "359.l.696366",    # The Great SF Draft (2016)
            "348.l.655822",    # Luke's Kingdom (2015)
            "331.l.355899",    # 10 Years 10 Assholes (2014)
            "314.l.319572",    # Rosterbaters Anonymous (2013)
            "273.l.107980",    # The League About Nothing (2012)
            "257.l.89145",     # Lock It Up (2011)
            "242.l.413666",    # Round 6 (2010)
            "222.l.222935",    # Engaged (2009)
            "199.l.42364",     # The Draft (2008)
            "175.l.658531",    # Oakdale Park (2007)
            "153.l.76788",     # Oakdale Park (2006)
            "124.l.109785"     # Oakdale Park (2005)
        }
        
        try:
            with self.engine.connect() as conn:
                # Get league-season combinations for leagues of record only
                result = conn.execute(text("""
                    SELECT DISTINCT m.league_id, l.season 
                    FROM public.matchups m
                    JOIN public.leagues l ON m.league_id = l.league_id
                    WHERE l.game_code = 'nfl'
                      AND m.league_id = ANY(:league_ids)
                    ORDER BY l.season DESC, m.league_id
                """), {"league_ids": list(HISTORICAL_LEAGUE_IDS)})
                
                league_seasons = list(result)
                logger.info(f"📊 Processing {len(league_seasons)} league-seasons...")
                
                success_count = 0
                
                for league_id, season_year in league_seasons:
                    if self.fix_league_championships(league_id, season_year):
                        success_count += 1
                
                logger.info(f"✅ Fixed {success_count}/{len(league_seasons)} league-seasons")
                self.verify_championship_fixes()
                return True
                
        except Exception as e:
            logger.error(f"❌ Fix process failed: {e}")
            return False
    
    def fix_league_championships(self, league_id: str, season_year: int) -> bool:
        """Fix championship flags for a specific league-season"""
        try:
            with self.engine.connect() as conn:
                # Find playoff weeks
                playoff_result = conn.execute(text("""
                    SELECT DISTINCT week
                    FROM public.matchups
                    WHERE league_id = :league_id AND is_playoffs = TRUE AND is_consolation = FALSE
                    ORDER BY week
                """), {"league_id": league_id})
                
                playoff_weeks = sorted([row[0] for row in playoff_result])
                if len(playoff_weeks) < 2:
                    logger.warning(f"⚠️ {league_id} {season_year}: only {len(playoff_weeks)} playoff weeks")
                    return False
                
                # Championship week is the LAST playoff week (Week 16 or 17)
                championship_week = playoff_weeks[-1]
                
                # Semifinal week is the SECOND-TO-LAST playoff week
                semifinal_week = playoff_weeks[-2]
                
                logger.info(f"🏈 {league_id} {season_year}: Semifinal={semifinal_week}, Championship={championship_week}")
                
                # Find the championship game participants first (they should be marked in championship week)
                champ_game_result = conn.execute(text("""
                    SELECT team1_id, team2_id, matchup_id
                    FROM public.matchups
                    WHERE league_id = :league_id AND week = :week
                      AND is_playoffs = TRUE AND is_consolation = FALSE
                      AND is_championship = TRUE
                """), {"league_id": league_id, "week": championship_week})
                
                champ_game_row = champ_game_result.fetchone()
                if champ_game_row:
                    # Found an existing championship game - we'll still reset all and set this one
                    team1, team2, championship_matchup_id = champ_game_row
                    logger.info(f"   Found existing championship: {team1} vs {team2}")
                else:
                    # If no championship marked, find it by looking for the non-consolation game 
                    # between teams who won semifinals
                    all_champ_week_games = conn.execute(text("""
                        SELECT team1_id, team2_id, matchup_id
                        FROM public.matchups
                        WHERE league_id = :league_id AND week = :week
                          AND is_playoffs = TRUE AND is_consolation = FALSE
                    """), {"league_id": league_id, "week": championship_week})
                    
                    # Get all semifinal winners
                    semifinal_winners = self.get_playoff_winners(conn, league_id, semifinal_week)
                    logger.info(f"   Semifinal week winners: {semifinal_winners}")
                    
                    # Find the game in championship week between semifinal winners
                    championship_matchup_id = None
                    team1, team2 = None, None
                    
                    for game_team1, game_team2, matchup_id in all_champ_week_games:
                        if game_team1 in semifinal_winners and game_team2 in semifinal_winners:
                            championship_matchup_id = matchup_id
                            team1, team2 = game_team1, game_team2
                            break
                    
                    if not championship_matchup_id:
                        logger.warning(f"⚠️ No championship matchup found between semifinal winners in week {championship_week}")
                        return False
                
                logger.info(f"   Championship finalists: {team1} vs {team2}")
                
                # Update database (unless dry run)
                if self.dry_run:
                    logger.info(f"🔍 DRY RUN: Would set championship for {championship_matchup_id} (Week {championship_week}) in {league_id} {season_year}")
                    return True
                else:
                    # Reset all championships for this league
                    conn.execute(text("""
                        UPDATE public.matchups SET is_championship = FALSE 
                        WHERE league_id = :league_id
                    """), {"league_id": league_id})
                    
                    # Set the correct championship
                    conn.execute(text("""
                        UPDATE public.matchups SET is_championship = TRUE 
                        WHERE matchup_id = :matchup_id
                    """), {"matchup_id": championship_matchup_id})
                    
                    conn.commit()
                    logger.info(f"✅ {league_id} {season_year}: Championship set for {championship_matchup_id} (Week {championship_week})")
                    return True
                
        except Exception as e:
            logger.error(f"❌ Failed to fix {league_id} {season_year}: {e}")
            return False
    
    def get_playoff_winners(self, conn, league_id: str, week: int) -> Set[str]:
        """Get teams that won playoff games in a specific week"""
        result = conn.execute(text("""
            SELECT winner_team_id FROM public.matchups
            WHERE league_id = :league_id AND week = :week
              AND is_playoffs = TRUE AND is_consolation = FALSE
              AND winner_team_id IS NOT NULL
        """), {"league_id": league_id, "week": week})
        
        return {row[0] for row in result if row[0]}
    
    def get_playoff_losers(self, conn, league_id: str, week: int) -> Set[str]:
        """Get teams that lost playoff games in a specific week"""
        result = conn.execute(text("""
            SELECT CASE 
                WHEN winner_team_id = team1_id THEN team2_id
                WHEN winner_team_id = team2_id THEN team1_id
                ELSE NULL
            END as loser_team_id
            FROM public.matchups
            WHERE league_id = :league_id AND week = :week
              AND is_playoffs = TRUE AND is_consolation = FALSE
              AND winner_team_id IS NOT NULL
        """), {"league_id": league_id, "week": week})
        
        return {row[0] for row in result if row[0]}
    
    def verify_championship_fixes(self) -> None:
        """Verify championship fixes for leagues of record"""
        # Same historical league IDs as in fix_all_championships
        HISTORICAL_LEAGUE_IDS = {
            "449.l.674707", "423.l.841006", "414.l.1194955", "406.l.1065326", "399.l.837311",
            "390.l.777720", "380.l.1143665", "371.l.1025465", "359.l.696366", "348.l.655822",
            "331.l.355899", "314.l.319572", "273.l.107980", "257.l.89145", "242.l.413666",
            "222.l.222935", "199.l.42364", "175.l.658531", "153.l.76788", "124.l.109785"
        }
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT m.league_id, l.season, COUNT(*) as championship_count
                    FROM public.matchups m
                    JOIN public.leagues l ON m.league_id = l.league_id
                    WHERE m.is_championship = TRUE AND l.game_code = 'nfl'
                      AND m.league_id = ANY(:league_ids)
                    GROUP BY m.league_id, l.season
                    ORDER BY l.season DESC, m.league_id
                """), {"league_ids": list(HISTORICAL_LEAGUE_IDS)})
                
                logger.info("\n🔍 CHAMPIONSHIP VERIFICATION:")
                multiple_championships = 0
                total_leagues = 0
                
                for league_id, season, count in result:
                    total_leagues += 1
                    status = "✅" if count == 1 else "❌"
                    logger.info(f"{status} {league_id} {season}: {count} championship(s)")
                    if count > 1:
                        multiple_championships += 1
                
                if multiple_championships == 0:
                    logger.info(f"🎉 SUCCESS: All {total_leagues} league-seasons have exactly 1 championship!")
                else:
                    logger.warning(f"⚠️ {multiple_championships} league-seasons still have multiple championships")
        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
    
    def run(self) -> bool:
        """Execute the complete fix process"""
        logger.info("🚀 Starting Championship Flag Fix Process")
        
        if not self.connect():
            return False
        
        return self.fix_all_championships()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Fix championship flags in public.matchups')
    parser.add_argument('--database-url', 
                       help='DATABASE_URL (or set DATABASE_URL env var)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be changed without making changes')
    
    args = parser.parse_args()
    
    try:
        fixer = ChampionshipFixer(database_url=args.database_url, dry_run=args.dry_run)
        
        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - No changes will be made")
        
        if fixer.run():
            if args.dry_run:
                logger.info("🔍 Dry run completed successfully!")
            else:
                logger.info("🏆 Championship flags fixed successfully!")
        else:
            logger.error("❌ Championship fix failed")
            return 1
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
