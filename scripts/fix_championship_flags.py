#!/usr/bin/env python3
"""
Fix Championship Flags in Public Matchups Table
Using proper playoff bracket logic to correctly identify championship games.
"""

import os
import logging
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChampionshipFixer:
    """Fix championship flags using playoff bracket logic"""
    
    # Historical league of record IDs (2005-2024)
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
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        self.engine = None
        
        if not self.database_url:
            raise ValueError("DATABASE_URL required: set as environment variable or pass directly")
    
    def connect(self) -> bool:
        """Connect to database"""
        try:
            logger.info("🔌 Connecting to database...")
            
            # Fix URL for newer SQLAlchemy
            url = self.database_url.replace('postgres://', 'postgresql://', 1)
            self.engine = create_engine(url)
            
            # Test connection
            with self.engine.connect() as conn:
                version = conn.execute(text("SELECT version()")).fetchone()[0]
                logger.info(f"✅ Connected: {version.split()[0:2]}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            return False
    
    def fix_all_championships(self) -> bool:
        """Fix championship flags for all leagues of record"""
        try:
            logger.info("🏆 Fixing championship flags using playoff bracket logic...")
            
            with self.engine.connect() as conn:
                # Get league-season combinations for leagues of record only
                result = conn.execute(text("""
                    SELECT DISTINCT m.league_id, l.season 
                    FROM public.matchups m
                    JOIN public.leagues l ON m.league_id = l.league_id
                    WHERE l.game_code = 'nfl'
                      AND m.league_id = ANY(:league_ids)
                    ORDER BY l.season DESC, m.league_id
                """), {"league_ids": list(self.HISTORICAL_LEAGUE_IDS)})
                
                league_seasons = list(result)
                success_count = 0
                
                for league_id, season_year in league_seasons:
                    if self._fix_league_championship(conn, league_id, season_year):
                        success_count += 1
                
                conn.commit()
                logger.info(f"✅ Fixed championships for {success_count}/{len(league_seasons)} league-seasons")
                
            return True
        except Exception as e:
            logger.error(f"❌ Championship fix failed: {e}")
            return False
    
    def _fix_league_championship(self, conn, league_id: str, season_year: int) -> bool:
        """Fix championship flags using proper playoff bracket logic"""
        try:
            # Step 1: Find playoff weeks (excluding consolation)
            playoff_result = conn.execute(text("""
                SELECT DISTINCT week
                FROM public.matchups
                WHERE league_id = :league_id AND is_playoffs = TRUE AND is_consolation = FALSE
                ORDER BY week
            """), {"league_id": league_id})
            
            playoff_weeks = sorted([row[0] for row in playoff_result])
            if len(playoff_weeks) < 3:
                logger.warning(f"⚠️ {league_id} {season_year}: only {len(playoff_weeks)} playoff weeks, need 3")
                return False
            
            # Step 2: Define the three key weeks
            championship_week = playoff_weeks[-1]      # Last week = championship + 3rd place
            semifinals_week = playoff_weeks[-2]        # Second to last = semifinals + 5th place  
            quarterfinals_week = playoff_weeks[-3]     # Third to last = quarterfinals
            
            logger.info(f"  📅 {league_id} {season_year}: Quarters={quarterfinals_week}, Semis={semifinals_week}, Championship={championship_week}")
            
            # Step 3: Find quarterfinals winners and losers
            quarterfinals_result = conn.execute(text("""
                SELECT team1_id, team2_id, winner_team_id, team1_score, team2_score
                FROM public.matchups
                WHERE league_id = :league_id AND week = :week
                  AND is_playoffs = TRUE AND is_consolation = FALSE
            """), {"league_id": league_id, "week": quarterfinals_week})
            
            quarterfinals_winners = set()
            quarterfinals_losers = set()
            
            for team1_id, team2_id, winner_team_id, team1_score, team2_score in quarterfinals_result:
                # Determine winner if not explicitly set
                if not winner_team_id:
                    if team1_score > team2_score:
                        winner_team_id = team1_id
                    elif team2_score > team1_score:
                        winner_team_id = team2_id
                
                if winner_team_id:
                    quarterfinals_winners.add(winner_team_id)
                    # Add the loser
                    loser_team_id = team2_id if winner_team_id == team1_id else team1_id
                    quarterfinals_losers.add(loser_team_id)
            
            logger.info(f"  🏆 Found {len(quarterfinals_winners)} quarterfinals winners, {len(quarterfinals_losers)} losers")
            
            # Step 4: Find semifinals winners (exclude 5th place game)
            semifinals_result = conn.execute(text("""
                SELECT team1_id, team2_id, winner_team_id, team1_score, team2_score, matchup_id
                FROM public.matchups
                WHERE league_id = :league_id AND week = :week
                  AND is_playoffs = TRUE AND is_consolation = FALSE
            """), {"league_id": league_id, "week": semifinals_week})
            
            semifinals_winners = set()
            
            for team1_id, team2_id, winner_team_id, team1_score, team2_score, matchup_id in semifinals_result:
                # Check if this is a semifinals game or 5th place game
                team1_won_quarters = team1_id in quarterfinals_winners
                team2_won_quarters = team2_id in quarterfinals_winners
                team1_lost_quarters = team1_id in quarterfinals_losers
                team2_lost_quarters = team2_id in quarterfinals_losers
                
                # Semifinals game = at least one team won quarterfinals
                # 5th place game = both teams lost quarterfinals
                if team1_won_quarters or team2_won_quarters:
                    # This is a semifinals game - determine winner
                    if not winner_team_id:
                        if team1_score > team2_score:
                            winner_team_id = team1_id
                        elif team2_score > team1_score:
                            winner_team_id = team2_id
                    
                    if winner_team_id:
                        semifinals_winners.add(winner_team_id)
                    logger.info(f"  🏆 Semifinals game: {team1_id} vs {team2_id}, winner: {winner_team_id}")
                elif team1_lost_quarters and team2_lost_quarters:
                    logger.info(f"  5️⃣ 5th place game: {team1_id} vs {team2_id}")
            
            logger.info(f"  🏆 Found {len(semifinals_winners)} semifinals winners")
            
            # Step 5: Find championship game = both teams are semifinals winners
            championship_result = conn.execute(text("""
                SELECT team1_id, team2_id, matchup_id
                FROM public.matchups
                WHERE league_id = :league_id AND week = :week
                  AND is_playoffs = TRUE AND is_consolation = FALSE
            """), {"league_id": league_id, "week": championship_week})
            
            championship_matchup_id = None
            
            for team1_id, team2_id, matchup_id in championship_result:
                team1_won_semis = team1_id in semifinals_winners
                team2_won_semis = team2_id in semifinals_winners
                
                if team1_won_semis and team2_won_semis:
                    championship_matchup_id = matchup_id
                    logger.info(f"  🏆 Championship game found: {team1_id} vs {team2_id}")
                    break
                else:
                    logger.info(f"  🥉 3rd place game: {team1_id} vs {team2_id}")
            
            # Step 6: Update the database
            if championship_matchup_id:
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
                
                logger.info(f"  ✅ {league_id} {season_year}: Championship fixed using bracket logic")
                return True
            else:
                logger.warning(f"  ⚠️ {league_id} {season_year}: Could not determine championship using bracket logic")
                return False
                
        except Exception as e:
            logger.warning(f"  ❌ {league_id} {season_year}: {e}")
            return False
    
    def verify_championships(self) -> bool:
        """Verify that each league has exactly one championship"""
        try:
            logger.info("🔍 Verifying championship fixes...")
            
            with self.engine.connect() as conn:
                # Check for leagues with wrong number of championships
                result = conn.execute(text("""
                    SELECT m.league_id, l.season, COUNT(*) as championship_count
                    FROM public.matchups m
                    JOIN public.leagues l ON m.league_id = l.league_id
                    WHERE m.is_championship = TRUE 
                      AND l.game_code = 'nfl'
                      AND m.league_id = ANY(:league_ids)
                    GROUP BY m.league_id, l.season
                    HAVING COUNT(*) != 1
                    ORDER BY l.season DESC, m.league_id
                """), {"league_ids": list(self.HISTORICAL_LEAGUE_IDS)})
                
                problem_leagues = list(result)
                if problem_leagues:
                    logger.warning(f"⚠️ CHAMPIONSHIP ISSUES ({len(problem_leagues)} leagues):")
                    for league_id, season, count in problem_leagues:
                        logger.warning(f"  {league_id} {season}: {count} championships")
                    return False
                else:
                    logger.info(f"✅ CHAMPIONSHIP STATUS: All leagues have exactly 1 championship!")
                    return True
        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return False
    
    def run(self) -> bool:
        """Execute the championship fix process"""
        steps = [
            ("Connect", self.connect),
            ("Fix Championships", self.fix_all_championships),
            ("Verify Championships", self.verify_championships)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"🚀 Step: {step_name}")
            if not step_func():
                logger.error(f"❌ Step '{step_name}' failed")
                return False
        
        logger.info("🎉 Championship fix process completed successfully!")
        return True

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fix championship flags in public.matchups')
    parser.add_argument('--database-url', help='Database URL (or set DATABASE_URL env var)')
    args = parser.parse_args()
    
    try:
        fixer = ChampionshipFixer(database_url=args.database_url)
        success = fixer.run()
        exit(0 if success else 1)
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        exit(1)

if __name__ == "__main__":
    main() 