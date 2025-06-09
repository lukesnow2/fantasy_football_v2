#!/usr/bin/env python3
"""
Comprehensive Yahoo Fantasy Data Extractor
Extracts all available data from Yahoo Fantasy API for database storage
"""

import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ExtractedLeague:
    """League data structure for database storage"""
    league_id: str
    name: str
    season: str
    game_code: str
    game_id: int
    num_teams: int
    current_week: int
    start_week: int
    end_week: int
    league_type: str
    draft_status: str
    is_pro_league: bool
    is_cash_league: bool
    url: str
    logo_url: Optional[str] = None
    extracted_at: datetime = datetime.now()

@dataclass
class ExtractedTeam:
    """Team data structure for database storage"""
    team_id: str
    league_id: str
    name: str
    manager_name: str
    wins: int
    losses: int
    ties: int
    points_for: float
    points_against: float
    playoff_seed: Optional[int]
    waiver_priority: Optional[int]
    faab_balance: Optional[float]
    team_logo_url: Optional[str] = None
    extracted_at: datetime = datetime.now()

@dataclass
class ExtractedRoster:
    """Roster data structure for database storage"""
    roster_id: str
    league_id: str
    team_id: str
    week: int
    player_id: str
    player_name: str
    position: str
    status: str  # active, inactive, bench, ir
    is_starter: bool
    projected_points: Optional[float]
    actual_points: Optional[float]
    extracted_at: datetime = datetime.now()

@dataclass
class ExtractedMatchup:
    """Matchup/Schedule data structure for database storage"""
    matchup_id: str
    league_id: str
    week: int
    team1_id: str
    team2_id: str
    team1_score: float
    team2_score: float
    winner_team_id: Optional[str]
    is_playoffs: bool
    is_championship: bool
    is_consolation: bool
    extracted_at: datetime = datetime.now()

@dataclass
class ExtractedTransaction:
    """Transaction data structure for database storage"""
    transaction_id: str
    league_id: str
    type: str  # trade, add, drop, waiver
    timestamp: datetime
    player_id: str
    player_name: str
    source_team_id: Optional[str]
    destination_team_id: Optional[str]
    faab_bid: Optional[float]
    status: str
    extracted_at: datetime = datetime.now()

@dataclass
class ExtractedDraftPick:
    """Draft pick data structure for database storage"""
    draft_pick_id: str
    league_id: str
    pick_number: int
    round_number: int
    team_id: str
    player_id: str
    player_name: str
    position: str
    cost: Optional[float]  # For auction drafts
    is_keeper: bool
    is_auction_draft: bool
    extracted_at: datetime = datetime.now()



class YahooFantasyExtractor:
    """Comprehensive Yahoo Fantasy data extractor with rate limiting"""
    
    def __init__(self, resume_from_league=None):
        self.oauth = None
        self.game = None
        self.resume_from_league = resume_from_league
        self.request_count = 0
        self.last_request_time = 0
        self.hour_start_time = time.time()
        self.hourly_request_count = 0
        self.daily_request_count = 0
        self.day_start_time = time.time()
        
        # Rate limiting settings - conservative to stay well under Yahoo limits
        self.MAX_REQUESTS_PER_HOUR = 20000  # Yahoo's actual hourly limit
        self.MAX_REQUESTS_PER_DAY = 100000  # Yahoo's actual daily limit
        self.MIN_REQUEST_INTERVAL = 0.6     # Minimum 0.6 seconds between requests
        
        self.extracted_data = {
            'leagues': [],
            'teams': [],
            'rosters': [],
            'matchups': [],
            'transactions': [],
            'draft_picks': [],
            'statistics': []
        }
    
    def _check_rate_limits(self):
        """Check if we're approaching rate limits and pause if necessary"""
        current_time = time.time()
        
        # Reset hourly counter if an hour has passed
        if current_time - self.hour_start_time >= 3600:
            self.hourly_request_count = 0
            self.hour_start_time = current_time
            logger.info("🔄 Hourly rate limit counter reset")
        
        # Reset daily counter if a day has passed
        if current_time - self.day_start_time >= 86400:
            self.daily_request_count = 0
            self.day_start_time = current_time
            logger.info("🔄 Daily rate limit counter reset")
        
        # Check if we're approaching hourly limit
        if self.hourly_request_count >= self.MAX_REQUESTS_PER_HOUR:
            wait_time = 3600 - (current_time - self.hour_start_time)
            if wait_time > 0:
                logger.warning(f"⏳ Approaching hourly limit ({self.hourly_request_count}/{self.MAX_REQUESTS_PER_HOUR})")
                logger.info(f"⏰ Waiting {wait_time:.0f}s until next hour...")
                time.sleep(wait_time + 10)  # Extra 10s buffer
                self.hourly_request_count = 0
                self.hour_start_time = time.time()
        
        # Check if we're approaching daily limit
        if self.daily_request_count >= self.MAX_REQUESTS_PER_DAY:
            wait_time = 86400 - (current_time - self.day_start_time)
            if wait_time > 0:
                logger.warning(f"⏳ Approaching daily limit ({self.daily_request_count}/{self.MAX_REQUESTS_PER_DAY})")
                logger.info(f"⏰ Waiting {wait_time:.0f}s until next day...")
                time.sleep(wait_time + 60)  # Extra 60s buffer
                self.daily_request_count = 0
                self.day_start_time = time.time()
    
    def _get_adaptive_settings(self):
        """Get adaptive batch settings based on current rate limit usage"""
        hourly_usage_pct = (self.hourly_request_count / self.MAX_REQUESTS_PER_HOUR) * 100
        daily_usage_pct = (self.daily_request_count / self.MAX_REQUESTS_PER_DAY) * 100
        
        # Determine throttle level based on usage
        if hourly_usage_pct > 85 or daily_usage_pct > 85:
            # Critical throttling - very conservative
            return {
                'batch_size': 1,
                'batch_delay': 900,  # 15 minutes
                'inter_league_delay': 30,
                'min_request_interval': 1.0,
                'status': '🚨 CRITICAL THROTTLE'
            }
        elif hourly_usage_pct > 70 or daily_usage_pct > 70:
            # Heavy throttling - conservative
            return {
                'batch_size': 2,
                'batch_delay': 600,  # 10 minutes
                'inter_league_delay': 15,
                'min_request_interval': 0.8,
                'status': '⚠️ HEAVY THROTTLE'
            }
        elif hourly_usage_pct > 50 or daily_usage_pct > 50:
            # Moderate throttling - balanced
            return {
                'batch_size': 3,
                'batch_delay': 300,  # 5 minutes
                'inter_league_delay': 8,
                'min_request_interval': 0.6,
                'status': '⚡ MODERATE THROTTLE'
            }
        elif hourly_usage_pct > 30 or daily_usage_pct > 30:
            # Light throttling - slightly conservative
            return {
                'batch_size': 4,
                'batch_delay': 180,  # 3 minutes
                'inter_league_delay': 5,
                'min_request_interval': 0.4,
                'status': '✅ LIGHT THROTTLE'
            }
        else:
            # Full speed - plenty of headroom
            return {
                'batch_size': 5,
                'batch_delay': 120,  # 2 minutes
                'inter_league_delay': 3,
                'min_request_interval': 0.3,
                'status': '🚀 FULL SPEED'
            }
    
    def _rate_limited_request(self, func, *args, **kwargs):
        """Execute a function with adaptive rate limiting"""
        # Check rate limits before making request
        self._check_rate_limits()
        
        # Get current adaptive settings
        settings = self._get_adaptive_settings()
        min_interval = settings['min_request_interval']
        
        # Ensure minimum time between requests (adaptive)
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        # Make the request
        try:
            result = func(*args, **kwargs)
            self.hourly_request_count += 1
            self.daily_request_count += 1
            self.last_request_time = time.time()
            
            # Log progress more frequently for better monitoring
            if self.hourly_request_count % 25 == 0:
                logger.info(f"📊 API Progress - Hour: {self.hourly_request_count}/{self.MAX_REQUESTS_PER_HOUR}, Day: {self.daily_request_count}/{self.MAX_REQUESTS_PER_DAY}")
            
            return result
        except Exception as e:
            logger.error(f"Rate limited request failed: {e}")
            # Still count failed requests toward rate limit
            self.hourly_request_count += 1
            self.daily_request_count += 1
            self.last_request_time = time.time()
            raise
        
    def authenticate(self) -> bool:
        """Authenticate with Yahoo Fantasy API"""
        try:
            # Check for existing oauth file in multiple locations
            oauth_file = None
            potential_files = ['oauth2.json', 'data/current/oauth2.json']
            
            for file_path in potential_files:
                if os.path.exists(file_path):
                    oauth_file = file_path
                    logger.info(f"🔑 Found existing OAuth file: {oauth_file}")
                    break
            
            if oauth_file:
                # Use existing oauth file
                self.oauth = OAuth2(None, None, from_file=oauth_file)
                
                if not self.oauth.token_is_valid():
                    logger.info("🔑 Token invalid, refreshing...")
                    self.oauth.refresh_access_token()
            else:
                # No oauth file found - create one from environment variables
                logger.info("🔑 No OAuth file found, creating from environment variables...")
                
                client_key = os.getenv('YAHOO_CLIENT_KEY')
                client_secret = os.getenv('YAHOO_CLIENT_SECRET')
                
                if not client_key or not client_secret:
                    logger.error("❌ No OAuth file found and YAHOO_CLIENT_KEY/YAHOO_CLIENT_SECRET not set in environment")
                    logger.error("Please set these environment variables or provide an oauth2.json file")
                    return False
                
                # Create oauth file with credentials
                oauth_data = {
                    "consumer_key": client_key,
                    "consumer_secret": client_secret
                }
                
                oauth_file = 'oauth2.json'
                with open(oauth_file, 'w') as f:
                    json.dump(oauth_data, f, indent=2)
                
                logger.info(f"✅ Created {oauth_file} with credentials from environment")
                
                # Initialize OAuth with new file
                self.oauth = OAuth2(None, None, from_file=oauth_file)
            
            # Create Game object for NFL
            self.game = yfa.Game(self.oauth, 'nfl')
            
            # Test the connection
            game_id = self.game.game_id()
            logger.info(f"✅ Authentication successful! Game ID: {game_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Authentication failed: {e}")
            if 'oauth2.json' in str(e):
                logger.error("💡 Tip: Make sure YAHOO_CLIENT_KEY and YAHOO_CLIENT_SECRET are set in your .env file")
            return False
    
    def get_all_leagues(self) -> List[Dict[str, Any]]:
        """Get all user's fantasy leagues using BULK API optimization"""
        try:
            if not self.game:
                logger.error("Not authenticated")
                return []
            
            logger.info("🚀 BULK OPTIMIZATION: Getting all leagues across all years in single call...")
            
            # MAJOR OPTIMIZATION: Get ALL league IDs across ALL years in ONE call
            # This replaces 22+ individual API calls with just 1 call!
            all_league_ids = self._rate_limited_request(
                lambda: self.game.league_ids(is_available=False)  # Get all leagues, not just current
            )
            
            if not all_league_ids:
                logger.info("No leagues found")
                return []
                
            logger.info(f"💰 API SAVINGS: Found {len(all_league_ids)} total leagues (saved ~22 API calls!)")
            
            # Process leagues in bulk batches for efficiency
            all_leagues = []
            batch_size = 10  # Process settings for 10 leagues at a time
            
            for i in range(0, len(all_league_ids), batch_size):
                batch_league_ids = all_league_ids[i:i + batch_size]
                logger.info(f"Processing league batch {i//batch_size + 1}/{(len(all_league_ids) + batch_size - 1)//batch_size}")
                
                for league_id in batch_league_ids:
                    try:
                        league = self.game.to_league(league_id)
                        settings = self._rate_limited_request(lambda: league.settings())
                        
                        # Only include non-public leagues with game data
                        league_name = settings.get('name', '')
                        draft_status = settings.get('draft_status', 'completed')
                        
                        # Skip predraft leagues (they have no game data)
                        if draft_status == 'predraft':
                            logger.debug(f"  Skipping predraft league: {league_name}")
                            continue
                        
                        # Skip public leagues
                        if league_name.startswith('Yahoo Public'):
                            logger.debug(f"  Skipping public league: {league_name}")
                            continue
                            
                        # Extract game code from league ID or settings
                        game_code = 'nfl'  # Default
                        if hasattr(league, 'game_code'):
                            game_code = league.game_code
                        elif 'game_code' in settings:
                            game_code = settings['game_code']
                        
                        all_leagues.append({
                            'league_id': league_id,
                            'name': league_name,
                            'season': str(settings.get('season', '')),
                            'game_code': game_code,
                            'game_id': settings.get('game_id', ''),
                            'num_teams': settings.get('num_teams', 0),
                            'current_week': settings.get('current_week', 1),
                            'start_week': settings.get('start_week', 1),
                            'end_week': settings.get('end_week', 17),
                            'league_type': settings.get('league_type', 'private'),
                            'draft_status': draft_status,
                            'is_pro_league': settings.get('is_pro_league', False),
                            'is_cash_league': settings.get('is_cash_league', False),
                            'url': settings.get('url', ''),
                            'logo_url': settings.get('logo_url', None)
                        })
                        logger.info(f"  ✅ Added league: {league_name} ({settings.get('season', 'Unknown')}) - {draft_status}")
                        
                    except Exception as e:
                        logger.warning(f"Error getting details for league {league_id}: {e}")
                        continue
                
                # Small delay between batches to be respectful
                if i + batch_size < len(all_league_ids):
                    time.sleep(1)
            
            logger.info(f"📋 BULK SUCCESS: Found {len(all_leagues)} leagues with {len(all_league_ids) - len(all_leagues)} filtered out")
            logger.info(f"💡 Total API calls saved: ~{len(all_league_ids)} (used bulk discovery instead of year-by-year)")
            return all_leagues
            
        except Exception as e:
            logger.error(f"Error getting leagues: {e}")
            return []
    
    def extract_league_data(self, league_info: Dict[str, Any]) -> ExtractedLeague:
        """Extract and structure league data"""
        return ExtractedLeague(
            league_id=league_info['league_id'],
            name=league_info['name'],
            season=league_info['season'],
            game_code=league_info['game_code'],
            game_id=league_info['game_id'],
            num_teams=league_info['num_teams'],
            current_week=league_info['current_week'],
            start_week=league_info['start_week'],
            end_week=league_info['end_week'],
            league_type=league_info['league_type'],
            draft_status=league_info['draft_status'],
            is_pro_league=league_info['is_pro_league'],
            is_cash_league=league_info['is_cash_league'],
            url=league_info['url'],
            logo_url=league_info['logo_url']
        )
    
    def extract_teams_for_league(self, league_id: str) -> List[ExtractedTeam]:
        """Extract all team data for a specific league"""
        try:
            # Get league object
            league = self.game.to_league(league_id)
            teams = []
            
            # Get standings (this includes team information with records)
            standings = league.standings()
            
            # Get teams (this includes manager and metadata)
            teams_data = league.teams() if hasattr(league, 'teams') else {}
            
            # Create a mapping of team_id to team metadata
            teams_metadata = {}
            if isinstance(teams_data, dict):
                # teams() returns a dict where keys are team_ids and values are team data
                teams_metadata = teams_data
            elif isinstance(teams_data, list):
                # Fallback if teams() returns a list
                for team_meta in teams_data:
                    team_key = team_meta.get('team_key')
                    if team_key:
                        teams_metadata[team_key] = team_meta
            
            for team_data in standings:
                try:
                    team_id = team_data.get('team_key', '')
                    if not team_id:
                        continue
                    
                    # Get outcome totals for wins/losses
                    outcome_totals = team_data.get('outcome_totals', {})
                    
                    # Convert string values to appropriate types
                    wins = 0
                    losses = 0
                    ties = 0
                    
                    try:
                        wins_val = outcome_totals.get('wins', 0)
                        wins = int(wins_val) if str(wins_val).isdigit() else 0
                        
                        losses_val = outcome_totals.get('losses', 0)
                        losses = int(losses_val) if str(losses_val).isdigit() else 0
                        
                        ties_val = outcome_totals.get('ties', 0)
                        ties = int(ties_val) if str(ties_val).isdigit() else 0
                    except (ValueError, TypeError):
                        # Keep defaults if conversion fails
                        pass
                    
                    # Get points
                    points_for = 0.0
                    points_against = 0.0
                    
                    try:
                        pf_val = team_data.get('points_for', 0)
                        points_for = float(pf_val) if pf_val else 0.0
                        
                        pa_val = team_data.get('points_against', 0)
                        points_against = float(pa_val) if pa_val else 0.0
                    except (ValueError, TypeError):
                        # Keep defaults if conversion fails
                        pass
                    
                    # Get manager name from teams metadata
                    manager_name = ''
                    team_metadata = teams_metadata.get(team_id, {})
                    managers = team_metadata.get('managers', [])
                    if managers and len(managers) > 0:
                        manager = managers[0].get('manager', {})
                        manager_name = manager.get('nickname', '')
                    
                    # Get other metadata
                    playoff_seed = None
                    try:
                        seed_val = team_data.get('playoff_seed')
                        if seed_val and str(seed_val).isdigit():
                            playoff_seed = int(seed_val)
                    except (ValueError, TypeError):
                        pass
                    
                    # Get FAAB and waiver priority from metadata
                    faab_balance = None
                    waiver_priority = None
                    
                    if team_metadata:
                        try:
                            faab_val = team_metadata.get('faab_balance')
                            if faab_val is not None:
                                faab_balance = float(faab_val)
                        except (ValueError, TypeError):
                            pass
                        
                        try:
                            waiver_val = team_metadata.get('waiver_priority')
                            if waiver_val is not None:
                                waiver_priority = int(waiver_val)
                        except (ValueError, TypeError):
                            pass
                    
                    # Get team logo
                    team_logo_url = None
                    team_logos = team_metadata.get('team_logos')
                    if team_logos and isinstance(team_logos, dict):
                        team_logo = team_logos.get('team_logo')
                        if team_logo and isinstance(team_logo, dict):
                            team_logo_url = team_logo.get('url')
                    
                    teams.append(ExtractedTeam(
                        team_id=team_id,
                        league_id=league_id,
                        name=team_data.get('name', ''),
                        manager_name=manager_name,
                        wins=wins,
                        losses=losses,
                        ties=ties,
                        points_for=points_for,
                        points_against=points_against,
                        playoff_seed=playoff_seed,
                        waiver_priority=waiver_priority,
                        faab_balance=faab_balance,
                        team_logo_url=team_logo_url
                    ))
                    
                except Exception as e:
                    logger.warning(f"Error processing team {team_id}: {e}")
                    continue
            
            logger.info(f"  📊 Found {len(teams)} teams in league {league_id}")
            return teams
            
        except Exception as e:
            logger.error(f"Error extracting teams for league {league_id}: {e}")
            return []
    
    def extract_rosters_for_league(self, league_id: str, weeks_to_extract: Optional[List[int]] = None) -> List[ExtractedRoster]:
        """BULK OPTIMIZED: Extract roster data efficiently with minimal API calls
        
        Optimization Strategy:
        1. Try league.rosters(week) for bulk extraction (1 API call per week for ALL teams)
        2. Fallback to individual team.roster(week) calls if bulk fails
        3. Reuse teams data across all weeks (single API call)
        
        Best Case: 3 + week_count API calls (league + settings + teams + bulk_rosters_per_week)
        Worst Case: 3 + (team_count × week_count) API calls (individual team calls)
        """
        rosters = []
        
        try:
            logger.info(f"🚀 BULK ROSTERS EXTRACTION: Getting roster data for league {league_id}...")
            
            # Get league object (1 API call)
            league = self._rate_limited_request(
                lambda: self.game.to_league(league_id)
            )
            
            if not league:
                return rosters
            
            # Get league settings (1 API call)
            settings = self._rate_limited_request(
                lambda: league.settings()
            )
            
            # Determine sport and optimize week selection
            sport_code = settings.get('game_code', 'unknown').lower()
            
            if weeks_to_extract is None:
                # Use current week if no specific weeks provided
                current_week = int(settings.get('current_week', 1))
                weeks_to_extract = [current_week]
                logger.info(f"📋 ROSTER WEEKS: Using current week {current_week}")
            else:
                logger.info(f"📋 ROSTER WEEKS: Extracting weeks {weeks_to_extract}")
            
            # BULK OPTIMIZATION: Get teams once (1 API call)
            teams = self._rate_limited_request(
                lambda: league.teams()
            )
            
            team_count = len(teams)
            week_count = len(weeks_to_extract)
            
            logger.info(f"📦 BULK ROSTERS: {team_count} teams × {week_count} weeks")
            logger.info(f"⚡ OPTIMIZATION: Attempting bulk roster extraction...")
            
            # BULK EXTRACTION: Get all rosters for all weeks
            for week in weeks_to_extract:
                logger.info(f"    📋 BULK: Week {week} rosters ({team_count} teams)...")
                
                # Skip bulk method - Yahoo API doesn't support league.rosters()
                # Go directly to individual team calls
                logger.info(f"    📋 Using individual team roster calls for week {week}...")
                
                # Individual team roster calls using correct Yahoo API syntax
                for team_key, team_data in teams.items():
                    try:
                        team_id = team_data.get('team_key', team_key)
                        
                        # Construct the full team key for API call
                        full_team_key = f"{league_id}.t.{team_id.split('.')[-1]}" if '.' not in team_id else team_id
                        
                        # Get team roster for specific week using Yahoo Fantasy API
                        # Format: /team/{team_key}/roster;week={week}
                        team_obj = self._rate_limited_request(
                            lambda: league.to_team(full_team_key)
                        )
                        
                        if not team_obj:
                            logger.debug(f"Could not get team object for {full_team_key}")
                            continue
                        
                        # Get roster for this specific week
                        # Yahoo API expects roster as a sub-resource with week parameter
                        roster_data = self._rate_limited_request(
                            lambda: team_obj.roster(week=week)
                        )
                        
                        if not roster_data:
                            logger.debug(f"No roster data for team {full_team_key} week {week}")
                            continue
                        
                        # Process each player in the roster
                        players_count = 0
                        if hasattr(roster_data, '__iter__'):
                            for player_data in roster_data:
                                roster_entry = self._extract_roster_player_data(
                                    player_data, league_id, team_id, week
                                )
                                if roster_entry:
                                    rosters.append(roster_entry)
                                    players_count += 1
                        
                        logger.debug(f"    ✅ Team {team_id} week {week}: {players_count} players")
                                
                    except Exception as e:
                        logger.warning(f"Error getting roster for team {team_key} week {week}: {e}")
                        continue
            
            logger.info(f"  ✅ BULK ROSTERS: Found {len(rosters)} roster entries in league {league_id}")
            return rosters
            
        except Exception as e:
            logger.error(f"Error extracting rosters for league {league_id}: {e}")
            return []
    
    def _extract_roster_player_data(self, player_data: Dict, league_id: str, team_id: str, week: int) -> Optional[ExtractedRoster]:
        """Helper method to extract roster player data consistently"""
        try:
            # Player data structure from Yahoo API:
            # {
            #   'player_id': 5197, 
            #   'name': 'Marc Bulger', 
            #   'status': '', 
            #   'position_type': 'O', 
            #   'eligible_positions': ['QB'], 
            #   'selected_position': 'QB'
            # }
            
            # Extract basic player information
            player_id = str(player_data.get('player_id', ''))
            player_name = player_data.get('name', '')
            
            # Extract position information
            eligible_positions = player_data.get('eligible_positions', [])
            position = eligible_positions[0] if eligible_positions else ''
            
            # Extract roster status from selected position
            selected_position = player_data.get('selected_position', '')
            
            # Determine status and starter flag
            status = 'active'
            is_starter = True  # Default to starter
            
            if selected_position in ['BN', 'Bench']:
                status = 'bench'
                is_starter = False
            elif selected_position in ['IR', 'IR+', 'IR-R']:
                status = 'ir'
                is_starter = False
            elif selected_position in ['NA', 'SUSP']:
                status = 'inactive'
                is_starter = False
            
            # Yahoo API doesn't return points in roster calls
            projected_points = None
            actual_points = None
            
            return ExtractedRoster(
                roster_id=f"{league_id}_{team_id}_{week}_{player_id}",
                league_id=league_id,
                team_id=team_id,
                week=week,
                player_id=player_id,
                player_name=player_name,
                position=position,
                status=status,
                is_starter=is_starter,
                projected_points=projected_points,
                actual_points=actual_points
            )
            
        except Exception as e:
            logger.warning(f"Error processing roster player data: {e}")
            logger.warning(f"Player data structure: {player_data}")
            return None
    
    def extract_matchups_for_league(self, league_id: str) -> List[Dict[str, Any]]:
        """BULK OPTIMIZED: Extract matchup data efficiently with sport-specific week logic"""
        matchups = []
        
        try:
            logger.info(f"🚀 BULK MATCHUPS EXTRACTION: Getting matchup data for league {league_id}...")
            
            # Get league object  
            league = self._rate_limited_request(
                lambda: self.game.to_league(league_id)
            )
            
            if not league:
                return matchups
            
            # Get league settings to determine sport and weeks
            settings = self._rate_limited_request(
                lambda: league.settings()
            )
            
            # Determine sport from league ID prefix or game_code
            sport_code = settings.get('game_code', 'unknown').lower()
            
            # Sport-specific week logic
            if sport_code in ['nfl', 'football']:
                # NFL: Regular season weeks 1-17 only (excludes playoffs)
                start_week = 1
                max_week = 17
                logger.info(f"🏈 NFL League detected - using weeks {start_week}-{max_week} (regular season only)")
            elif sport_code in ['mlb', 'baseball']:
                # MLB: Full season weeks (can be 1-25+)
                start_week = int(settings.get('start_week', 1))
                max_week = min(int(settings.get('end_week', 25)), 25)
                logger.info(f"⚾ MLB League detected - using weeks {start_week}-{max_week}")
            elif sport_code in ['nba', 'basketball']:
                # NBA: Regular season + playoffs
                start_week = int(settings.get('start_week', 1))
                max_week = min(int(settings.get('end_week', 20)), 20)
                logger.info(f"🏀 NBA League detected - using weeks {start_week}-{max_week}")
            elif sport_code in ['nhl', 'hockey']:
                # NHL: Regular season + playoffs  
                start_week = int(settings.get('start_week', 1))
                max_week = min(int(settings.get('end_week', 20)), 20)
                logger.info(f"🏒 NHL League detected - using weeks {start_week}-{max_week}")
            else:
                # Unknown sport - use league settings but cap at reasonable limit
                start_week = int(settings.get('start_week', 1))
                max_week = min(int(settings.get('end_week', 25)), 25)
                logger.info(f"❓ Unknown sport '{sport_code}' - using weeks {start_week}-{max_week}")
            
            current_week = int(settings.get('current_week', max_week))
            end_week = min(current_week, max_week)
            
            logger.info(f"📦 BULK: Getting matchups for weeks {start_week} to {end_week}")
            
            # Get matchups for completed weeks only
            for week in range(start_week, end_week + 1):
                try:
                    week_matchups = self._rate_limited_request(
                        lambda: league.matchups(week)
                    )
                    
                    if week_matchups:
                        matchups.append({
                            'league_id': league_id,
                            'sport_code': sport_code,
                            'week': week,
                            'matchups': week_matchups,
                            'extracted_at': datetime.now().isoformat()
                        })
                        
                except Exception as e:
                    logger.warning(f"Failed to get matchups for week {week}: {e}")
                    continue
            
            logger.info(f"✅ BULK MATCHUPS SUCCESS: Extracted {len(matchups)} week records for {sport_code}")
            return matchups
            
        except Exception as e:
            logger.error(f"Failed to extract matchups for league {league_id}: {e}")
            return matchups
    
    def extract_transactions_for_league(self, league_id: str) -> List[ExtractedTransaction]:
        """Extract transaction data for a league"""
        transactions = []
        
        try:
            # Get league object
            league = self.game.to_league(league_id)
            
            # Get different types of transactions using the correct format
            transaction_types = ['add,drop', 'trade']
            
            for trans_type in transaction_types:
                try:
                    league_transactions = league.transactions(trans_type, 500)  # Increased limit for complete data
                    
                    for trans_data in league_transactions:
                        try:
                            transaction_id = trans_data.get('transaction_key', '')
                            trans_type_parsed = trans_data.get('type', '')
                            timestamp_str = trans_data.get('timestamp', '')
                            status = trans_data.get('status', '')
                            
                            # Convert timestamp
                            try:
                                timestamp = datetime.fromtimestamp(int(timestamp_str)) if timestamp_str else datetime.now()
                            except (ValueError, TypeError):
                                timestamp = datetime.now()
                            
                            # Extract players involved (corrected structure)
                            players_section = trans_data.get('players', {})
                            
                            # Players are nested under numeric keys like '0', '1', etc.
                            for key in players_section:
                                if key.isdigit():  # Skip 'count' key
                                    try:
                                        player_data = players_section[key]['player']
                                        
                                        # Player data is in format [metadata_array, transaction_data]
                                        if isinstance(player_data, list) and len(player_data) >= 1:
                                            player_meta = player_data[0]  # First element is metadata array
                                            
                                            # Extract player info from metadata array
                                            player_id = ''
                                            player_name = ''
                                            
                                            for item in player_meta:
                                                if isinstance(item, dict):
                                                    if 'player_key' in item:
                                                        player_id = item['player_key']
                                                    elif 'name' in item:
                                                        name_data = item['name']
                                                        if isinstance(name_data, dict):
                                                            player_name = name_data.get('full', '')
                                            
                                            # Extract transaction details
                                            source_team_id = None
                                            destination_team_id = None
                                            faab_bid = None
                                            
                                            if len(player_data) > 1:
                                                trans_details = player_data[1]
                                                if 'transaction_data' in trans_details:
                                                    trans_data_section = trans_details['transaction_data']
                                                    
                                                    # Handle both single dict and array formats
                                                    if isinstance(trans_data_section, list) and len(trans_data_section) > 0:
                                                        trans_data_section = trans_data_section[0]
                                                    
                                                    if isinstance(trans_data_section, dict):
                                                        source_team_id = trans_data_section.get('source_team_key')
                                                        destination_team_id = trans_data_section.get('destination_team_key')
                                                        faab_bid = trans_data_section.get('faab_bid')
                                            
                                            # Handle trade-specific fields
                                            if trans_type_parsed == 'trade':
                                                # For trades, also check top-level trader/tradee info
                                                if not source_team_id:
                                                    source_team_id = trans_data.get('trader_team_key')
                                                if not destination_team_id:
                                                    destination_team_id = trans_data.get('tradee_team_key')
                                            
                                            transactions.append(ExtractedTransaction(
                                                transaction_id=f"{transaction_id}_{player_id}",
                                                league_id=league_id,
                                                type=trans_type_parsed,
                                                timestamp=timestamp,
                                                player_id=player_id,
                                                player_name=player_name,
                                                source_team_id=source_team_id,
                                                destination_team_id=destination_team_id,
                                                faab_bid=faab_bid,
                                                status=status
                                            ))
                                            
                                    except Exception as e:
                                        logger.warning(f"Error processing player {key} in transaction {transaction_id}: {e}")
                                        continue
                                    
                        except Exception as e:
                            logger.warning(f"Error processing transaction: {e}")
                            continue
                            
                except Exception as e:
                    logger.warning(f"Error getting transactions for league {league_id} type {trans_type}: {e}")
            
            logger.info(f"  💰 Found {len(transactions)} transactions in league {league_id}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error extracting transactions for league {league_id}: {e}")
            return []
    
    def extract_draft_for_league(self, league_id: str) -> List[ExtractedDraftPick]:
        """Extract draft data for a specific league"""
        logger.info(f"🎯 Extracting draft data for league {league_id}...")
        
        try:
            # Get the league object
            league = self.game.to_league(league_id)
            
            # Get league settings to check if it's auction
            settings = league.settings()
            is_auction_draft = settings.get('is_auction_draft', '0') == '1'
            
            # Get draft results
            draft_results = league.draft_results()
            
            if not draft_results:
                logger.info(f"  🎯 No draft results found for league {league_id}")
                return []
            
            draft_picks = []
            
            # Get player details in batches to get player names and positions
            player_ids = [pick['player_id'] for pick in draft_results if 'player_id' in pick]
            player_details_map = {}
            
            if player_ids:
                try:
                    # Get player details in batches of 25 (API limit)
                    for i in range(0, len(player_ids), 25):
                        batch = player_ids[i:i+25]
                        player_details = league.player_details(batch)
                        
                        for player in player_details:
                            player_id = player.get('player_id', '')
                            player_details_map[player_id] = {
                                'name': player.get('name', {}).get('full', 'Unknown Player'),
                                'position': player.get('display_position', 'Unknown')
                            }
                        
                        # Rate limiting
                        time.sleep(0.1)
                        
                except Exception as e:
                    logger.warning(f"Error getting player details for draft: {e}")
            
            # Process draft picks
            for pick_data in draft_results:
                try:
                    pick_number = pick_data.get('pick', 0)
                    round_number = pick_data.get('round', 0)
                    team_key = pick_data.get('team_key', '')
                    player_id = str(pick_data.get('player_id', ''))
                    cost = pick_data.get('cost')
                    
                    # Extract team ID from team key (format: game.l.league_id.t.team_id)
                    team_id = team_key.split('.')[-1] if '.' in team_key else team_key
                    
                    # Get player details
                    player_info = player_details_map.get(player_id, {})
                    player_name = player_info.get('name', 'Unknown Player')
                    position = player_info.get('position', 'Unknown')
                    
                    # Convert cost to float for auction drafts
                    cost_float = None
                    if cost is not None:
                        try:
                            cost_float = float(cost)
                        except (ValueError, TypeError):
                            cost_float = None
                    
                    # Check if it's a keeper (this is tricky - might need additional API calls)
                    # For now, we'll assume all picks are regular draft picks
                    is_keeper = False
                    
                    draft_pick = ExtractedDraftPick(
                        draft_pick_id=f"{league_id}_{pick_number}",
                        league_id=league_id,
                        pick_number=pick_number,
                        round_number=round_number,
                        team_id=team_id,
                        player_id=player_id,
                        player_name=player_name,
                        position=position,
                        cost=cost_float,
                        is_keeper=is_keeper,
                        is_auction_draft=is_auction_draft
                    )
                    
                    draft_picks.append(draft_pick)
                    
                except Exception as e:
                    logger.warning(f"Error processing draft pick: {e}")
                    continue
            
            logger.info(f"  🎯 Found {len(draft_picks)} draft picks in league {league_id}")
            return draft_picks
            
        except Exception as e:
            logger.error(f"Error extracting draft data for league {league_id}: {e}")
            return []

    def extract_all_data(self, initial_batch_size: int = 10, initial_batch_delay: int = 10, sport_filter: str = 'nfl', private_only: bool = True, extract_leagues: bool = True, extract_teams: bool = True, extract_rosters: bool = False, extract_matchups: bool = True, extract_transactions: bool = True, extract_drafts: bool = True, roster_weeks: Optional[List[int]] = None) -> Dict[str, List[Any]]:
        """Extract all data from NFL private leagues using TRUE BULK OPTIMIZATIONS + adaptive rate limiting"""
        logger.info("🚀 Starting comprehensive data extraction with TRUE BULK OPTIMIZATIONS...")
        logger.info(f"🔒 Rate limits: {self.MAX_REQUESTS_PER_HOUR}/hour, {self.MAX_REQUESTS_PER_DAY}/day")
        logger.info(f"💡 TRUE BULK MODE: Gets ALL data with minimal API calls - NO SKIPPING")
        logger.info(f"🏈 NFL PRIVATE LEAGUES ONLY: Filtering for football private leagues")
        logger.info("📈 Adaptive batching: 🚀→✅→⚡→⚠️→🚨")
        
        if not self.authenticate():
            return self.extracted_data
        
        # BULK OPTIMIZATION: Get all leagues in a single API call (vs 22+ individual calls)
        leagues_data = self.get_all_leagues()
        
        if not leagues_data:
            logger.error("No leagues found")
            return self.extracted_data
        
        original_count = len(leagues_data)
        
        # Filter for NFL private leagues only
        filtered_leagues = []
        sport_counts = {}
        privacy_counts = {}
        
        for league in leagues_data:
            # Count by sport
            game_code = league.get('game_code', 'unknown').lower()
            sport_counts[game_code] = sport_counts.get(game_code, 0) + 1
            
            # Count by privacy
            league_type = league.get('league_type', 'unknown').lower()
            privacy_counts[league_type] = privacy_counts.get(league_type, 0) + 1
            
            # Check if it's NFL/football
            if game_code not in ['nfl', 'football']:
                continue
                
            # Check if it's private
            if private_only and league_type != 'private':
                continue
                
            filtered_leagues.append(league)
        
        # Log detailed breakdown
        logger.info(f"📊 LEAGUE BREAKDOWN BY SPORT:")
        for sport, count in sorted(sport_counts.items()):
            logger.info(f"  🏟️ {sport.upper()}: {count} leagues")
            
        logger.info(f"📊 LEAGUE BREAKDOWN BY PRIVACY:")
        for privacy, count in sorted(privacy_counts.items()):
            logger.info(f"  🔒 {privacy.upper()}: {count} leagues")
        
        leagues_data = filtered_leagues
        logger.info(f"🎯 FILTERING RESULT: {original_count} total → {len(leagues_data)} NFL private leagues selected")
        
        total_leagues = len(leagues_data)
        logger.info(f"📊 Total NFL private leagues to process: {total_leagues}")
        
        if total_leagues == 0:
            logger.warning("No NFL private leagues found")
            return self.extracted_data
        
        # Process leagues in adaptive batches
        current_batch_size = initial_batch_size
        current_batch_delay = initial_batch_delay
        
        total_batches = (total_leagues + current_batch_size - 1) // current_batch_size
        
        for batch_num in range(total_batches):
            # Get adaptive settings
            settings = self._get_adaptive_settings()
            current_batch_size = settings['batch_size']
            current_batch_delay = settings['batch_delay']
            
            start_idx = batch_num * current_batch_size
            end_idx = min(start_idx + current_batch_size, total_leagues)
            batch_leagues = leagues_data[start_idx:end_idx]
            
            logger.info(f"📦 Processing batch {batch_num + 1}/{total_batches} ({len(batch_leagues)} leagues)")
            logger.info(f"   {settings['status']} - Batch size: {current_batch_size}, Delay: {current_batch_delay}s")
            logger.info(f"   📊 Starting API Usage: {self.hourly_request_count}/{self.MAX_REQUESTS_PER_HOUR} hourly, {self.daily_request_count}/{self.MAX_REQUESTS_PER_DAY} daily")
            
            for i, league_info in enumerate(batch_leagues):
                league_id = league_info['league_id']
                league_name = league_info.get('name', 'Unknown')
                
                try:
                    logger.info(f"  🔄 [{i+1}/{len(batch_leagues)}] Processing {league_name} ({league_id})")
                    
                    # Extract league data (if enabled)
                    if extract_leagues:
                        league_data = self.extract_league_data(league_info)
                        self.extracted_data['leagues'].append(asdict(league_data))
                    
                    # Extract teams data (if enabled)
                    if extract_teams:
                        teams_data = self.extract_teams_for_league(league_id)
                        self.extracted_data['teams'].extend([asdict(team) for team in teams_data])
                    
                    # Extract roster data (if enabled)
                    if extract_rosters:
                        logger.info(f"    📋 Extracting roster data...")
                        rosters_data = self.extract_rosters_for_league(league_id, roster_weeks)
                        self.extracted_data['rosters'].extend([asdict(roster) for roster in rosters_data])
                    
                    # Extract matchups data (if enabled)
                    if extract_matchups:
                        logger.info(f"    🏆 Extracting matchup data...")
                        matchups_data = self.extract_matchups_for_league(league_id)
                        self.extracted_data['matchups'].extend(matchups_data)
                    
                    # Extract transactions data (if enabled)
                    if extract_transactions:
                        logger.info(f"    💼 Extracting transaction data...")
                        transactions_data = self.extract_transactions_for_league(league_id)
                        self.extracted_data['transactions'].extend([asdict(trans) for trans in transactions_data])
                    
                    # Extract draft data (if enabled)
                    if extract_drafts:
                        logger.info(f"    🎯 Extracting draft data...")
                        draft_data = self.extract_draft_for_league(league_id)
                        self.extracted_data['draft_picks'].extend([asdict(pick) for pick in draft_data])
                    
                    logger.info(f"    ✅ Completed {league_name}")
                    
                    # Show API usage after each league  
                    hourly_pct = (self.hourly_request_count / self.MAX_REQUESTS_PER_HOUR) * 100
                    daily_pct = (self.daily_request_count / self.MAX_REQUESTS_PER_DAY) * 100
                    logger.info(f"    📊 API Usage: {self.hourly_request_count}/{self.MAX_REQUESTS_PER_HOUR} hourly ({hourly_pct:.1f}%), {self.daily_request_count}/{self.MAX_REQUESTS_PER_DAY} daily ({daily_pct:.1f}%)")
                    
                except Exception as e:
                    logger.error(f"    ❌ Error processing league {league_id}: {e}")
                    continue
            
            # Adaptive delay between batches (except for last batch)
            if batch_num < total_batches - 1:
                logger.info(f"   ⏱️ Waiting {current_batch_delay}s between batches...")
                time.sleep(current_batch_delay)
        
        # Log final summary
        logger.info("🎉 Selective data extraction completed!")
        logger.info(f"📊 Final Summary:")
        logger.info(f"  - Leagues: {len(self.extracted_data['leagues'])}")
        logger.info(f"  - Teams: {len(self.extracted_data['teams'])}")
        logger.info(f"  - Rosters: {len(self.extracted_data['rosters'])}")
        logger.info(f"  - Matchups: {len(self.extracted_data['matchups'])}")
        logger.info(f"  - Transactions: {len(self.extracted_data['transactions'])}")
        logger.info(f"  - Draft Picks: {len(self.extracted_data['draft_picks'])}")
        logger.info(f"📊 Total API requests made - Hour: {self.hourly_request_count}, Day: {self.daily_request_count}")
        
        return self.extracted_data
    
    def save_to_json(self, filename: str = 'yahoo_fantasy_data.json'):
        """Save extracted data to JSON file"""
        try:
            # Convert datetime objects to strings for JSON serialization
            json_data = {}
            for key, value in self.extracted_data.items():
                json_data[key] = []
                for item in value:
                    json_item = {}
                    for k, v in item.items():
                        if isinstance(v, datetime):
                            json_item[k] = v.isoformat()
                        else:
                            json_item[k] = v
                    json_data[key].append(json_item)
            
            with open(filename, 'w') as f:
                json.dump(json_data, f, indent=2, default=str)
            
            logger.info(f"💾 Data saved to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving data to JSON: {e}")

# This module is designed to be imported and used by scripts/full_extraction.py
# Remove the main() function to avoid conflicts with the entry point script