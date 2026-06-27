#!/usr/bin/env python3
"""
Season-Specific Yahoo Fantasy Data Extractor
Extracts data from Yahoo Fantasy API for a specific season/year
Usage: python season_data_extractor.py --year=2024
"""

import os
import json
import logging
import time
import argparse
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa
import requests
from dotenv import load_dotenv

# Database support (optional import)
try:
    import psycopg2
    from psycopg2.extras import execute_values
    DATABASE_SUPPORT = True
except ImportError:
    DATABASE_SUPPORT = False

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

@dataclass
class ExtractedPlayerStatistics:
    """Player statistics data structure for database storage"""
    stat_id: str  # Generated unique identifier
    league_id: str
    player_id: str
    player_name: str
    position_type: str
    season_year: int
    week_number: int  # Added for weekly stats
    weekly_fantasy_points: float  # Changed from total_fantasy_points
    game_code: str
    extracted_at: datetime = datetime.now()


class SeasonYahooFantasyExtractor:
    """Season-specific Yahoo Fantasy data extractor with rate limiting"""
    
    def __init__(self, target_season: int, resume_from_league=None):
        """Initialize the extractor with target season"""
        self.target_season = target_season
        self.oauth = None
        self.game = None
        
        # Rate limiting settings - VERY conservative to avoid Yahoo rate limit denials
        self.MAX_REQUESTS_PER_HOUR = 15000  # Reduced from 20000 to stay well under
        self.MAX_REQUESTS_PER_DAY = 80000   # Reduced from 100000 to stay well under  
        self.MIN_REQUEST_INTERVAL = 1.5     # Increased from 0.6 to 1.5 seconds (much more conservative)
        
        # Request counting and timing
        self.hourly_request_count = 0
        self.daily_request_count = 0
        self.last_request_time = 0
        self.last_hour_reset = time.time()
        self.last_day_reset = time.time()
        
        # Resume functionality
        self.resume_from_league = resume_from_league
        self.current_resume_file = None
        self.total_items_processed = 0
        
        # Database integration
        self.db_url = None
        self.output_mode = 'json'  # 'json' or 'database'
        
        # Progress tracking
        self.extraction_stats = {
            'leagues_processed': 0,
            'teams_processed': 0,
            'rosters_processed': 0,
            'matchups_processed': 0,
            'transactions_processed': 0,
            'drafts_processed': 0,
            'errors_encountered': 0,
            'leagues_skipped': 0
        }
        
        # Enhanced error recovery
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3
        self.failed_leagues = []
        
        self.extracted_data = {
            'leagues': [],
            'teams': [],
            'rosters': [],
            'matchups': [],
            'transactions': [],
            'draft_picks': [],
            'statistics': []
        }
        
        # Season-specific game ID mapping (Yahoo uses different game IDs per season)
        self.season_game_mapping = {
            2024: '449',
            2023: '423', 
            2022: '414',
            2021: '406',
            2020: '399',
            2019: '390',
            2018: '380',
            2017: '371',
            2016: '359',
            2015: '348',
            2014: '331',
            2013: '314',
            2012: '273',
            2011: '257',
            2010: '242',
            2009: '222',
            2008: '199',
            2007: '175',
            2006: '153',
            2005: '124'
        }
        
        logger.info(f"🗓️ Season Data Extractor initialized for {target_season}")
    
    def _check_rate_limits(self):
        """Check if we're approaching rate limits and pause if necessary"""
        current_time = time.time()
        
        # Reset hourly counter if an hour has passed
        if current_time - self.last_hour_reset >= 3600:
            self.hourly_request_count = 0
            self.last_hour_reset = current_time
            logger.info("🔄 Hourly rate limit counter reset")
        
        # Reset daily counter if a day has passed
        if current_time - self.last_day_reset >= 86400:
            self.daily_request_count = 0
            self.last_day_reset = current_time
            logger.info("🔄 Daily rate limit counter reset")
        
        # Check if we're approaching hourly limit
        if self.hourly_request_count >= self.MAX_REQUESTS_PER_HOUR:
            wait_time = 3600 - (current_time - self.last_hour_reset)
            if wait_time > 0:
                logger.warning(f"⏳ Approaching hourly limit ({self.hourly_request_count}/{self.MAX_REQUESTS_PER_HOUR})")
                logger.info(f"⏰ Waiting {wait_time:.0f}s until next hour...")
                time.sleep(wait_time + 10)  # Extra 10s buffer
                self.hourly_request_count = 0
                self.last_hour_reset = time.time()
        
        # Check if we're approaching daily limit
        if self.daily_request_count >= self.MAX_REQUESTS_PER_DAY:
            wait_time = 86400 - (current_time - self.last_day_reset)
            if wait_time > 0:
                logger.warning(f"⏳ Approaching daily limit ({self.daily_request_count}/{self.MAX_REQUESTS_PER_DAY})")
                logger.info(f"⏰ Waiting {wait_time:.0f}s until next day...")
                time.sleep(wait_time + 60)  # Extra 60s buffer
                self.daily_request_count = 0
                self.last_day_reset = time.time()
    
    def _rate_limited_request(self, func, *args, **kwargs):
        """Execute a function with adaptive rate limiting and automatic token refresh"""
        # Check rate limits before making request
        self._check_rate_limits()
        
        # Ensure minimum time between requests
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.MIN_REQUEST_INTERVAL:
            sleep_time = self.MIN_REQUEST_INTERVAL - time_since_last
            time.sleep(sleep_time)
        
        # Make the request with automatic token refresh retry
        max_retries = 2
        for attempt in range(max_retries):
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
                error_str = str(e)
                
                # Check for Yahoo API timeouts (504 Gateway Timeout)
                if ('504' in error_str or 
                    'Activity Timeout' in error_str or 
                    'Gateway Timeout' in error_str or
                    'Will be right back' in error_str or
                    '<!DOCTYPE html>' in error_str):
                    
                    logger.warning(f"⏰ Yahoo API timeout (504) detected: {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"🔄 Retrying in 60 seconds (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(60)  # Wait 1 minute for Yahoo servers to recover
                        continue
                    else:
                        logger.error(f"❌ Yahoo API timeout persists after {max_retries} attempts - skipping")
                        # Count the failed request and re-raise as a specific timeout error
                        self.hourly_request_count += 1
                        self.daily_request_count += 1
                        self.last_request_time = time.time()
                        raise Exception("YAHOO_TIMEOUT")
                
                # Check if this is a token expiration error
                if ('token_expired' in error_str or 
                    'token_rejected' in error_str or 
                    'Please provide valid credentials' in error_str):
                    
                    if attempt < max_retries - 1:  # Not the last attempt
                        logger.warning(f"🔄 OAuth token expired, attempting refresh (attempt {attempt + 1}/{max_retries})...")
                        try:
                            # Refresh the token
                            if self.oauth and hasattr(self.oauth, 'refresh_access_token'):
                                self.oauth.refresh_access_token()
                                logger.info("✅ OAuth token refreshed successfully!")
                                
                                # Longer delay before retry to ensure token propagation
                                time.sleep(3)  # Increased from 1 to 3 seconds
                                
                                # Also reset the game object to ensure new token is used
                                try:
                                    import yahoo_fantasy_api as yfa
                                    self.game = yfa.Game(self.oauth, 'nfl')
                                    logger.info("🔄 Game object refreshed with new token")
                                except Exception as refresh_game_error:
                                    logger.warning(f"⚠️ Could not refresh game object: {refresh_game_error}")
                                
                                continue  # Retry the request
                            else:
                                logger.error("❌ OAuth refresh not available")
                                break
                                
                        except Exception as refresh_error:
                            logger.error(f"❌ Failed to refresh OAuth token: {refresh_error}")
                            break
                    else:
                        logger.error(f"❌ Max token refresh retries ({max_retries}) exceeded")
                
                # Check for Yahoo rate limiting specifically
                if 'Request denied' in error_str or 'request_denied' in error_str.lower():
                    logger.warning(f"🚫 Yahoo rate limit hit: {e}")
                    logger.info("⏰ Waiting 30 seconds before continuing due to rate limit...")
                    time.sleep(30)  # Wait 30 seconds on rate limit
                    
                    # Count this against our limits and update timing
                    self.hourly_request_count += 1
                    self.daily_request_count += 1
                    self.last_request_time = time.time()
                    raise
                
                # For non-token errors or final retry, log and raise
                logger.error(f"Rate limited request failed: {e}")
                # Still count failed requests toward rate limit
                self.hourly_request_count += 1
                self.daily_request_count += 1
                self.last_request_time = time.time()
                raise
    
    def is_target_season_league(self, league_id: str, season_year: int) -> bool:
        """
        Determine if a league belongs to the target season.
        
        Args:
            league_id: League ID from Yahoo API
            season_year: Season year from league settings
            
        Returns:
            bool: True if league is from target season
        """
        # Primary filter: exact season match
        if season_year == self.target_season:
            return True
        
        # Secondary filter: game ID prefix match for additional validation
        try:
            game_id_part = league_id.split('.')[0]
            expected_game_id = self.season_game_mapping.get(self.target_season)
            
            if expected_game_id and game_id_part == expected_game_id:
                logger.debug(f"✅ League {league_id} matches target season {self.target_season} via game ID")
                return True
        except Exception:            # If we can't parse the league ID, rely on season_year only
            pass
        
        return False
    
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
    
    def get_all_leagues_for_season(self) -> List[Dict[str, Any]]:
        """Get all leagues for the target season using optimized API calls"""
        try:
            if not self.game:
                logger.error("Not authenticated")
                return []
            
            logger.info(f"🗓️ Getting all leagues for {self.target_season} season...")
            
            # Get ALL league IDs across all years in one call
            all_league_ids = self._rate_limited_request(
                lambda: self.game.league_ids(is_available=False)
            )
            
            if not all_league_ids:
                logger.info("No leagues found")
                return []
                
            logger.info(f"📋 Found {len(all_league_ids)} total leagues, filtering for {self.target_season}...")
            
            # Filter leagues for target season
            target_season_leagues = []
            batch_size = 10
            
            for i in range(0, len(all_league_ids), batch_size):
                batch_league_ids = all_league_ids[i:i + batch_size]
                logger.info(f"Processing league batch {i//batch_size + 1}/{(len(all_league_ids) + batch_size - 1)//batch_size}")
                
                for league_id in batch_league_ids:
                    try:
                        league = self.game.to_league(league_id)
                        settings = self._rate_limited_request(lambda: league.settings())
                        
                        # Get season from settings
                        season_str = str(settings.get('season', ''))
                        try:
                            season_year = int(season_str) if season_str else 2024
                        except (ValueError, TypeError):
                            logger.warning(f"  ⚠️ Invalid season '{season_str}' for {league_id}")
                            continue
                        
                        # Check if this league is from our target season
                        if not self.is_target_season_league(league_id, season_year):
                            logger.debug(f"  🚫 Skipping {settings.get('name', 'Unknown')} - season {season_year}")
                            continue
                        
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
                        
                        target_season_leagues.append({
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
                        logger.info(f"  ✅ Added {self.target_season} league: {league_name} - {draft_status}")
                        
                    except Exception as e:
                        logger.warning(f"Error getting details for league {league_id}: {e}")
                        continue
                
                # Small delay between batches to be respectful
                if i + batch_size < len(all_league_ids):
                    time.sleep(1)
            
            logger.info(f"🗓️ SEASON FILTER SUCCESS: Found {len(target_season_leagues)} leagues for {self.target_season}")
            return target_season_leagues
            
        except Exception as e:
            logger.error(f"Error getting leagues for season {self.target_season}: {e}")
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
            standings = self._rate_limited_request(lambda: league.standings())
            
            # Get teams (this includes manager and metadata)
            teams_data = self._rate_limited_request(lambda: league.teams()) if hasattr(league, 'teams') else {}
            
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
    
    def extract_matchups_for_league(self, league_id: str) -> List[Dict[str, Any]]:
        """Extract matchup data efficiently for the target season"""
        matchups = []
        
        try:
            logger.info(f"🏆 Extracting matchup data for league {league_id}...")
            
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
            
            logger.info(f"📦 Getting matchups for weeks {start_week} to {end_week}")
            
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
            
            logger.info(f"✅ Extracted {len(matchups)} week records for {sport_code}")
            return matchups
            
        except Exception as e:
            logger.error(f"Failed to extract matchups for league {league_id}: {e}")
            return matchups
    
    def extract_statistics_for_league(self, league_id: str, weeks: Optional[List[int]] = None) -> List[ExtractedPlayerStatistics]:
        """Extract weekly player fantasy points for the target season"""
        statistics = []
        
        try:
            # Get league object
            league = self._rate_limited_request(
                lambda: self.game.to_league(league_id)
            )
            
            if not league:
                logger.warning(f"Could not get league {league_id} for statistics extraction")
                return statistics
            
            # Get league settings for context
            settings = self._rate_limited_request(lambda: league.settings())
            if not settings:
                logger.warning(f"Could not get settings for league {league_id}")
                return statistics
            
            season_year = int(settings.get('season', self.target_season))
            game_code = settings.get('game_code', 'nfl')
            league_name = settings.get('name', 'Unknown League')
            current_week = int(settings.get('current_week', 17))
            end_week = int(settings.get('end_week', 17))
            
            logger.info(f"    📊 Extracting {season_year} statistics for {league_name}")
            
            # Determine which weeks to extract
            if weeks is None:
                # Extract all completed weeks (current week - 1 to avoid incomplete data)
                extract_weeks = list(range(1, min(current_week, end_week + 1)))
            else:
                extract_weeks = [w for w in weeks if w <= end_week]
            
            if not extract_weeks:
                logger.warning(f"No valid weeks to extract for league {league_id}")
                return statistics
            
            # Get all players who were taken in this league
            taken_players = self._rate_limited_request(
                lambda: league.taken_players()
            )
            
            if not taken_players:
                logger.warning(f"No taken players found for league {league_id}")
                return statistics
            
            # Extract all player IDs for bulk API call
            all_player_ids = [int(player.get('player_id')) for player in taken_players if player.get('player_id')]
            
            if not all_player_ids:
                logger.warning(f"No valid player IDs found for league {league_id}")
                return statistics
            
            logger.info(f"    📈 Processing {len(all_player_ids)} players × {len(extract_weeks)} weeks")
            
            total_stats_extracted = 0
            total_points = 0.0
            
            # Process each week efficiently
            for week_num in extract_weeks:
                logger.info(f"        📅 Week {week_num}: Processing {len(all_player_ids)} players...")
                
                try:
                    # Get weekly statistics for ALL players at once
                    weekly_stats = self._rate_limited_request(
                        lambda: league.player_stats(all_player_ids, 'week', week=week_num)
                    )
                    
                    if not weekly_stats:
                        logger.warning(f"No stats returned for league {league_id} week {week_num}")
                        continue
                    
                    week_players_count = 0
                    week_total_points = 0.0
                    
                    # Process all players from bulk response for this week
                    for player_data in weekly_stats:
                        try:
                            player_id = str(player_data.get('player_id'))
                            # Extract fantasy points from API response
                            weekly_points = float(player_data.get('total_points', 0.0))
                            
                            # Create unique stat record for this player-week combination
                            stat_id = f"{league_id}_{player_id}_{season_year}_w{week_num}"
                            
                            player_stats = ExtractedPlayerStatistics(
                                stat_id=stat_id,
                                league_id=league_id,
                                player_id=player_id,
                                player_name=player_data.get('name', 'Unknown'),
                                position_type=player_data.get('position_type', 'Unknown'),
                                season_year=season_year,
                                week_number=week_num,
                                weekly_fantasy_points=weekly_points,
                                game_code=game_code
                            )
                            
                            statistics.append(player_stats)
                            week_players_count += 1
                            week_total_points += weekly_points
                            total_stats_extracted += 1
                            total_points += weekly_points
                            
                        except Exception as e:
                            logger.debug(f"          ⚠️ Error processing player data for week {week_num}: {e}")
                            continue
                    
                    # Log week results
                    players_with_points = len([s for s in weekly_stats if float(s.get('total_points', 0)) > 0])
                    logger.info(f"        ✅ Week {week_num}: {week_players_count} players, {week_total_points:,.1f} total pts, {players_with_points} with points")
                
                except Exception as e:
                    logger.warning(f"        ❌ Error extracting week {week_num} for league {league_id}: {e}")
                    continue
            
            # Log final extraction results
            if statistics:
                players_with_points = len([s for s in statistics if s.weekly_fantasy_points > 0])
                logger.info(f"    ✅ Statistics extracted: {total_stats_extracted:,} records, {total_points:,.1f} total points")
                logger.info(f"        📈 {players_with_points:,} players with points ({(players_with_points/total_stats_extracted)*100:.1f}%)")
            else:
                logger.warning(f"    ⚠️ No statistics extracted for {league_name}")
            
            return statistics
            
        except Exception as e:
            logger.error(f"Error extracting statistics for league {league_id}: {e}")
            return []
    
    def extract_rosters_for_league(self, league_id: str, weeks_to_extract: Optional[List[int]] = None) -> List[ExtractedRoster]:
        """Extract roster data using Yahoo API best practices with minimal calls"""
        rosters = []
        
        try:
            logger.info(f"📋 Getting roster data for league {league_id}...")
            
            # Get league object
            league = self._rate_limited_request(
                lambda: self.game.to_league(league_id)
            )
            
            if not league:
                return rosters
            
            # Get league settings
            settings = self._rate_limited_request(
                lambda: league.settings()
            )
            
            if weeks_to_extract is None:
                # Smart week selection for efficiency
                current_week = int(settings.get('current_week', 1))
                start_week = int(settings.get('start_week', 1))
                end_week = int(settings.get('end_week', current_week))
                
                # For roster extraction, focus on current week only for efficiency
                weeks_to_extract = [min(current_week, end_week)]
                logger.info(f"📋 Using current week {current_week} for roster data")
            else:
                logger.info(f"📋 Using specific weeks {weeks_to_extract}")
            
            # Get teams data once
            teams_dict = self._rate_limited_request(
                lambda: league.teams()
            )
            
            if not teams_dict:
                logger.warning(f"No teams found for league {league_id}")
                return rosters
            
            team_count = len(teams_dict)
            week_count = len(weeks_to_extract)
            
            logger.info(f"📦 Processing {team_count} teams × {week_count} weeks")
            
            # Process each week
            for week in weeks_to_extract:
                logger.info(f"    📋 Week {week}: Processing {team_count} teams...")
                
                try:
                    # Get matchups for the week (includes roster data)
                    week_matchups = self._rate_limited_request(
                        lambda w=week: league.matchups(w)
                    )
                    
                    if week_matchups:
                        week_rosters_count = 0
                        
                        # Extract roster data from matchup response
                        for matchup in week_matchups:
                            try:
                                teams_in_matchup = matchup.get('teams', {})
                                
                                # Process each team in the matchup
                                for team_key, team_data in teams_in_matchup.items():
                                    if isinstance(team_data, dict) and 'roster' in team_data:
                                        roster_data = team_data['roster']
                                        team_id = team_data.get('team_key', team_key)
                                        
                                        # Process roster players
                                        if roster_data and 'players' in roster_data:
                                            players = roster_data['players']
                                            for player_key, player_data in players.items():
                                                if isinstance(player_data, dict):
                                                    roster_entry = self._extract_roster_player_data(
                                                        player_data, league_id, team_id, week
                                                    )
                                                    if roster_entry:
                                                        rosters.append(roster_entry)
                                                        week_rosters_count += 1
                                            
                            except Exception as e:
                                logger.debug(f"        Error processing matchup roster data: {e}")
                                continue
                        
                        if week_rosters_count > 0:
                            logger.info(f"        ✅ Week {week}: {week_rosters_count} roster entries")
                            continue
                    
                    # Fallback: individual team calls
                    logger.info(f"        📋 Fallback: Individual team calls for week {week}")
                    week_rosters_count = 0
                    
                    for team_key, team_data in teams_dict.items():
                        try:
                            team_id = team_data.get('team_key', team_key).split('.')[-1]
                            full_team_key = f"{league_id}.t.{team_id}"
                            
                            # Get team roster for week
                            roster_data = self._rate_limited_request(
                                lambda tk=full_team_key, w=week: league.to_team(tk).roster(week=w)
                            )
                            
                            if roster_data:
                                for player_data in roster_data:
                                    roster_entry = self._extract_roster_player_data(
                                        player_data, league_id, team_id, week
                                    )
                                    if roster_entry:
                                        rosters.append(roster_entry)
                                        week_rosters_count += 1
                            
                        except Exception as e:
                            logger.debug(f"        Error getting roster for team {team_key} week {week}: {e}")
                            continue
                    
                    logger.info(f"        ✅ Week {week}: {week_rosters_count} roster entries from individual calls")
                    
                except Exception as e:
                    logger.warning(f"    ❌ Error processing week {week}: {e}")
                    continue
            
            logger.info(f"  ✅ Found {len(rosters)} total roster entries")
            return rosters
            
        except Exception as e:
            logger.error(f"Error extracting rosters for league {league_id}: {e}")
            return []
    
    def _extract_roster_player_data(self, player_data: Dict, league_id: str, team_id: str, week: int) -> Optional[ExtractedRoster]:
        """Helper method to extract roster player data consistently"""
        try:
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
            return None
    
    def extract_transactions_for_league(self, league_id: str) -> List[ExtractedTransaction]:
        """Extract transaction data for a league"""
        transactions = []
        
        try:
            # Get league object
            league = self.game.to_league(league_id)
            
            # Get different types of transactions
            transaction_types = ['add,drop', 'trade']
            
            for trans_type in transaction_types:
                try:
                    league_transactions = self._rate_limited_request(
                        lambda tt=trans_type: league.transactions(tt, 500)
                    )
                    
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
                            
                            # Extract players involved
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
            settings = self._rate_limited_request(lambda: league.settings())
            is_auction_draft = settings.get('is_auction_draft', '0') == '1'
            
            # Get draft results
            draft_results = self._rate_limited_request(lambda: league.draft_results())
            
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
                        player_details = self._rate_limited_request(
                            lambda b=batch: league.player_details(b)
                        )
                        
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
                    
                    # Check if it's a keeper (assume regular draft picks for now)
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
    
    def extract_season_data(self, 
                           extract_leagues: bool = True,
                           extract_teams: bool = True,
                           extract_rosters: bool = True,
                           extract_matchups: bool = True,
                           extract_transactions: bool = True,
                           extract_drafts: bool = True,
                           extract_statistics: bool = True,
                           roster_weeks: Optional[List[int]] = None) -> Dict[str, List[Any]]:
        """Extract all data for the target season"""
        logger.info(f"🚀 Starting {self.target_season} season data extraction...")
        
        if not self.authenticate():
            return self.extracted_data
        
        # Get leagues for target season
        leagues_data = self.get_all_leagues_for_season()
        
        if not leagues_data:
            logger.error(f"No leagues found for {self.target_season} season")
            return self.extracted_data
        
        total_leagues = len(leagues_data)
        logger.info(f"📊 Processing {total_leagues} leagues from {self.target_season} season")
        
        for i, league_info in enumerate(leagues_data, 1):
            league_id = league_info['league_id']
            league_name = league_info.get('name', 'Unknown')
            
            try:
                logger.info(f"\n📋 [{i}/{total_leagues}] Processing: {league_name} ({league_id})")
                
                # Extract league data (if enabled)
                if extract_leagues:
                    league_data = self.extract_league_data(league_info)
                    self.extracted_data['leagues'].append(asdict(league_data))
                
                # Extract teams data (if enabled)
                if extract_teams:
                    teams_data = self.extract_teams_for_league(league_id)
                    self.extracted_data['teams'].extend([asdict(team) for team in teams_data])
                    self.extraction_stats['teams_processed'] += len(teams_data)
                
                # Extract roster data (if enabled)
                if extract_rosters:
                    logger.info(f"    📋 Extracting roster data...")
                    rosters_data = self.extract_rosters_for_league(league_id, roster_weeks)
                    self.extracted_data['rosters'].extend([asdict(roster) for roster in rosters_data])
                    self.extraction_stats['rosters_processed'] += len(rosters_data)
                
                # Extract matchups data (if enabled)
                if extract_matchups:
                    logger.info(f"    🏆 Extracting matchup data...")
                    matchups_data = self.extract_matchups_for_league(league_id)
                    self.extracted_data['matchups'].extend(matchups_data)
                    self.extraction_stats['matchups_processed'] += len(matchups_data)
                
                # Extract transactions data (if enabled)
                if extract_transactions:
                    logger.info(f"    💼 Extracting transaction data...")
                    transactions_data = self.extract_transactions_for_league(league_id)
                    self.extracted_data['transactions'].extend([asdict(trans) for trans in transactions_data])
                    self.extraction_stats['transactions_processed'] += len(transactions_data)
                
                # Extract draft data (if enabled)
                if extract_drafts:
                    logger.info(f"    🎯 Extracting draft data...")
                    draft_data = self.extract_draft_for_league(league_id)
                    self.extracted_data['draft_picks'].extend([asdict(pick) for pick in draft_data])
                    self.extraction_stats['drafts_processed'] += len(draft_data)
                
                # Extract statistics data (if enabled)
                if extract_statistics:
                    logger.info(f"    📊 Extracting statistics data...")
                    statistics_data = self.extract_statistics_for_league(league_id)
                    # Convert ExtractedPlayerStatistics objects to dictionaries for storage
                    stats_dicts = [asdict(stat) for stat in statistics_data]
                    self.extracted_data['statistics'].extend(stats_dicts)
                
                self.extraction_stats['leagues_processed'] += 1
                logger.info(f"    ✅ Completed {league_name}")
                
                # Show progress
                hourly_pct = (self.hourly_request_count / self.MAX_REQUESTS_PER_HOUR) * 100
                daily_pct = (self.daily_request_count / self.MAX_REQUESTS_PER_DAY) * 100
                logger.info(f"    📊 API Usage: {self.hourly_request_count}/{self.MAX_REQUESTS_PER_HOUR} hourly ({hourly_pct:.1f}%), {self.daily_request_count}/{self.MAX_REQUESTS_PER_DAY} daily ({daily_pct:.1f}%)")
                
            except Exception as e:
                logger.error(f"    ❌ Error processing league {league_id}: {e}")
                self.extraction_stats['errors_encountered'] += 1
                continue
        
        # Log final summary
        logger.info(f"\n🎉 {self.target_season} SEASON EXTRACTION COMPLETED!")
        logger.info(f"📊 Final Data Summary:")
        logger.info(f"  - Leagues: {len(self.extracted_data['leagues'])}")
        logger.info(f"  - Teams: {len(self.extracted_data['teams'])}")
        logger.info(f"  - Rosters: {len(self.extracted_data['rosters'])}")
        logger.info(f"  - Matchups: {len(self.extracted_data['matchups'])}")
        logger.info(f"  - Transactions: {len(self.extracted_data['transactions'])}")
        logger.info(f"  - Draft Picks: {len(self.extracted_data['draft_picks'])}")
        
        # Enhanced statistics summary
        stats_count = len(self.extracted_data['statistics'])
        logger.info(f"  - Weekly Statistics: {stats_count:,} player-week records")
        
        if stats_count > 0:
            total_fantasy_points = sum(stat.get('weekly_fantasy_points', 0) for stat in self.extracted_data['statistics'])
            players_with_points = len([s for s in self.extracted_data['statistics'] if s.get('weekly_fantasy_points', 0) > 0])
            unique_weeks = len(set(stat.get('week_number', 0) for stat in self.extracted_data['statistics']))
            unique_players = len(set(stat.get('player_id', '') for stat in self.extracted_data['statistics']))
            
            logger.info(f"      💰 Total fantasy points: {total_fantasy_points:,.1f}")
            logger.info(f"      👥 Unique players: {unique_players:,}")
            logger.info(f"      📅 Weeks covered: {unique_weeks}")
            logger.info(f"      📈 Players with points: {players_with_points:,} ({(players_with_points/stats_count)*100:.1f}%)")
        
        logger.info(f"📊 Total API requests made - Hour: {self.hourly_request_count}, Day: {self.daily_request_count}")
        
        return self.extracted_data
    
    def save_to_json(self, filename: str = None):
        """Save extracted data to JSON file"""
        if filename is None:
            filename = f'yahoo_fantasy_{self.target_season}_data.json'
        
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
            
            logger.info(f"💾 {self.target_season} season data saved to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving data to JSON: {e}")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Extract Yahoo Fantasy data for a specific season/year",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --year=2024                    # Extract 2024 season data
  %(prog)s --year=2023 --output=nfl_2023.json  # Extract 2023 data to custom file
  %(prog)s --year=2025 --no-teams        # Extract 2025 data without team details
  %(prog)s --year=2022 --stats-only      # Extract only statistics for 2022
        """
    )
    
    parser.add_argument(
        '--year', 
        type=int, 
        required=True,
        help='Season year to extract data for (e.g., 2024, 2023, 2025)'
    )
    
    parser.add_argument(
        '--output', 
        type=str, 
        default=None,
        help='Output filename for JSON data (default: yahoo_fantasy_YEAR_data.json)'
    )
    
    parser.add_argument(
        '--no-leagues', 
        action='store_true',
        help='Skip league data extraction'
    )
    
    parser.add_argument(
        '--no-teams', 
        action='store_true',
        help='Skip team data extraction'
    )
    
    parser.add_argument(
        '--no-rosters', 
        action='store_true',
        help='Skip roster data extraction'
    )
    
    parser.add_argument(
        '--no-matchups', 
        action='store_true',
        help='Skip matchup data extraction'
    )
    
    parser.add_argument(
        '--no-transactions', 
        action='store_true',
        help='Skip transaction data extraction'
    )
    
    parser.add_argument(
        '--no-drafts', 
        action='store_true',
        help='Skip draft data extraction'
    )
    
    parser.add_argument(
        '--no-statistics', 
        action='store_true',
        help='Skip statistics data extraction'
    )
    
    parser.add_argument(
        '--stats-only', 
        action='store_true',
        help='Extract only statistics data (implies --no-leagues --no-teams --no-matchups)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()


def main():
    """Main entry point for the season data extractor"""
    args = parse_arguments()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate year argument
    current_year = datetime.now().year
    if args.year < 2005 or args.year > current_year + 2:
        logger.error(f"❌ Invalid year: {args.year}. Must be between 2005 and {current_year + 2}")
        sys.exit(1)
    
    logger.info(f"🗓️ Yahoo Fantasy Season Data Extractor - {args.year} Season")
    logger.info(f"🎯 Target Season: {args.year}")
    
    # Handle stats-only flag
    if args.stats_only:
        args.no_leagues = True
        args.no_teams = True
        args.no_rosters = True
        args.no_matchups = True
        args.no_transactions = True
        args.no_drafts = True
        logger.info("📊 Statistics-only mode enabled")
    
    # Log extraction settings
    extractions = []
    if not args.no_leagues: extractions.append("leagues")
    if not args.no_teams: extractions.append("teams")
    if not args.no_rosters: extractions.append("rosters")
    if not args.no_matchups: extractions.append("matchups")
    if not args.no_transactions: extractions.append("transactions")
    if not args.no_drafts: extractions.append("drafts")
    if not args.no_statistics: extractions.append("statistics")
    
    logger.info(f"📋 Extracting: {', '.join(extractions)}")
    
    try:
        # Initialize extractor
        extractor = SeasonYahooFantasyExtractor(target_season=args.year)
        
        # Extract data
        extracted_data = extractor.extract_season_data(
            extract_leagues=not args.no_leagues,
            extract_teams=not args.no_teams,
            extract_rosters=not args.no_rosters,
            extract_matchups=not args.no_matchups,
            extract_transactions=not args.no_transactions,
            extract_drafts=not args.no_drafts,
            extract_statistics=not args.no_statistics
        )
        
        # Save to JSON
        extractor.save_to_json(args.output)
        
        # Final summary
        total_records = sum(len(data) for data in extracted_data.values())
        logger.info(f"🎉 Extraction completed successfully!")
        logger.info(f"📊 Total records extracted: {total_records:,}")
        
        if extracted_data['statistics']:
            total_points = sum(stat.get('weekly_fantasy_points', 0) for stat in extracted_data['statistics'])
            logger.info(f"💰 Total fantasy points: {total_points:,.1f}")
        
    except KeyboardInterrupt:
        logger.info("\n⏹️ Extraction interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
