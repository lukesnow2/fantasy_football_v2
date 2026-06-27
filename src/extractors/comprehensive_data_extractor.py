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



class YahooFantasyExtractor:
    """Comprehensive Yahoo Fantasy data extractor with rate limiting"""
    
    # League of Record Filtering (for upstream API call reduction)
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
    
    FUTURE_SEASON_THRESHOLD = 2025
    EXCLUDED_LEAGUE_IDS = set()
    
    def __init__(self, resume_from_league=None):
        """Initialize the extractor with optional resume capability"""
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
    
    def _get_adaptive_settings(self):
        """Get adaptive batch settings based on current rate limit usage"""
        hourly_usage_pct = (self.hourly_request_count / self.MAX_REQUESTS_PER_HOUR) * 100
        daily_usage_pct = (self.daily_request_count / self.MAX_REQUESTS_PER_DAY) * 100
        
        # Determine throttle level based on usage - MUCH MORE CONSERVATIVE
        if hourly_usage_pct > 85 or daily_usage_pct > 85:
            # Critical throttling - extremely conservative
            return {
                'batch_size': 1,
                'batch_delay': 120,  # 2 minutes between batches
                'inter_league_delay': 60,
                'min_request_interval': 3.0,  # 3 seconds minimum
                'status': '🚨 CRITICAL THROTTLE'
            }
        elif hourly_usage_pct > 70 or daily_usage_pct > 70:
            # Heavy throttling - very conservative
            return {
                'batch_size': 1,
                'batch_delay': 90,  # 1.5 minutes between batches
                'inter_league_delay': 30,
                'min_request_interval': 2.5,  # 2.5 seconds minimum
                'status': '⚠️ HEAVY THROTTLE'
            }
        elif hourly_usage_pct > 50 or daily_usage_pct > 50:
            # Moderate throttling - conservative
            return {
                'batch_size': 2,
                'batch_delay': 60,  # 1 minute between batches
                'inter_league_delay': 20,
                'min_request_interval': 2.0,  # 2 seconds minimum
                'status': '⚡ MODERATE THROTTLE'
            }
        elif hourly_usage_pct > 30 or daily_usage_pct > 30:
            # Light throttling - still conservative
            return {
                'batch_size': 3,
                'batch_delay': 30,  # 30 seconds between batches
                'inter_league_delay': 10,
                'min_request_interval': 1.8,  # 1.8 seconds minimum
                'status': '✅ LIGHT THROTTLE'
            }
        else:
            # Full speed - but still respect our base minimum interval
            return {
                'batch_size': 5,
                'batch_delay': 15,  # 15 seconds between batches
                'inter_league_delay': 5,
                'min_request_interval': self.MIN_REQUEST_INTERVAL,  # Use our base 1.5s
                'status': '🚀 FULL SPEED'
            }
    
    def _rate_limited_request(self, func, *args, **kwargs):
        """Execute a function with adaptive rate limiting and automatic token refresh"""
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
        
    def is_league_of_record(self, league_id: str, season_year: int) -> bool:
        """
        Determine if a league should be included in extraction.
        
        Rules:
        1. Always include historical leagues (hard-coded)
        2. Automatically include future leagues (2025+)
        3. Exclude any manually specified leagues
        
        This filtering dramatically reduces API calls by only processing needed leagues.
        """
        # Never include manually excluded leagues
        if league_id in self.EXCLUDED_LEAGUE_IDS:
            logger.info(f"🚫 Excluding manually excluded league: {league_id}")
            return False
            
        # Always include historical leagues
        if league_id in self.HISTORICAL_LEAGUE_IDS:
            return True
            
        # Automatically include future leagues
        if season_year >= self.FUTURE_SEASON_THRESHOLD:
            logger.info(f"🔄 Auto-including future league: {league_id} ({season_year})")
            return True
            
        # Exclude everything else
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
    
    def get_all_leagues(self, filter_leagues_of_record: bool = False) -> List[Dict[str, Any]]:
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
                        # 🎯 EARLY FILTERING: Check if this is a league of record BEFORE expensive API calls
                        if filter_leagues_of_record:
                            # Extract potential season year from league_id for quick filtering
                            # Most league IDs follow pattern: 123.l.456789 where 123 relates to season
                            potential_season = None
                            try:
                                game_id_part = league_id.split('.')[0]
                                # Map known game IDs to seasons (this is approximate but catches most)
                                season_mapping = {
                                    '449': 2024, '423': 2023, '414': 2022, '406': 2021, '399': 2020,
                                    '390': 2019, '380': 2018, '371': 2017, '359': 2016, '348': 2015,
                                    '331': 2014, '314': 2013, '273': 2012, '257': 2011, '242': 2010,
                                    '222': 2009, '199': 2008, '175': 2007, '153': 2006, '124': 2005
                                }
                                potential_season = season_mapping.get(game_id_part, 2024)
                            except Exception:                                potential_season = 2024  # Default fallback
                            
                            # Fast pre-filter before expensive API call
                            if not self.is_league_of_record(league_id, potential_season):
                                logger.debug(f"  🚫 EARLY FILTER: Skipping {league_id} (not league of record)")
                                continue
                        
                        league = self.game.to_league(league_id)
                        settings = self._rate_limited_request(lambda: league.settings())
                        
                        # Only include non-public leagues with game data
                        league_name = settings.get('name', '')
                        draft_status = settings.get('draft_status', 'completed')
                        season_str = str(settings.get('season', ''))
                        
                        # 🎯 PRECISE FILTERING: Double-check with actual season from API
                        if filter_leagues_of_record:
                            try:
                                actual_season = int(season_str) if season_str else 2024
                                if not self.is_league_of_record(league_id, actual_season):
                                    logger.debug(f"  🚫 PRECISE FILTER: Skipping {league_name} ({league_id}) - {actual_season}")
                                    continue
                            except (ValueError, TypeError):
                                logger.warning(f"  ⚠️ Invalid season '{season_str}' for {league_id}")
                                continue
                        
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
    
    def extract_rosters_for_league(self, league_id: str, weeks_to_extract: Optional[List[int]] = None) -> List[ExtractedRoster]:
        """OPTIMIZED: Extract roster data using Yahoo API best practices with minimal calls
        
        Optimization Strategy (Yahoo API Best Practices):
        1. Get teams once and cache team objects
        2. Use bulk roster calls where possible 
        3. Minimize individual team calls
        4. Smart week batching for current season vs historical
        
        Target: 2-3 API calls per week instead of (team_count × week_count × 2)
        """
        rosters = []
        
        try:
            logger.info(f"🚀 OPTIMIZED ROSTERS: Getting roster data for league {league_id}...")
            
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
            
            if weeks_to_extract is None:
                # Smart week selection for efficiency
                current_week = int(settings.get('current_week', 1))
                start_week = int(settings.get('start_week', 1))
                end_week = int(settings.get('end_week', current_week))
                
                # For roster extraction, focus on completed weeks only
                weeks_to_extract = list(range(start_week, min(current_week, end_week + 1)))
                logger.info(f"📋 ROSTER WEEKS: Using current week {current_week} only (most efficient)")
            else:
                logger.info(f"📋 ROSTER WEEKS: Using specific weeks {weeks_to_extract}")
            
            # Get teams data once (1 API call) - keep this efficient
            teams_dict = self._rate_limited_request(
                lambda: league.teams()
            )
            
            if not teams_dict:
                logger.warning(f"No teams found for league {league_id}")
                return rosters
            
            team_count = len(teams_dict)
            week_count = len(weeks_to_extract)
            
            logger.info(f"📦 EFFICIENT ROSTERS: {team_count} teams × {week_count} weeks")
            
            # MAJOR OPTIMIZATION: Process each week with minimal API calls
            for week in weeks_to_extract:
                logger.info(f"    📋 Week {week}: Processing {team_count} teams efficiently...")
                
                try:
                    # OPTIMIZATION 1: Try to get all rosters for the week in one call
                    # Yahoo API: league.matchups(week) includes roster data
                    week_matchups = self._rate_limited_request(
                        lambda w=week: league.matchups(w)
                    )
                    
                    if week_matchups:
                        logger.info(f"        🚀 BULK SUCCESS: Got week {week} data via matchups")
                        week_rosters_count = 0
                        
                        # Extract roster data from matchup response (includes lineups)
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
                            logger.info(f"        ✅ Week {week}: {week_rosters_count} roster entries from bulk call")
                            continue  # Successfully got week data, move to next week
                    
                    # OPTIMIZATION 2: Fallback to efficient individual team calls only if needed
                    logger.info(f"        📋 Fallback: Individual team calls for week {week}")
                    week_rosters_count = 0
                    
                    for team_key, team_data in teams_dict.items():
                        try:
                            team_id = team_data.get('team_key', team_key).split('.')[-1]
                            
                            # Build proper team key for API
                            full_team_key = f"{league_id}.t.{team_id}"
                            
                            # SINGLE EFFICIENT CALL: Get team roster for week
                            # Yahoo API: /league/{league_key}/team/{team_key}/roster;week={week}
                            roster_data = self._rate_limited_request(
                                lambda tk=full_team_key, w=week: league.to_team(tk).roster(week=w)
                            )
                            
                            if roster_data:
                                # Process roster players
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
            
            logger.info(f"  ✅ OPTIMIZED ROSTERS: Found {len(rosters)} total roster entries")
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

    def extract_all_data(self, initial_batch_size: int = 10, initial_batch_delay: int = 10, sport_filter: str = 'nfl', private_only: bool = True, extract_leagues: bool = True, extract_teams: bool = True, extract_rosters: bool = False, extract_matchups: bool = True, extract_transactions: bool = True, extract_drafts: bool = True, extract_statistics: bool = True, roster_weeks: Optional[List[int]] = None, statistics_weeks: Optional[List[int]] = None) -> Dict[str, List[Any]]:
        """Extract all data from NFL private leagues using TRUE BULK OPTIMIZATIONS + adaptive rate limiting"""
        logger.info("🚀 Starting comprehensive data extraction with TRUE BULK OPTIMIZATIONS...")
        logger.info(f"🔒 Rate limits: {self.MAX_REQUESTS_PER_HOUR}/hour, {self.MAX_REQUESTS_PER_DAY}/day")
        logger.info(f"💡 TRUE BULK MODE: Gets ALL data with minimal API calls - NO SKIPPING")
        
        # Dynamic filtering message
        sport_desc = "ALL SPORTS" if sport_filter == 'all' else f"{sport_filter.upper()} ONLY"
        privacy_desc = "ALL LEAGUES" if not private_only else "PRIVATE LEAGUES ONLY"
        logger.info(f"🎯 FILTERING: {sport_desc} + {privacy_desc}")
        logger.info("📈 Adaptive batching: 🚀→✅→⚡→⚠️→🚨")
        
        if not self.authenticate():
            return self.extracted_data
        
        # BULK OPTIMIZATION: Get all leagues in a single API call (vs 22+ individual calls)
        leagues_data = self.get_all_leagues()
        
        if not leagues_data:
            logger.error("No leagues found")
            return self.extracted_data
        
        original_count = len(leagues_data)
        
        # Filter leagues based on sport and privacy settings
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
            
            # Check sport filter
            if sport_filter != 'all':
                if game_code not in [sport_filter.lower(), 'football' if sport_filter.lower() == 'nfl' else sport_filter.lower()]:
                    continue
                
            # Check privacy filter
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
        
        # Dynamic filtering message
        sport_msg = "ALL SPORTS" if sport_filter == 'all' else sport_filter.upper()
        privacy_msg = "ALL LEAGUES" if not private_only else "PRIVATE LEAGUES"
        logger.info(f"🎯 FILTERING RESULT: {original_count} total → {len(leagues_data)} {sport_msg} {privacy_msg} selected")
        
        total_leagues = len(leagues_data)
        logger.info(f"📊 Total {sport_msg} {privacy_msg} to process: {total_leagues}")
        
        if total_leagues == 0:
            logger.warning(f"No {sport_msg} {privacy_msg} found")
            return self.extracted_data
        
        # Process leagues in adaptive batches
        current_batch_size = initial_batch_size
        current_batch_delay = initial_batch_delay
        
        processed_leagues = 0
        batch_num = 0
        
        while processed_leagues < total_leagues:
            # Get adaptive settings
            settings = self._get_adaptive_settings()
            current_batch_size = settings['batch_size']
            current_batch_delay = settings['batch_delay']
            
            # Calculate current batch indices
            start_idx = processed_leagues
            end_idx = min(start_idx + current_batch_size, total_leagues)
            batch_leagues = leagues_data[start_idx:end_idx]
            
            # Calculate total batches remaining for logging
            remaining_leagues = total_leagues - processed_leagues  
            remaining_batches = (remaining_leagues + current_batch_size - 1) // current_batch_size
            
            logger.info(f"📦 Processing batch {batch_num + 1} ({len(batch_leagues)} leagues) - Progress: {processed_leagues}/{total_leagues}")
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
                    
                    # Extract player statistics using BULK SEASON OPTIMIZATION (if enabled)
                    if extract_statistics:
                        logger.info(f"    📊 Extracting weekly fantasy points using BULK SEASON optimization...")
                        statistics_data = self.extract_statistics_for_league(league_id, statistics_weeks)
                        # Convert ExtractedPlayerStatistics objects to dictionaries for storage
                        stats_dicts = [asdict(stat) for stat in statistics_data]
                        self.extracted_data['statistics'].extend(stats_dicts)
                        
                        # Log statistics extraction results
                        if statistics_data:
                            total_points = sum(stat.weekly_fantasy_points for stat in statistics_data)
                            players_with_points = len([s for s in statistics_data if s.weekly_fantasy_points > 0])
                            weeks_covered = len(set(stat.week_number for stat in statistics_data))
                            logger.info(f"    ✅ Statistics: {len(statistics_data):,} records, {weeks_covered} weeks, {total_points:,.1f} total pts, {players_with_points} active players")
                    
                    logger.info(f"    ✅ Completed {league_name}")
                    
                    # Show API usage after each league  
                    hourly_pct = (self.hourly_request_count / self.MAX_REQUESTS_PER_HOUR) * 100
                    daily_pct = (self.daily_request_count / self.MAX_REQUESTS_PER_DAY) * 100
                    logger.info(f"    📊 API Usage: {self.hourly_request_count}/{self.MAX_REQUESTS_PER_HOUR} hourly ({hourly_pct:.1f}%), {self.daily_request_count}/{self.MAX_REQUESTS_PER_DAY} daily ({daily_pct:.1f}%)")
                    
                except Exception as e:
                    logger.error(f"    ❌ Error processing league {league_id}: {e}")
                    continue
            
            # Update progress counters
            processed_leagues += len(batch_leagues)
            batch_num += 1
            
            # Adaptive delay between batches (except for last batch)
            if processed_leagues < total_leagues:
                logger.info(f"   ⏱️ Waiting {current_batch_delay}s between batches...")
                time.sleep(current_batch_delay)
        
        # Log final summary with enhanced statistics reporting
        logger.info("🎉 BULK SEASON EXTRACTION COMPLETED!")
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
            # Calculate additional statistics metrics
            total_fantasy_points = sum(stat.get('weekly_fantasy_points', 0) for stat in self.extracted_data['statistics'])
            players_with_points = len([s for s in self.extracted_data['statistics'] if s.get('weekly_fantasy_points', 0) > 0])
            unique_weeks = len(set(stat.get('week_number', 0) for stat in self.extracted_data['statistics']))
            unique_players = len(set(stat.get('player_id', '') for stat in self.extracted_data['statistics']))
            
            logger.info(f"      💰 Total fantasy points: {total_fantasy_points:,.1f}")
            logger.info(f"      👥 Unique players: {unique_players:,}")
            logger.info(f"      📅 Weeks covered: {unique_weeks}")
            logger.info(f"      📈 Players with points: {players_with_points:,} ({(players_with_points/stats_count)*100:.1f}%)")
            
            if unique_weeks > 0 and unique_players > 0:
                logger.info(f"      ⚡ API efficiency: ~{unique_weeks} bulk calls vs ~{stats_count:,} individual calls")
                efficiency_gain = ((stats_count - unique_weeks) / stats_count) * 100 if stats_count > 0 else 0
                logger.info(f"      🚀 Efficiency gain: {efficiency_gain:.1f}%")
        
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

    def extract_statistics_for_league(self, league_id: str, weeks: Optional[List[int]] = None) -> List[ExtractedPlayerStatistics]:
        """Extract weekly player fantasy points using optimized bulk season loading
        
        Uses efficient bulk API calls to get all weekly data for a season with minimal requests.
        
        Args:
            league_id: League ID to extract statistics for
            weeks: List of weeks to extract. If None, extracts all completed weeks
        
        Returns:
            List of ExtractedPlayerStatistics objects with weekly fantasy points
        """
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
            
            season_year = int(settings.get('season', 2024))
            game_code = settings.get('game_code', 'nfl')
            league_name = settings.get('name', 'Unknown League')
            current_week = int(settings.get('current_week', 17))
            end_week = int(settings.get('end_week', 17))
            
            logger.info(f"    🏈 BULK SEASON EXTRACTION: {league_name} ({season_year} season)")
            
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
            
            logger.info(f"    📊 BULK OPTIMIZATION: Processing {len(all_player_ids)} players × {len(extract_weeks)} weeks")
            logger.info(f"    🚀 Target: {len(extract_weeks)} bulk API calls instead of {len(extract_weeks) * len(all_player_ids)} individual calls")
            
            total_stats_extracted = 0
            total_points = 0.0
            
            # OPTIMIZED BULK PROCESSING: Get all weeks efficiently
            for week_num in extract_weeks:
                logger.info(f"        📈 Week {week_num}: Bulk processing {len(all_player_ids)} players...")
                
                try:
                    # SINGLE BULK API CALL: Get weekly statistics for ALL players at once
                    weekly_stats = self._rate_limited_request(
                        lambda: league.player_stats(all_player_ids, 'week', week=week_num)
                    )
                    
                    if not weekly_stats:
                        logger.warning(f"No stats returned for league {league_id} week {week_num}")
                        continue
                    
                    week_players_count = 0
                    week_total_points = 0.0
                    week_stats_processed = 0
                    
                    # BULK PROCESSING: Process all players from bulk response for this week
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
                            week_stats_processed += 1
                            
                        except Exception as e:
                            logger.debug(f"          ⚠️ Error processing player data for week {week_num}: {e}")
                            continue
                    
                    # Log week results with efficiency metrics
                    avg_week_points = week_total_points / week_players_count if week_players_count > 0 else 0
                    players_with_points = len([s for s in weekly_stats if float(s.get('total_points', 0)) > 0])
                    
                    logger.info(f"        ✅ Week {week_num}: {week_players_count} players, {week_total_points:,.1f} total pts")
                    logger.info(f"            💰 {players_with_points} players scored points (avg: {avg_week_points:.1f})")
                    logger.info(f"            🚀 Processed {week_stats_processed} records in 1 bulk API call")
                
                except Exception as e:
                    logger.warning(f"        ❌ Error extracting week {week_num} for league {league_id}: {e}")
                    continue
            
            # Log final extraction results with efficiency summary
            if statistics:
                avg_points = total_points / total_stats_extracted if total_stats_extracted > 0 else 0
                players_with_points = len([s for s in statistics if s.weekly_fantasy_points > 0])
                api_calls_made = len(extract_weeks)
                api_calls_saved = (len(extract_weeks) * len(all_player_ids)) - api_calls_made
                efficiency_percent = ((api_calls_saved / (api_calls_saved + api_calls_made)) * 100) if api_calls_saved > 0 else 0
                
                logger.info(f"    🎉 BULK SEASON SUCCESS: Extracted {total_stats_extracted:,} weekly records")
                logger.info(f"        📅 Coverage: {len(extract_weeks)} weeks × {len(all_player_ids)} players")
                logger.info(f"        💰 Total fantasy points: {total_points:,.1f}")
                logger.info(f"        📈 Players with points: {players_with_points:,} ({(players_with_points/total_stats_extracted)*100:.1f}%)")
                logger.info(f"        ⚡ API Efficiency: {api_calls_made} calls vs {api_calls_made + api_calls_saved} individual calls")
                logger.info(f"        🚀 Efficiency gain: {efficiency_percent:.1f}% ({api_calls_saved:,} calls saved)")
            else:
                logger.warning(f"    ⚠️ No weekly fantasy points extracted for any players in {league_name}")
            
            return statistics
            
        except Exception as e:
            logger.error(f"Error extracting weekly statistics for league {league_id}: {e}")
            return []

    # ==========================================
    # DATABASE INTEGRATION METHODS
    # ==========================================
    
    def configure_database(self, db_url: str = None):
        """Configure database connection for direct streaming"""
        if not DATABASE_SUPPORT:
            raise Exception("Database support not available. Install psycopg2 and python-dotenv")
        
        if db_url:
            self.db_url = db_url
        else:
            # Try to get from environment
            load_dotenv()
            self.db_url = self._get_database_url_from_env()
        
        self.output_mode = 'database'
        logger.info(f"📦 Database mode enabled")
    
    def _get_database_url_from_env(self):
        """Extract DATABASE_URL from environment or .env file"""
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            return db_url
            
        # If not in env, try to extract from .env file manually
        try:
            with open('.env', 'r') as f:
                content = f.read()
                # Look for DATABASE_URL in the content
                for line in content.split('\n'):
                    if 'DATABASE_URL=' in line and not line.strip().startswith('#'):
                        return line.split('DATABASE_URL=')[1].split()[0]
                    # Handle case where it's at end of comment line
                    if 'DATABASE_URL=' in line:
                        parts = line.split('DATABASE_URL=')
                        if len(parts) > 1:
                            return parts[1].split()[0]
        except Exception as e:
            logger.error(f"Error reading .env file: {e}")
            
        # Fallback: try common environment variable names
        for var_name in ['DB_URL', 'POSTGRES_URL', 'POSTGRESQL_URL']:
            url = os.getenv(var_name)
            if url:
                logger.info(f"Found database URL in {var_name}")
                return url
                
        raise Exception("DATABASE_URL not found in environment or .env file")
    
    def get_db_connection(self):
        """Get database connection"""
        if not self.db_url:
            raise Exception("Database not configured. Call configure_database() first")
        try:
            return psycopg2.connect(self.db_url)
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def stream_to_database(self, data_type: str, data_items: List[Any], table_name: str = None):
        """Stream data directly to database"""
        if not data_items:
            logger.info(f"No {data_type} records to insert")
            return 0
        
        if not table_name:
            table_name = f"public.{data_type.lower()}"
        
        try:
            # Convert data objects to dictionaries if needed
            data_dicts = []
            for item in data_items:
                if hasattr(item, '__dict__'):
                    data_dict = item.__dict__.copy()
                else:
                    data_dict = item.copy()
                data_dicts.append(data_dict)
            
            # Get column names from first record
            if data_dicts:
                columns = list(data_dicts[0].keys())
                
                # Prepare SQL
                columns_str = ','.join(columns)
                sql = f"INSERT INTO {table_name} ({columns_str}) VALUES %s ON CONFLICT DO NOTHING"
                
                # Prepare data
                data = [tuple(data_dict[col] for col in columns) for data_dict in data_dicts]
                
                # Execute batch insert
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        execute_values(cur, sql, data, template=None, page_size=1000)
                        conn.commit()
                
                inserted_count = len(data_dicts)
                self.total_items_processed += inserted_count
                self.extraction_stats[f"{data_type.lower()}_processed"] += inserted_count
                
                logger.info(f"✅ Inserted {inserted_count} {data_type} records")
                logger.info(f"📊 Total items processed: {self.total_items_processed}")
                return inserted_count
            
        except Exception as e:
            logger.error(f"❌ Failed to insert {data_type} data: {e}")
            self.extraction_stats['errors_encountered'] += 1
            raise
        
        return 0
    
    # ==========================================
    # RESUME/CHECKPOINT SYSTEM
    # ==========================================
    
    def save_resume_point(self, league_id: str, resume_file: str = "resume_extraction.txt"):
        """Save the current league ID and progress for resuming later"""
        try:
            with open(resume_file, 'w') as f:
                f.write(f"{league_id}\n{self.total_items_processed}\n{datetime.now().isoformat()}\n")
                f.write(json.dumps(self.extraction_stats))
            logger.info(f"💾 Saved resume point: {league_id}")
            self.current_resume_file = resume_file
        except Exception as e:
            logger.error(f"Failed to save resume point: {e}")
    
    def load_resume_point(self, resume_file: str = "resume_extraction.txt"):
        """Load the resume point if it exists"""
        if not os.path.exists(resume_file):
            return None, 0, {}
        
        try:
            with open(resume_file, 'r') as f:
                lines = f.read().strip().split('\n')
                league_id = lines[0] if lines else None
                items_count = int(lines[1]) if len(lines) > 1 else 0
                timestamp = lines[2] if len(lines) > 2 else "Unknown"
                stats = json.loads(lines[3]) if len(lines) > 3 else {}
                
            if league_id:
                logger.info(f"🔄 Found resume point: {league_id} (last saved: {timestamp})")
                self.total_items_processed = items_count
                self.extraction_stats.update(stats)
                self.current_resume_file = resume_file
                return league_id, items_count, stats
        except Exception as e:
            logger.error(f"Error loading resume point: {e}")
        
        return None, 0, {}
    
    def clear_resume_point(self, resume_file: str = "resume_extraction.txt"):
        """Clear the resume point file"""
        try:
            if os.path.exists(resume_file):
                os.remove(resume_file)
                logger.info("🗑️ Cleared resume point")
                self.current_resume_file = None
        except Exception as e:
            logger.error(f"Error clearing resume point: {e}")
    
    # ==========================================
    # ENHANCED PROGRESS TRACKING
    # ==========================================
    
    def log_extraction_progress(self):
        """Log current extraction progress and statistics"""
        stats = self.extraction_stats
        logger.info("📊 === EXTRACTION PROGRESS ===")
        logger.info(f"📋 Leagues processed: {stats['leagues_processed']}")
        logger.info(f"👥 Teams processed: {stats['teams_processed']}")
        logger.info(f"🔢 Rosters processed: {stats['rosters_processed']}")
        logger.info(f"⚔️ Matchups processed: {stats['matchups_processed']}")
        logger.info(f"💸 Transactions processed: {stats['transactions_processed']}")
        logger.info(f"📋 Drafts processed: {stats['drafts_processed']}")
        logger.info(f"❌ Errors encountered: {stats['errors_encountered']}")
        logger.info(f"⏭️ Leagues skipped: {stats['leagues_skipped']}")
        logger.info(f"📊 Total items: {self.total_items_processed}")
        logger.info("===============================")
    
    def handle_league_error(self, league_id: str, league_name: str, error: Exception) -> bool:
        """Handle errors at the league level with enhanced recovery logic
        
        Returns:
            bool: True if extraction should continue, False if it should stop
        """
        error_str = str(error)
        
        # Track the failure
        self.consecutive_failures += 1
        self.extraction_stats['errors_encountered'] += 1
        self.failed_leagues.append({'league_id': league_id, 'name': league_name, 'error': error_str})
        
        # Check for specific error types
        if ('token_expired' in error_str or 
            'token_rejected' in error_str or 
            'Please provide valid credentials' in error_str or
            'Max token refresh retries' in error_str):
            
            logger.error(f"🔑 OAuth authentication failed for {league_name}: {error}")
            
            if self.consecutive_failures >= self.max_consecutive_failures:
                logger.error(f"💥 STOPPING: {self.max_consecutive_failures} consecutive authentication failures!")
                logger.error("🔑 OAuth token refresh is completely broken. Manual intervention required.")
                return False
            else:
                logger.warning(f"⚠️ Skipping {league_name} due to auth failure, continuing...")
                self.extraction_stats['leagues_skipped'] += 1
                return True
                
        elif str(error) == "YAHOO_TIMEOUT":
            logger.warning(f"⏰ Yahoo API timeout for {league_name} - continuing with next league")
            logger.info(f"💡 This is a temporary Yahoo server issue, not a script problem")
            self.extraction_stats['leagues_skipped'] += 1
            # Reset consecutive failures for timeouts (not our fault)
            self.consecutive_failures = 0
            return True
            
        elif 'Request denied' in error_str or 'rate limit' in error_str.lower():
            logger.warning(f"🚫 Rate limited while extracting {league_name}: {error}")
            # Don't increment consecutive failures for rate limits
            self.consecutive_failures = 0
            raise error  # Let the calling code handle rate limit waits
            
        else:
            logger.error(f"❌ Unexpected error processing {league_name}: {error}")
            if self.consecutive_failures >= self.max_consecutive_failures:
                logger.error(f"💥 STOPPING: {self.max_consecutive_failures} consecutive unexpected failures!")
                return False
            else:
                logger.warning(f"⚠️ Skipping {league_name} due to unexpected error, continuing...")
                self.extraction_stats['leagues_skipped'] += 1
                return True
    
    def reset_consecutive_failures(self):
        """Reset consecutive failure counter (call after successful league processing)"""
        self.consecutive_failures = 0
    
    # ==========================================
    # ENHANCED EXTRACTION WITH DATABASE & RESUME
    # ==========================================
    
    def extract_all_data_with_resume(self, 
                                   output_mode: str = 'json',
                                   db_url: str = None,
                                   resume_file: str = "resume_extraction.txt",
                                   sleep_between_leagues: int = 0,
                                   initial_batch_size: int = 10, 
                                   initial_batch_delay: int = 10,
                                   sport_filter: str = 'nfl', 
                                   private_only: bool = True, 
                                   extract_leagues: bool = True, 
                                   extract_teams: bool = True, 
                                   extract_rosters: bool = False, 
                                   extract_matchups: bool = True, 
                                   extract_transactions: bool = True, 
                                   extract_drafts: bool = True, 
                                   extract_statistics: bool = True, 
                                   roster_weeks: Optional[List[int]] = None, 
                                   statistics_weeks: Optional[List[int]] = None) -> Dict[str, List[Any]]:
        """Enhanced extraction with database streaming and resume capability
        
        Args:
            output_mode: 'json' or 'database' 
            db_url: Database URL for streaming mode
            resume_file: File to save/load resume points
            sleep_between_leagues: Seconds to sleep between leagues (0 = no sleep)
            ... (other args same as extract_all_data)
        """
        
        # Configure output mode
        if output_mode == 'database':
            if not DATABASE_SUPPORT:
                raise Exception("Database mode requires psycopg2 and python-dotenv. Install with: pip install psycopg2-binary python-dotenv")
            self.configure_database(db_url)
            logger.info("📦 Database streaming mode enabled")
        else:
            self.output_mode = 'json'
            logger.info("📄 JSON output mode enabled")
        
        # Authenticate first
        if not self.authenticate():
            raise Exception("Authentication failed")
        
        # Load resume point if it exists
        resume_league_id, resume_items, resume_stats = self.load_resume_point(resume_file)
        start_index = 0
        
        # Get all leagues
        logger.info(f"📋 Getting leagues...")
        leagues_data = self.get_all_leagues(filter_leagues_of_record=True)
        if not leagues_data:
            logger.error("No leagues found")
            return self.extracted_data
            
        logger.info(f"Found {len(leagues_data)} leagues")
        
        # Find resume point if specified
        if resume_league_id:
            found = False
            for i, league in enumerate(leagues_data):
                if league['league_id'] == resume_league_id:
                    start_index = i
                    found = True
                    logger.info(f"🔄 Resuming from league {i+1}/{len(leagues_data)}: {league.get('name', resume_league_id)}")
                    break
            
            if not found:
                logger.warning(f"⚠️ Resume league {resume_league_id} not found, starting from beginning")
                start_index = 0
                self.clear_resume_point(resume_file)
        
        # Process leagues starting from resume point
        leagues_to_process = leagues_data[start_index:]
        logger.info(f"📋 Processing {len(leagues_to_process)} leagues (starting from index {start_index})")
        
        for i, league_info in enumerate(leagues_to_process, start=start_index):
            league_id = league_info['league_id']
            league_name = league_info.get('name', 'Unknown')
            
            try:
                logger.info(f"\n📋 [{i+1}/{len(leagues_data)}] Processing: {league_name} ({league_id})")
                
                league_extracted_data = {}
                
                # Extract league data
                if extract_leagues:
                    league_data = self.extract_league_data(league_info)
                    league_extracted_data['leagues'] = [league_data]
                
                # Extract teams data
                if extract_teams:
                    teams_data = self.extract_teams_for_league(league_id)
                    league_extracted_data['teams'] = teams_data
                    self.extraction_stats['teams_processed'] += len(teams_data)
                
                # Extract roster data
                if extract_rosters:
                    logger.info(f"    📋 Extracting roster data...")
                    rosters_data = self.extract_rosters_for_league(league_id, roster_weeks)
                    league_extracted_data['rosters'] = rosters_data
                    self.extraction_stats['rosters_processed'] += len(rosters_data)
                
                # Extract matchups data
                if extract_matchups:
                    logger.info(f"    🏆 Extracting matchup data...")
                    matchups_data = self.extract_matchups_for_league(league_id)
                    league_extracted_data['matchups'] = matchups_data
                    self.extraction_stats['matchups_processed'] += len(matchups_data)
                
                # Extract transactions data
                if extract_transactions:
                    logger.info(f"    💼 Extracting transaction data...")
                    transactions_data = self.extract_transactions_for_league(league_id)
                    league_extracted_data['transactions'] = transactions_data
                    self.extraction_stats['transactions_processed'] += len(transactions_data)
                
                # Extract draft data
                if extract_drafts:
                    logger.info(f"    🎯 Extracting draft data...")
                    draft_data = self.extract_draft_for_league(league_id)
                    league_extracted_data['drafts'] = draft_data
                    self.extraction_stats['drafts_processed'] += len(draft_data)
                
                # Extract statistics data
                if extract_statistics:
                    logger.info(f"    📊 Extracting statistics data...")
                    statistics_data = self.extract_statistics_for_league(league_id, statistics_weeks)
                    league_extracted_data['statistics'] = statistics_data
                
                # Output data based on mode
                if self.output_mode == 'database':
                    # Stream each data type to database immediately
                    for data_type, data_items in league_extracted_data.items():
                        if data_items:
                            self.stream_to_database(data_type, data_items)
                else:
                    # Accumulate in memory for JSON output
                    for data_type, data_items in league_extracted_data.items():
                        if data_type in self.extracted_data:
                            if hasattr(data_items[0], '__dict__'):
                                self.extracted_data[data_type].extend([asdict(item) for item in data_items])
                            else:
                                self.extracted_data[data_type].extend(data_items)
                
                # Update stats and save progress
                self.extraction_stats['leagues_processed'] += 1
                self.reset_consecutive_failures()
                self.save_resume_point(league_id, resume_file)
                
                logger.info(f"    ✅ Completed {league_name}")
                self.log_extraction_progress()
                
                # Sleep between leagues if specified
                if sleep_between_leagues > 0 and i < len(leagues_data) - 1:
                    sleep_minutes = sleep_between_leagues // 60
                    logger.info(f"😴 Sleeping {sleep_minutes} minutes before next league...")
                    time.sleep(sleep_between_leagues)
                
            except Exception as e:
                # Use enhanced error handling
                should_continue = self.handle_league_error(league_id, league_name, error=e)
                if not should_continue:
                    logger.error("💥 Stopping extraction due to repeated failures")
                    break
                    
                # Save progress even on error
                self.save_resume_point(league_id, resume_file)
                continue
        
        # Clear resume point on successful completion
        if start_index + len(leagues_to_process) >= len(leagues_data):
            self.clear_resume_point(resume_file)
            logger.info("🎉 Extraction completed successfully!")
        
        # Final progress log
        self.log_extraction_progress()
        
        # Log failed leagues if any
        if self.failed_leagues:
            logger.warning(f"⚠️ {len(self.failed_leagues)} leagues had errors:")
            for failed in self.failed_leagues[-5:]:  # Show last 5
                logger.warning(f"  - {failed['name']} ({failed['league_id']}): {failed['error'][:100]}...")
        
        return self.extracted_data





# This module is designed to be imported and used by scripts/full_extraction.py
# Remove the main() function to avoid conflicts with the entry point script