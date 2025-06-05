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
    """Roster/Player data structure for database storage"""
    roster_id: str
    team_id: str
    league_id: str
    week: int
    player_id: str
    player_name: str
    position: str
    eligible_positions: str
    status: str
    is_starter: bool
    acquisition_date: Optional[str] = None
    acquisition_type: Optional[str] = None
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
    """Comprehensive Yahoo Fantasy data extractor"""
    
    def __init__(self):
        self.oauth = None
        self.game = None
        self.extracted_data = {
            'leagues': [],
            'teams': [],
            'rosters': [],
            'matchups': [],
            'transactions': [],
            'draft_picks': [],
            'statistics': []
        }
        
    def authenticate(self) -> bool:
        """Authenticate with Yahoo Fantasy API"""
        try:
            # Initialize OAuth using existing token file (check multiple locations)
            oauth_file = 'oauth2.json'
            if not os.path.exists(oauth_file):
                oauth_file = 'data/current/oauth2.json'
            
            self.oauth = OAuth2(None, None, from_file=oauth_file)
            
            if not self.oauth.token_is_valid():
                logger.info("🔑 Token invalid, refreshing...")
                self.oauth.refresh_access_token()
            
            # Create Game object for NFL
            self.game = yfa.Game(self.oauth, 'nfl')
            
            # Test the connection
            game_id = self.game.game_id()
            logger.info(f"✅ Authentication successful! Game ID: {game_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Authentication failed: {e}")
            logger.error("Make sure oauth2.json file exists with valid tokens")
            return False
    
    def get_all_leagues(self) -> List[Dict[str, Any]]:
        """Get all user's fantasy leagues"""
        try:
            if not self.game:
                logger.error("Not authenticated")
                return []
            
            all_leagues = []
            
            # Get leagues for each year (2004-2025)
            for year in range(2004, 2026):
                try:
                    logger.info(f"🔍 Checking leagues for {year}...")
                    
                    # Get league IDs for the year using yahoo_fantasy_api
                    league_ids = self.game.league_ids(year=year)
                    
                    if not league_ids:
                        logger.info(f"  No leagues found for {year}")
                        continue
                    
                    logger.info(f"  Found {len(league_ids)} leagues for {year}")
                    
                    # Get detailed info for each league
                    for league_id in league_ids:
                        try:
                            league = self.game.to_league(league_id)
                            settings = league.settings()
                            
                            # Only include non-public leagues
                            league_name = settings.get('name', '')
                            draft_status = settings.get('draft_status', 'completed')
                            
                            # Skip predraft leagues (they have no game data)
                            if draft_status == 'predraft':
                                logger.info(f"  Skipping predraft league: {league_name} ({league_id})")
                                continue
                            
                            if not league_name.startswith('Yahoo Public'):
                                all_leagues.append({
                                    'league_id': league_id,
                                    'name': league_name,
                                    'season': str(settings.get('season', year)),
                                    'game_code': 'nfl',
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
                                logger.info(f"  Added postdraft league: {league_name} ({draft_status})")
                            
                            # Rate limiting
                            time.sleep(0.1)
                            
                        except Exception as e:
                            logger.warning(f"Error getting details for league {league_id}: {e}")
                            continue
                    
                except Exception as e:
                    logger.warning(f"Error getting leagues for {year}: {e}")
                    continue
            
            logger.info(f"📋 Found {len(all_leagues)} non-public leagues across all years")
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
    
    def extract_rosters_for_league(self, league_id: str, teams: List[ExtractedTeam]) -> List[ExtractedRoster]:
        """Extract roster data for all teams in a league"""
        try:
            # Get league object
            league = self.game.to_league(league_id)
            settings = league.settings()
            start_week = int(settings.get('start_week', 1))
            end_week = int(settings.get('end_week', 17))
            current_week = int(settings.get('current_week', 17))
            
            # Get teams
            teams_data = league.teams()
            rosters = []
            
            # Extract rosters for key weeks (start, middle, current)
            sample_weeks = [start_week, min(8, end_week), current_week]
            sample_weeks = list(set([w for w in sample_weeks if w <= end_week]))  # Remove duplicates and invalid weeks
            
            for team_id in teams_data.keys():
                try:
                    team = league.to_team(team_id)
                    
                    for week in sample_weeks:
                        try:
                            roster_data = team.roster(week=week)
                            
                            for player in roster_data:
                                rosters.append(ExtractedRoster(
                                    roster_id=f"{team_id}_w{week}_{player.get('player_id', '')}",
                                    team_id=team_id,
                                    league_id=league_id,
                                    week=week,
                                    player_id=str(player.get('player_id', '')),
                                    player_name=player.get('name', ''),
                                    position=player.get('selected_position', ''),
                                    eligible_positions=','.join(player.get('eligible_positions', [])),
                                    status=player.get('status', ''),
                                    is_starter=player.get('selected_position') not in ['BN', 'IR'],
                                    acquisition_date=None,  # Not available in roster data
                                    acquisition_type=None
                                ))
                        except Exception as e:
                            logger.warning(f"Error getting roster for {team_id} week {week}: {e}")
                            
                except Exception as e:
                    logger.warning(f"Error processing team {team_id} rosters: {e}")
            
            logger.info(f"  👥 Found {len(rosters)} roster entries in league {league_id}")
            return rosters
            
        except Exception as e:
            logger.error(f"Error extracting rosters for league {league_id}: {e}")
            return []
    
    def extract_matchups_for_league(self, league_id: str) -> List[ExtractedMatchup]:
        """Extract matchup/schedule data for a league"""
        matchups = []
        
        try:
            # Get league object
            league = self.game.to_league(league_id)
            settings = league.settings()
            start_week = int(settings.get('start_week', 1))
            end_week = int(settings.get('end_week', 17))
            current_week = int(settings.get('current_week', 17))
            
            # Sample key weeks to avoid too many API calls
            sample_weeks = [start_week, min(8, end_week), current_week]
            sample_weeks = list(set([w for w in sample_weeks if w <= end_week]))
            
            for week in sample_weeks:
                try:
                    matchup_data = league.matchups(week=week)
                    
                    # Extract matchups from the API response (corrected structure)
                    if 'fantasy_content' in matchup_data:
                        content = matchup_data['fantasy_content']
                        if 'league' in content and isinstance(content['league'], list) and len(content['league']) > 1:
                            # The scoreboard is in league[1], not league[0]
                            scoreboard_data = content['league'][1]
                            if 'scoreboard' in scoreboard_data:
                                scoreboard = scoreboard_data['scoreboard']
                                if '0' in scoreboard and 'matchups' in scoreboard['0']:
                                    matchups_section = scoreboard['0']['matchups']
                                    
                                    # Iterate through matchups (they are numbered: '0', '1', etc.)
                                    for key in matchups_section.keys():
                                        if key != 'count' and key.isdigit():
                                            try:
                                                matchup = matchups_section[key]['matchup']
                                                
                                                # Get winner from matchup level
                                                winner_team_id = matchup.get('winner_team_key')
                                                is_playoffs = matchup.get('is_playoffs', '0') == '1'
                                                is_consolation = matchup.get('is_consolation', '0') == '1'
                                                
                                                # Get teams from the nested structure
                                                if '0' in matchup and 'teams' in matchup['0']:
                                                    teams_section = matchup['0']['teams']
                                                    
                                                    if '0' in teams_section and '1' in teams_section:
                                                        team1_data = teams_section['0']['team']
                                                        team2_data = teams_section['1']['team']
                                                        
                                                        # Extract team IDs from nested arrays
                                                        team1_id = ''
                                                        team2_id = ''
                                                        team1_score = 0.0
                                                        team2_score = 0.0
                                                        
                                                        # Team data is in array format [metadata, scores]
                                                        if isinstance(team1_data, list) and len(team1_data) > 1:
                                                            team1_meta = team1_data[0]
                                                            team1_scores = team1_data[1]
                                                            
                                                            # Find team_key in metadata array
                                                            for item in team1_meta:
                                                                if isinstance(item, dict) and 'team_key' in item:
                                                                    team1_id = item['team_key']
                                                                    break
                                                            
                                                            # Get score
                                                            if 'team_points' in team1_scores:
                                                                team1_score = float(team1_scores['team_points'].get('total', 0))
                                                        
                                                        if isinstance(team2_data, list) and len(team2_data) > 1:
                                                            team2_meta = team2_data[0]
                                                            team2_scores = team2_data[1]
                                                            
                                                            # Find team_key in metadata array
                                                            for item in team2_meta:
                                                                if isinstance(item, dict) and 'team_key' in item:
                                                                    team2_id = item['team_key']
                                                                    break
                                                            
                                                            # Get score
                                                            if 'team_points' in team2_scores:
                                                                team2_score = float(team2_scores['team_points'].get('total', 0))
                                                        
                                                        # Only add if we have valid team IDs
                                                        if team1_id and team2_id:
                                                            # If no winner specified, determine from scores
                                                            if not winner_team_id:
                                                                if team1_score > team2_score:
                                                                    winner_team_id = team1_id
                                                                elif team2_score > team1_score:
                                                                    winner_team_id = team2_id
                                                            
                                                            # Determine if championship (usually weeks 16-17)
                                                            is_championship = week >= 16 and is_playoffs
                                                            
                                                            matchups.append(ExtractedMatchup(
                                                                matchup_id=f"{league_id}_week{week}_{team1_id}_{team2_id}",
                                                                league_id=league_id,
                                                                week=week,
                                                                team1_id=team1_id,
                                                                team2_id=team2_id,
                                                                team1_score=team1_score,
                                                                team2_score=team2_score,
                                                                winner_team_id=winner_team_id,
                                                                is_playoffs=is_playoffs,
                                                                is_championship=is_championship,
                                                                is_consolation=is_consolation
                                                            ))
                                                    
                                            except Exception as e:
                                                logger.warning(f"Error processing matchup {key} for week {week}: {e}")
                                                continue
                                                
                except Exception as e:
                    logger.warning(f"Error getting matchups for league {league_id} week {week}: {e}")
            
            logger.info(f"  🏆 Found {len(matchups)} matchups in league {league_id}")
            return matchups
            
        except Exception as e:
            logger.error(f"Error extracting matchups for league {league_id}: {e}")
            return []
    
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
                    league_transactions = league.transactions(trans_type, 25)  # Limit to 25 per type
                    
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

    
    def extract_all_data(self) -> Dict[str, List[Any]]:
        """Extract all data from all leagues"""
        logger.info("🚀 Starting comprehensive data extraction...")
        
        if not self.authenticate():
            return self.extracted_data
        
        # Get all leagues
        leagues_data = self.get_all_leagues()
        
        if not leagues_data:
            logger.error("No leagues found")
            return self.extracted_data
        
        # Extract data for each league
        for i, league_info in enumerate(leagues_data, 1):
            league_id = league_info['league_id']
            league_name = league_info['name']
            logger.info(f"📋 Processing league {i}/{len(leagues_data)}: {league_name} ({league_info['season']})")
            
            try:
                # Extract league data
                league_data = self.extract_league_data(league_info)
                self.extracted_data['leagues'].append(asdict(league_data))
                
                # Extract teams
                teams = self.extract_teams_for_league(league_id)
                for team in teams:
                    self.extracted_data['teams'].append(asdict(team))
                
                # Extract rosters
                rosters = self.extract_rosters_for_league(league_id, teams)
                for roster in rosters:
                    self.extracted_data['rosters'].append(asdict(roster))
                
                # Extract matchups
                matchups = self.extract_matchups_for_league(league_id)
                for matchup in matchups:
                    self.extracted_data['matchups'].append(asdict(matchup))
                
                # Extract transactions
                transactions = self.extract_transactions_for_league(league_id)
                for transaction in transactions:
                    self.extracted_data['transactions'].append(asdict(transaction))
                
                # Extract draft picks
                draft_picks = self.extract_draft_for_league(league_id)
                for draft_pick in draft_picks:
                    self.extracted_data['draft_picks'].append(asdict(draft_pick))
                
                # Rate limiting between leagues
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing league {league_name}: {e}")
                continue
        
        # Log summary
        logger.info("✅ Data extraction completed!")
        logger.info(f"📊 Summary:")
        logger.info(f"  - Leagues: {len(self.extracted_data['leagues'])}")
        logger.info(f"  - Teams: {len(self.extracted_data['teams'])}")
        logger.info(f"  - Rosters: {len(self.extracted_data['rosters'])}")
        logger.info(f"  - Matchups: {len(self.extracted_data['matchups'])}")
        logger.info(f"  - Transactions: {len(self.extracted_data['transactions'])}")
        logger.info(f"  - Draft Picks: {len(self.extracted_data['draft_picks'])}")
        
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

def main():
    """Main execution function"""
    extractor = YahooFantasyExtractor()
    
    # Extract all data
    data = extractor.extract_all_data()
    
    # Save to JSON
    extractor.save_to_json()
    
    return data

if __name__ == "__main__":
    main()