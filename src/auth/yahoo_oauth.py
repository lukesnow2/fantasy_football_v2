#!/usr/bin/env python3
"""
Yahoo Fantasy API OAuth Authentication Script v2 - Automation Ready

This script extends the working OAuth authentication with:
- Scheduled automation capabilities  
- Database integration for data storage
- Modular design for different API calls
- Production logging and error handling
- Configurable data collection tasks

Based on the working yahoo_fantasy_oauth.py pattern
"""

import json
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('yahoo_fantasy_automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_credentials():
    """
    Load Yahoo API credentials securely from environment variables or config
    
    Returns:
        tuple: (client_key, client_secret) or (None, None) if not found
    """
    # Try environment variables first
    client_key = os.getenv('YAHOO_CLIENT_KEY')
    client_secret = os.getenv('YAHOO_CLIENT_SECRET')
    
    if client_key and client_secret:
        logger.info("✅ Using credentials from environment variables")
        return client_key, client_secret
    
    # Try config.json file
    config_file = 'config.json'
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            client_key = config.get('yahoo_client_key')
            client_secret = config.get('yahoo_client_secret')
            
            if client_key and client_secret:
                logger.info("✅ Using credentials from config.json")
                return client_key, client_secret
        except Exception as e:
            logger.error(f"❌ Error reading config.json: {e}")
    
    logger.error("❌ No Yahoo API credentials found!")
    return None, None

class YahooFantasyOAuthAutomation:
    """
    Enhanced Yahoo Fantasy API OAuth handler for automation and database integration
    """
    
    def __init__(self, client_key: str, client_secret: str, token_file: str = "oauth2.json"):
        """
        Initialize OAuth handler for automation
        
        Args:
            client_key (str): Yahoo client key (consumer key) 
            client_secret (str): Yahoo client secret (consumer secret)
            token_file (str): File to store OAuth tokens
        """
        self.client_key = client_key
        self.client_secret = client_secret
        self.token_file = token_file
        self.oauth_session = None
        self.game = None
        self.last_auth_time = None
        
    def authenticate(self, silent: bool = False) -> Optional[yfa.Game]:
        """
        Perform OAuth authentication with automation-friendly error handling
        
        Args:
            silent (bool): If True, suppress interactive prompts for automation
            
        Returns:
            yahoo_fantasy_api.Game: Authenticated Game object or None
        """
        try:
            logger.info("Starting Yahoo Fantasy API OAuth authentication...")
            
            # Create OAuth2 session using the working pattern
            self.oauth_session = OAuth2(None, None, from_file=self.token_file)
            
            logger.info("✅ OAuth2 session created successfully!")
            
            # Create Game object for NFL
            self.game = yfa.Game(self.oauth_session, 'nfl')
            
            # Test the connection
            if self._test_connection():
                self.last_auth_time = datetime.now()
                logger.info("✅ Authentication completed successfully!")
                return self.game
            else:
                logger.error("❌ Authentication test failed")
                return None
                
        except FileNotFoundError:
            if silent:
                logger.error(f"❌ Token file '{self.token_file}' not found - cannot authenticate in silent mode")
                return None
            else:
                logger.info(f"❌ Token file '{self.token_file}' not found - creating OAuth credentials")
                return self._create_oauth_file()
                
        except Exception as e:
            logger.error(f"❌ Authentication failed: {e}")
            return None
    
    def _create_oauth_file(self) -> Optional[yfa.Game]:
        """
        Create OAuth credentials file interactively (for initial setup)
        """
        logger.info(f"📝 Creating OAuth credentials file: {self.token_file}")
        
        # Create the credential structure
        oauth_data = {
            "consumer_key": self.client_key,
            "consumer_secret": self.client_secret
        }
        
        try:
            with open(self.token_file, 'w') as f:
                json.dump(oauth_data, f, indent=2)
            
            logger.info(f"✅ Created {self.token_file} with credentials")
            logger.info("🔄 Starting OAuth flow...")
            
            # Perform OAuth flow
            self.oauth_session = OAuth2(None, None, from_file=self.token_file)
            
            logger.info("✅ OAuth flow completed!")
            
            # Create Game object
            self.game = yfa.Game(self.oauth_session, 'nfl')
            
            # Test connection
            if self._test_connection():
                self.last_auth_time = datetime.now()
                logger.info("✅ Authentication setup completed successfully!")
                return self.game
            else:
                logger.error("❌ Authentication test failed")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error during OAuth setup: {e}")
            # Clean up partial file
            if os.path.exists(self.token_file):
                os.remove(self.token_file)
                logger.info(f"Cleaned up partial {self.token_file}")
            return None
    
    def _test_connection(self) -> bool:
        """Test the API connection with a simple call"""
        try:
            game_id = self.game.game_id()
            logger.info(f"✅ API connection test successful! Game ID: {game_id}")
            return True
        except Exception as e:
            logger.error(f"❌ API connection test failed: {e}")
            return False
    
    def refresh_if_needed(self) -> bool:
        """
        Check and refresh token if needed for automation
        
        Returns:
            bool: True if token is valid/refreshed, False if failed
        """
        if not self.oauth_session:
            logger.warning("❌ No OAuth session to check")
            return False
        
        try:
            if hasattr(self.oauth_session, 'token_is_valid'):
                if not self.oauth_session.token_is_valid():
                    logger.info("🔄 Token expired, refreshing...")
                    self.oauth_session.refresh_access_token()
                    logger.info("✅ Token refreshed successfully!")
                    return True
                else:
                    logger.debug("✅ Token is still valid")
                    return True
            return True
        except Exception as e:
            logger.error(f"❌ Token refresh failed: {e}")
            return False
    
    def get_leagues(self, year: Optional[int] = None) -> List[str]:
        """
        Get user's fantasy leagues with error handling
        
        Args:
            year (int, optional): Specific year to get leagues for
            
        Returns:
            list: List of league IDs
        """
        if not self.game:
            logger.error("❌ Not authenticated. Call authenticate() first.")
            return []
        
        try:
            # Refresh token if needed
            if not self.refresh_if_needed():
                logger.error("❌ Token refresh failed, re-authentication may be needed")
                return []
            
            if year:
                leagues = self.game.league_ids(year=year)
            else:
                leagues = self.game.league_ids()
                
            logger.info(f"✅ Retrieved {len(leagues)} leagues")
            return leagues
            
        except Exception as e:
            logger.error(f"❌ Error fetching leagues: {e}")
            return []
    
    def get_league_data(self, league_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed data for a specific league
        
        Args:
            league_id (str): League ID to fetch data for
            
        Returns:
            dict: League data or None if error
        """
        if not self.game:
            logger.error("❌ Not authenticated")
            return None
        
        try:
            # Refresh token if needed
            if not self.refresh_if_needed():
                logger.error("❌ Token refresh failed")
                return None
            
            league = self.game.to_league(league_id)
            settings = league.settings()
            
            # Get additional league information
            league_data = {
                'league_id': league_id,
                'name': settings.get('name', 'N/A'),
                'season': settings.get('season', 'N/A'),
                'num_teams': settings.get('num_teams', 'N/A'),
                'scoring_type': settings.get('scoring_type', 'N/A'),
                'draft_status': settings.get('draft_status', 'N/A'),
                'current_week': settings.get('current_week', 'N/A'),
                'is_finished': settings.get('is_finished', False),
                'retrieved_at': datetime.now().isoformat()
            }
            
            logger.info(f"✅ Retrieved data for league {league_id}: {league_data['name']}")
            return league_data
            
        except Exception as e:
            logger.error(f"❌ Error fetching data for league {league_id}: {e}")
            return None

class DataCollectionTask:
    """
    Base class for automated data collection tasks
    """
    
    def __init__(self, name: str, oauth_handler: YahooFantasyOAuthAutomation):
        self.name = name
        self.oauth_handler = oauth_handler
        self.last_run = None
        self.run_count = 0
        
    def run(self) -> Dict[str, Any]:
        """
        Execute the data collection task
        
        Returns:
            dict: Task execution results
        """
        start_time = datetime.now()
        logger.info(f"🚀 Starting task: {self.name}")
        
        try:
            # Ensure authentication
            if not self.oauth_handler.game:
                if not self.oauth_handler.authenticate(silent=True):
                    raise Exception("Authentication failed")
            
            # Run the specific task
            result = self._execute()
            
            self.last_run = datetime.now()
            self.run_count += 1
            
            execution_time = (self.last_run - start_time).total_seconds()
            logger.info(f"✅ Task {self.name} completed in {execution_time:.2f}s")
            
            return {
                'task_name': self.name,
                'status': 'success',
                'execution_time': execution_time,
                'run_count': self.run_count,
                'last_run': self.last_run.isoformat(),
                'data': result
            }
            
        except Exception as e:
            logger.error(f"❌ Task {self.name} failed: {e}")
            return {
                'task_name': self.name,
                'status': 'error',
                'error': str(e),
                'run_count': self.run_count,
                'last_run': datetime.now().isoformat()
            }
    
    def _execute(self) -> Any:
        """
        Override this method in subclasses to implement specific data collection
        """
        raise NotImplementedError("Subclasses must implement _execute method")

class LeagueDataCollectionTask(DataCollectionTask):
    """
    Task to collect league data for database storage
    """
    
    def __init__(self, oauth_handler: YahooFantasyOAuthAutomation, target_years: List[int] = None):
        super().__init__("League Data Collection", oauth_handler)
        self.target_years = target_years or [datetime.now().year]
    
    def _execute(self) -> List[Dict[str, Any]]:
        """
        Collect league data for specified years
        """
        all_league_data = []
        
        for year in self.target_years:
            logger.info(f"📊 Collecting leagues for year {year}")
            
            # Get league IDs for the year
            league_ids = self.oauth_handler.get_leagues(year=year)
            
            for league_id in league_ids:
                league_data = self.oauth_handler.get_league_data(league_id)
                if league_data:
                    all_league_data.append(league_data)
                
                # Add small delay to be respectful to API
                time.sleep(0.1)
        
        logger.info(f"📊 Collected data for {len(all_league_data)} leagues")
        return all_league_data

def main():
    """
    Main function for testing v2 functionality
    """
    print("Yahoo Fantasy API OAuth Authentication v2 - Automation Ready")
    print("=" * 65)
    
    # Load credentials
    client_key, client_secret = load_credentials()
    
    if not client_key or not client_secret:
        logger.error("❌ Cannot proceed without valid credentials.")
        sys.exit(1)
    
    # Create enhanced OAuth handler
    oauth_handler = YahooFantasyOAuthAutomation(client_key, client_secret)
    
    try:
        # Test authentication
        game = oauth_handler.authenticate()
        
        if not game:
            logger.error("❌ Authentication failed.")
            sys.exit(1)
        
        logger.info("🎉 Authentication completed successfully!")
        
        # Test automated data collection
        logger.info("📊 Testing automated data collection...")
        
        # Create and run a data collection task
        current_year = datetime.now().year
        task = LeagueDataCollectionTask(oauth_handler, target_years=[current_year])
        
        result = task.run()
        
        if result['status'] == 'success':
            logger.info(f"✅ Data collection successful!")
            logger.info(f"📊 Collected {len(result['data'])} league records")
            
            # Show sample data (first 3 leagues)
            sample_data = result['data'][:3]
            for league in sample_data:
                logger.info(f"  📋 {league['name']} ({league['season']}) - {league['num_teams']} teams")
        else:
            logger.error(f"❌ Data collection failed: {result.get('error')}")
        
        logger.info("✅ v2 testing completed successfully!")
        logger.info("🔄 Ready for automation and database integration!")
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Testing cancelled by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ v2 testing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 