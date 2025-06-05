#!/usr/bin/env python3
"""
Yahoo Fantasy API OAuth Authentication Script

This script handles OAuth authentication with the Yahoo Fantasy API
using the proper pattern from the official library documentation.
Based on: https://github.com/spilchen/yahoo_fantasy_api examples
"""

import json
import os
import sys
from dotenv import load_dotenv
from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

# Load environment variables
load_dotenv()

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
        print("✅ Using credentials from environment variables")
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
                print("✅ Using credentials from config.json")
                return client_key, client_secret
        except Exception as e:
            print(f"❌ Error reading config.json: {e}")
    
    # Show help if no credentials found
    print("❌ No Yahoo API credentials found!")
    print("\nPlease provide your credentials using one of these methods:")
    print("\n1. Environment variables:")
    print("   export YAHOO_CLIENT_KEY='your_client_key_here'")
    print("   export YAHOO_CLIENT_SECRET='your_client_secret_here'")
    print("\n2. Create a .env file:")
    print("   YAHOO_CLIENT_KEY=your_client_key_here")
    print("   YAHOO_CLIENT_SECRET=your_client_secret_here")
    print("\n3. Create a config.json file:")
    print("   {")
    print('     "yahoo_client_key": "your_client_key_here",')
    print('     "yahoo_client_secret": "your_client_secret_here"')
    print("   }")
    print("\nTo get these credentials:")
    print("1. Go to https://developer.yahoo.com/apps/")
    print("2. Create a new app or use an existing one")
    print("3. Set redirect URI to 'oob' (out of band)")
    print("4. Note down your Client ID (consumer key) and Client Secret")
    
    return None, None

class YahooFantasyOAuth:
    """
    Yahoo Fantasy API OAuth handler using the official library pattern
    """
    
    def __init__(self, client_key, client_secret, token_file="oauth2.json"):
        """
        Initialize OAuth handler
        
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
        
    def authenticate(self):
        """
        Perform OAuth authentication following the official pattern
        
        Returns:
            yahoo_fantasy_api.Game: Authenticated Game object
        """
        print("Starting Yahoo Fantasy API OAuth authentication...")
        
                try:
            # Create OAuth2 session using the official pattern
            # Pass None, None and use from_file parameter as shown in documentation
            self.oauth_session = OAuth2(None, None, from_file=self.token_file)
            
            print("✅ OAuth2 session created successfully!")
            
            # Create Game object for NFL as shown in examples  
            self.game = yfa.Game(self.oauth_session, 'nfl')
            
            # Test the connection
            self._test_connection()
            
            print("✅ Authentication completed successfully!")
            return self.game
            
        except FileNotFoundError:
            print(f"❌ Token file '{self.token_file}' not found.")
            print("You need to create OAuth credentials first.")
            return self._create_oauth_file()
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Make sure your token file exists and is valid")
            print("2. Try regenerating your OAuth credentials")
            print("3. Check your Yahoo app permissions")
            return None
    
    def _create_oauth_file(self):
        """
        Create OAuth credentials file interactively
        """
        print(f"\n📝 Creating OAuth credentials file: {self.token_file}")
        print("=" * 50)
        
        # Create the credential structure as shown in the documentation
        oauth_data = {
            "consumer_key": self.client_key,
            "consumer_secret": self.client_secret
        }
        
        try:
            with open(self.token_file, 'w') as f:
                json.dump(oauth_data, f, indent=2)
            
            print(f"✅ Created {self.token_file} with your credentials")
            print("\n🔄 Now attempting OAuth flow...")
            
            # Now try the OAuth flow
            self.oauth_session = OAuth2(None, None, from_file=self.token_file)
            
            # The OAuth2 constructor will handle the interactive auth flow
            print("✅ OAuth flow completed!")
            
            # Create Game object
            self.game = yfa.Game(self.oauth_session, 'nfl')
            
            # Test connection
            self._test_connection()
            
            print("✅ Authentication setup completed successfully!")
            return self.game
            
        except Exception as e:
            print(f"❌ Error during OAuth setup: {e}")
            # Clean up partial file
            if os.path.exists(self.token_file):
                os.remove(self.token_file)
                print(f"Cleaned up partial {self.token_file}")
            return None
    
    def _test_connection(self):
        """Test the API connection with a simple call"""
        try:
            game_id = self.game.game_id()
            print(f"✅ API connection test successful! Game ID: {game_id}")
            return True
        except Exception as e:
            print(f"❌ API connection test failed: {e}")
            return False
    
    def get_leagues(self, year=None):
        """
        Get user's fantasy leagues
        
        Args:
            year (int, optional): Specific year to get leagues for
            
        Returns:
            list: List of league IDs
        """
        if not self.game:
            raise Exception("Not authenticated. Call authenticate() first.")
        
        try:
            if year:
                return self.game.league_ids(year=year)
            else:
                return self.game.league_ids()
        except Exception as e:
            print(f"❌ Error fetching leagues: {e}")
            return []
    
    def refresh_token(self):
        """
        Refresh the OAuth token if needed
        
        Returns:
            bool: True if refresh successful, False otherwise
        """
        if not self.oauth_session:
            print("❌ No OAuth session to refresh")
            return False
        
        try:
            if hasattr(self.oauth_session, 'token_is_valid'):
                if not self.oauth_session.token_is_valid():
                    print("🔄 Token expired, refreshing...")
                    self.oauth_session.refresh_access_token()
                    print("✅ Token refreshed successfully!")
                    return True
                else:
                    print("✅ Token is still valid")
                    return True
            return True
        except Exception as e:
            print(f"❌ Token refresh failed: {e}")
            return False

def main():
    """
    Main function demonstrating OAuth authentication
    """
    print("Yahoo Fantasy API OAuth Authentication")
    print("=" * 50)
    
    # Load credentials securely
    client_key, client_secret = load_credentials()
    
    if not client_key or not client_secret:
        print("❌ Cannot proceed without valid credentials.")
        sys.exit(1)
    
    # Create OAuth handler
    oauth_handler = YahooFantasyOAuth(client_key, client_secret)
    
    try:
        # Authenticate
        game = oauth_handler.authenticate()
        
        if not game:
            print("❌ Authentication failed.")
            sys.exit(1)
        
        print(f"\n🎉 Authentication completed successfully!")
        print("=" * 50)
        
        # Get and display leagues
        print("\n📋 Fetching your fantasy leagues...")
        leagues = oauth_handler.get_leagues()
        
        if leagues:
            print(f"\n🏈 Found {len(leagues)} league(s):")
            print("-" * 40)
            for i, league_id in enumerate(leagues, 1):
                print(f"{i:2d}. {league_id}")
            
            # Show detailed info for first few leagues
            print(f"\n📊 League Details (first 3):")
            print("-" * 40)
            for league_id in leagues[:3]:
                try:
                    league = game.to_league(league_id)
                    settings = league.settings()
                    print(f"\nLeague: {league_id}")
                    print(f"  Name: {settings.get('name', 'N/A')}")
                    print(f"  Season: {settings.get('season', 'N/A')}")
                    print(f"  Teams: {settings.get('num_teams', 'N/A')}")
        except Exception as e:
                    print(f"\nLeague: {league_id} - Error: {e}")
        else:
            print("\n❌ No leagues found.")
            print("This might be normal if you don't have active fantasy leagues.")
        
        print(f"\n✅ OAuth authentication script completed successfully!")
        print(f"Your credentials are saved in '{oauth_handler.token_file}'")
        print("You can now use this authentication in other scripts.")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Authentication cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ OAuth script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 