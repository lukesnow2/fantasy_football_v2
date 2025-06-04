#!/usr/bin/env python3
"""
Yahoo Fantasy API OAuth Authentication Script

This script demonstrates how to authenticate with the Yahoo Fantasy API
using the yahoo_fantasy_api library and OAuth 2.0 flow.
"""

import json
import os
from yahoo_fantasy_api import Game

# Configuration - Replace these placeholders with your actual credentials
YAHOO_APP_ID = "YOUR_APP_ID_HERE"
YAHOO_CLIENT_KEY = "YOUR_CLIENT_KEY_HERE"  
YAHOO_CLIENT_SECRET = "YOUR_CLIENT_SECRET_HERE"

# File to store OAuth tokens
TOKEN_FILE = "oauth_token.json"

class YahooFantasyOAuth:
    def __init__(self, app_id, client_key, client_secret):
        """
        Initialize the Yahoo Fantasy OAuth handler
        
        Args:
            app_id (str): Your Yahoo app ID
            client_key (str): Your Yahoo client key (consumer key)
            client_secret (str): Your Yahoo client secret (consumer secret)
        """
        self.app_id = app_id
        self.client_key = client_key
        self.client_secret = client_secret
        self.game = None
        
    def authenticate(self):
        """
        Perform OAuth authentication with Yahoo Fantasy API
        
        Returns:
            Game: Authenticated Game object for making API calls
        """
        try:
            # Check if we have saved tokens
            if os.path.exists(TOKEN_FILE):
                print("Found existing token file, attempting to use saved credentials...")
                try:
                    self.game = Game(
                        consumer_key=self.client_key,
                        consumer_secret=self.client_secret,
                        token_file=TOKEN_FILE
                    )
                    # Test the connection
                    self._test_connection()
                    print("Successfully authenticated using saved tokens!")
                    return self.game
                except Exception as e:
                    print(f"Saved tokens invalid or expired: {e}")
                    print("Starting fresh authentication...")
            
            # Perform new OAuth flow
            print("Starting OAuth authentication flow...")
            print("You will be redirected to Yahoo to authorize the application.")
            
            self.game = Game(
                consumer_key=self.client_key,
                consumer_secret=self.client_secret
            )
            
            # Test the connection
            self._test_connection()
            print("Authentication successful!")
            
            # Save tokens for future use
            self._save_tokens()
            
            return self.game
            
        except Exception as e:
            print(f"Authentication failed: {e}")
            raise
    
    def _test_connection(self):
        """Test the API connection by making a simple request"""
        try:
            # Try to get game information for NFL (code 423)
            nfl = self.game.to_league(423)  # 423 is NFL game code
            print("Connection test successful!")
        except Exception as e:
            print(f"Connection test failed: {e}")
            raise
    
    def _save_tokens(self):
        """Save OAuth tokens to file for future use"""
        try:
            # The yahoo_fantasy_api library should handle token saving automatically
            # but we can verify the token file exists
            if os.path.exists(TOKEN_FILE):
                print(f"Tokens saved to {TOKEN_FILE}")
            else:
                print("Warning: Token file not found after authentication")
        except Exception as e:
            print(f"Error saving tokens: {e}")

def main():
    """Main function to demonstrate Yahoo Fantasy API OAuth"""
    
    # Validate credentials
    if (YAHOO_APP_ID == "YOUR_APP_ID_HERE" or 
        YAHOO_CLIENT_KEY == "YOUR_CLIENT_KEY_HERE" or 
        YAHOO_CLIENT_SECRET == "YOUR_CLIENT_SECRET_HERE"):
        print("ERROR: Please update the credential placeholders in the script!")
        print("You need to replace:")
        print("- YAHOO_APP_ID with your actual Yahoo app ID")
        print("- YAHOO_CLIENT_KEY with your actual Yahoo client key")
        print("- YAHOO_CLIENT_SECRET with your actual Yahoo client secret")
        print("\nTo get these credentials:")
        print("1. Go to https://developer.yahoo.com/apps/")
        print("2. Create a new app or use an existing one")
        print("3. Note down your App ID, Client ID (Consumer Key), and Client Secret")
        return
    
    print("Yahoo Fantasy API OAuth Authentication")
    print("=" * 40)
    
    # Initialize OAuth handler
    oauth_handler = YahooFantasyOAuth(
        app_id=YAHOO_APP_ID,
        client_key=YAHOO_CLIENT_KEY,
        client_secret=YAHOO_CLIENT_SECRET
    )
    
    try:
        # Authenticate
        game = oauth_handler.authenticate()
        
        print("\nAuthentication completed successfully!")
        print("You can now use the 'game' object to make API calls.")
        
        # Example: Get current user's leagues
        print("\nExample: Fetching your leagues...")
        try:
            # This is just an example - you might need to adjust based on your needs
            leagues = game.league_ids()
            print(f"Found {len(leagues)} leagues")
            for league_id in leagues[:3]:  # Show first 3 leagues
                print(f"- League ID: {league_id}")
        except Exception as e:
            print(f"Error fetching leagues: {e}")
            print("This might be normal if you don't have any active leagues")
        
    except Exception as e:
        print(f"OAuth authentication failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check your credentials are correct")
        print("2. Ensure your Yahoo app has the correct permissions")
        print("3. Make sure you're using the correct redirect URI in your Yahoo app settings")

if __name__ == "__main__":
    main() 