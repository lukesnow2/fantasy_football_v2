# Yahoo Fantasy API OAuth Script

This Python script provides an easy way to authenticate with the Yahoo Fantasy API using OAuth 2.0 through the `yahoo_fantasy_api` library.

## Prerequisites

1. **Yahoo Developer Account**: You need a Yahoo Developer account and a registered application.
2. **Python 3.7+**: This script requires Python 3.7 or higher.

## Setup Instructions

### 1. Create a Yahoo Developer Application

1. Go to [Yahoo Developer Console](https://developer.yahoo.com/apps/)
2. Click "Create an App"
3. Fill out the application details:
   - **Application Name**: Choose any name for your app
   - **Application Type**: Select "Web Application"
   - **Description**: Brief description of your app
   - **Home Page URL**: Can be `http://localhost` for testing
   - **Redirect URI(s)**: Use `oob` (out-of-band) for desktop applications
4. Select the required permissions:
   - **Fantasy Sports**: Read access
5. Note down your:
   - **App ID**
   - **Client ID** (Consumer Key)
   - **Client Secret** (Consumer Secret)

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

Or install individually:
```bash
pip install yahoo_fantasy_api requests requests-oauthlib
```

### 3. Configure Credentials

Edit the `yahoo_oauth.py` file and replace the placeholder values:

```python
YAHOO_APP_ID = "your_actual_app_id"
YAHOO_CLIENT_KEY = "your_actual_client_key"  
YAHOO_CLIENT_SECRET = "your_actual_client_secret"
```

## Usage

### Basic Authentication

Run the script:
```bash
python yahoo_oauth.py
```

The script will:
1. Check for existing saved tokens
2. If no valid tokens exist, initiate the OAuth flow
3. Open your browser for Yahoo authorization (or provide a URL to visit)
4. Save the tokens for future use
5. Test the connection with a simple API call

### Token Management

- Tokens are automatically saved to `oauth_token.json`
- The script will reuse valid tokens on subsequent runs
- If tokens expire, the script will automatically start a new OAuth flow

### Using the Authenticated Game Object

After successful authentication, you can use the `game` object to make API calls:

```python
# Example usage after authentication
game = oauth_handler.authenticate()

# Get your leagues
leagues = game.league_ids()
print(f"Your leagues: {leagues}")

# Access a specific league
league = game.to_league('your_league_key')

# Get league settings
settings = league.settings()
print(f"League settings: {settings}")

# Get teams in the league
teams = league.teams()
for team in teams:
    print(f"Team: {team['name']}")
```

## Features

- **Automatic Token Management**: Saves and reuses OAuth tokens
- **Error Handling**: Comprehensive error handling and user feedback
- **Connection Testing**: Validates authentication with test API calls
- **Credential Validation**: Ensures all required credentials are provided
- **Flexible**: Easy to extend for additional Yahoo Fantasy API operations

## Common Issues and Troubleshooting

### Authentication Errors
- **Invalid credentials**: Double-check your App ID, Client Key, and Client Secret
- **Redirect URI mismatch**: Ensure your Yahoo app is configured with the correct redirect URI
- **Permissions**: Make sure your Yahoo app has Fantasy Sports read permissions

### API Errors
- **No leagues found**: This is normal if you don't have any active fantasy leagues
- **Rate limiting**: Yahoo has API rate limits; wait a few minutes if you hit them
- **Token expiration**: Delete `oauth_token.json` to force re-authentication

### Browser Issues
- If the browser doesn't open automatically, copy the URL from the console
- For headless environments, use the manual authorization process

## File Structure

```
.
├── yahoo_oauth.py      # Main OAuth script
├── requirements.txt    # Python dependencies
├── README.md          # This file
└── oauth_token.json   # Saved OAuth tokens (created after first auth)
```

## Security Notes

- Never commit your actual credentials to version control
- Keep your `oauth_token.json` file secure and private
- Consider using environment variables for credentials in production

## Next Steps

After successful authentication, you can:
- Fetch league information and standings
- Get player data and statistics
- Retrieve matchup information
- Access historical data
- Build fantasy sports applications

For more information on available API endpoints, refer to the [yahoo_fantasy_api documentation](https://yahoo-fantasy-api.readthedocs.io/). 