# 🚀 Setup Guide

## Prerequisites

- Python 3.9+
- Yahoo Developer Account ([create here](https://developer.yahoo.com/apps/))
- PostgreSQL Database (Heroku recommended for free tier)

## Step 1: Authentication Setup

### Get Yahoo API Credentials
1. Visit [Yahoo Developer Console](https://developer.yahoo.com/apps/)
2. Create new app with **Fantasy Sports** permissions
3. Note your Client ID and Client Secret

### Configure Authentication
```bash
# Copy template and add your credentials
cp data/templates/config.template.json config.json

# Edit config.json:
{
  "consumer_key": "your_yahoo_client_id",
  "consumer_secret": "your_yahoo_client_secret"
}
```

### Complete OAuth Flow
```bash
# Install dependencies
pip install -r requirements.txt

# Run authentication (opens browser)
python3 src/auth/yahoo_oauth.py
# This creates oauth2.json automatically
```

## Step 2: Extract Data

```bash
# Test extraction (works any time of year)
python3 scripts/weekly_extraction.py --force

# This creates: data/current/data.json
```

## Step 3: Deploy to Database

### Setup Database
```bash
# Get a free PostgreSQL database from Heroku
heroku addons:create heroku-postgresql:mini --app your-app

# Get connection URL
heroku config:get DATABASE_URL --app your-app
```

### Load Data
```bash
# Set database URL
export DATABASE_URL="your-postgres-connection-string"

# Deploy data
python3 src/deployment/incremental_loader.py --data-file data/current/data.json
```

## Step 4: Automation (Optional)

### GitHub Actions Setup
Add these secrets to your GitHub repository (Settings → Secrets):

```
YAHOO_CLIENT_ID=your_yahoo_client_id
YAHOO_CLIENT_SECRET=your_yahoo_client_secret  
YAHOO_REFRESH_TOKEN=your_refresh_token
HEROKU_DATABASE_URL=your_postgres_url
```

Get refresh token from oauth2.json:
```bash
cat oauth2.json | grep refresh_token
```

The pipeline will automatically run every Sunday during fantasy season (Aug-Jan).

## Verification

### Check Data Extraction
```bash
# View extracted data structure
python3 scripts/analyze_data_structure.py --data-file data/current/data.json
```

### Check Database
```bash
# Connect to your database and verify:
psql $DATABASE_URL

# Check record counts
SELECT 'leagues' as table_name, COUNT(*) FROM leagues
UNION ALL  
SELECT 'teams', COUNT(*) FROM teams
UNION ALL
SELECT 'rosters', COUNT(*) FROM rosters;
```

## Troubleshooting

### Common Issues

**Authentication Error**: Verify Yahoo API credentials and OAuth flow
```bash
python3 src/auth/yahoo_oauth.py --verbose
```

**Database Connection**: Test connection string
```bash
psql $DATABASE_URL -c "SELECT version();"
```

**No Data**: Check if you have fantasy leagues
```bash
python3 scripts/weekly_extraction.py --force --verbose
```

### Support

- Check existing [GitHub Issues](../../issues)
- Review [Security Guidelines](../SECURITY.md) for credential issues
- Verify you have active Yahoo Fantasy leagues

---

**Total setup time: ~15 minutes** ⚡ 