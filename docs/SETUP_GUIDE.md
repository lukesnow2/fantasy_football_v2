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
# Install dependencies
pip install -r requirements.txt

# Provide credentials via environment (or a pre-existing oauth2.json):
export YAHOO_CLIENT_KEY="your_yahoo_client_id"
export YAHOO_CLIENT_SECRET="your_yahoo_client_secret"

# First run opens a browser for the one-time OAuth verifier, then writes
# oauth2.json (gitignored). Later runs refresh the token automatically.
python3 scripts/incremental_load.py --dry-run
```

## Step 2: Extract Data

```bash
# Show what the pipeline would load (any time of year):
python3 scripts/incremental_load.py --dry-run --force

# Load it (writes a gzipped, sanitized run snapshot under data/runs/):
python3 scripts/incremental_load.py --force
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
python3 scripts/incremental_load.py
```

## Step 4: Automation (Optional)

### GitHub Actions Setup
Add these secrets to your GitHub repository (Settings → Secrets):

```
YAHOO_CLIENT_KEY=your_yahoo_client_id
YAHOO_CLIENT_SECRET=your_yahoo_client_secret
YAHOO_REFRESH_TOKEN=your_refresh_token
DATABASE_URL=your_postgres_url
```

Get refresh token from oauth2.json:
```bash
cat oauth2.json | grep refresh_token
```

The pipeline runs Wednesdays 10:00 UTC in-season, plus a monthly heartbeat.
Failures file a GitHub issue; run scripts/staleness_check.py from an external
host (laptop cron) as the dead-man's check.

## Verification

### Check Data Extraction
```bash
# View extracted data structure
psql "$DATABASE_URL" -c "SELECT * FROM public.pipeline_runs ORDER BY run_id DESC LIMIT 5"
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

**Authentication Error**: delete oauth2.json and re-run any pipeline command
to redo the one-time OAuth flow; verify YAHOO_CLIENT_KEY/SECRET are set.

**Database Connection**: Test connection string
```bash
psql $DATABASE_URL -c "SELECT version();"
```

**No Data**: Check if you have fantasy leagues
```bash
python3 scripts/incremental_load.py --dry-run --force
```

### Support

- Check existing [GitHub Issues](../../issues)
- Review [Security Guidelines](../SECURITY.md) for credential issues
- Verify you have active Yahoo Fantasy leagues

---

**Total setup time: ~15 minutes** ⚡ 