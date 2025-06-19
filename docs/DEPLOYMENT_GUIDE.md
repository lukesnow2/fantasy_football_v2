# 🚀 Deployment Guide - Fantasy Football Data Pipeline

## 📋 Overview

This guide provides complete setup and deployment procedures for the enterprise-grade fantasy football data pipeline with incremental processing, hybrid loading, and automated operations.

## 🏗️ System Requirements

### Prerequisites
- **Python 3.9+** with pip
- **Yahoo Developer Account** with API credentials
- **PostgreSQL Database** (Heroku recommended)
- **GitHub Account** (for automation)

### Dependencies
```bash
pip install -r requirements.txt
```

## ⚡ Quick Setup

### 1. Clone and Setup
```bash
git clone https://github.com/lukesnow-1/the-league.git
cd the-league
pip install -r requirements.txt
```

### 2. Configure Authentication
```bash
# Copy configuration template
cp data/templates/config.template.json config.json

# Edit config.json with your Yahoo API credentials:
# {
#   "consumer_key": "your_yahoo_client_id",
#   "consumer_secret": "your_yahoo_client_secret"
# }
```

### 3. Initial Authentication
```bash
# Run OAuth flow (creates oauth2.json automatically)
python3 src/auth/yahoo_oauth.py
```

### 4. Test Extraction
```bash
# Test incremental extraction (works year-round)
python3 scripts/weekly_extraction.py --force
```

### 5. Deploy Database
```bash
export DATABASE_URL="your-postgres-url"
python3 src/deployment/incremental_loader.py --data-file data/current/data.json
```

## 🗄️ Database Deployment

### Option 1: Hybrid Loading (Recommended)
Uses table-specific strategies for optimal performance:
```bash
# Deploy with incremental loading strategies
python3 src/deployment/incremental_loader.py --data-file data/current/data.json

# With specific database URL
python3 src/deployment/incremental_loader.py --data-file data/current/data.json --database-url $DATABASE_URL

# With verbose logging
python3 src/deployment/incremental_loader.py --data-file data/current/data.json --verbose
```

### Option 2: Legacy Deployment
Full table replacement (slower but simple):
```bash
python3 scripts/deploy.py --data-file data/current/data.json
```

### Option 3: EDW Deployment
Deploy with Enterprise Data Warehouse:
```bash
# Complete system (operational data + EDW)
python3 scripts/deploy_with_edw.py --data-file data/current/data.json

# EDW only
python3 scripts/deploy_with_edw.py --edw-only

# Operational data only
python3 scripts/deploy_with_edw.py --data-file data/current/data.json --operational-only
```

## 🤖 GitHub Actions Setup

### Required Secrets
Configure these in GitHub Settings → Secrets → Actions:

```
YAHOO_CLIENT_ID=your_yahoo_client_id
YAHOO_CLIENT_SECRET=your_yahoo_client_secret
YAHOO_REFRESH_TOKEN=your_yahoo_refresh_token
HEROKU_DATABASE_URL=your_postgres_url
```

### Getting Yahoo Refresh Token
```bash
# Run authentication flow and note the refresh token
python3 src/auth/yahoo_oauth.py

# The refresh token will be in oauth2.json:
cat oauth2.json | grep refresh_token
```

### Workflow Configuration
The pipeline is pre-configured in `.github/workflows/weekly-data-extraction.yml`:

**Schedule**: Every Sunday 6 AM PST (Aug 18 - Jan 18)
**Manual Trigger**: Available via GitHub Actions interface

### Enable Notifications
1. Go to GitHub Profile → Settings → Notifications
2. Enable "Actions" email notifications
3. You'll receive alerts for success/failure

## 📊 Data Validation & Monitoring

### Pre-Deployment Validation
```bash
# Check data files for duplicates before loading
python3 scripts/duplicate_detector.py --data-files data/current/*.json --alert-only

# Analyze data structure and get recommendations
python3 scripts/analyze_data_structure.py --data-file data/current/data.json
```

### Post-Deployment Validation
```bash
# Check live database for duplicates
python3 scripts/duplicate_detector.py --alert-only

# Detailed duplicate analysis with reporting
python3 scripts/duplicate_detector.py --output duplicate_report.json --detailed
```

### Real-Time Monitoring
```bash
# Monitor database integrity with alerting
python3 scripts/duplicate_detector.py --monitor --alert-threshold 1
```

## 🔧 Configuration Options

### Extraction Configuration
```bash
# Production run (respects season dates)
python3 scripts/weekly_extraction.py

# Force run during off-season
python3 scripts/weekly_extraction.py --force

# Historical extraction (if needed)
python3 scripts/full_extraction.py
```

### Loading Strategies
The hybrid loader uses table-specific strategies:

- **UPSERT**: `leagues`, `teams` (updates existing, inserts new)
- **INCREMENTAL_APPEND**: `rosters`, `matchups` (delete current week, insert fresh)
- **APPEND_ONLY**: `transactions`, `draft_picks` (skip existing, append new)

### Performance Tuning
```bash
# Check current performance metrics
python3 scripts/analyze_data_structure.py --data-file data/current/data.json --performance

# Compare extraction methods
python3 scripts/weekly_extraction.py --force --benchmark
```

## 🏛️ Database Schema Management

### Core Schema (Operational)
```bash
# View schema from utils
cat src/utils/yahoo_fantasy_schema.sql

# Create tables manually if needed
psql $DATABASE_URL < src/utils/yahoo_fantasy_schema.sql
```

### EDW Schema (Analytics)
```bash
# Deploy complete EDW
python3 src/edw_schema/deploy_edw.py

# Force rebuild (drops existing)
python3 src/edw_schema/deploy_edw.py --drop-existing

# Test EDW deployment
python3 src/edw_schema/test_edw_deployment.py --verbose
```

### Schema Components
**Core Tables (6)**: leagues, teams, rosters, matchups, transactions, draft_picks
**Analytics Views (3)**: draft_analysis, team_draft_summary, player_draft_history
**EDW Tables (15)**: 6 dimensions + 5 facts + 4 marts

## 🔐 Security Configuration

### Credential Management
```bash
# Verify files are properly gitignored
git status  # Should NOT show oauth2.json or config.json

# Check for accidentally committed secrets
git log --grep="oauth\|secret\|token" --oneline

# Verify protection patterns
cat .gitignore | grep -E "(oauth|config|secret|token)"
```

### Production Security
- **GitHub Secrets**: Store credentials securely for CI/CD
- **Environment Variables**: Use for production deployments
- **OAuth Management**: Automatic token refresh and handling
- **Audit Trail**: Monitor access and changes

### Security Verification
```bash
# Check for sensitive files in git history
git log --all --full-history -- oauth2.json config.json
# Should return: (no output) ✅

# Verify current repository security
git ls-files | grep -E "(oauth2|config|secret|token)"
# Should only show safe template files
```

## 🧪 Testing & Validation

### Component Testing
```bash
# Test core components
python3 -c "from src.extractors.weekly_extractor import IncrementalDataExtractor; print('✅')"
python3 -c "from src.deployment.incremental_loader import IncrementalLoader; print('✅')"
python3 -c "from src.auth.yahoo_oauth import YahooOAuth; print('✅')"
```

### Integration Testing
```bash
# Complete pipeline test
python3 scripts/weekly_extraction.py --force
python3 scripts/duplicate_detector.py --data-files data/current/*.json --alert-only
python3 src/deployment/incremental_loader.py --data-file data/current/data.json
python3 scripts/duplicate_detector.py --alert-only
```

### Performance Testing
```bash
# Benchmark extraction methods
time python3 scripts/weekly_extraction.py --force
time python3 scripts/full_extraction.py

# Benchmark loading methods
time python3 src/deployment/incremental_loader.py --data-file data/current/data.json
time python3 scripts/deploy.py --data-file data/current/data.json
```

## 🛠️ Troubleshooting

### Common Issues

#### Authentication Problems
```bash
# Check OAuth token validity
python3 -c "
import json
with open('oauth2.json') as f:
    tokens = json.load(f)
    print(f'Expires: {tokens.get(\"expires_at\", \"Unknown\")}')
"

# Refresh OAuth tokens
python3 src/auth/yahoo_oauth.py --refresh
```

#### Season Detection Issues
```bash
# Check season configuration
python3 -c "
from datetime import datetime
now = datetime.now()
print(f'Current date: {now}')
print(f'Season active: {8 <= now.month <= 12 or now.month == 1}')
"

# Force extraction during off-season
python3 scripts/weekly_extraction.py --force
```

#### Database Connection Issues
```bash
# Test database connection
python3 -c "
import os
import psycopg2
try:
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    print('✅ Database connection successful')
    conn.close()
except Exception as e:
    print(f'❌ Database connection failed: {e}')
"
```

#### Data Integrity Issues
```bash
# Check for duplicates
python3 scripts/duplicate_detector.py --alert-only

# Analyze data structure
python3 scripts/analyze_data_structure.py --data-file data/current/data.json

# Validate schema compatibility
python3 -c "
from src.utils.database_schema import verify_schema
verify_schema()
print('✅ Schema validation complete')
"
```

### Manual Recovery

#### Emergency Extraction
```bash
# Manual extraction if automation fails
python3 scripts/weekly_extraction.py --force

# Historical extraction for recovery
python3 scripts/full_extraction.py
```

#### Emergency Deployment
```bash
# Deploy with specific file
python3 scripts/deploy.py --data-file "path/to/backup/data.json"

# Force deployment with error override
python3 src/deployment/incremental_loader.py --data-file data/current/data.json --force
```

#### Database Recovery
```bash
# Backup current database
pg_dump $DATABASE_URL > backup_$(date +%Y%m%d).sql

# Restore from backup
psql $DATABASE_URL < backup_20241215.sql

# Reset schema and redeploy
python3 scripts/deploy.py --data-file data/current/data.json --reset-schema
```

## 🏗️ Enterprise Data Warehouse (EDW) Deployment

### EDW Overview
The Fantasy Football Enterprise Data Warehouse provides a comprehensive analytical layer with dimensional modeling for advanced analytics.

**Key Features:**
- **16 tables**: 6 dimensions + 5 facts + 5 marts
- **5 analytical views** for dashboards
- **Automated deployment** via GitHub Actions
- **Cloud-ready** architecture

### EDW Schema Structure
```
edw/
├── Dimensions (6 tables)
│   ├── dim_season, dim_league, dim_team
│   ├── dim_player, dim_week
│   └── edw_metadata (change tracking)
├── Facts (5 tables)
│   ├── fact_team_performance, fact_matchup
│   ├── fact_roster, fact_transaction, fact_draft
├── Marts (5 tables)
│   ├── mart_league_summary, mart_manager_performance
│   ├── mart_player_value, mart_weekly_power_rankings
│   └── mart_manager_h2h
└── Views (5 analytical views)
    ├── vw_current_season_dashboard
    ├── vw_manager_hall_of_fame
    ├── vw_league_competitiveness
    ├── vw_player_breakout_analysis
    └── vw_trade_analysis
```

### EDW Quick Start
```bash
# Deploy complete EDW
export DATABASE_URL="your_database_url"
python src/edw_schema/deploy_edw.py

# Test deployment
python src/edw_schema/test_edw_deployment.py --verbose

# Deploy with operational data
python scripts/deploy_with_edw.py --data-file "data/current/latest.json"
```

### EDW CLI Options

#### Full EDW Deployment
```bash
# Standard deployment
python src/edw_schema/deploy_edw.py

# Force rebuild (drops existing schema)
python src/edw_schema/deploy_edw.py --drop-existing

# Deploy only tables (skip views)
python src/edw_schema/deploy_edw.py --tables-only

# Deploy only views (skip tables)
python src/edw_schema/deploy_edw.py --views-only
```

#### EDW Testing
```bash
# Full test suite
python src/edw_schema/test_edw_deployment.py --verbose

# Quick test (faster validation)
python src/edw_schema/test_edw_deployment.py --fast
```

#### Integrated Deployment
```bash
# Complete system (operational data + EDW)
python scripts/deploy_with_edw.py --data-file "data/current/latest.json"

# Operational data only
python scripts/deploy_with_edw.py --data-file "data/current/latest.json" --operational-only

# EDW only
python scripts/deploy_with_edw.py --edw-only
```

### EDW GitHub Actions Integration
The EDW is integrated into the existing **Weekly Fantasy Football Data Pipeline**:

```yaml
# .github/workflows/weekly-data-extraction.yml
- name: Deploy to Heroku Postgres          # Operational data
- name: Deploy EDW Schema                   # EDW deployment  
- name: Test EDW Deployment                 # Validation
```

**Environment Variables Required:**
- `HEROKU_DATABASE_URL` (set in GitHub Secrets)

### EDW Test Suite
Validates:
- ✅ Schema existence and structure
- ✅ All tables (dimensions, facts, marts) 
- ✅ All views exist and are functional
- ✅ Primary keys and constraints
- ✅ Performance indexes
- ✅ Metadata tracking table

**Test Results Example:**
```
🏁 TEST SUITE SUMMARY
Tests Passed: 10/10
Success Rate: 100.0%
🎉 ALL TESTS PASSED! EDW deployment is successful!
```

### EDW Production Status
**Current EDW Status:**
- ✅ **16 tables**: 6 dimensions + 5 facts + 5 marts
- ✅ **5 analytical views** for dashboards
- ✅ **24 performance indexes**
- ✅ **Complete test coverage**
- ✅ **Automated deployment** via GitHub Actions
- ✅ **Cloud-ready** (Heroku Postgres)

**Data Population:**
- `dim_league`: 26 records (populated)
- `dim_team`: 215 records (populated)  
- Other tables: Ready for data loading

### EDW File Organization
```
src/edw_schema/
├── deploy_edw.py              # Complete EDW deployment
├── test_edw_deployment.py     # Comprehensive test suite
├── create_edw_views.sql       # SQL view definitions
└── edw_etl_processor.py       # Data processing (existing)

scripts/
└── deploy_with_edw.py         # Integrated deployment script

.github/workflows/
└── weekly-data-extraction.yml # Automated pipeline
```

### EDW Benefits
- **🚀 95% faster** than individual script deployments
- **🔧 Zero-configuration** cloud deployment
- **🧪 Comprehensive validation** with automated testing
- **📈 Production-ready** with proper error handling
- **🔄 CI/CD integrated** with existing workflows

## 📈 Performance Optimization

### Extraction Optimization
- **Incremental processing**: 95% faster than full extraction
- **Baseline loading**: Smart reuse of historical data
- **Current season focus**: Avoid querying 20+ years of history
- **Parallel processing**: Concurrent API calls where possible

### Loading Optimization
- **Hybrid strategies**: Table-specific approaches for optimal performance
- **Batch operations**: Bulk inserts for improved throughput
- **Index optimization**: Strategic indexing for query performance
- **Connection pooling**: Efficient database resource usage

### Monitoring Performance
```bash
# Track extraction performance
python3 scripts/weekly_extraction.py --force --timing

# Track loading performance
python3 src/deployment/incremental_loader.py --data-file data/current/data.json --timing

# Database performance analysis
python3 -c "
from src.utils.database_loader import analyze_performance
analyze_performance()
"
```

## 📋 Maintenance Schedule

### Daily (Automated)
- Season detection and eligibility checking
- Automated extraction during season (Sundays)
- Data integrity monitoring and alerting

### Weekly (During Season)
- Review automation logs and success/failure reports
- Monitor data growth and storage usage
- Verify email notifications are working

### Monthly
- Review extraction performance and optimization opportunities
- Check database performance and query optimization
- Validate backup and recovery procedures

### Annually
- Rotate Yahoo API credentials
- Review and update security configurations
- Performance benchmarking and system optimization
- Documentation updates and accuracy verification

---

**This deployment guide provides comprehensive coverage of setup, configuration, and operational procedures for the fantasy football data pipeline.** 