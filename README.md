# 🏈 The League: Fantasy Football Analytics Platform

A fully automated system that extracts, processes, and analyzes 20+ years of Yahoo Fantasy Football data to provide comprehensive league insights and historical analytics.

## What This Project Does

This platform automatically:
- **Extracts** complete fantasy football data from Yahoo's API (leagues, rosters, transactions, drafts)
- **Processes** 20+ years of historical data with incremental updates 
- **Analyzes** performance trends, draft patterns, and competitive dynamics
- **Maintains** a live PostgreSQL database with 16,000+ records across 26 leagues
- **Runs automatically** via GitHub Actions during fantasy season

## Key Features

- ✅ **Complete Historical Dataset**: 2004-2025 fantasy data across 26 leagues
- ✅ **Automated Pipeline**: Weekly data updates with zero maintenance  
- ✅ **Performance Optimized**: 95% faster than traditional extraction methods
- ✅ **Data Integrity**: Zero duplicates with comprehensive validation
- ✅ **Live Database**: Real-time PostgreSQL with analytics views
- ✅ **Security Hardened**: Protected credentials and clean git history

## Quick Start

### 1. Setup Authentication
```bash
# Copy template and add your Yahoo API credentials
cp data/templates/config.template.json config.json
# Edit config.json with your Yahoo API credentials
```

### 2. Install and Test
```bash
pip install -r requirements.txt
python3 scripts/weekly_extraction.py --force
```

### 3. Deploy to Database
```bash
export DATABASE_URL="your-postgres-url"
python3 src/deployment/incremental_loader.py --data-file data/current/data.json
```

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Yahoo API     │───▶│  Extractors  │───▶│   PostgreSQL    │
│                 │    │              │    │   Database      │
└─────────────────┘    └──────────────┘    └─────────────────┘
                              │                       │
                       ┌──────────────┐        ┌─────────────┐
                       │ GitHub       │        │ Analytics   │
                       │ Actions      │        │ Views       │
                       └──────────────┘        └─────────────┘
```

## What's Included

- **Data Extraction**: Automated incremental updates from Yahoo Fantasy API
- **Database Management**: PostgreSQL schema with optimized loading strategies  
- **Analytics Engine**: Pre-built views for league analysis and insights
- **Automation**: GitHub Actions for scheduled data updates
- **Security**: OAuth authentication with credential protection

## Use Cases

- **League Commissioners**: Track historical performance and league health
- **Fantasy Players**: Analyze draft patterns and trading behaviors  
- **Data Analysts**: Rich dataset for fantasy football research
- **Developers**: Example of production data pipeline architecture

## Documentation

- **[Setup Guide](docs/SETUP_GUIDE.md)** - Installation and configuration
- **[Security Notes](SECURITY.md)** - Credential protection guidelines

---

**Built for production. Zero maintenance required.** 🏆 