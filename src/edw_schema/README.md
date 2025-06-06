# Fantasy Football Enterprise Data Warehouse (EDW)

## 🏗️ Architecture Overview

This Enterprise Data Warehouse transforms your operational fantasy football data into a **star schema optimized for analytics and web application serving**. The design follows data warehousing best practices with clear separation between:

- **Dimension Tables**: Master data (seasons, leagues, teams, players, weeks)
- **Fact Tables**: Transactional data (performance, matchups, rosters, transactions, drafts)  
- **Data Marts**: Pre-aggregated analytics (league summaries, manager performance)
- **Optimized Views**: Ready-to-use data for web applications

## 📊 Schema Design

### Dimension Tables (SCD Type 2)
- `dim_season` - Season metadata with playoff structure
- `dim_league` - League configurations with change tracking
- `dim_team` - Team and manager information  
- `dim_player` - Player profiles with position data
- `dim_week` - Granular time dimension for weekly analysis

### Fact Tables (Event-driven)
- `fact_team_performance` - Weekly team metrics and rankings
- `fact_matchup` - Head-to-head game results and statistics
- `fact_roster` - Player ownership and lineup decisions
- `fact_transaction` - All player movement events
- `fact_draft` - Draft results with value analysis

### Data Marts (Pre-aggregated)
- `mart_league_summary` - League-level statistics and competitiveness metrics
- `mart_manager_performance` - Cross-season manager career statistics

## 🚀 Deployment Process

### 1. Create the EDW Schema

```bash
# Deploy schema to Heroku Postgres
psql $DATABASE_URL < src/edw_schema/fantasy_edw_schema.sql

# Or use the ETL processor with auto-schema creation
python3 src/edw_schema/edw_etl_processor.py --create-schema
```

### 2. Run ETL Process

```bash
# Transform operational data into EDW format
python3 src/edw_schema/edw_etl_processor.py \
    --data-file data/current/yahoo_fantasy_COMPLETE_with_drafts_*.json \
    --database-url $DATABASE_URL
```

### 3. Start Web API Service

```bash
# Start FastAPI data service
cd src/edw_schema
python3 web_app_data_service.py

# Access API documentation at: http://localhost:8000/docs
```

## 📱 API Documentation

### Available Endpoints

- `GET /` - Health check and service status
- `GET /leagues` - All leagues with summary statistics
- `GET /dashboard/current-season` - Comprehensive current season data

### Example Usage

```bash
# Get all leagues for 2024 season
curl "http://localhost:8000/leagues?season_year=2024"

# Get current season dashboard
curl "http://localhost:8000/dashboard/current-season"
```

## 🎯 Key Benefits

✅ **Star Schema Design** - Optimized for analytical queries  
✅ **High Performance** - Pre-aggregated data marts for sub-second responses  
✅ **REST API Ready** - Clean endpoints for web/mobile applications  
✅ **Scalable Architecture** - Handles growing data volumes efficiently  

**Your web app can now leverage 20+ years of fantasy football data with enterprise-grade performance!** 🏆 