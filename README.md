# Yahoo Fantasy Football Data Extraction System

A comprehensive data extraction system for Yahoo Fantasy Football leagues spanning 20 years (2005-2024).

## 🎯 Final Results
- **📅 20 years of data:** 2005-2024
- **🏆 26 leagues:** All postdraft leagues
- **📊 12,160 database records:** Ready for relational database import
- **💾 5.1MB dataset:** Complete JSON extraction

## 📁 Project Structure

### Core Files (Main Directory)
- `comprehensive_data_extractor.py` - Main extraction engine
- `run_final_complete_extraction.py` - Final extraction script  
- `yahoo_fantasy_FINAL_complete_data_20250605_101225.json` - Complete 20-year dataset (5.1MB)
- `yahoo_fantasy_oauth_v2.py` - OAuth2 authentication (current)
- `yahoo_fantasy_oauth.py` - Alternative OAuth implementation
- `oauth2.json` - OAuth tokens (gitignored)
- `requirements.txt` - Python dependencies

### Utils Folder
- `database_loader.py` - Database import utilities (CSV export, SQL generation)
- `database_schema.py` - Database schema definitions  
- `yahoo_fantasy_schema.sql` - SQL schema for database creation

### Tests Folder
- `test_complete_extraction.py` - Main test script
- `test_comprehensive_extractor.py` - Comprehensive testing
- `test_v2.py` - Alternative test approach
- `test_fixed_extraction.py` - Fixed extraction tests

### Debug Folder
- Analysis and debugging scripts from development
- Sample data files and extraction logs
- Historical extraction scripts with useful logic

### Archive Data Folder
- Previous extraction results and intermediate data files

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set up OAuth
- Copy `config.template.json` to `config.json` 
- Add your Yahoo API credentials
- Run authentication to generate `oauth2.json`

### 3. Run Extraction
```bash
# Test on single league
python3 tests/test_complete_extraction.py

# Full 20-year extraction
python3 run_final_complete_extraction.py
```

### 4. Database Import
```bash
# Generate CSV files
python3 utils/database_loader.py

# Create database schema
# Use utils/yahoo_fantasy_schema.sql
```

## 📊 Data Types Extracted

1. **Leagues** - Basic league information, settings, years
2. **Teams** - Team names, managers, records, points  
3. **Rosters** - Player rosters across multiple weeks
4. **Matchups** - Game results with scores and winners
5. **Transactions** - Trades, adds, drops with player details

## 📈 Data Quality
- **80.5% overall completeness** across all teams
- **95.8% success rate** for team records and points
- **Recent years (2020-2024):** 100% complete data
- **Early years (2005-2011):** 60-80% complete (API limitations)

## 🔧 Technical Details
- **OAuth2 authentication** with automatic token refresh
- **Rate limiting** (0.5s delays between API calls)
- **Error handling** for API failures and missing data
- **JSON output** with datetime serialization
- **Comprehensive logging** for debugging

## 📋 Dataset Contents
- **12,160 total records** across 5 data types
- **1,139 unique players** rostered over 20 years
- **2,718 games played** with complete scoring data
- **1,256 transactions** including trades and roster moves
- **343,673.9 total points** scored across all teams

## 🎯 Ready for Analysis
The final dataset is production-ready for:
- Fantasy football trend analysis
- Player performance tracking across decades  
- League evolution studies
- Competitive balance analysis
- Transaction pattern research 