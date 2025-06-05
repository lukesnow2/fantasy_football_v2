# 🏈 The League: Enterprise Fantasy Football Data Pipeline

**A fully automated, production-ready system for incremental extraction and analysis of 20+ years of Yahoo Fantasy Football data.**

[![Automated Pipeline](https://img.shields.io/badge/Pipeline-Automated-brightgreen)](https://github.com/lukesnow-1/the-league/actions)
[![Data Coverage](https://img.shields.io/badge/Data-2004--2025-blue)](#data-coverage)
[![Status](https://img.shields.io/badge/Status-Production-success)](#production-status)
[![Extraction](https://img.shields.io/badge/Extraction-Incremental-orange)](#incremental-system)

## ✨ **What This Is**

This is an **enterprise-grade incremental data pipeline** that automatically:
- 📈 **Incrementally extracts** only new fantasy football data since last run
- 🏈 **Auto-detects** new leagues and extracts their complete draft data  
- 🗄️ **Deploys** complete updated dataset to live PostgreSQL on Heroku
- ⚡ **Runs weekly** during fantasy season (Aug 18 - Jan 18) via GitHub Actions
- 📧 **Sends notifications** on success/failure via email
- 🔄 **Requires zero maintenance** - fully automated incremental updates
- 🧪 **Testable year-round** with `--force` flag for off-season development

## 🚀 **Professional Project Structure**

```
the-league/
├── 📁 src/                          # Core source code (modular design)
│   ├── extractors/                  # Data extraction modules
│   │   ├── comprehensive_data_extractor.py  # Historical extraction engine
│   │   ├── weekly_extractor.py             # 🔥 Incremental production system
│   │   └── draft_extractor.py              # Specialized draft processing
│   ├── deployment/                  # Database deployment
│   │   └── heroku_deployer.py              # Streamlined Postgres deployer
│   ├── auth/                        # Authentication
│   │   └── yahoo_oauth.py                  # Yahoo API OAuth handler
│   └── utils/                       # Database & utilities
│       ├── database_schema.py              # Database structure definitions
│       ├── database_loader.py              # Data loading utilities
│       ├── query_database.py               # Database query helpers
│       └── yahoo_fantasy_schema.sql        # Complete PostgreSQL schema
├── 📁 scripts/                      # Clean entry point scripts
│   ├── weekly_extraction.py         # 🔥 Primary incremental extraction
│   ├── full_extraction.py           # Historical extraction (completed)
│   └── deploy.py                    # Database deployment
├── 📁 data/                         # Organized data storage
│   ├── current/                     # Active dataset files (16,000+ records)
│   └── templates/                   # Configuration templates
├── 📁 docs/                         # Comprehensive documentation
├── 📁 .github/workflows/            # GitHub Actions automation
│   └── weekly-data-extraction.yml   # Incremental pipeline automation
└── 📋 requirements.txt              # Python dependencies
```

## 🔥 **Incremental System**

### **Smart Incremental Updates**
The system loads the previous complete dataset as a baseline, then:

- **🆕 New League Detection**: Identifies any new leagues since last run
- **📅 Current Season Focus**: Only queries current season (not 20+ years of history)  
- **📋 Recent Rosters**: Current + previous week (captures lineup changes)
- **💰 Recent Transactions**: Last 30 days (captures all player movements)
- **🏆 Recent Matchups**: Current + 2 previous weeks (captures game results)
- **🏈 Auto-Draft Extraction**: Extracts complete draft data for any new leagues
- **🔄 Baseline Merging**: Combines incremental data with historical baseline

### **Production Efficiency**
- **⚡ 95% faster**: Seconds vs. minutes (current season only)
- **🎯 Precise targeting**: Only extracts what's actually new
- **📊 Complete output**: Always maintains full historical + current dataset
- **🔄 Zero data loss**: Preserves all historical data while adding new

## 📊 **Complete Dataset**

**20+ Year Historical Foundation:**
- **26 Fantasy Leagues** (2004-2025)
- **215 Teams** across all seasons
- **10,395 Roster Records** (weekly player assignments)
- **268 Matchups** (head-to-head games)
- **1,256 Transactions** (trades, add/drops, waivers)
- **3,888 Draft Picks** (complete draft history)

**Live Database:** 16,000+ records across 6 normalized tables with advanced analytics views.

**Incremental Updates:** New data automatically merged with historical baseline.

## ⚡ **Quick Start**

### **1. Setup Authentication**
```bash
# Copy templates
cp data/templates/config.template.json config.json
cp data/templates/oauth2.template.json oauth2.json

# Add your Yahoo API credentials to both files
```

### **2. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **3. Run Incremental Extraction**
```bash
# Production: incremental updates during season
python3 scripts/weekly_extraction.py

# Testing: force extraction during off-season  
python3 scripts/weekly_extraction.py --force

# Historical: complete data extraction (already completed)
python3 scripts/full_extraction.py
```

### **4. Deploy to Database**
```bash
export DATABASE_URL="your-postgres-url"
python3 scripts/deploy.py
```

## 🤖 **Automated Production Pipeline**

**Zero-maintenance incremental automation** via GitHub Actions:

### **Smart Scheduling**
- **📅 Weekly runs**: Every Sunday 6 AM PST during fantasy season
- **🎯 Season detection**: Automatically pauses during off-season (Jun-Aug)
- **🔄 Auto-resume**: Restarts August 18th each year

### **Incremental Operations**
- **📈 Baseline loading**: Loads previous complete dataset
- **🆕 New league detection**: Identifies any new leagues  
- **⚡ Current season focus**: Only current year data (not historical scan)
- **🏈 Auto-draft extraction**: Complete draft data for new leagues
- **📊 Complete output**: Full updated dataset for deployment

### **Reliable Deployment**
- **☁️ Direct to Heroku PostgreSQL**: Streamlined database deployment
- **📧 Email notifications**: Success/failure alerts to lukesnow2@gmail.com
- **🔄 Error recovery**: Graceful handling and detailed logging

## 🗄️ **Database Schema**

**Production PostgreSQL with 6 normalized tables:**

- **`leagues`** - League configurations and settings
- **`teams`** - Team information and current standings  
- **`rosters`** - Weekly player assignments and lineup changes
- **`matchups`** - Head-to-head game results and scores
- **`transactions`** - All player movements (trades, waivers, add/drops)
- **`draft_picks`** - Complete draft history with pick analysis

**Advanced Analytics Views:**
- **`draft_analysis`** - Draft performance metrics and trends
- **`team_draft_summary`** - Team drafting patterns and success
- **`player_draft_history`** - Player draft trends across seasons

## 📈 **Production Status**

✅ **Fully Operational - Incremental System**
- **🔥 Incremental extraction**: Live and optimized for production
- **🗄️ Live Heroku PostgreSQL**: Complete 20+ year dataset deployed
- **🤖 GitHub Actions pipeline**: Active incremental automation
- **📧 Email notifications**: Configured and functional
- **🧪 Year-round testing**: `--force` flag for off-season development
- **🔄 Zero maintenance**: Fully automated incremental updates

**🎯 Next Season Auto-Resume:** August 18, 2025

## 🛠️ **Development & Testing**

### **Production Architecture**
- **🏗️ Modular design**: Clean separation of extraction, deployment, auth
- **📈 Incremental system**: Smart baseline loading and incremental updates  
- **🔄 Error handling**: Comprehensive logging and graceful failure recovery
- **⚡ Rate limiting**: Yahoo API compliance with smart throttling
- **✅ Data validation**: Type checking and consistency verification

### **Testing Capabilities**
```bash
# Test incremental extraction during off-season
python3 scripts/weekly_extraction.py --force

# Test core components
python3 -c "from src.extractors.weekly_extractor import IncrementalDataExtractor; print('✅')"

# Test database deployment
python3 -c "from src.deployment.heroku_deployer import HerokuPostgresDeployer; print('✅')"
```

### **Incremental Development**
- **📊 Baseline testing**: Uses existing complete dataset as foundation
- **🆕 New league simulation**: Test new league detection and draft extraction
- **⚡ Performance optimization**: Current season only (vs. 20+ year scans)
- **🔄 Integration testing**: End-to-end pipeline verification

## 📧 **Support & Monitoring**

- **📋 Issues**: GitHub Issues for bugs/feature requests
- **📧 Notifications**: Pipeline alerts via GitHub → lukesnow2@gmail.com  
- **📊 Monitoring**: GitHub Actions dashboard for pipeline status
- **🧪 Testing**: Year-round testing with `--force` flag

---

**🏆 This is a production-ready, enterprise-grade fantasy football analytics platform with true incremental data processing. The automation handles everything from new league detection to complete dataset maintenance, requiring zero ongoing maintenance.** 