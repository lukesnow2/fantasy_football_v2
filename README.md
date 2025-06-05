# 🏈 The League: Enterprise Fantasy Football Data Pipeline

**A fully automated, production-ready system for extracting, processing, and analyzing 20+ years of Yahoo Fantasy Football data.**

[![Automated Pipeline](https://img.shields.io/badge/Pipeline-Automated-brightgreen)](https://github.com/lukesnow-1/the-league/actions)
[![Data Coverage](https://img.shields.io/badge/Data-2004--2025-blue)](#data-coverage)
[![Status](https://img.shields.io/badge/Status-Production-success)](#production-status)

## ✨ **What This Is**

This is an **enterprise-grade data pipeline** that automatically:
- 📊 **Extracts** complete fantasy football data from Yahoo Fantasy API
- 🏈 **Includes draft data** (3,888 picks across 25+ leagues)  
- 🗄️ **Deploys** to live PostgreSQL database on Heroku
- ⚡ **Runs weekly** during fantasy season (Aug 18 - Jan 18)
- 📧 **Sends notifications** on success/failure
- 🔄 **Requires zero maintenance** - fully automated

## 🚀 **Project Structure**

```
the-league/
├── 📁 src/                          # Core source code
│   ├── extractors/                  # Data extraction modules
│   │   ├── comprehensive_data_extractor.py  # Main extraction engine
│   │   ├── weekly_extractor.py             # Optimized weekly updates
│   │   └── draft_extractor.py              # Draft data processing
│   ├── deployment/                  # Database deployment
│   │   └── heroku_deployer.py              # Streamlined Postgres deployer
│   ├── auth/                        # Authentication
│   │   └── yahoo_oauth.py                  # Yahoo API OAuth handler
│   └── utils/                       # Database & utilities
│       ├── database_schema.py              # Database structure
│       ├── database_loader.py              # Data loading utilities
│       ├── query_database.py               # Database queries
│       └── yahoo_fantasy_schema.sql        # PostgreSQL schema
├── 📁 scripts/                      # Entry point scripts
│   ├── weekly_extraction.py         # Weekly automation entry point
│   ├── full_extraction.py           # Complete data extraction
│   └── deploy.py                    # Database deployment
├── 📁 data/                         # Data files
│   ├── current/                     # Active dataset files
│   └── templates/                   # Configuration templates
├── 📁 docs/                         # Documentation
├── 📁 .github/workflows/            # GitHub Actions automation
│   └── weekly-data-extraction.yml   # Weekly pipeline automation
└── 📋 requirements.txt              # Python dependencies
```

## 📊 **Data Coverage**

**Complete 20+ Year Dataset:**
- **26 Fantasy Leagues** (2004-2025)
- **215 Teams** across all seasons
- **10,395 Roster Records** (weekly player assignments)
- **268 Matchups** (head-to-head games)
- **1,256 Transactions** (trades, add/drops, waivers)
- **3,888 Draft Picks** (complete draft history)

**Live Database:** 16,000+ records across 6 normalized tables with advanced analytics views.

## ⚡ **Quick Start**

### **1. Setup Authentication**
```bash
# Copy templates
cp data/templates/config.template.json config.json
cp data/templates/oauth2.template.json oauth2.json

# Add your Yahoo API credentials to config.json and oauth2.json
```

### **2. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **3. Run Extraction**
```bash
# Weekly update (current season only)
python3 scripts/weekly_extraction.py

# Full historical extraction
python3 scripts/full_extraction.py
```

### **4. Deploy to Database**
```bash
export DATABASE_URL="your-postgres-url"
python3 scripts/deploy.py
```

## 🤖 **Automated Pipeline**

**Zero-maintenance automation** via GitHub Actions:

- **📅 Schedule:** Every Sunday 6 AM PST during fantasy season
- **🎯 Smart Season Detection:** Automatically pauses off-season
- **📊 Weekly Updates:** Current rosters, matchups, transactions
- **🏈 Full Extraction:** Complete historical data + draft picks
- **📧 Email Notifications:** Success/failure alerts
- **☁️ Cloud Deployment:** Direct to Heroku PostgreSQL

### **Manual Triggers**
- Force full extraction via GitHub Actions UI
- Emergency runs with custom parameters
- Development testing with dry-run options

## 🗄️ **Database Schema**

**Production PostgreSQL with 6 normalized tables:**

- `leagues` - League configurations and settings
- `teams` - Team information and standings  
- `rosters` - Weekly player assignments
- `matchups` - Head-to-head game results
- `transactions` - All player movements
- `draft_picks` - Complete draft history

**Plus analytics views:**
- `draft_analysis` - Draft performance metrics
- `team_draft_summary` - Team drafting patterns
- `player_draft_history` - Player draft trends

## 📈 **Production Status**

✅ **Fully Operational**
- Live Heroku PostgreSQL database
- Complete 20+ year dataset deployed
- GitHub Actions pipeline active
- Email notifications configured
- Zero maintenance required

🔄 **Next Season Auto-Resume:** August 18, 2025

## 🛠️ **Development**

### **Project Architecture**
- **Modular Design:** Clean separation of extraction, deployment, auth
- **Class-Based:** Object-oriented extractors with elegant interfaces
- **Error Handling:** Comprehensive logging and graceful failure recovery
- **Rate Limiting:** Yahoo API compliance with smart throttling
- **Data Validation:** Type checking and consistency verification

### **Testing**
```bash
# Test core components
python3 -c "from src.extractors.weekly_extractor import WeeklyDataExtractor; print('✅ Extractor loads')"

# Test database deployment
python3 -c "from src.deployment.heroku_deployer import HerokuPostgresDeployer; print('✅ Deployer loads')"
```

## 📧 **Support**

- **Issues:** GitHub Issues for bugs/features
- **Notifications:** Pipeline alerts via GitHub → lukesnow2@gmail.com
- **Monitoring:** GitHub Actions dashboard for pipeline status

---

**🏆 This is a production-ready, enterprise-grade fantasy football analytics platform. The automation is live and requires zero ongoing maintenance.** 