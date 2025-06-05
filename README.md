# 🏈 The League: Enterprise Fantasy Football Data Pipeline

**A fully automated, production-ready system for incremental extraction and analysis of 20+ years of Yahoo Fantasy Football data.**

[![Automated Pipeline](https://img.shields.io/badge/Pipeline-Automated-brightgreen)](https://github.com/lukesnow-1/the-league/actions)
[![Data Coverage](https://img.shields.io/badge/Data-2004--2025-blue)](#data-coverage)
[![Status](https://img.shields.io/badge/Status-Production-success)](#production-status)
[![Extraction](https://img.shields.io/badge/Extraction-Incremental-orange)](#incremental-system)
[![Security](https://img.shields.io/badge/Security-Hardened-green)](#security)

## ✨ **What This Is**

This is an **enterprise-grade incremental data pipeline** that automatically:
- 📈 **Incrementally extracts** only new fantasy football data since last run (95% faster)
- 🏈 **Auto-detects** new leagues and extracts their complete draft data  
- 🗄️ **Deploys** with hybrid loading strategies and zero-duplicate guarantees
- ⚡ **Runs weekly** during fantasy season (Aug 18 - Jan 18) via GitHub Actions
- 📧 **Sends notifications** on success/failure via email
- 🔄 **Requires zero maintenance** - fully automated incremental updates
- 🧪 **Testable year-round** with `--force` flag for off-season development
- 🔐 **Security hardened** with comprehensive credential protection

## 🚀 **Professional Project Structure**

```
the-league/
├── 📁 src/                          # Core source code (modular design)
│   ├── extractors/                  # Data extraction modules
│   │   ├── comprehensive_data_extractor.py  # Historical extraction engine
│   │   ├── weekly_extractor.py             # 🔥 Incremental production system
│   │   └── draft_extractor.py              # Specialized draft processing
│   ├── deployment/                  # Database deployment with hybrid loading
│   │   ├── heroku_deployer.py              # Streamlined Postgres deployer
│   │   └── incremental_loader.py           # 🔥 Hybrid merge strategies
│   ├── auth/                        # Secure authentication
│   │   └── yahoo_oauth.py                  # Yahoo API OAuth handler
│   └── utils/                       # Database & utilities
│       ├── database_schema.py              # Database structure definitions
│       ├── database_loader.py              # Data loading utilities
│       ├── query_database.py               # Database query helpers
│       └── yahoo_fantasy_schema.sql        # Complete PostgreSQL schema
├── 📁 scripts/                      # Clean entry point scripts
│   ├── weekly_extraction.py         # 🔥 Primary incremental extraction
│   ├── full_extraction.py           # Historical extraction (completed)
│   ├── deploy.py                    # Database deployment
│   ├── duplicate_detector.py        # 🛡️ Comprehensive duplicate detection
│   └── analyze_data_structure.py    # Data structure analysis
├── 📁 data/                         # Organized data storage
│   ├── current/                     # Active dataset files (16,000+ records)
│   └── templates/                   # Configuration templates (SECURE)
├── 📁 docs/                         # Comprehensive documentation
├── 📁 .github/workflows/            # GitHub Actions automation
│   └── weekly-data-extraction.yml   # Incremental pipeline automation
├── 📋 SECURITY_NOTES.md             # 🔐 Critical security guidelines
├── 📋 INCREMENTAL_LOADING_SUMMARY.md # Complete loading system docs
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

### **Hybrid Loading System** 🆕
- **UPSERT Strategy**: Leagues & Teams (updates existing, inserts new)
- **INCREMENTAL_APPEND**: Rosters & Matchups (delete current week, append fresh)
- **APPEND_ONLY**: Transactions & Drafts (skip existing, append new only)
- **Zero Duplicates**: Multi-level detection with 100% data integrity

### **Production Efficiency**
- **⚡ 95% faster**: Seconds vs. minutes (current season only)
- **🎯 Precise targeting**: Only extracts what's actually new
- **📊 Complete output**: Always maintains full historical + current dataset
- **🔄 Zero data loss**: Preserves all historical data while adding new
- **🛡️ Duplicate prevention**: Comprehensive validation and alerting

## 📊 **Complete Dataset**

**20+ Year Historical Foundation:**
- **26 Fantasy Leagues** (2004-2025)
- **215 Teams** across all seasons
- **10,395 Roster Records** (weekly player assignments)
- **268 Matchups** (head-to-head games)
- **1,256 Transactions** (trades, add/drops, waivers)
- **3,888 Draft Picks** (complete draft history)

**Live Database:** 16,000+ records across 6 normalized tables with advanced analytics views.

**Data Integrity:** ✅ **ZERO duplicates** detected across all tables with comprehensive validation.

## ⚡ **Quick Start**

### **1. Setup Authentication (SECURE)**
```bash
# Copy templates (NEVER commit real credentials)
cp data/templates/config.template.json config.json

# Add your Yahoo API credentials to config.json
# oauth2.json will be created automatically during first run
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

### **4. Deploy with Hybrid Loading**
```bash
export DATABASE_URL="your-postgres-url"

# Deploy with incremental loading strategies
python3 src/deployment/incremental_loader.py --data-file data/current/data.json

# Or use legacy deployment
python3 scripts/deploy.py
```

### **5. Validate Data Integrity**
```bash
# Check for duplicates in live database
python3 scripts/duplicate_detector.py --alert-only

# Analyze data structure
python3 scripts/analyze_data_structure.py --data-file data/current/data.json
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
- **🛡️ Duplicate validation**: Pre and post-load integrity checks

### **Reliable Deployment**
- **☁️ Hybrid loading to PostgreSQL**: Table-specific merge strategies
- **📧 Email notifications**: Success/failure alerts via GitHub notifications
- **🔄 Error recovery**: Graceful handling and detailed logging
- **🚨 Duplicate alerting**: Critical/High severity notifications

## 🗄️ **Database Schema & Loading**

**Production PostgreSQL with 6 normalized tables:**

- **`leagues`** - League configurations (UPSERT strategy)
- **`teams`** - Team information and standings (UPSERT strategy)
- **`rosters`** - Weekly player assignments (INCREMENTAL_APPEND strategy)
- **`matchups`** - Head-to-head game results (INCREMENTAL_APPEND strategy)
- **`transactions`** - Player movements (APPEND_ONLY strategy)
- **`draft_picks`** - Complete draft history (APPEND_ONLY strategy)

**Advanced Analytics Views:**
- **`draft_analysis`** - Draft performance metrics and trends
- **`team_draft_summary`** - Team drafting patterns and success
- **`player_draft_history`** - Player draft trends across seasons

**Loading Strategies:**
- **Zero duplicates guaranteed** through multi-level validation
- **Table-optimized performance** with hybrid merge strategies
- **Complete data integrity** with comprehensive checks

## 🔐 **Security**

### **Credentials Protection**
- ✅ **Never commit secrets**: Enhanced `.gitignore` protection
- ✅ **Template-based setup**: Safe configuration management
- ✅ **Git history cleaned**: Sensitive data completely removed
- ✅ **Security documentation**: Comprehensive guidelines in `SECURITY_NOTES.md`

### **Best Practices**
- 🔒 OAuth tokens automatically managed and refreshed
- 🛡️ Environment variables for production deployments
- 📋 Regular credential rotation recommendations
- 🚨 Security incident response procedures

## 📈 **Production Status**

✅ **Fully Operational - Complete Enterprise System**
- **🔥 Incremental extraction**: Live and optimized (95% performance gain)
- **🛡️ Hybrid loading system**: Zero-duplicate guarantees with optimal performance
- **🗄️ Live Heroku PostgreSQL**: Complete 20+ year dataset with 100% integrity
- **🤖 GitHub Actions pipeline**: Active incremental automation
- **📧 Email notifications**: Configured and functional
- **🧪 Year-round testing**: `--force` flag for off-season development
- **🔄 Zero maintenance**: Fully automated incremental updates
- **🔐 Security hardened**: Credentials protected, git history cleaned

**🎯 Next Season Auto-Resume:** August 18, 2025

## 🛠️ **Development & Testing**

### **Production Architecture**
- **🏗️ Modular design**: Clean separation of extraction, deployment, auth
- **📈 Incremental system**: Smart baseline loading and incremental updates  
- **🔄 Error handling**: Comprehensive logging and graceful failure recovery
- **⚡ Rate limiting**: Yahoo API compliance with smart throttling
- **✅ Data validation**: Type checking and consistency verification
- **🛡️ Duplicate prevention**: Multi-level detection and alerting

### **Testing Capabilities**
```bash
# Test incremental extraction during off-season
python3 scripts/weekly_extraction.py --force

# Test core components
python3 -c "from src.extractors.weekly_extractor import IncrementalDataExtractor; print('✅')"
python3 -c "from src.deployment.incremental_loader import IncrementalLoader; print('✅')"
python3 -c "from src.deployment.heroku_deployer import HerokuPostgresDeployer; print('✅')"

# Test duplicate detection
python3 scripts/duplicate_detector.py --data-files data/current/*.json --alert-only

# Test data structure analysis
python3 scripts/analyze_data_structure.py --data-file data/current/data.json
```

### **Incremental Development**
- **📊 Baseline testing**: Uses existing complete dataset as foundation
- **🆕 New league simulation**: Test new league detection and draft extraction
- **⚡ Performance optimization**: Current season only (vs. 20+ year scans)
- **🔄 Integration testing**: End-to-end pipeline verification
- **🛡️ Integrity validation**: Comprehensive duplicate detection and prevention

## 📧 **Support & Monitoring**

- **📋 Issues**: GitHub Issues for bugs/feature requests
- **📧 Notifications**: Pipeline alerts via GitHub notifications  
- **📊 Monitoring**: GitHub Actions dashboard for pipeline status
- **🧪 Testing**: Year-round testing with `--force` flag
- **🔐 Security**: Comprehensive security guidelines and incident response
- **🛡️ Data integrity**: Real-time duplicate detection and alerting

## 📋 **Documentation**

- **📖 Main README**: This file - complete system overview
- **📊 Project Status**: `docs/PROJECT_STATUS.md` - detailed operational status
- **🔧 Pipeline Setup**: `docs/PIPELINE_SETUP.md` - technical implementation details
- **📚 Documentation**: `docs/DOCUMENTATION.md` - comprehensive technical docs
- **🔐 Security**: `SECURITY_NOTES.md` - critical security guidelines
- **🔄 Incremental Loading**: `INCREMENTAL_LOADING_SUMMARY.md` - complete loading system

---

**The League represents a complete enterprise-grade data pipeline with incremental processing, hybrid loading strategies, comprehensive duplicate prevention, and security hardening - delivering production-ready fantasy football analytics with zero maintenance requirements.** 