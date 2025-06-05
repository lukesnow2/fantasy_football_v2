# Fantasy Football Data Pipeline

A **fully automated** Yahoo Fantasy Football data extraction and analysis system with 20+ years of historical data and real-time weekly updates.

## 🏆 **Current Capabilities**

### **📊 Complete Dataset**
- **📅 20 years of data:** 2005-2024 (16,000+ records)
- **🏆 26 leagues:** All historical leagues with complete data
- **🎯 Draft data:** 3,888 draft picks across 25 leagues
- **📈 Real-time updates:** Automated weekly extraction during fantasy season
- **🗄️ Production database:** Live Heroku PostgreSQL deployment

### **🤖 Fully Automated Pipeline**
- **⏰ Scheduled runs:** Every Sunday 6 AM PST (Aug 18 - Jan 18)
- **🔄 Smart scheduling:** Automatic season detection with off-season pause
- **📧 Email alerts:** Success/failure notifications via GitHub
- **🚀 Zero maintenance:** Runs automatically with error recovery

### **🗄️ Advanced Database Features**
- **📋 Complete schema:** 6 tables with relationships and views
- **🔍 Draft analysis:** Advanced views for draft performance tracking
- **📊 Analytics ready:** Pre-built views for common queries
- **🔗 Live deployment:** Heroku PostgreSQL with web interface

## 🚀 **Quick Start**

### **Option 1: Use Existing Data (Recommended)**
```bash
# Clone repository
git clone https://github.com/lukesnow2/fantasy_football_v2.git
cd fantasy_football_v2

# Install dependencies
pip install -r requirements.txt

# Query the live database
python3 query_database.py
```

### **Option 2: Set Up Your Own Pipeline**
```bash
# 1. Configure Yahoo API credentials
cp config.template.json config.json
# Add your Yahoo API credentials

# 2. Authenticate
python3 yahoo_fantasy_oauth_v2.py

# 3. Test extraction
python3 weekly_data_extraction.py

# 4. Set up automation (see Automation section)
```

## 📊 **Data Types & Schema**

### **Core Tables**
1. **`leagues`** - League settings, seasons, draft status (26 records)
2. **`teams`** - Team info, managers, records, standings (260 records)  
3. **`rosters`** - Player assignments by week (8,500+ records)
4. **`matchups`** - Weekly game results and scores (2,700+ records)
5. **`transactions`** - Trades, adds, drops, waivers (1,200+ records)
6. **`draft_picks`** - Complete draft history (3,888 records)

### **Analytics Views**
- **`draft_analysis`** - Draft performance by round, position, cost
- **`team_draft_summary`** - Team-level draft statistics  
- **`player_draft_history`** - Player draft tracking across years
- **`league_standings`** - Complete standings with playoff info
- **`transaction_activity`** - Transaction patterns and trends

## 🤖 **Automation Setup**

### **GitHub Actions (Recommended)**
The pipeline runs automatically via GitHub Actions:

1. **Add GitHub Secrets** (Settings → Secrets → Actions):
   ```
   YAHOO_CLIENT_ID: your_yahoo_client_id
   YAHOO_CLIENT_SECRET: your_yahoo_client_secret  
   YAHOO_REFRESH_TOKEN: your_refresh_token
   HEROKU_DATABASE_URL: your_heroku_database_url
   ```

2. **Enable Notifications**:
   - GitHub Profile → Notifications → Actions (enable email alerts)

3. **Manual Testing**:
   - Actions tab → "Weekly Fantasy Football Data Pipeline" → Run workflow

### **Schedule Details**
- **When:** Every Sunday 6 AM PST 
- **Season:** August 18 - January 18 (automatic season detection)
- **Runtime:** 2-5 minutes for weekly updates
- **Notifications:** Email alerts for success/failure

### **Pipeline Modes**
- **Weekly Mode** (automatic): Current season data only - fast updates
- **Full Mode** (manual): Complete historical + draft data - comprehensive

## 📁 **Project Structure**

```
fantasy_football_v2/
├── 🤖 Automation/
│   ├── .github/workflows/weekly-data-extraction.yml  # GitHub Actions workflow
│   ├── weekly_data_extraction.py                    # Optimized weekly script
│   └── PIPELINE_SETUP.md                           # Automation setup guide
│
├── 📊 Core Data Pipeline/
│   ├── comprehensive_data_extractor.py             # Main extraction engine
│   ├── run_final_complete_extraction.py            # Full historical extraction
│   ├── extract_draft_data.py                       # Draft data processor
│   └── yahoo_fantasy_oauth_v2.py                   # OAuth authentication
│
├── 🗄️ Database Layer/
│   ├── utils/database_schema.py                    # Complete schema + views
│   ├── utils/database_loader.py                    # Data loading utilities
│   ├── utils/yahoo_fantasy_schema.sql              # PostgreSQL schema
│   ├── deploy_to_heroku_postgres.py                # Production deployment
│   └── query_database.py                           # Interactive queries
│
├── 📈 Production Data/
│   ├── yahoo_fantasy_COMPLETE_with_drafts_*.json   # Complete dataset with drafts
│   └── yahoo_fantasy_FINAL_complete_data_*.json    # Historical dataset
│
├── 🧪 Testing & Development/
│   ├── tests/                                      # Test scripts
│   ├── debug/                                      # Development tools
│   └── archive_data/                               # Historical extractions
│
└── 📚 Documentation/
    ├── README.md                                   # This file
    ├── DOCUMENTATION.md                            # Detailed technical docs
    └── PIPELINE_SETUP.md                          # Automation setup guide
```

## 🔧 **Usage Examples**

### **Query Live Database**
```python
# Interactive queries
python3 query_database.py

# Example queries available:
# - League standings with playoff status
# - Draft analysis by position/round
# - Transaction activity patterns
# - Team performance across seasons
# - Player ownership history
```

### **Manual Data Extraction**
```bash
# Current season only (fast)
python3 weekly_data_extraction.py

# Complete historical data
python3 run_final_complete_extraction.py

# Draft data only
python3 extract_draft_data.py
```

### **Database Operations**
```bash
# Deploy to Heroku
python3 deploy_to_heroku_postgres.py

# Load local data
python3 utils/database_loader.py

# Export to CSV
python3 utils/database_loader.py --export-csv
```

## 📈 **Data Quality & Coverage**

### **Historical Coverage**
- **2005-2011:** 60-80% complete (Yahoo API limitations)
- **2012-2019:** 85-95% complete  
- **2020-2024:** 100% complete data
- **Draft Data:** 97% complete (25/26 leagues)

### **Current Stats**
- **📊 16,000+ total records** across all tables
- **🎯 3,888 draft picks** with complete details
- **🏆 343K+ fantasy points** scored historically
- **👥 1,139 unique players** rostered over 20 years
- **💰 FAAB data:** Complete auction budget tracking

## 🌐 **Live Deployment**

The system is deployed to **Heroku** with:
- **PostgreSQL database** with complete schema
- **Automated backups** and monitoring
- **API endpoints** for data access
- **Web interface** for database exploration

**Database URL:** Available via Heroku config (production-ready)

## 🔮 **Upcoming Features**

- **📱 Mobile dashboard** for league insights
- **📊 Advanced analytics** with ML predictions  
- **🔗 Multi-platform support** (ESPN, Sleeper)
- **📈 Real-time scoring** during games
- **🤝 League comparison** tools

## 🛠️ **Technical Stack**

- **Backend:** Python 3.9+, SQLAlchemy, Pandas
- **Database:** PostgreSQL with advanced views
- **API:** Yahoo Fantasy Sports API with OAuth2
- **Automation:** GitHub Actions with smart scheduling
- **Deployment:** Heroku with PostgreSQL addon
- **Monitoring:** Email alerts + GitHub notifications

## 📞 **Support & Contributing**

- **Issues:** GitHub Issues for bug reports
- **Features:** Pull requests welcome
- **Documentation:** Comprehensive guides included
- **Contact:** Built by @lukesnow2

---

## 🏆 **Why This Pipeline?**

1. **🔄 Fully Automated** - Set it and forget it seasonal operation
2. **📊 Complete Data** - 20 years of historical + real-time updates  
3. **🗄️ Production Ready** - Live database with advanced analytics
4. **🤖 Smart Scheduling** - Season-aware automation with error recovery
5. **📧 Reliable Monitoring** - Email alerts for all pipeline activities
6. **🚀 Zero Maintenance** - Runs automatically throughout each season

**Your fantasy football data is now fully automated and production-ready!** 🏆 