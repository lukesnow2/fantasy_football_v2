# 🏈 The League: Fantasy Football Data Pipeline

A fully automated, enterprise-grade system for incremental extraction and analysis of 20+ years of Yahoo Fantasy Football data.

[![Status](https://img.shields.io/badge/Status-Production-success)](docs/PROJECT_STATUS.md)
[![Pipeline](https://img.shields.io/badge/Pipeline-Automated-brightgreen)](https://github.com/lukesnow-1/the-league/actions)
[![Data Coverage](https://img.shields.io/badge/Data-2004--2025-blue)](#data-coverage)

## 🚀 What This Is

An **enterprise-grade incremental data pipeline** that:
- 📈 **Incrementally extracts** only new fantasy football data (95% faster)
- 🏈 **Auto-detects** new leagues and extracts complete draft data  
- 🗄️ **Deploys** with hybrid loading strategies and zero-duplicate guarantees
- ⚡ **Runs weekly** during fantasy season via GitHub Actions automation
- 📧 **Sends notifications** on success/failure
- 🔐 **Security hardened** with comprehensive credential protection

## 📊 Complete Dataset

**Current Production Data:**
- **26 Fantasy Leagues** (2004-2025)  
- **16,000+ Records** across 6 normalized tables
- **100% Data Integrity** - zero duplicates detected
- **Live PostgreSQL Database** with advanced analytics views

## ⚡ Quick Start

### 1. Setup Authentication
```bash
# Copy template and add your Yahoo API credentials
cp data/templates/config.template.json config.json
# oauth2.json will be created automatically during first run
```

### 2. Install & Test
```bash
pip install -r requirements.txt

# Test incremental extraction (works year-round)
python3 scripts/weekly_extraction.py --force
```

### 3. Deploy Database
```bash
export DATABASE_URL="your-postgres-url"
python3 src/deployment/incremental_loader.py --data-file data/current/data.json
```

## 🤖 Automated Production

**Zero-maintenance automation** via GitHub Actions:
- **📅 Scheduled**: Every Sunday 6 AM PST (Aug 18 - Jan 18)
- **⚡ Incremental**: Only extracts new data since last run
- **🚀 Fast**: 2-5 minutes vs. hours for full extraction
- **📧 Monitored**: Email alerts for success/failure

## 🏗️ Architecture

```
📁 src/                    # Core modules
├── extractors/           # Data extraction (incremental + historical)
├── deployment/           # Database deployment with hybrid loading  
├── auth/                # OAuth authentication
└── utils/               # Database schema & utilities

📁 scripts/               # Entry points
├── weekly_extraction.py  # Primary incremental system
├── deploy.py            # Database deployment
└── duplicate_detector.py # Data integrity validation
```

## 🔥 Key Features

### Incremental Processing
- **95% performance improvement** over full extraction
- **Smart baseline loading** with historical data
- **New league detection** and automatic draft extraction
- **Current season focus** instead of 20+ year scans

### Hybrid Loading System
- **Table-specific strategies**: UPSERT, INCREMENTAL_APPEND, APPEND_ONLY
- **Zero-duplicate guarantees** with comprehensive validation
- **Optimized performance** with minimal database I/O

### Enterprise Security
- **Git history cleaned** of all sensitive data using BFG Repo-Cleaner
- **Template-based setup** prevents credential commits
- **OAuth token management** with automatic refresh

## 📚 Documentation

- **[Technical Guide](docs/TECHNICAL_GUIDE.md)** - Comprehensive system documentation
- **[Deployment Guide](docs/DEPLOYMENT_GUIDE.md)** - Setup and automation procedures  
- **[Project Status](docs/PROJECT_STATUS.md)** - Current operational status
- **[Security Guidelines](SECURITY.md)** - Credential protection and best practices

## 📈 Production Status

✅ **Fully Operational Enterprise System**
- 🔥 **Incremental extraction**: Live and optimized
- 🛡️ **Hybrid loading**: Zero-duplicate guarantees  
- 🗄️ **Live PostgreSQL**: Complete dataset with 100% integrity
- 🤖 **GitHub Actions**: Active automation with monitoring
- 🔐 **Security hardened**: Credentials protected, git history cleaned

---

**Built for production. Zero maintenance required.** 🏆 