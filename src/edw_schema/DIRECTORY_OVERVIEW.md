# EDW Schema Directory Overview

This directory contains the production-ready Fantasy Football EDW deployment system.

## 🏗️ Core Files

### **Deployment & Operations**
- **`deploy_complete_edw.py`** - Main deployment script with enhanced verification
- **`deploy.sh`** - Shell wrapper for easy deployment commands
- **`edw_etl_processor.py`** - Core ETL processor with league of record filtering

### **Schema & Configuration**
- **`fantasy_edw_schema.sql`** - Complete EDW schema definition

### **Documentation**
- **`README_EDW_DEPLOYMENT.md`** - Complete deployment guide
- **`DEPLOYMENT_SYSTEM_OVERVIEW.md`** - Executive summary and architecture
- **`EDW_VERIFICATION_CHECKLIST.md`** - Step-by-step verification guide

## 🚀 Quick Start

```bash
# Deploy complete EDW
./deploy.sh deploy

# Rebuild from scratch  
./deploy.sh rebuild

# Verify only
./deploy.sh verify
```

## 🎯 Key Features

- ✅ **Automatic season rollover** using `fact_draft` data
- ✅ **League of record filtering** (20 historical leagues 2005-2024)
- ✅ **Enhanced verification** with data quality thresholds
- ✅ **Automatic view fixing** during deployment
- ✅ **Production-ready** error handling and logging

## 📊 Data Pipeline

**Operational DB** → **ETL Processor** → **EDW** → **Analytics**

The system automatically filters to your 20 core leagues and will include future leagues starting from 2025. 