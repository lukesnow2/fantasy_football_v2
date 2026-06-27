# 🏈 The League: Fantasy Football Analytics Platform

A fully automated system that extracts, processes, and analyzes 20+ years of Yahoo Fantasy Football data to provide comprehensive league insights and historical analytics through both API and web interface.

## What This Project Does

This platform automatically:
- **Extracts** complete fantasy football data from Yahoo's API (leagues, rosters, transactions, drafts)
- **Processes** 20+ years of historical data with incremental updates 
- **Analyzes** performance trends, draft patterns, and competitive dynamics
- **Maintains** a Neon (serverless PostgreSQL) database with 70,000+ records across 26 leagues
- **Serves** data through a modern SvelteKit web application with interactive visualizations
- **Runs automatically** via GitHub Actions during fantasy season

## Key Features

- ✅ **Complete Historical Dataset**: 2004-2025 fantasy data across 26 leagues
- ✅ **Automated Pipeline**: Weekly data updates with zero maintenance  
- ✅ **Performance Optimized**: 95% faster than traditional extraction methods
- ✅ **Data Integrity**: Zero duplicates with comprehensive validation
- ✅ **Live Database**: Neon serverless PostgreSQL with analytics views
- ✅ **Modern Web Interface**: SvelteKit frontend with interactive dashboards
- ✅ **Internationalization**: Multi-language support (English/Spanish)
- ✅ **Security Hardened**: Protected credentials and clean git history

## Web Application

The platform includes a comprehensive web frontend built with SvelteKit that provides:

### Features
- **League Overview**: Current standings, manager performance, and season summaries
- **Historical Analytics**: Multi-year trends and performance analysis  
- **Draft Analysis**: Interactive draft boards and pick analysis
- **Trade Dashboard**: Complete trade history and fairness analysis
- **Hall of Fame**: All-time records and achievements
- **Manager Profiles**: Individual performance tracking and statistics
- **Rule Proposals**: Democratic voting system for league rule changes
- **Constitution**: League rules and governance documentation

### Technology Stack
- **Frontend**: SvelteKit with TypeScript and TailwindCSS
- **Database**: PostgreSQL with Drizzle ORM
- **Visualization**: D3.js for interactive charts and graphs
- **Testing**: Playwright (E2E) and Vitest (unit testing)
- **Documentation**: Storybook for component library
- **Deployment**: Vercel-ready with adapter configuration

### Getting Started with Web App
```bash
cd web
npm install
npm run dev
```

The web application connects directly to your PostgreSQL database and provides a beautiful interface for exploring your fantasy football data.

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

### 4. Launch Web Interface
```bash
cd web
npm install
npm run dev
```

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Yahoo API     │───▶│  Extractors  │───▶│   PostgreSQL    │───▶│   SvelteKit     │
│                 │    │              │    │   Database      │    │   Web App       │
└─────────────────┘    └──────────────┘    └─────────────────┘    └─────────────────┘
                              │                       │                       │
                       ┌──────────────┐        ┌─────────────┐        ┌─────────────┐
                       │ GitHub       │        │ Analytics   │        │ Interactive │
                       │ Actions      │        │ Views       │        │ Dashboards  │
                       └──────────────┘        └─────────────┘        └─────────────┘
```

## What's Included

- **Data Extraction**: Automated incremental updates from Yahoo Fantasy API
- **Database Management**: PostgreSQL schema with optimized loading strategies  
- **Analytics Engine**: Pre-built views for league analysis and insights
- **Web Frontend**: Modern SvelteKit application with interactive visualizations
- **API Endpoints**: RESTful API for accessing league data programmatically
- **Automation**: GitHub Actions for scheduled data updates
- **Security**: OAuth authentication with credential protection

## Use Cases

- **League Commissioners**: Track historical performance and league health through web dashboard
- **Fantasy Players**: Analyze draft patterns and trading behaviors via interactive charts
- **Data Analysts**: Rich dataset for fantasy football research with both web and API access
- **Developers**: Example of production data pipeline architecture with modern frontend

## Documentation

- **[Setup Guide](docs/SETUP_GUIDE.md)** - Installation and configuration
- **[Security Notes](SECURITY.md)** - Credential protection guidelines
- **[Web App Documentation](web/README.md)** - Frontend development and deployment

---

**Built for production. Zero maintenance required.** 🏆 