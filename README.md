# 🏈 The League: Fantasy Football Analytics Platform

A fully automated system that extracts, processes, and analyzes 20+ years of Yahoo Fantasy Football data to provide comprehensive league insights and historical analytics through both API and web interface.

## What This Project Does

This platform automatically:
- **Extracts** complete fantasy football data from Yahoo's API (leagues, rosters, transactions, drafts)
- **Processes** 20+ years of historical data with incremental updates 
- **Analyzes** performance trends, draft patterns, and competitive dynamics
- **Maintains** a Neon (serverless PostgreSQL) database with 70,000+ records across 26 leagues
- **Serves** data through a modern SvelteKit web application with interactive visualizations
- **Runs weekly** via GitHub Actions in-season (Wednesdays), with an external dead-man check

## Key Features

- **Complete Historical Dataset**: 2005–2025 fantasy data, one league of record per season
- **Incremental Pipeline**: `scripts/incremental_load.py` computes the gap between
  what the warehouse has verified and what Yahoo has completed, and loads the
  difference — the same mechanism serves weekly updates, backfills, and repairs
- **Database-Enforced Safety**: advisory-lock concurrency, scoped deletes,
  unique-index idempotency, snapshot-guarded EDW publication with atomic restore
- **Live Database**: Neon serverless PostgreSQL with analytics views
- **Modern Web Interface**: SvelteKit frontend reading `edw.*` live (fresh data
  needs no redeploy)
- **Internationalization**: Multi-language support (English/Spanish)
- **Sanitized Snapshots**: tracked data snapshots pass a privacy audit and an
  ETL-equivalence proof before commit (`scripts/sanitize_snapshot.py`)

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
export DATABASE_URL="your-postgres-url"
python3 scripts/incremental_load.py --dry-run   # shows the computed gap
```

### 3. Load the Database
```bash
python3 scripts/incremental_load.py             # loads the gap
```
For a full rebuild from the tracked baseline snapshot, see RUNBOOK.md.

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

**Built for production.** Operations live in RUNBOOK.md; the pipeline is self-healing but monitored, not maintenance-free. 