# Database Setup Guide

## Environment Variables

To connect your Next.js application to your PostgreSQL database, you need to set up the following environment variable:

### 1. Create `.env.local` file

In the `web/` directory, create a `.env.local` file with your database connection:

```bash
# PostgreSQL Database Connection
DATABASE_URL="postgresql://username:password@host:port/database"
```

### 2. Example configurations:

**Local PostgreSQL:**
```bash
DATABASE_URL="postgresql://postgres:password@localhost:5432/yahoo_fantasy"
```

**Heroku Postgres:**
```bash
DATABASE_URL="postgresql://username:password@hostname:5432/database_name"
```

**Production (with SSL):**
```bash
DATABASE_URL="postgresql://username:password@hostname:5432/database_name?sslmode=require"
```

## Required Database Tables

The application expects the following PostgreSQL tables to exist:

1. **leagues** - League information by season
2. **teams** - Team data with wins, losses, points
3. **matchups** - Weekly matchup results
4. **transactions** - Trades, waivers, add/drops
5. **draft_picks** - Draft history and picks
6. **rosters** - Player assignments to teams

## Database Schema

The complete database schema is available in `/src/utils/database_schema.py`. The schema includes:

- Primary tables for leagues, teams, matchups, transactions, draft_picks, rosters
- Views for common queries (league_summary, team_performance, etc.)
- Proper indexes for optimal query performance
- Foreign key relationships for data integrity

## Testing Database Connection

You can test your database connection by running:

```bash
# From the web directory
npm run dev
```

Then visit:
- `http://localhost:3000/api/dashboard` - Should return dashboard data
- `http://localhost:3000/api/analytics` - Should return analytics data  
- `http://localhost:3000/api/overview` - Should return overview data

## Data Loading

If you need to populate your database with data, use the existing Python utilities:

```bash
# From the project root
python src/deployment/heroku_deployer.py --data-file your_data.json
```

Or use the incremental loader:

```bash
python src/deployment/incremental_loader.py --data-file your_data.json
```

## API Endpoints

The Next.js application provides these API endpoints:

- `GET /api/dashboard` - Current season standings, highlights, recent activity
- `GET /api/analytics` - Analytics data with metrics and charts
- `GET /api/overview` - Overview stats for landing page

All endpoints return JSON in the format:
```json
{
  "data": { ... },
  "success": true
}
```

Or on error:
```json
{
  "error": "Error message",
  "success": false
}
```

## Troubleshooting

**Connection Issues:**
1. Verify DATABASE_URL is correctly formatted
2. Ensure database server is running and accessible
3. Check firewall settings for database port
4. Verify SSL requirements match your database setup

**No Data Issues:**
1. Confirm tables exist and contain data
2. Check table names match the schema exactly
3. Verify the `current_week` and `season` fields in leagues table
4. Ensure `manager_name` fields are populated in teams table

**Performance Issues:**
1. Database indexes should be created per the schema
2. Consider connection pooling for high traffic
3. Monitor slow queries and optimize as needed 