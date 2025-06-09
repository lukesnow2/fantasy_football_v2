# EDW Deployment Verification Checklist

## Pre-Deployment Checklist

- [ ] Database URL configured and accessible
- [ ] Operational database contains required data:
  - [ ] Leagues table populated
  - [ ] Teams table populated  
  - [ ] Matchups table populated
  - [ ] Transactions table populated
  - [ ] Draft picks table populated
- [ ] Python dependencies installed
- [ ] Sufficient database storage available

## Deployment Verification

### ✅ Required Record Counts
Run: `python3 verify_edw_data.py`

- [ ] **Leagues: 20** (exactly, no more, no less)
- [ ] **Seasons: 20** (2005-2024)
- [ ] **Teams: ~196** (only from leagues of record)
- [ ] **Players: ~1,425** (extracted from transactions/drafts)
- [ ] **Weeks: ~324** (17 weeks × 20 seasons)
- [ ] **Matchups: ~1,499** (historical completeness)
- [ ] **Transactions: ~9,691** (all transaction history)
- [ ] **Draft Picks: ~3,192** (all draft data)
- [ ] **Team Performance: ~2,998** (weekly stats)

### ✅ Data Quality Checks

- [ ] **No null league names** in dim_league
- [ ] **No null team names** in dim_team  
- [ ] **No teams without matchups** (should be 0)
- [ ] **Reasonable matchup scores** (no widespread 0-0 games)
- [ ] **League distribution**: 1 league per season (2005-2024)

### ✅ League of Record Verification

Verify these 20 leagues exist in EDW:
- [ ] 2024: Idaho's DEI Quota
- [ ] 2023: Move the Raiders to PDX
- [ ] 2022: Wet Hot Tahoe Summer
- [ ] 2021: Rocky Mountain High
- [ ] 2020: The Lost Year
- [ ] 2019: Women & Women First
- [ ] 2018: Sleepless In Seattle
- [ ] 2017: Go Fuck Yourself San Diego
- [ ] 2016: The Great SF Draft
- [ ] 2015: Luke's Kingdom
- [ ] 2014: 10 Years 10 Assholes
- [ ] 2013: Rosterbaters Anonymous
- [ ] 2012: The League About Nothing
- [ ] 2011: Lock It Up
- [ ] 2010: Round 6
- [ ] 2009: Engaged
- [ ] 2008: The Draft
- [ ] 2007: Oakdale Park
- [ ] 2006: Oakdale Park
- [ ] 2005: Oakdale Park

### ✅ Analytical Views Working

- [ ] `vw_current_season_dashboard` - Returns data
- [ ] `vw_manager_hall_of_fame` - Returns data
- [ ] `vw_league_competitiveness` - Returns data  
- [ ] `vw_player_breakout_analysis` - Returns data
- [ ] `vw_trade_analysis` - Returns data

### ✅ Performance Verification

- [ ] Indexes created successfully
- [ ] Query performance acceptable (<5 seconds for basic queries)
- [ ] No missing foreign key relationships

## Post-Deployment Validation

### Sample Queries
Run these to verify data integrity:

```sql
-- League count (should be exactly 20)
SELECT COUNT(*) FROM edw.dim_league;

-- Season distribution (should be 1 per year)
SELECT season_year, COUNT(*) 
FROM edw.dim_league 
GROUP BY season_year 
ORDER BY season_year;

-- Recent matchups (should show current season data)
SELECT dl.league_name, dw.week_number, fm.team1_points, fm.team2_points
FROM edw.fact_matchup fm
JOIN edw.dim_league dl ON fm.league_key = dl.league_key
JOIN edw.dim_week dw ON fm.week_key = dw.week_key
WHERE dl.season_year = 2024
ORDER BY dw.week_number DESC
LIMIT 10;

-- Transaction activity by type
SELECT transaction_type, COUNT(*) 
FROM edw.fact_transaction 
GROUP BY transaction_type 
ORDER BY COUNT(*) DESC;
```

## Troubleshooting Quick Reference

| Issue | Likely Cause | Solution |
|-------|--------------|----------|
| Wrong league count | League filtering bug | Check `HISTORICAL_LEAGUE_IDS` |
| Missing matchups | Week dimension issue | Verify `extract_weeks()` logic |
| Zero fact records | ETL filtering error | Check league of record logic |
| Performance issues | Missing indexes | Re-run schema deployment |
| View errors | Dependency issues | Check view creation order |

## Sign-off

**Deployment Date:** _______________  
**Deployed By:** _______________  
**Verified By:** _______________  

**Overall Status:** 
- [ ] ✅ PASSED - Ready for production use
- [ ] ❌ FAILED - Issues require resolution

**Notes:**
_________________________________  
_________________________________  
_________________________________ 