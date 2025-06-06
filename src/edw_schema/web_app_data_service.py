#!/usr/bin/env python3
"""
Fantasy Football Web App Data Service
FastAPI service providing optimized REST endpoints for the EDW
"""

import os
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import pandas as pd
from sqlalchemy import create_engine, text
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for API responses
class SeasonSummary(BaseModel):
    season_year: int
    total_leagues: int
    total_teams: int
    total_games: int
    is_current_season: bool

class LeagueInfo(BaseModel):
    league_key: int
    league_name: str
    season_year: int
    num_teams: int
    league_type: str
    total_points: float
    avg_team_points: float
    competitive_balance_index: float

class TeamStandings(BaseModel):
    team_key: int
    team_name: str
    manager_name: str
    wins: int
    losses: int
    ties: int
    points_for: float
    points_against: float
    win_percentage: float
    season_rank: Optional[int]
    playoff_seed: Optional[int]

class ManagerProfile(BaseModel):
    manager_id: str
    manager_name: str
    total_seasons: int
    championships_won: int
    career_win_percentage: float
    total_points_scored: float
    avg_points_per_season: float
    hall_of_fame_rank: Optional[int]

class PlayerValue(BaseModel):
    player_name: str
    primary_position: str
    season_year: int
    times_drafted: int
    avg_draft_position: float
    total_fantasy_points: float
    draft_value_score: float

class MatchupResult(BaseModel):
    league_name: str
    season_year: int
    week_number: int
    team1_name: str
    team1_points: float
    team2_name: str
    team2_points: float
    winner: str
    point_difference: float
    matchup_type: str

# Database connection class
class EDWConnection:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = None
        self.connect()
    
    def connect(self):
        """Connect to EDW database"""
        try:
            # Fix URL for newer SQLAlchemy
            url = self.database_url.replace('postgres://', 'postgresql://', 1)
            self.engine = create_engine(url, pool_pre_ping=True)
            logger.info("✅ Connected to EDW database")
        except Exception as e:
            logger.error(f"❌ EDW connection failed: {e}")
            raise e
    
    def execute_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

# Initialize FastAPI app
app = FastAPI(
    title="Fantasy Football EDW API",
    description="REST API for Fantasy Football Enterprise Data Warehouse",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database dependency
def get_db():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise HTTPException(status_code=500, detail="DATABASE_URL not configured")
    return EDWConnection(database_url)

# API Endpoints

@app.get("/", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Fantasy Football EDW API",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/seasons", response_model=List[SeasonSummary], tags=["Seasons"])
async def get_seasons(db: EDWConnection = Depends(get_db)):
    """Get all available seasons with summary stats"""
    query = """
        SELECT 
            ds.season_year,
            COUNT(DISTINCT dl.league_key) as total_leagues,
            SUM(dl.num_teams) as total_teams,
            COUNT(DISTINCT fm.matchup_key) as total_games,
            ds.is_current_season
        FROM dim_season ds
        LEFT JOIN dim_league dl ON ds.season_year = dl.season_year
        LEFT JOIN fact_matchup fm ON dl.league_key = fm.league_key
        GROUP BY ds.season_year, ds.is_current_season
        ORDER BY ds.season_year DESC
    """
    
    df = db.execute_query(query)
    return df.to_dict('records')

@app.get("/leagues", response_model=List[LeagueInfo], tags=["Leagues"])
async def get_leagues(
    season_year: Optional[int] = Query(None, description="Filter by season year"),
    db: EDWConnection = Depends(get_db)
):
    """Get leagues with summary statistics"""
    base_query = """
        SELECT 
            dl.league_key,
            dl.league_name,
            dl.season_year,
            dl.num_teams,
            dl.league_type,
            COALESCE(mls.total_points, 0) as total_points,
            COALESCE(mls.avg_team_points, 0) as avg_team_points,
            COALESCE(mls.competitive_balance_index, 0) as competitive_balance_index
        FROM dim_league dl
        LEFT JOIN mart_league_summary mls ON dl.league_key = mls.league_key
        WHERE dl.is_active = true
    """
    
    params = {}
    if season_year:
        base_query += " AND dl.season_year = :season_year"
        params['season_year'] = season_year
    
    base_query += " ORDER BY dl.season_year DESC, dl.league_name"
    
    df = db.execute_query(base_query, params)
    return df.to_dict('records')

@app.get("/leagues/{league_key}/standings", response_model=List[TeamStandings], tags=["Standings"])
async def get_league_standings(
    league_key: int,
    db: EDWConnection = Depends(get_db)
):
    """Get current standings for a specific league"""
    query = """
        SELECT 
            dt.team_key,
            dt.team_name,
            dt.manager_name,
            COALESCE(ftp.wins, 0) as wins,
            COALESCE(ftp.losses, 0) as losses,
            COALESCE(ftp.ties, 0) as ties,
            COALESCE(ftp.points_for, 0) as points_for,
            COALESCE(ftp.points_against, 0) as points_against,
            COALESCE(ftp.win_percentage, 0) as win_percentage,
            ftp.season_rank,
            ftp.playoff_seed
        FROM dim_team dt
        LEFT JOIN fact_team_performance ftp ON dt.team_key = ftp.team_key
        WHERE dt.league_key = :league_key AND dt.is_active = true
        ORDER BY COALESCE(ftp.season_rank, 999), COALESCE(ftp.points_for, 0) DESC
    """
    
    df = db.execute_query(query, {'league_key': league_key})
    if df.empty:
        raise HTTPException(status_code=404, detail="League not found")
    
    return df.to_dict('records')

@app.get("/managers", response_model=List[ManagerProfile], tags=["Managers"])
async def get_manager_hall_of_fame(
    min_seasons: int = Query(3, description="Minimum seasons to qualify"),
    limit: int = Query(50, description="Maximum number of managers to return"),
    db: EDWConnection = Depends(get_db)
):
    """Get manager hall of fame rankings"""
    query = """
        SELECT 
            mmp.manager_id,
            mmp.manager_name,
            mmp.total_seasons,
            mmp.championships_won,
            mmp.career_win_percentage,
            mmp.total_points_scored,
            mmp.avg_points_per_season,
            RANK() OVER (
                ORDER BY mmp.championships_won DESC, 
                         mmp.career_win_percentage DESC
            ) as hall_of_fame_rank
        FROM mart_manager_performance mmp
        WHERE mmp.total_seasons >= :min_seasons
        ORDER BY hall_of_fame_rank
        LIMIT :limit
    """
    
    df = db.execute_query(query, {'min_seasons': min_seasons, 'limit': limit})
    return df.to_dict('records')

@app.get("/managers/{manager_id}", response_model=ManagerProfile, tags=["Managers"])
async def get_manager_profile(
    manager_id: str,
    db: EDWConnection = Depends(get_db)
):
    """Get detailed profile for a specific manager"""
    query = """
        SELECT 
            mmp.manager_id,
            mmp.manager_name,
            mmp.total_seasons,
            mmp.championships_won,
            mmp.career_win_percentage,
            mmp.total_points_scored,
            mmp.avg_points_per_season,
            RANK() OVER (
                ORDER BY mmp.championships_won DESC, 
                         mmp.career_win_percentage DESC
            ) as hall_of_fame_rank
        FROM mart_manager_performance mmp
        WHERE mmp.manager_id = :manager_id
    """
    
    df = db.execute_query(query, {'manager_id': manager_id})
    if df.empty:
        raise HTTPException(status_code=404, detail="Manager not found")
    
    return df.iloc[0].to_dict()

@app.get("/players/breakouts", response_model=List[PlayerValue], tags=["Players"])
async def get_player_breakouts(
    season_year: Optional[int] = Query(None, description="Filter by season year"),
    min_value_score: float = Query(1.3, description="Minimum draft value score"),
    limit: int = Query(100, description="Maximum number of players to return"),
    db: EDWConnection = Depends(get_db)
):
    """Get players with breakout performances"""
    query = """
        SELECT 
            dp.player_name,
            dp.primary_position,
            mpv.season_year,
            mpv.times_drafted,
            mpv.avg_draft_position,
            mpv.total_fantasy_points,
            mpv.draft_value_score
        FROM mart_player_value mpv
        JOIN dim_player dp ON mpv.player_key = dp.player_key
        WHERE mpv.draft_value_score >= :min_value_score
    """
    
    params = {'min_value_score': min_value_score, 'limit': limit}
    
    if season_year:
        query += " AND mpv.season_year = :season_year"
        params['season_year'] = season_year
    
    query += """
        ORDER BY mpv.draft_value_score DESC
        LIMIT :limit
    """
    
    df = db.execute_query(query, params)
    return df.to_dict('records')

@app.get("/matchups", response_model=List[MatchupResult], tags=["Matchups"])
async def get_recent_matchups(
    league_key: Optional[int] = Query(None, description="Filter by league"),
    season_year: Optional[int] = Query(None, description="Filter by season"),
    week_number: Optional[int] = Query(None, description="Filter by week"),
    limit: int = Query(50, description="Maximum number of matchups to return"),
    db: EDWConnection = Depends(get_db)
):
    """Get recent matchup results"""
    query = """
        SELECT 
            dl.league_name,
            dw.season_year,
            dw.week_number,
            dt1.team_name as team1_name,
            fm.team1_points,
            dt2.team_name as team2_name,
            fm.team2_points,
            CASE 
                WHEN fm.winner_team_key = fm.team1_key THEN dt1.team_name
                WHEN fm.winner_team_key = fm.team2_key THEN dt2.team_name
                ELSE 'Tie'
            END as winner,
            fm.point_difference,
            fm.matchup_type
        FROM fact_matchup fm
        JOIN dim_league dl ON fm.league_key = dl.league_key
        JOIN dim_week dw ON fm.week_key = dw.week_key
        JOIN dim_team dt1 ON fm.team1_key = dt1.team_key
        JOIN dim_team dt2 ON fm.team2_key = dt2.team_key
        WHERE 1=1
    """
    
    params = {'limit': limit}
    
    if league_key:
        query += " AND fm.league_key = :league_key"
        params['league_key'] = league_key
    
    if season_year:
        query += " AND fm.season_year = :season_year"
        params['season_year'] = season_year
    
    if week_number:
        query += " AND dw.week_number = :week_number"
        params['week_number'] = week_number
    
    query += """
        ORDER BY dw.season_year DESC, dw.week_number DESC, fm.total_points DESC
        LIMIT :limit
    """
    
    df = db.execute_query(query, params)
    return df.to_dict('records')

@app.get("/analytics/league-competitiveness", tags=["Analytics"])
async def get_league_competitiveness(
    season_year: Optional[int] = Query(None, description="Filter by season year"),
    db: EDWConnection = Depends(get_db)
):
    """Get league competitiveness analysis"""
    query = """
        SELECT 
            mls.league_name,
            mls.season_year,
            mls.competitive_balance_index,
            mls.avg_margin_of_victory,
            mls.close_games_count,
            mls.blowout_games_count,
            mls.total_transactions,
            mls.waiver_activity_index,
            CASE 
                WHEN mls.competitive_balance_index < 0.15 THEN 'Highly Competitive'
                WHEN mls.competitive_balance_index < 0.25 THEN 'Competitive'
                WHEN mls.competitive_balance_index < 0.35 THEN 'Moderately Competitive'
                ELSE 'Low Competition'
            END as competitiveness_tier
        FROM mart_league_summary mls
        WHERE 1=1
    """
    
    params = {}
    if season_year:
        query += " AND mls.season_year = :season_year"
        params['season_year'] = season_year
    
    query += " ORDER BY mls.competitive_balance_index ASC"
    
    df = db.execute_query(query, params)
    return {"leagues": df.to_dict('records')}

@app.get("/analytics/power-rankings/{league_key}", tags=["Analytics"])
async def get_power_rankings(
    league_key: int,
    week_number: Optional[int] = Query(None, description="Specific week (defaults to latest)"),
    db: EDWConnection = Depends(get_db)
):
    """Get power rankings for a league"""
    query = """
        SELECT 
            dt.team_name,
            dt.manager_name,
            mwpr.power_rank,
            mwpr.power_score,
            mwpr.recent_form_score,
            mwpr.strength_of_schedule,
            mwpr.playoff_odds,
            mwpr.rank_change,
            dw.week_number
        FROM mart_weekly_power_rankings mwpr
        JOIN dim_team dt ON mwpr.team_key = dt.team_key
        JOIN dim_week dw ON mwpr.week_key = dw.week_key
        WHERE mwpr.league_key = :league_key
    """
    
    params = {'league_key': league_key}
    
    if week_number:
        query += " AND dw.week_number = :week_number"
        params['week_number'] = week_number
    else:
        # Get latest week
        query += """
            AND dw.week_number = (
                SELECT MAX(dw2.week_number) 
                FROM mart_weekly_power_rankings mwpr2
                JOIN dim_week dw2 ON mwpr2.week_key = dw2.week_key
                WHERE mwpr2.league_key = :league_key
            )
        """
    
    query += " ORDER BY mwpr.power_rank"
    
    df = db.execute_query(query, params)
    if df.empty:
        raise HTTPException(status_code=404, detail="No power rankings found for this league")
    
    return {"rankings": df.to_dict('records')}

@app.get("/analytics/draft-analysis", tags=["Analytics"])
async def get_draft_analysis(
    league_key: Optional[int] = Query(None, description="Filter by league"),
    season_year: Optional[int] = Query(None, description="Filter by season"),
    position: Optional[str] = Query(None, description="Filter by position"),
    db: EDWConnection = Depends(get_db)
):
    """Get draft analysis and value picks"""
    query = """
        SELECT 
            dl.league_name,
            fd.season_year,
            dp.player_name,
            dp.primary_position,
            dt.team_name,
            dt.manager_name,
            fd.overall_pick,
            fd.round_number,
            fd.draft_cost,
            fd.season_points,
            fd.draft_grade,
            CASE 
                WHEN fd.season_points > 0 AND fd.overall_pick > 0 
                THEN fd.season_points / fd.overall_pick 
                ELSE 0 
            END as value_per_pick
        FROM fact_draft fd
        JOIN dim_league dl ON fd.league_key = dl.league_key
        JOIN dim_player dp ON fd.player_key = dp.player_key
        JOIN dim_team dt ON fd.team_key = dt.team_key
        WHERE 1=1
    """
    
    params = {}
    
    if league_key:
        query += " AND fd.league_key = :league_key"
        params['league_key'] = league_key
    
    if season_year:
        query += " AND fd.season_year = :season_year"
        params['season_year'] = season_year
    
    if position:
        query += " AND dp.primary_position = :position"
        params['position'] = position
    
    query += """
        ORDER BY fd.season_year DESC, fd.overall_pick ASC
        LIMIT 200
    """
    
    df = db.execute_query(query, params)
    return {"draft_picks": df.to_dict('records')}

# Dashboard endpoint that combines multiple data sources
@app.get("/dashboard/current-season", tags=["Dashboard"])
async def get_current_season_dashboard(db: EDWConnection = Depends(get_db)):
    """Get comprehensive current season dashboard data"""
    current_year = datetime.now().year
    
    # Get leagues for current season
    leagues_query = """
        SELECT 
            league_name,
            num_teams,
            league_type
        FROM dim_league 
        WHERE season_year = :current_year AND is_active = true
        ORDER BY league_name
    """
    
    leagues_df = db.execute_query(leagues_query, {'current_year': current_year})
    
    return {
        "leagues": leagues_df.to_dict('records'),
        "generated_at": datetime.now().isoformat()
    }

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return {
        "error": {
            "status_code": exc.status_code,
            "detail": exc.detail,
            "timestamp": datetime.now().isoformat()
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 