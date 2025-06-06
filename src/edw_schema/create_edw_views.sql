-- ============================================================================
-- EDW VIEWS FOR FANTASY FOOTBALL DATA WAREHOUSE
-- ============================================================================
-- This file creates optimized views for the EDW schema that are used
-- by web applications and dashboards.
-- ============================================================================

-- Drop existing views if they exist
DROP VIEW IF EXISTS edw.vw_current_season_dashboard CASCADE;
DROP VIEW IF EXISTS edw.vw_manager_hall_of_fame CASCADE;
DROP VIEW IF EXISTS edw.vw_league_competitiveness CASCADE;
DROP VIEW IF EXISTS edw.vw_player_breakout_analysis CASCADE;
DROP VIEW IF EXISTS edw.vw_trade_analysis CASCADE;

-- ============================================================================
-- VIEW 1: Current Season Dashboard
-- ============================================================================
-- Provides current season team performance for dashboard display

CREATE VIEW edw.vw_current_season_dashboard AS
SELECT 
    dl.league_name,
    dl.season_year,
    dt.team_name,
    dt.manager_name,
    ftp.wins,
    ftp.losses,
    ftp.ties,
    ftp.points_for,
    ftp.points_against,
    ftp.point_differential,
    ftp.win_percentage,
    ftp.season_rank,
    ftp.playoff_probability,
    ftp.is_playoff_team,
    ftp.playoff_seed
FROM edw.fact_team_performance ftp
JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
WHERE dl.season_year = EXTRACT(YEAR FROM CURRENT_DATE)
  AND dw.is_current_week = TRUE
  AND dt.is_active = TRUE
ORDER BY dl.league_name, ftp.season_rank;

-- ============================================================================
-- VIEW 2: Manager Hall of Fame
-- ============================================================================
-- Ranks managers by career performance across all leagues

CREATE VIEW edw.vw_manager_hall_of_fame AS
SELECT 
    manager_name,
    total_seasons,
    championships_won,
    career_win_percentage,
    total_points_scored,
    avg_points_per_season,
    playoff_appearances,
    season_consistency_score,
    RANK() OVER (ORDER BY championships_won DESC, career_win_percentage DESC) as hall_of_fame_rank
FROM edw.mart_manager_performance
WHERE total_seasons >= 3 -- Minimum 3 seasons to qualify
ORDER BY hall_of_fame_rank;

-- ============================================================================
-- VIEW 3: League Competitiveness Analysis
-- ============================================================================
-- Analyzes how competitive each league is based on various metrics

CREATE VIEW edw.vw_league_competitiveness AS
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
FROM edw.mart_league_summary mls
ORDER BY mls.competitive_balance_index ASC;

-- ============================================================================
-- VIEW 4: Player Breakout Analysis
-- ============================================================================
-- Identifies players who significantly outperformed their draft position

CREATE VIEW edw.vw_player_breakout_analysis AS
SELECT 
    dp.player_name,
    dp.primary_position,
    mpv.season_year,
    mpv.avg_draft_position,
    mpv.total_fantasy_points,
    mpv.draft_value_score,
    mpv.waiver_pickup_value,
    CASE 
        WHEN mpv.avg_draft_position > 100 AND mpv.draft_value_score > 2.0 THEN 'Major Breakout'
        WHEN mpv.avg_draft_position > 50 AND mpv.draft_value_score > 1.5 THEN 'Solid Breakout'
        WHEN mpv.waiver_pickup_value > 1.5 THEN 'Waiver Wire Gem'
        ELSE 'Standard Performance'
    END as breakout_type
FROM edw.mart_player_value mpv
JOIN edw.dim_player dp ON mpv.player_key = dp.player_key
WHERE mpv.draft_value_score > 1.3 OR mpv.waiver_pickup_value > 1.3
ORDER BY mpv.draft_value_score DESC;

-- ============================================================================
-- VIEW 5: Trade Analysis
-- ============================================================================
-- Provides detailed trade history and analysis

CREATE VIEW edw.vw_trade_analysis AS
SELECT 
    dl.league_name,
    dl.season_year,
    ft.transaction_date,
    dp.player_name,
    dt1.team_name as from_team,
    dt1.manager_name as from_manager,
    dt2.team_name as to_team,
    dt2.manager_name as to_manager,
    ft.trade_group_id,
    -- Could add win-win analysis, value comparison, etc.
    COUNT(*) OVER (PARTITION BY ft.trade_group_id) as players_in_trade
FROM edw.fact_transaction ft
JOIN edw.dim_league dl ON ft.league_key = dl.league_key
JOIN edw.dim_player dp ON ft.player_key = dp.player_key
JOIN edw.dim_team dt1 ON ft.from_team_key = dt1.team_key
JOIN edw.dim_team dt2 ON ft.to_team_key = dt2.team_key
WHERE ft.transaction_type = 'trade'
ORDER BY ft.transaction_date DESC;

-- ============================================================================
-- COMMENTS AND DOCUMENTATION
-- ============================================================================

COMMENT ON VIEW edw.vw_current_season_dashboard IS 'Current season team performance for dashboard display';
COMMENT ON VIEW edw.vw_manager_hall_of_fame IS 'Manager career performance rankings across all leagues';
COMMENT ON VIEW edw.vw_league_competitiveness IS 'League competitiveness analysis with tier classifications';
COMMENT ON VIEW edw.vw_player_breakout_analysis IS 'Player breakout identification based on draft position vs performance';
COMMENT ON VIEW edw.vw_trade_analysis IS 'Detailed trade history and analysis with participant information';

-- ============================================================================
-- END OF EDW VIEWS
-- ============================================================================ 