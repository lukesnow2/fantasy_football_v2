-- Fix Hall of Fame View with Complete Definition
-- This includes all missing columns: career_wins, career_losses, career_ties, hall_of_fame_index, avg_points_per_game

DROP VIEW IF EXISTS edw.vw_manager_hall_of_fame;

CREATE VIEW edw.vw_manager_hall_of_fame AS
WITH manager_championships AS (
    -- Count championships from mart_league_summary
    SELECT 
        champion_manager as manager_name,
        COUNT(*) as championships_won
    FROM edw.mart_league_summary
    WHERE champion_manager IS NOT NULL
    GROUP BY champion_manager
),
manager_stats AS (
    SELECT 
        dt.manager_name,
        COUNT(DISTINCT dl.season_year) as total_seasons,
        SUM(ftp.wins) as career_wins,
        SUM(ftp.losses) as career_losses,
        SUM(ftp.ties) as career_ties,
        ROUND(
            CASE 
                WHEN (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) > 0 
                THEN (SUM(ftp.wins) + 0.5 * SUM(ftp.ties))::decimal / (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties))
                ELSE 0
            END, 3
        ) as career_win_percentage,
        SUM(ftp.points_for) as total_points_scored,
        ROUND(AVG(ftp.points_for), 1) as avg_points_per_game,
        ROUND(SUM(ftp.points_for) / COUNT(DISTINCT dl.season_year), 1) as avg_points_per_season,
        COUNT(DISTINCT CASE WHEN ftp.is_playoff_team THEN dl.season_year END) as playoff_appearances,
        ROUND(STDDEV(ftp.win_percentage), 3) as season_consistency_score
    FROM edw.fact_team_performance ftp
    JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
    JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
    LEFT JOIN edw.dim_manager dm ON dt.manager_name = dm.manager_name
    WHERE dt.manager_name IS NOT NULL
      AND (
          dm.include_in_analysis = TRUE
          OR EXISTS (
              SELECT 1 FROM edw.mart_league_summary mls 
              WHERE mls.champion_manager = dt.manager_name
          )
      )
    GROUP BY dt.manager_name
)
SELECT 
    ms.manager_name,
    ms.total_seasons::INT as total_seasons,
    COALESCE(mc.championships_won, 0)::INT as championships_won,
    ms.career_wins::INT as career_wins,
    ms.career_losses::INT as career_losses,
    ms.career_ties::INT as career_ties,
    ms.career_win_percentage::DECIMAL(6,3) as career_win_percentage,
    ms.total_points_scored::DECIMAL(10,2) as total_points_scored,
    ms.avg_points_per_game::DECIMAL(6,1) as avg_points_per_game,
    ms.avg_points_per_season::DECIMAL(6,1) as avg_points_per_season,
    ms.playoff_appearances::INT as playoff_appearances,
    ms.season_consistency_score::DECIMAL(6,3) as season_consistency_score,
    -- Hall of Fame Index: 60% Championships + 40% Win Percentage
    ROUND(
        (COALESCE(mc.championships_won, 0)::decimal / NULLIF((SELECT MAX(championships_won) FROM manager_championships), 0) * 0.6) + 
        (ms.career_win_percentage * 0.4),
        3
    )::DECIMAL(6,3) as hall_of_fame_index,
    RANK() OVER (ORDER BY 
        (COALESCE(mc.championships_won, 0)::decimal / NULLIF((SELECT MAX(championships_won) FROM manager_championships), 0) * 0.6) + 
        (ms.career_win_percentage * 0.4) DESC
    )::INT as hall_of_fame_rank
FROM manager_stats ms
LEFT JOIN manager_championships mc ON ms.manager_name = mc.manager_name
WHERE ms.total_seasons >= 3
ORDER BY hall_of_fame_rank; 