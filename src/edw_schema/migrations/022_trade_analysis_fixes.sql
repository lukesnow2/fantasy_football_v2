-- Trade analysis fixes (metric card audit, 2026-11-17).
--
-- edw.vw_trade_analysis scores both sides of every trade on four 0-100
-- components (production 40%, playoffs 30%, opportunity cost 20%, timing 10%)
-- and names a winner when the final scores are 8 or more apart. Four bugs:
--
-- 1. Timing added the same amount to both sides, so it could never move the
--    verdict. It now widens the gap: the side ahead on the other three
--    components gets 50 + (multiplier - 1) x 50, the side behind the mirror.
-- 2. The winner test used uncapped component scores while the page shows
--    them held to 0-100. The final scores are now built from the shown
--    component scores, and the winner is read from the final scores as shown.
-- 3. The title join compared fm.season_year with itself. It now holds the
--    championship game to the trade's season.
-- 4. "In Progress" went by calendar year, so a season's trades read Complete
--    from January. A season is now Complete once its title game has a winner.
--
-- The ETL drops and recreates this view from edw_etl_processor.py on every
-- transactions load; this file carries the same SQL so the fix lands without
-- waiting for one (tests/test_trade_analysis_view.py holds them equal).
--
-- Idempotent: safe to re-run.

BEGIN;

DROP VIEW IF EXISTS edw.vw_trade_analysis;

-- TRADE ANALYSIS VIEW (v3, migration 022)
-- Each side gets four 0-100 component scores, weighted
-- Production 40%, Playoffs 30%, Opportunity cost 20%, Timing 10%.
-- The final scores are those weighted averages, and the winner
-- is read from the final scores as shown: 8 or more apart, or
-- it's an Even Trade.
CREATE VIEW edw.vw_trade_analysis AS
WITH trade_groups AS (
    -- Create synthetic trade group IDs by grouping reciprocal trades
    SELECT 
        ft.*,
        -- Generate consistent trade_group_id for reciprocal trades
        CONCAT(
            ft.league_key, '_', 
            ft.transaction_date, '_',
            CASE 
                WHEN ft.from_team_key < ft.to_team_key 
                THEN CONCAT(ft.from_team_key, '_', ft.to_team_key)
                ELSE CONCAT(ft.to_team_key, '_', ft.from_team_key)
            END
        ) AS synthetic_trade_group_id
    FROM edw.fact_transaction ft
    WHERE ft.transaction_type = 'trade'
),
trade_synthesis AS (
    -- Synthesize trades into single rows with player aggregation
    SELECT 
        tg.synthetic_trade_group_id,
        MIN(tg.league_key) as league_key,
        MIN(tg.season_year) as season_year,
        MIN(tg.transaction_date) as transaction_date,
        MIN(tg.transaction_week) as transaction_week,
        -- Determine the two teams involved (consistent ordering)
        MIN(CASE WHEN tg.from_team_key < tg.to_team_key THEN tg.from_team_key ELSE tg.to_team_key END) as team_a_key,
        MAX(CASE WHEN tg.from_team_key < tg.to_team_key THEN tg.to_team_key ELSE tg.from_team_key END) as team_b_key,
        MIN(CASE WHEN tg.from_team_key < tg.to_team_key THEN tg.from_manager_key ELSE tg.to_manager_key END) as team_a_manager_key,
        MAX(CASE WHEN tg.from_team_key < tg.to_team_key THEN tg.to_manager_key ELSE tg.from_manager_key END) as team_b_manager_key,
        -- Count players on each side
        COUNT(*) FILTER (WHERE tg.from_team_key = LEAST(tg.from_team_key, tg.to_team_key)) as team_a_player_count,
        COUNT(*) FILTER (WHERE tg.from_team_key = GREATEST(tg.from_team_key, tg.to_team_key)) as team_b_player_count,
        COUNT(*) as total_players
    FROM trade_groups tg
    GROUP BY tg.synthetic_trade_group_id
),
trade_players AS (
    -- Get player names for each side of trade
    SELECT 
        ts.synthetic_trade_group_id,
        STRING_AGG(
            CASE WHEN tg.from_team_key = ts.team_a_key 
                 THEN dp.player_name ELSE NULL END, 
            ', ' ORDER BY dp.player_name
        ) FILTER (WHERE tg.from_team_key = ts.team_a_key) as team_a_gives,
        STRING_AGG(
            CASE WHEN tg.from_team_key = ts.team_b_key 
                 THEN dp.player_name ELSE NULL END, 
            ', ' ORDER BY dp.player_name
        ) FILTER (WHERE tg.from_team_key = ts.team_b_key) as team_b_gives
    FROM trade_synthesis ts
    JOIN trade_groups tg ON ts.synthetic_trade_group_id = tg.synthetic_trade_group_id
    JOIN edw.dim_player dp ON tg.player_key = dp.player_key
    GROUP BY ts.synthetic_trade_group_id
),
fantasy_production AS (
    -- Calculate fantasy points for traded players after trade date (40% weight)
    SELECT 
        ts.synthetic_trade_group_id,
        -- Team A production (players they received)
        COALESCE(SUM(
            CASE WHEN tg.to_team_key = ts.team_a_key 
                 THEN fps.weekly_fantasy_points ELSE 0 END
        ), 0) as team_a_production,
        -- Team B production (players they received)  
        COALESCE(SUM(
            CASE WHEN tg.to_team_key = ts.team_b_key 
                 THEN fps.weekly_fantasy_points ELSE 0 END
        ), 0) as team_b_production
    FROM trade_synthesis ts
    JOIN trade_groups tg ON ts.synthetic_trade_group_id = tg.synthetic_trade_group_id
    LEFT JOIN edw.fact_player_statistics fps ON (
        fps.player_key = tg.player_key 
        AND fps.league_key = tg.league_key
        AND fps.season_year = tg.season_year
        AND fps.week_number >= tg.transaction_week
    )
    GROUP BY ts.synthetic_trade_group_id
),
opportunity_cost AS (
    -- Evaluate pre-trade value differential (20% weight)
    SELECT 
        ts.synthetic_trade_group_id,
        -- Average pre-trade weekly points of players traded away
        AVG(CASE WHEN tg.from_team_key = ts.team_a_key 
                 THEN fps.weekly_fantasy_points ELSE NULL END) as team_a_gave_avg_value,
        AVG(CASE WHEN tg.from_team_key = ts.team_b_key 
                 THEN fps.weekly_fantasy_points ELSE NULL END) as team_b_gave_avg_value
    FROM trade_synthesis ts
    JOIN trade_groups tg ON ts.synthetic_trade_group_id = tg.synthetic_trade_group_id
    LEFT JOIN edw.fact_player_statistics fps ON (
        fps.player_key = tg.player_key 
        AND fps.league_key = tg.league_key
        AND fps.season_year = tg.season_year
        AND fps.week_number < tg.transaction_week
        AND fps.week_number >= GREATEST(1, tg.transaction_week - 4)  -- Last 4 weeks before trade
    )
    GROUP BY ts.synthetic_trade_group_id
),
playoff_impact AS (
    -- Playoff implications (30% weight). Both joins are held to
    -- the trade's season: a title in another season of the same
    -- league is not this trade's title.
    SELECT 
        ts.synthetic_trade_group_id,
        MAX(CASE WHEN ftp.team_key = ts.team_a_key AND ftp.is_playoff_team THEN 1 ELSE 0 END) as team_a_made_playoffs,
        MAX(CASE WHEN ftp.team_key = ts.team_b_key AND ftp.is_playoff_team THEN 1 ELSE 0 END) as team_b_made_playoffs,
        MAX(CASE WHEN fm.winner_team_key = ts.team_a_key THEN 1 ELSE 0 END) as team_a_champion,
        MAX(CASE WHEN fm.winner_team_key = ts.team_b_key THEN 1 ELSE 0 END) as team_b_champion
    FROM trade_synthesis ts
    LEFT JOIN edw.fact_team_performance ftp ON (
        ftp.league_key = ts.league_key 
        AND ftp.season_year = ts.season_year
        AND ftp.team_key IN (ts.team_a_key, ts.team_b_key))
    LEFT JOIN edw.fact_matchup fm ON (
        fm.league_key = ts.league_key
        AND fm.season_year = ts.season_year
        AND fm.winner_team_key IN (ts.team_a_key, ts.team_b_key)
        AND fm.is_championship = TRUE
    )
    GROUP BY ts.synthetic_trade_group_id
),
contextual_factors AS (
    -- Trade timing (10% weight): late-season trades count for more.
    SELECT 
        ts.synthetic_trade_group_id,
        CASE 
            WHEN ts.transaction_week >= 10 THEN 1.5
            WHEN ts.transaction_week >= 6 THEN 1.2  
            ELSE 1.0 
        END as timing_multiplier,
        -- A season's trades are settled once its title game has a
        -- winner, not on New Year's Day: the calendar year turned
        -- every trade Complete in January, and the season still
        -- being played Complete once its bracket ran into the next year.
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM edw.fact_matchup fm
                WHERE fm.league_key = ts.league_key
                  AND fm.season_year = ts.season_year
                  AND fm.is_championship = TRUE
                  AND fm.winner_team_key IS NOT NULL)
            THEN 'Complete'
            ELSE 'In Progress'
        END as evaluation_status
    FROM trade_synthesis ts
),
components AS (
    -- The component scores for each side, each held to 0-100 (the
    -- scores the page shows). Production and opportunity cost are
    -- mirror images: B's is 100 minus A's.
    SELECT 
        ts.synthetic_trade_group_id,
        LEAST(100, GREATEST(0, 50 + (fp.team_a_production - fp.team_b_production) * 0.15)) as a_production,
        LEAST(100, GREATEST(0, 50 - (fp.team_a_production - fp.team_b_production) * 0.15)) as b_production,
        CASE 
            WHEN pi.team_a_champion = 1 THEN 85
            WHEN pi.team_a_made_playoffs = 1 AND pi.team_b_made_playoffs = 0 THEN 70
            WHEN pi.team_a_made_playoffs = 0 AND pi.team_b_made_playoffs = 1 THEN 30
            ELSE 50
        END as a_playoff,
        CASE 
            WHEN pi.team_b_champion = 1 THEN 85
            WHEN pi.team_b_made_playoffs = 1 AND pi.team_a_made_playoffs = 0 THEN 70
            WHEN pi.team_b_made_playoffs = 0 AND pi.team_a_made_playoffs = 1 THEN 30
            ELSE 50
        END as b_playoff,
        LEAST(100, GREATEST(0, 50 + (COALESCE(oc.team_b_gave_avg_value, 0) - COALESCE(oc.team_a_gave_avg_value, 0)) * 2)) as a_opportunity,
        LEAST(100, GREATEST(0, 50 + (COALESCE(oc.team_a_gave_avg_value, 0) - COALESCE(oc.team_b_gave_avg_value, 0)) * 2)) as b_opportunity,
        cf.timing_multiplier
    FROM trade_synthesis ts
    LEFT JOIN fantasy_production fp ON ts.synthetic_trade_group_id = fp.synthetic_trade_group_id
    LEFT JOIN opportunity_cost oc ON ts.synthetic_trade_group_id = oc.synthetic_trade_group_id
    LEFT JOIN playoff_impact pi ON ts.synthetic_trade_group_id = pi.synthetic_trade_group_id
    LEFT JOIN contextual_factors cf ON ts.synthetic_trade_group_id = cf.synthetic_trade_group_id
),
timed AS (
    -- Timing widens the gap: the side ahead on the other three
    -- components gets 50 + (multiplier - 1) x 50 (75 at week 10+,
    -- 60 at weeks 6-9), the side behind the mirror image, and an
    -- even trade 50 each. Adding the same amount to both sides,
    -- as before, could never move the verdict.
    SELECT 
        c.*,
        50 + SIGN((c.a_production - c.b_production) * 0.40
                + (c.a_playoff - c.b_playoff) * 0.30
                + (c.a_opportunity - c.b_opportunity) * 0.20)
           * (c.timing_multiplier - 1) * 50 as a_context
    FROM components c
),
scored AS (
    SELECT 
        t.*,
        100 - t.a_context as b_context,
        ROUND(t.a_production * 0.40 + t.a_playoff * 0.30 + t.a_opportunity * 0.20 + t.a_context * 0.10, 1) as a_final,
        ROUND(t.b_production * 0.40 + t.b_playoff * 0.30 + t.b_opportunity * 0.20 + (100 - t.a_context) * 0.10, 1) as b_final
    FROM timed t
)
SELECT 
    dl.league_name,
    ts.season_year,
    ts.transaction_date,
    ts.transaction_week,
    dta.team_name as team_a_name,
    dma.manager_name as team_a_manager,
    tp.team_a_gives,
    dtb.team_name as team_b_name, 
    dmb.manager_name as team_b_manager,
    tp.team_b_gives,
    CONCAT(ts.team_a_player_count, '-for-', ts.team_b_player_count) as trade_type,
    ts.total_players,

    -- Production metrics
    ROUND(fp.team_a_production, 1) as team_a_production,
    ROUND(fp.team_b_production, 1) as team_b_production,
    ROUND(fp.team_a_production - fp.team_b_production, 1) as production_differential,

    -- Opportunity cost metrics
    ROUND(COALESCE(oc.team_a_gave_avg_value, 0), 1) as team_a_pre_trade_avg,
    ROUND(COALESCE(oc.team_b_gave_avg_value, 0), 1) as team_b_pre_trade_avg,
    ROUND(COALESCE(oc.team_b_gave_avg_value, 0) - COALESCE(oc.team_a_gave_avg_value, 0), 1) as opportunity_differential,

    -- Playoff outcomes
    pi.team_a_made_playoffs,
    pi.team_b_made_playoffs,
    pi.team_a_champion,
    pi.team_b_champion,

    -- Side A's component scores (0-100)
    ROUND(sc.a_production, 1) as team_a_production_score,
    ROUND(sc.a_playoff, 1) as team_a_playoff_score,
    ROUND(sc.a_opportunity, 1) as team_a_opportunity_score,
    ROUND(sc.a_context, 1) as team_a_context_score,

    -- Final weighted scores (0-100)
    sc.a_final as team_a_final_score,
    sc.b_final as team_b_final_score,

    -- The winner, from the final scores as shown
    CASE 
        WHEN ABS(sc.a_final - sc.b_final) >= 8 
        THEN CASE WHEN sc.a_final > sc.b_final THEN dma.manager_name ELSE dmb.manager_name END
        ELSE 'Even Trade'
    END as trade_winner,

    -- Trade analysis summary
    CASE 
        WHEN ABS(fp.team_a_production - fp.team_b_production) >= 50.0 THEN 
            CONCAT('Production-driven: ', 
                CASE WHEN fp.team_a_production > fp.team_b_production THEN dma.manager_name ELSE dmb.manager_name END,
                ' gained ', ROUND(ABS(fp.team_a_production - fp.team_b_production), 1), ' more points')
        WHEN (pi.team_a_champion = 1 OR pi.team_b_champion = 1) THEN 
            CONCAT('Championship impact: ', 
                CASE WHEN pi.team_a_champion = 1 THEN dma.manager_name ELSE dmb.manager_name END,
                ' won title')
        WHEN (pi.team_a_made_playoffs != pi.team_b_made_playoffs) THEN
            CONCAT('Playoff impact: ',
                CASE WHEN pi.team_a_made_playoffs = 1 THEN dma.manager_name ELSE dmb.manager_name END,
                ' made playoffs')
        ELSE 'Balanced trade with minimal impact'
    END as trade_analysis,

    -- Evaluation status
    cf.evaluation_status,

    ts.synthetic_trade_group_id as trade_group_id

FROM trade_synthesis ts
JOIN edw.dim_league dl ON ts.league_key = dl.league_key
LEFT JOIN edw.dim_team dta ON ts.team_a_key = dta.team_key
LEFT JOIN edw.dim_manager dma ON ts.team_a_manager_key = dma.manager_key
LEFT JOIN edw.dim_team dtb ON ts.team_b_key = dtb.team_key
LEFT JOIN edw.dim_manager dmb ON ts.team_b_manager_key = dmb.manager_key
LEFT JOIN trade_players tp ON ts.synthetic_trade_group_id = tp.synthetic_trade_group_id
LEFT JOIN fantasy_production fp ON ts.synthetic_trade_group_id = fp.synthetic_trade_group_id
LEFT JOIN opportunity_cost oc ON ts.synthetic_trade_group_id = oc.synthetic_trade_group_id
LEFT JOIN playoff_impact pi ON ts.synthetic_trade_group_id = pi.synthetic_trade_group_id
LEFT JOIN contextual_factors cf ON ts.synthetic_trade_group_id = cf.synthetic_trade_group_id
LEFT JOIN scored sc ON ts.synthetic_trade_group_id = sc.synthetic_trade_group_id
ORDER BY dl.league_name, ts.transaction_date DESC;

COMMIT;
