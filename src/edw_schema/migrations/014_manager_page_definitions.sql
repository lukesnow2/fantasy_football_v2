-- Dictionary entries for the manager pages' StatTips (WP6 S6.3, S6.8).
--
-- The Managers page's "Moves per season" and "Total moves" read
-- `transactions`; the profile's Achievements badges read the four badge_*
-- entries, each giving the ladder /api/managers/performance names badges by.
-- Until these rows exist, those labels show with no ⓘ.
--
-- Idempotent: safe to re-run. A re-run brings the copy up to date.

BEGIN;

INSERT INTO meta_data.metric_definitions (
    metric_id, metric_name, metric_category, short_description, detailed_description,
    calculation_formula, example_calculation, interpretation_guide,
    data_type, unit_of_measure, typical_range, good_value_threshold, excellent_value_threshold,
    display_format, sort_order
) VALUES
('transactions', 'Transactions', 'trade_analysis',
'Players a manager brought onto their roster: free-agent adds, waiver claims and trades.',
'Every transaction in league history where the player went to this manager. Moves per season spreads that total over the seasons they played.',
'Total moves: every add, claim and trade that sent a player to this manager. Moves per season: total moves divided by seasons played.',
'A manager with 409 moves over 16 seasons: 409 ÷ 16 = 25.6 moves per season',
'More moves means a busier manager, not a better one. Read it next to FAAB efficiency to see whether the activity paid off.',
'integer', 'transactions', '0-800', null, null, 'integer', 22),

('badge_championships', 'Championships badge', 'manager_performance',
'A badge for the titles a manager has won.',
'Named for the number of championships a manager has won in their career.',
'3 or more titles: Dynasty builder. 2: Repeat champion. 1: Champion.',
'A manager with 7 titles: Dynasty builder',
'No badge until a manager wins a title.',
'text', 'badge', null, null, null, 'text', 23),

('badge_scoring', 'Scoring badge', 'manager_performance',
'A badge for how much a manager scores per game over their career.',
'Named for a manager''s career points per game, regular season and playoffs.',
'130 or more points per game: Offensive powerhouse. 120 or more: High scorer.',
'A manager averaging 129.1 points per game: High scorer',
'No badge below 120 points per game.',
'text', 'badge', null, null, null, 'text', 24),

('badge_consistency', 'Consistency badge', 'manager_performance',
'A badge for a manager''s career win rate.',
'Named for a manager''s career win rate, ties counted as half a win.',
'A win rate of .700 or better: Dominance. .600 or better: Consistent winner. .500 or better: Above average.',
'A manager winning .563 of their games: Above average',
'No badge below .500.',
'text', 'badge', null, null, null, 'text', 25),

('badge_longevity', 'Longevity badge', 'manager_performance',
'A badge for how many seasons a manager has played.',
'Named for the number of seasons a manager has played in the league.',
'15 or more seasons: League veteran. 10 or more: Long timer. 5 or more: Established. Fewer: Newcomer.',
'A manager in their 22nd season: League veteran',
'Every manager has one; it only counts seasons, not results.',
'text', 'badge', null, null, null, 'text', 26)
ON CONFLICT (metric_id) DO UPDATE SET
    metric_name = EXCLUDED.metric_name,
    metric_category = EXCLUDED.metric_category,
    short_description = EXCLUDED.short_description,
    detailed_description = EXCLUDED.detailed_description,
    calculation_formula = EXCLUDED.calculation_formula,
    example_calculation = EXCLUDED.example_calculation,
    interpretation_guide = EXCLUDED.interpretation_guide,
    data_type = EXCLUDED.data_type,
    unit_of_measure = EXCLUDED.unit_of_measure,
    typical_range = EXCLUDED.typical_range,
    display_format = EXCLUDED.display_format,
    sort_order = EXCLUDED.sort_order,
    updated_at = CURRENT_TIMESTAMP;

COMMIT;
