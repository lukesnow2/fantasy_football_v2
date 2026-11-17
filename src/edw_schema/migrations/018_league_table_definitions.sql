-- Dictionary entries for the League Table's StatTips (WP14 S14.5, D13).
--
-- The table's "Luck" and "Playoff win %" headers read career_luck and
-- playoff_win_percentage. Neither had an entry, so those labels showed with
-- no ⓘ. career_luck's one-line definition is the one Luke accepted in D13.6.
--
-- faab_efficiency_rating is brought up to date with how the mart computes it
-- (D13.3): starter points from paid claims ÷ dollars spent, not total points
-- ÷ FAAB. populate_metric_definitions.sql carries the same copy.
--
-- Idempotent: safe to re-run. A re-run brings the copy up to date.

BEGIN;

INSERT INTO meta_data.metric_definitions (
    metric_id, metric_name, metric_category, short_description, detailed_description,
    calculation_formula, example_calculation, interpretation_guide,
    data_type, unit_of_measure, typical_range, good_value_threshold, excellent_value_threshold,
    display_format, sort_order
) VALUES
('career_luck', 'Career luck', 'manager_performance',
'Regular-season wins minus expected wins. Above zero means the schedule helped.',
'Every season''s luck added up: a manager''s regular-season wins, a tie as half, minus the Pythagorean wins their points for and against should have earned. The season in progress counts as it''s played, like the career record.',
'Σ over every season of (regular-season wins + ½ ties − Pythagorean wins)',
'156.5 regular-season wins against 158.3 expected: 156.5 − 158.3 = -1.8 wins',
'Zero means the record matched the points. One season''s luck runs about a win either way and doesn''t carry into the next, so a career total past 5 wins either way is a lot of breaks.',
'decimal', 'games', '-8 to +8', null, null, 'decimal_1', 30),

('playoff_win_percentage', 'Playoff win %', 'manager_performance',
'The share of winners'' bracket games a manager has won, title games included.',
'Every playoff game in the winners'' bracket, the championship included. Consolation games don''t count.',
'Playoff and title-game wins ÷ (playoff wins + playoff losses)',
'16 bracket wins and 8 losses: 16 ÷ 24 = 66.7%',
'Brackets are short, so a few games move it a lot. Read it next to playoff trips: a high rate over two trips says less than a good one over ten.',
'percentage', 'percent', '0% to 70%', null, null, 'percentage_1', 31),

('faab_efficiency_rating', 'FAAB Efficiency Rating', 'efficiency',
'Starter points produced by FAAB pickups, per FAAB dollar spent',
'For every waiver claim a manager paid for, the fantasy points that player scored while in that manager''s starting lineup, from the week of the claim until the same team claimed him again. Divided by the dollars spent. Bench weeks earn nothing: the question is what the money put in the lineup.',
'Σ Starter Points from Paid Claims ÷ Σ FAAB Dollars Spent',
'$40 on a back who started 6 weeks for 72 points and $10 on a receiver who never started: (72 + 0) ÷ (40 + 10) = 1.44 points per dollar',
'Higher is better. Across this league (FAAB since 2022) it runs 0.21 to 3.13 with most managers near 1.5-2.1: above 2.5 is sharp bidding, below 1.0 is money left on the bench or the waiver wire.',
'decimal', 'points_per_dollar', '0.00-3.50', 2.0000, 2.5000, 'decimal_2', 3)
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
    good_value_threshold = EXCLUDED.good_value_threshold,
    excellent_value_threshold = EXCLUDED.excellent_value_threshold,
    display_format = EXCLUDED.display_format,
    sort_order = EXCLUDED.sort_order,
    updated_at = CURRENT_TIMESTAMP;

COMMIT;
