-- Dictionary entries for Draft Central's StatTips (WP8 S8.5).
--
-- The Career draft grades table's "Grade", "Draft value per season" and
-- "Hit rate" headers read draft_grade, draft_value_per_season and
-- draft_hit_rate. The copy follows web/src/lib/draft-value.ts. Until these
-- rows exist, those labels show with no ⓘ. draft_value_score, an older
-- per-pick ratio, is left as it is.
--
-- Idempotent: safe to re-run. A re-run brings the copy up to date.

BEGIN;

INSERT INTO meta_data.metric_definitions (
    metric_id, metric_name, metric_category, short_description, detailed_description,
    calculation_formula, example_calculation, interpretation_guide,
    data_type, unit_of_measure, typical_range, good_value_threshold, excellent_value_threshold,
    display_format, sort_order
) VALUES
('draft_grade', 'Draft grade', 'draft_analysis',
'A manager''s career draft grade: how sure we can be that they draft better or worse than average.',
'Each season''s draft gets a score: its picks'' points over what their positions and rounds usually score in this league, in average picks. The career grade divides a manager''s average season score by its standard error, so one big year can''t carry it. Most managers land on a C, which is the honest result when drafting skill is mostly noise.',
'Average season draft score ÷ (spread of every finished season score ÷ √seasons). A at +2 or more, B at +0.75 or more, C within 0.75 either way, D down to -2, F below that.',
'A manager averaging +0.94 a season over 18 seasons, with a league spread of 3.0: 0.94 ÷ (3.0 ÷ √18) = 1.33, a B',
'Read it as how settled the record is, not how big it is. A B means a manager beats their slots season after season; a C means their seasons don''t settle it either way.',
'text', 'grade', 'A to F', null, null, 'text', 27),

('draft_value_per_season', 'Draft value per season', 'draft_analysis',
'How many average picks'' worth of points a manager''s drafts beat their slots by, per season.',
'Every pick is judged against what players at the same position, taken around the same round, have scored in this league''s finished seasons. A draft''s value adds up its picks'' points over that expectation and divides by what the season''s average pick scored, so seasons with different scoring read on one scale. This is the average over a manager''s finished seasons.',
'For each finished season: the sum over the draft''s picks of (points minus expected points), divided by the season''s average pick. Then the average of those seasons.',
'A draft whose picks beat their slots by 280 points in a season where the average pick scored 140: 280 ÷ 140 = +2.0',
'0 is an average draft. +1.0 is about one extra average pick''s worth of points a year. Most managers sit within one of zero.',
'decimal', 'average picks', '-2 to +2', null, null, 'decimal_2', 28),

('draft_hit_rate', 'Hit rate', 'draft_analysis',
'The share of a manager''s picks that scored at least 80% of what their slot was expected to.',
'A hit is any pick that came within 20% of what players at the same position, taken around the same round, have scored in this league, or beat it. Every pick counts, kickers and defenses included.',
'Picks that scored at least 0.8 × their expected points ÷ all picks',
'196 hits from 329 picks: 196 ÷ 329 = 60%',
'Most managers hit on 55% to 65% of their picks. A higher rate means fewer wasted picks, not bigger wins, so read it next to steals and busts.',
'percentage', 'percent', '50% to 70%', null, null, 'percentage', 29)
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
