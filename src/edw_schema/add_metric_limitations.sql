-- ============================================================================
-- ADD LIMITATIONS TO METRIC DEFINITIONS
-- ============================================================================
-- Adds a `limitations` column to meta_data.metric_definitions and populates it
-- for all 21 seeded metrics.
--
-- Every entry below is derived from the actual computation in
-- src/edw_schema/edw_etl_processor.py, not from general knowledge of the
-- metric. Where the stored definition contradicted the code or the data, the
-- definition itself is corrected here rather than merely annotated.
--
-- Idempotent: safe to re-run.
--
-- Author: Luke Snow
-- Date: 2026-08-12
-- ============================================================================

ALTER TABLE meta_data.metric_definitions
    ADD COLUMN IF NOT EXISTS limitations TEXT;

COMMENT ON COLUMN meta_data.metric_definitions.limitations IS
    'What this metric does not capture, and where it can mislead. Grounded in the ETL implementation.';

-- ============================================================================
-- CORRECTIONS: stored text that contradicts the pipeline or the data
-- ============================================================================

-- power_score is produced by edw_etl_processor.py as
--   win_pct*0.4 + (points/league-week max)*0.3 + (form/league-week max)*0.2 + (1-SOS)*0.1
-- which is bounded by 0-1, not 0-100. Observed across 2,407 team-weeks:
-- min 0.198, max 1.000, mean 0.649. The published 0-100 range and its 70/85
-- thresholds would file every real team under "struggling".
UPDATE meta_data.metric_definitions SET
    calculation_formula = '(Win% × 0.4) + (Points ÷ Best Points That Week × 0.3) + (Recent Form ÷ Best Form That Week × 0.2) + ((1 − Strength of Schedule) × 0.1)',
    example_calculation = 'Team at 70% wins, 90% of the week''s top score, 80% of the top recent form, 0.55 SOS: (0.70×0.4) + (0.90×0.3) + (0.80×0.2) + (0.45×0.1) = 0.755',
    interpretation_guide = 'Runs 0 to 1 and is relative to the rest of the league that week. Observed league history: 0.65 is average, 0.75+ is a strong week, above 0.85 is dominant, below 0.45 is struggling.',
    typical_range = '0.000-1.000',
    good_value_threshold = 0.7500,
    excellent_value_threshold = 0.8500,
    display_format = 'decimal_3'
WHERE metric_id = 'power_score';

-- strength_of_schedule is AVG(opponent win percentage), not an average of
-- opponent power scores, and it is not recency weighted. Observed: 0.000-1.000,
-- mean 0.502. The published "40-90" range and "80+ is very tough" never occur.
UPDATE meta_data.metric_definitions SET
    detailed_description = 'The average win percentage of every opponent a team has faced. A value above 0.500 means the team drew opponents who were better than a coin flip against the rest of the league.',
    calculation_formula = 'Average of (Opponent Win Percentage) across all games played',
    example_calculation = 'Opponents finished 0.60, 0.45, 0.55 and 0.52: (0.60+0.45+0.55+0.52) ÷ 4 = 0.530',
    interpretation_guide = 'Centred on 0.500 by construction. 0.550+ is a hard schedule, 0.450-0.550 is ordinary, below 0.450 is soft.',
    unit_of_measure = 'win_percentage',
    typical_range = '0.000-1.000',
    display_format = 'decimal_3'
WHERE metric_id = 'strength_of_schedule';

-- season_consistency_score is a standard deviation, so lower is better. The
-- thresholds are already stored that way (excellent < good), which is how the
-- UI now detects the direction. Observed across 11 managers: 0.112-0.191, so
-- the stored "0-5% is extremely consistent" band is unreachable in this league.
UPDATE meta_data.metric_definitions SET
    interpretation_guide = 'Lower is better. Observed league history spans 0.112 to 0.191, so read it relatively: below 0.130 is steady year to year, 0.130-0.170 is typical, above 0.170 is boom-or-bust.',
    good_value_threshold = 0.1400,
    excellent_value_threshold = 0.1200,
    typical_range = '0.000-0.500'
WHERE metric_id = 'season_consistency_score';

-- competitiveness_index: observed 39.0-65.9 across 21 seasons, so the stored
-- "excellent" threshold of 70 has never once been reached.
UPDATE meta_data.metric_definitions SET
    interpretation_guide = 'Higher is more competitive. Across this league''s 21 seasons the index has run 39.0 to 65.9 with a mean of 56.3, so 60+ is a genuinely tight year and below 50 is a season somebody ran away with.',
    good_value_threshold = 55.0000,
    excellent_value_threshold = 60.0000
WHERE metric_id = 'competitiveness_index';

-- ============================================================================
-- LIMITATIONS
-- ============================================================================

UPDATE meta_data.metric_definitions SET limitations =
'Only managers with at least three seasons are ranked at all, so newer managers are absent rather than scored low. Championships are divided by the current league maximum, so every new title rescales the metric and a score from one year is not comparable to a score from another. A title and a regular season win rate are weighted the same regardless of league size, schedule strength or era.'
WHERE metric_id = 'hall_of_fame_index';

UPDATE meta_data.metric_definitions SET limitations =
'Ties count as half a win here, but the manager analytics path computes the same field without that adjustment, so the two can disagree for managers who have tied. Every game across twenty seasons is weighted equally despite changes in league size, roster rules and scoring settings.'
WHERE metric_id = 'career_win_percentage';

UPDATE meta_data.metric_definitions SET limitations =
'Currently reads 0.00 for every manager in this league, because no FAAB spend is recorded in the warehouse and the calculation falls back to zero rather than to null. Even with spend data the numerator is a manager''s entire career point total, not the points produced by the players actually bought with FAAB, so the metric rewards spending little more than it rewards spending well.'
WHERE metric_id = 'faab_efficiency_rating';

UPDATE meta_data.metric_definitions SET limitations =
'Lower is better, which is the opposite direction to most metrics on this page. It is the standard deviation of weekly win percentage across a career rather than of season final records, so a manager with few seasons has an unstable value. It measures steadiness, not quality: a reliably poor manager scores as well as a reliably good one.'
WHERE metric_id = 'season_consistency_score';

UPDATE meta_data.metric_definitions SET limitations =
'The four components are weighted equally by choice, not because the data says they matter equally. Two of them floor at zero, so leagues past that point cannot be told apart. The playoff race component counts teams within one game of the last playoff spot and scales at ten points per team, which assumes a ten team league.'
WHERE metric_id = 'competitiveness_index';

UPDATE meta_data.metric_definitions SET limitations =
'The ×300 scale factor is arbitrary, picked to put ordinary leagues in a readable range. The floor at zero means any league with a win percentage spread beyond roughly a third cannot be distinguished from a worse one. It says nothing about whether the same team wins every year, only about how one season''s wins were spread.'
WHERE metric_id = 'win_parity_score';

UPDATE meta_data.metric_definitions SET limitations =
'Uses only the highest and lowest scoring teams, so one outlier sets the score and the shape of the middle is invisible. Like win parity it floors at zero once the spread exceeds the league average.'
WHERE metric_id = 'point_spread_score';

UPDATE meta_data.metric_definitions SET limitations =
'Uses a fixed exponent of 2. Fantasy football scoring is usually fitted nearer 2.4, so this compresses the spread between lucky and unlucky teams. When a team has no recorded points the value silently falls back to actual wins, which makes luck factor read exactly zero instead of unknown. It also assumes points scored and points allowed are independent, which head-to-head scheduling breaks.'
WHERE metric_id = 'pythagorean_wins';

UPDATE meta_data.metric_definitions SET limitations =
'Inherits every assumption in Pythagorean wins, including the fixed exponent and the fallback that manufactures a spurious zero. Over a fourteen game season the noise is about the size of the effect, so a single season''s luck factor is not evidence of anything.'
WHERE metric_id = 'luck_factor';

UPDATE meta_data.metric_definitions SET limitations =
'Counts only fantasy points produced after the trade. It ignores what each manager would otherwise have started, positional scarcity, and whether the player was even in the lineup, so points from a traded player who sat on a bench still count. Draft picks and FAAB included in a deal are not valued at all.'
WHERE metric_id = 'production_differential';

UPDATE meta_data.metric_definitions SET limitations =
'Built on production differential, so it inherits its blind spots. The threshold for declaring a winner is a fixed point margin, which means trades near that line flip verdict on small scoring changes rather than on anything meaningful.'
WHERE metric_id = 'trade_winner';

UPDATE meta_data.metric_definitions SET limitations =
'Expected points for a draft slot come from this league''s own history, so the baseline moves whenever scoring settings change and the score is not comparable to public draft value figures. A player who is drafted and immediately injured scores near zero no matter how sound the pick was at the time.'
WHERE metric_id = 'draft_value_score';

UPDATE meta_data.metric_definitions SET limitations =
'Divides by weeks actually played, so a player who missed most of a season can post an excellent rate off a handful of games. It is not position adjusted, which makes a quarterback and a kicker incomparable on this number alone.'
WHERE metric_id = 'points_per_week';

UPDATE meta_data.metric_definitions SET limitations =
'The replacement baseline is derived from this league''s own waiver pool, so the values are not comparable to public VORP or PAR figures and shift with league size and roster requirements.'
WHERE metric_id = 'points_above_replacement';

UPDATE meta_data.metric_definitions SET limitations =
'Lower is better, which is the opposite direction to most metrics on this page. Weeks with zero points are dropped from the standard deviation, so a player who missed games looks steadier than he was. It is not scale free either: high scoring positions have larger deviations by construction, which is why the thresholds are position dependent.'
WHERE metric_id = 'consistency_rating';

UPDATE meta_data.metric_definitions SET limitations =
'Not a simulation. It is a weighted blend of current rank, games back and weeks remaining, so it does not know the specific remaining schedule, tiebreakers, or who is on which roster. The weights are fixed rather than fitted to what actually happened in past seasons.'
WHERE metric_id = 'playoff_probability';

UPDATE meta_data.metric_definitions SET limitations =
'Points and recent form are normalised against the best team in that same league week, so the score describes standing relative to the field at that moment and cannot be compared across weeks or seasons. Where strength of schedule is missing it defaults to 0.5, which quietly pushes the schedule term to neutral rather than flagging the gap.'
WHERE metric_id = 'power_score';

UPDATE meta_data.metric_definitions SET limitations =
'Depends on transaction records, which are sparse or entirely absent for the earliest seasons. A low value in an old season may mean the data was never captured rather than that the league was quiet.'
WHERE metric_id = 'waiver_activity_index';

UPDATE meta_data.metric_definitions SET limitations =
'Averaging opponents'' win percentages is circular: beating your opponents lowers their win percentage and therefore lowers your own strength of schedule. Weeks with no matched opponent default to 0.500, so a team with missing games drifts toward average rather than showing a gap.'
WHERE metric_id = 'strength_of_schedule';

UPDATE meta_data.metric_definitions SET limitations =
'Only victories count. A manager''s largest margin in a loss records as zero rather than as a negative, so this cannot be read as a general blowout tendency. One extreme game defines the value for an entire career.'
WHERE metric_id = 'biggest_win_margin';

UPDATE meta_data.metric_definitions SET limitations =
'A single game extreme across the whole history, so it is driven by outliers and by how many times two managers happen to have played. It says nothing about the typical margin between them.'
WHERE metric_id = 'most_lopsided_game';

-- ============================================================================
-- REBUILD THE VIEW WITH THE NEW COLUMN
-- ============================================================================

DROP VIEW IF EXISTS meta_data.vw_active_metrics;

CREATE VIEW meta_data.vw_active_metrics AS
SELECT
    md.metric_id,
    md.metric_name,
    md.metric_category,
    mc.category_name,
    mc.category_description,
    md.short_description,
    md.detailed_description,
    md.calculation_formula,
    md.example_calculation,
    md.interpretation_guide,
    md.limitations,
    md.data_type,
    md.unit_of_measure,
    md.typical_range,
    md.good_value_threshold,
    md.excellent_value_threshold,
    md.display_format,
    md.sort_order,
    mc.display_order as category_order,
    mc.icon_name as category_icon,
    mc.color_scheme as category_color
FROM meta_data.metric_definitions md
LEFT JOIN meta_data.metric_categories mc ON md.metric_category = mc.category_id
WHERE md.is_active = TRUE
ORDER BY mc.display_order, md.sort_order, md.metric_name;
