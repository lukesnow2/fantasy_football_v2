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

-- faab_efficiency_rating was career points ÷ FAAB spent, and read 0 for
-- everyone because no bid had ever been stored (the parser looked for it on
-- the player, not the transaction). Rebuilt 2026-11 on bids reconciled to
-- Yahoo: for every team-season 2022-2026, bids + remaining balance = $100.
-- Observed across the 10 managers who have bid: 0.21-3.13.
UPDATE meta_data.metric_definitions SET
    short_description = 'Starter points produced by FAAB pickups, per FAAB dollar spent',
    detailed_description = 'For every waiver claim a manager paid for, the fantasy points that player scored while in that manager''s starting lineup, from the week of the claim until the same team claimed him again. Divided by the dollars spent. Bench weeks earn nothing: the question is what the money put in the lineup.',
    calculation_formula = 'Σ Starter Points from Paid Claims ÷ Σ FAAB Dollars Spent',
    example_calculation = '$40 on a back who started 6 weeks for 72 points and $10 on a receiver who never started: (72 + 0) ÷ (40 + 10) = 1.44 points per dollar',
    interpretation_guide = 'Higher is better. Across this league (FAAB since 2022) it runs 0.21 to 3.13 with most managers near 1.5-2.1: above 2.5 is sharp bidding, below 1.0 is money left on the bench or the waiver wire.',
    typical_range = '0.00-3.50',
    good_value_threshold = 2.0000,
    excellent_value_threshold = 2.5000,
    display_format = 'decimal_2'
WHERE metric_id = 'faab_efficiency_rating';

-- pythagorean_wins used a fixed exponent of 2. Fitted to this league's
-- completed regular seasons the exponent is 6.26 (6.54 for 2005-15, 5.91 for
-- 2016-25), cutting the miss against actual wins from 1.69 to 1.07 a season.
-- At 2, luck correlated +0.77 with a team's share of points scored (good
-- teams always read lucky); at the fitted exponent the correlation is 0.00.
UPDATE meta_data.metric_definitions SET
    detailed_description = 'Estimates how many regular-season games a team "should have" won from points scored and allowed alone. The exponent is fitted to this league''s own completed seasons rather than borrowed from another sport.',
    calculation_formula = 'Points For^x ÷ (Points For^x + Points Against^x) × Games Played, with x fitted to league history (6.26 through 2025)',
    example_calculation = 'Team with 1,700 PF, 1,600 PA, 14 games, x = 6.26: (1700^6.26 ÷ (1700^6.26 + 1600^6.26)) × 14 = 8.3 expected wins',
    interpretation_guide = 'Compare with regular-season wins. A record more than about a game and a half from this, either way, is unusual in a single season; over a career it should come close to even.'
WHERE metric_id = 'pythagorean_wins';

UPDATE meta_data.metric_definitions SET
    calculation_formula = 'Regular-Season Wins (ties as half) − Pythagorean Wins',
    interpretation_guide = 'Positive is lucky, negative unlucky. One season''s standard deviation in this league is about 1.1 wins, so ±1.5 is a notable year and ±2.5 is rare. It does not persist: first-half luck predicts nothing about the second half (r = −0.04), and one season''s luck nothing about the next (r = −0.09).'
WHERE metric_id = 'luck_factor';

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
'Credits the whole of a player''s starting production to the claim, with no allowance for who would otherwise have started, so it rewards claims that filled an empty slot as much as genuine upgrades. A player dropped and later re-added for free still credits the original paid claim. $0 claims are excluded because a rate per zero dollars is undefined, so a manager who wins players for nothing gets no credit here. Only 2022 onward: the league did not use FAAB before then.'
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
'The exponent is one number for the whole league across twenty years, refitted only when a season finishes. It describes a season well (it is fitted to describe) but predicts badly: from the first seven weeks, Pythagorean win percentage at 6.26 forecasts the rest of the season worse than a plain .500 guess, because early-season scoring is noisy. It assumes points scored and allowed are independent, which head-to-head scheduling breaks.'
WHERE metric_id = 'pythagorean_wins';

UPDATE meta_data.metric_definitions SET limitations =
'Measures only the gap between a record and its points; schedule luck (whom you drew in your best and worst weeks) and points luck (opponents having big weeks) are mixed together. Over a fourteen game season the spread is about one win, so a single season''s value is weak evidence; a career total is more telling.'
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
'Not a simulation and not fitted. It is a hand-set ladder of rank, games back and week number. Backtested on 2005-2025 (2007 excluded, its bracket labels are unreliable) it beats simple baselines (Brier 0.159, against 0.213 for "in a playoff spot today") but is badly calibrated in the middle: teams given 40-50% made it 26% of the time, 70-80% made it 55%, 80-90% made it 73%. Odds across a league add to about 7.1 playoff spots in week 1 when there are 6, and do not settle to 0% or 100% by the final regular week.'
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
