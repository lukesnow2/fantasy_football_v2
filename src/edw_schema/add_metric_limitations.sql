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

-- power_score, pythagorean_wins and luck_factor were rewritten by migration
-- 019 (WP15, 2026-11); populate_metric_definitions.sql carries that copy, so
-- their old corrections are gone from here. Their limitations, and those of
-- the rows 019 added, are at the end of the LIMITATIONS section.

-- strength_of_schedule is the average record of the teams played, each
-- opponent's record taken over its games against everyone else (migration
-- 020). The fields here match 020, so re-running this file keeps that copy.
UPDATE meta_data.metric_definitions SET
    detailed_description = 'For each game a team has played, the opponent''s record so far in its other games, with a tie as half a win and every game against this team left out. Those records are averaged over the team''s games, so an opponent met twice counts twice. Leaving out the games against the team keeps its own results from moving the number: a win doesn''t make the schedule look easier and a loss doesn''t make it look harder. It''s season to date, regular season only, and it''s stored with the weekly power rankings but isn''t shown on any page.',
    calculation_formula = 'Average over games of (opponent''s win % in its games against everyone else)',
    example_calculation = 'Three weeks in, a team has played A, B and C. Leaving out their games against this team, A is 2-0, a 1.000 record, and B and C are each 1-1, or .500. 1.000 + .500 + .500 = 2.000, and 2.000 ÷ 3 = .667, a tougher schedule than average.',
    interpretation_guide = '0.500 is an average schedule and higher is tougher. The team''s own games are left out of its opponents'' records, so beating them doesn''t make the schedule look easier. It measures opponents'' records, not how much they scored against you.',
    unit_of_measure = 'win_percentage',
    typical_range = '0.439 to 0.568 at season''s end',
    display_format = 'decimal_3'
WHERE metric_id = 'strength_of_schedule';

-- season_consistency_score is a standard deviation, so lower is better. The
-- thresholds are already stored that way (excellent < good), which is how the
-- UI now detects the direction. Observed across 11 managers: 0.112-0.191, so
-- the stored "0-5% is extremely consistent" band is unreachable in this league.
UPDATE meta_data.metric_definitions SET
    interpretation_guide = 'Lower means steadier, the opposite of most columns; careers here run ±.112 to ±.194 of win rate. ±.150 means a typical season lands about two wins above or below their average over 15 games. It''s steadiness, not quality: always going 5-10 scores as well as always going 10-5.',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    typical_range = '±.112 to ±.194 (career)'
WHERE metric_id = 'season_consistency_score';

-- competitiveness_index: observed 39.0-65.9 across 21 seasons, so the stored
-- "excellent" threshold of 70 has never once been reached.
UPDATE meta_data.metric_definitions SET
    interpretation_guide = 'Higher means a more evenly matched season. Finished seasons here have run 39.0 to 65.9, and the middle one is 56.5. It''s a 0 to 100 score, not a percent, and a strong part can hide a weak one.',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL
WHERE metric_id = 'competitiveness_index';

-- faab_efficiency_rating was career points ÷ FAAB spent, and read 0 for
-- everyone because no bid had ever been stored (the parser looked for it on
-- the player, not the transaction). Rebuilt 2026-11 on bids reconciled to
-- Yahoo: for every team-season 2022-2026, bids + remaining balance = $100.
-- Observed across the 10 managers who have bid: 0.21-3.13.
UPDATE meta_data.metric_definitions SET
    short_description = 'Starter points a manager''s paid waiver claims produced for each FAAB dollar spent.',
    detailed_description = 'For every waiver claim a manager paid FAAB for, the fantasy points that player scored in the manager''s starting lineup from the claim''s week until the same team paid for that player again. Those points are added up over every paid claim and divided by every dollar spent. Bench weeks earn nothing, since the question is what the money put in the lineup. $0 claims are left out, and the season in progress counts. The league has used FAAB since 2022.',
    calculation_formula = 'Starter points from paid claims ÷ FAAB dollars spent',
    example_calculation = '$40 on a running back who started 6 weeks for 72 points, and $10 on a receiver who never started: (72 + 0) ÷ (40 + 10) = 1.44 pts/$',
    interpretation_guide = 'Higher is better. Most careers here run 1.5 to 2.6 points per dollar, so $50 at 2.0 put about 100 points in lineups. It credits every starting point to the claim, not just the gain over who''d have started.',
    typical_range = '0.2 to 3.4',
    good_value_threshold = 2.5,
    excellent_value_threshold = 3.0,
    display_format = 'decimal_2'
WHERE metric_id = 'faab_efficiency_rating';

-- ============================================================================
-- LIMITATIONS
-- ============================================================================

UPDATE meta_data.metric_definitions SET limitations =
'Only managers with at least three seasons are ranked at all, so newer managers are absent rather than scored low. Championships are divided by the current league maximum, so every new title rescales the metric and a score from one year is not comparable to a score from another. A title and a regular season win rate are weighted the same regardless of league size, schedule strength or era.'
WHERE metric_id = 'hall_of_fame_index';

UPDATE meta_data.metric_definitions SET limitations =
'Looks back over a whole career and counts the season in progress, so it moves every week during a season. Playoff, title and consolation games all count, so a deep playoff run raises it and a consolation game counts the same as a regular-season one. Every game weighs the same across eras, league sizes and scoring rules.'
WHERE metric_id = 'career_win_percentage';

UPDATE meta_data.metric_definitions SET limitations =
'Credits all of a player''s starting points to the claim, with no allowance for who would have started otherwise, so filling an empty slot scores the same as a real upgrade. A player dropped and later re-added for free still credits the original paid claim. $0 claims are left out, so winning players for nothing earns no credit here. Covers 2022 on, when the league started using FAAB, and counts the season in progress.'
WHERE metric_id = 'faab_efficiency_rating';

UPDATE meta_data.metric_definitions SET limitations =
'It only counts finished seasons, so the season in progress doesn''t move it until its title game is played. Win rates include playoff and consolation games, which a playoff team plays more of. It needs several seasons to mean much: with one finished season there''s no spread to measure and it shows as ±.000. A manager who ran two teams in one season gets two win rates for that year. It looks back over a career and says nothing about next season.'
WHERE metric_id = 'season_consistency_score';

UPDATE meta_data.metric_definitions SET limitations =
'The four parts are weighted equally by choice, not because the data says they matter equally. The playoff race part always measures from sixth place, whatever the playoff field was that year, and the top six teams always count, so with six or more teams it can''t drop below 60. Records and season points include playoff and consolation games, and teams don''t all play the same number of those. Win parity and point spread stop at zero, though no season here has come close. It includes the season in progress, which moves every week and reads low early on, when a few games spread records wide. It describes a season after the fact and isn''t a forecast.'
WHERE metric_id = 'competitiveness_index';

UPDATE meta_data.metric_definitions SET limitations =
'The × 300 scale is arbitrary, picked to land ordinary seasons in a readable range. It stops at zero once the spread passes about .333 of win rate, though no season here has gotten near that. Win rates include playoff and consolation games. It includes the season in progress, where a few games spread records wide, so it reads low early on. It only looks at one season, so it can''t tell whether the same team tops the standings every year.'
WHERE metric_id = 'win_parity_score';

UPDATE meta_data.metric_definitions SET limitations =
'Only the highest and lowest scoring teams count, so one outlier sets the number and the middle of the table is invisible. Season totals include playoff and consolation games, and teams don''t all play the same number of those, which can widen the gap. It stops at zero once the gap passes the average total, though no season here has come close. It includes the season in progress, scored on the games played so far.'
WHERE metric_id = 'point_spread_score';

UPDATE meta_data.metric_definitions SET limitations =
'Counts points only through the end of the season the trade was made in. Counts weeks a player sat on a bench and weeks after the player moved on to a third team, but not weeks the player went unrostered. The trade''s own week counts in full, even games played before the deal. Draft picks and FAAB in a deal aren''t valued. Trades in the season in progress count on points so far.'
WHERE metric_id = 'production_differential';

UPDATE meta_data.metric_definitions SET limitations =
'The playoff and title parts judge the whole team''s season, of which the trade is one piece. Timing adds the same to both sides, so it never decides a winner. Draft picks and FAAB in a deal count for nothing, and points only run to the end of that season. The 8-point line is fixed, so trades near it can flip on small scoring changes. Trades in the season in progress are judged on points so far.'
WHERE metric_id = 'trade_winner';

UPDATE meta_data.metric_definitions SET limitations =
'Divides by pick number, not by what that pick usually scores, so a 10th-round flier and a first-rounder with the same points differ about tenfold. The average pick covers every season the player was drafted, including later ones, so a season can be judged by a draft that came after it. Not position adjusted. Points count only weeks the player was on a roster in this league. Players never drafted read 0.'
WHERE metric_id = 'draft_value_score';

UPDATE meta_data.metric_definitions SET limitations =
'Divides by a fixed 15, 16 or 17 weeks, not the weeks the player was rostered or started, and those lengths are set by year rather than read from each season''s schedule. The season in progress is divided by 17 too, so its values read low until it ends. Points count only weeks the player was on a roster in this league. Not position adjusted.'
WHERE metric_id = 'points_per_week';

UPDATE meta_data.metric_definitions SET limitations =
'One offensive baseline covers four positions, so quarterbacks rate high and tight ends low. Divides by 17 even in 15- and 16-week seasons. The list holds only players rostered in this league, not the free-agent pool. It''s a weekly number, not a season total.'
WHERE metric_id = 'points_above_replacement';

UPDATE meta_data.metric_definitions SET limitations =
'Weeks at zero or below are dropped, so a player who missed games or posted a real zero looks steadier than they were. It isn''t scale free: a player who averages 20 points will swing more than one who averages 6, so it can''t rank a quarterback against a kicker. A player with a single scoring week gets 0, which reads as perfectly steady. It covers one season at a time and looks back only.'
WHERE metric_id = 'consistency_rating';

UPDATE meta_data.metric_definitions SET limitations =
'It looks forward from the games played so far this season, and it only sees points. Injuries, bye weeks, trades and next week''s lineups don''t count until they show up in a team''s scores, so a team that just lost its best player looks as strong as it did last week. Early in the season the odds lean on the league average, because two or three games can''t tell a good team from a lucky one. The middle of the scale has run a little high: teams given 50 to 70% made it 159 times in 295, or 54%. It beats a simpler model that uses only record and schedule, but not by much (Brier score 0.145 against 0.148, lower is better). All of these checks replay 2008 to 2025 from week 3 on, with at least two weeks left.'
WHERE metric_id = 'playoff_probability';

UPDATE meta_data.metric_definitions SET limitations =
'Nothing computes it. An old view definition in the schema file refers to the column, but the warehouse doesn''t have it.'
WHERE metric_id = 'waiver_activity_index';

UPDATE meta_data.metric_definitions SET limitations =
'Early on it rests on a game or two per opponent, and in week 1 no opponent has another game yet, so it reads .500. Each week''s number uses opponents'' records through that week, not as they were the day you played them. It sees wins, not points: Schedule, which compares what your opponents scored against you with the league''s average that week, says more about who you drew.'
WHERE metric_id = 'strength_of_schedule';

UPDATE meta_data.metric_definitions SET limitations =
'One game sets it, so it says nothing about how often anyone wins big. It leaves out games against short-stint managers (fewer than five seasons) and Yahoo''s hidden accounts, so the Record Book, which counts every game, can show a bigger number. It counts playoff and consolation games and the season in progress, so a game played this week can take the record. When two games tie for the largest margin, the tile names only one winner.'
WHERE metric_id = 'biggest_win_margin';

UPDATE meta_data.metric_definitions SET limitations =
'One game sets it, so pairs who''ve played more often (anywhere from 7 to 41 games here) have had more chances at a big one. It counts playoff and consolation games and the season in progress, so it can grow this week. Only pairs with at least five games together are listed, and games involving managers with fewer than five seasons aren''t counted at all.'
WHERE metric_id = 'most_lopsided_game';

-- WP15 (migration 019): luck against all-play wins, the rebuilt power score,
-- and the new field and rivalry rows.
UPDATE meta_data.metric_definitions SET limitations =
'It mixes two kinds of luck: who a team drew and how the close games broke. Regular season only. Early in a season one game can move it by most of a win. A team''s luck in one season says nothing about the next: across back-to-back seasons for the same manager the two are slightly opposed (correlation −0.21). Before November 2026 it was wins minus a formula on points for and against (Pythagorean wins), which counted points against as earned and missed most schedule luck.'
WHERE metric_id = 'luck_factor';

UPDATE meta_data.metric_definitions SET limitations =
'Rivalries here run 7 to 41 games, so one result moves it a lot. It squares the points, which is flatter than the power of 6.26 that fits whole seasons in this league, so the side that outscores the other usually reads as lucky. It looks back over every meeting and isn''t a prediction of the next one.'
WHERE metric_id = 'pythagorean_wins';

UPDATE meta_data.metric_definitions SET limitations =
'It looks back: it''s a rating of games played, not a forecast, though from week 5 on the higher score has won the next week''s game 57% of the time (the better record, 56%). The weights are a choice, not fitted. The scoring terms are relative to that week''s best team, so a score isn''t comparable across seasons. From week 5 on its order matches the standings closely (rank correlation 0.97). In the first few weeks one game swings the win % term a lot. Before November 2026 its points term read only that week''s score and it added a schedule term that was really the team''s own record again.'
WHERE metric_id = 'power_score';

UPDATE meta_data.metric_definitions SET limitations =
'Every week counts the same, so a week when the whole league scored close together moves it as much as a week with big gaps. It looks back and counts the regular season only, the season in progress as it''s played. A season''s total depends on its length (13 or 14 weeks here), and a career''s on how many seasons were played, so compare like with like.'
WHERE metric_id = 'all_play_wins';

UPDATE meta_data.metric_definitions SET limitations =
'It looks back and covers one season to date. One season''s number barely predicts the next (correlation 0.10 for the same manager, back to back), because rosters reset every year. It compares a team with the league it played in, so it can''t say whether one era''s league was stronger than another''s. In the 8-team seasons (2005 and 2007) each other team is a bigger share of the average, so the extremes run wider.'
WHERE metric_id = 'points_vs_league';

UPDATE meta_data.metric_definitions SET limitations =
'It says how many points a team faced, not whether it lost: a big opponent week against a bigger week of its own costs nothing. Luck shows the effect on wins, and the two line up (correlation −0.67 at season''s end). The ''other teams'' average includes the opponent itself, which pulls the number slightly toward zero. It looks back and covers one season to date.'
WHERE metric_id = 'schedule_vs_league';

UPDATE meta_data.metric_definitions SET limitations =
'A sum, so it grows with the number of seasons played: compare managers with similar careers. It includes the season in progress, so it moves every week during the season. Chance alone spreads one season''s luck by about 1.4 wins, so a 16-season career by about 6. Before November 2026 it was built on a formula from points for and against (Pythagorean wins) and missed most schedule luck.'
WHERE metric_id = 'career_luck';

UPDATE meta_data.metric_definitions SET limitations =
'Rivalries here run 7 to 41 games, so a single close game moves it a lot. Expected wins square the points, flatter than the 6.26 power that fits whole seasons here, so the side with more points in the series tends to show as luckier (a side''s share of the points and its luck correlate at 0.59). It looks back over every meeting and doesn''t predict the next.'
WHERE metric_id = 'rivalry_luck';

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
