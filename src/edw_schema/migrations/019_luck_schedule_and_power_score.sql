-- Luck, schedule and the power score (WP15, Luke's spec of 2026-11-17).
--
-- Points for is the only skill signal in this league: there's no defense, so
-- a team's score is independent of its opponent's (p = 0.79 against random
-- re-pairings, 2005-2026), and the spread of season points against matches
-- random schedules exactly (6.6 pts/g both). Points against is the schedule.
--
-- 1. edw.mart_weekly_power_rankings gains three season-to-date columns:
--    all_play_wins, points_vs_league and schedule_vs_league. The ETL also
--    redefines luck_factor as wins minus all-play wins (it was wins minus
--    Pythagorean wins, which credited points against as earned and hid
--    schedule luck) and rebuilds power_score (D4 "R").
-- 2. The Data Dictionary rows behind every new or redefined StatTip.
--    Head-to-head "Luckier" gets its own row, rivalry_luck, because
--    luck_factor now means season luck. populate_metric_definitions.sql and
--    add_metric_limitations.sql carry the same copy, so re-running either
--    doesn't put the old definitions back.
--
-- Idempotent: safe to re-run. A re-run brings the copy up to date.

BEGIN;

ALTER TABLE edw.mart_weekly_power_rankings
    ADD COLUMN IF NOT EXISTS all_play_wins DECIMAL(8,4),
    ADD COLUMN IF NOT EXISTS points_vs_league DECIMAL(8,2),
    ADD COLUMN IF NOT EXISTS schedule_vs_league DECIMAL(8,2);

-- Four places, like luck_factor, so wins minus expected wins is the luck on
-- every row. A no-op where the column was added that way; widens it where an
-- earlier draft of this migration added it at two.
ALTER TABLE edw.mart_weekly_power_rankings
    ALTER COLUMN all_play_wins TYPE DECIMAL(8,4);

ALTER TABLE meta_data.metric_definitions
    ADD COLUMN IF NOT EXISTS limitations TEXT;

INSERT INTO meta_data.metric_definitions (
    metric_id, metric_name, metric_category, short_description, detailed_description,
    calculation_formula, example_calculation, interpretation_guide,
    data_type, unit_of_measure, typical_range, good_value_threshold, excellent_value_threshold,
    display_format, sort_order, limitations
) VALUES
('all_play_wins', 'Expected wins', 'team_performance',
'The wins your scores would have earned if you''d played every team every week.',
'Each week, the share of the other teams you outscored, with a tie counting half. Added up over the season, it''s the record your scoring earned no matter who you drew, scaled to the games you actually played.',
'Sum over games of (teams outscored + half the teams tied) ÷ (teams − 1)',
'You beat 7 of the 9 other scores in week 1, worth 0.78, and 3 of 9 in week 2, worth 0.33: 0.78 + 0.33 = 1.11 expected wins from 2 games.',
'Read it next to actual wins. The gap between the two is luck, and across the league the two add up to the same total every season.',
'decimal', 'games', '0-14', null, null, 'decimal_1', 32,
'Every week counts the same, so a week when the whole league scored close together moves it as much as a week with big gaps. Regular season only.'),

('luck_factor', 'Luck', 'team_performance',
'Wins minus expected wins. Above zero means the schedule and close games broke your way.',
'Regular-season wins, a tie as half, minus expected wins. Expected wins only look at your own scores, so what''s left in the gap is who you drew and how the close games went. The league''s luck adds up to zero every season.',
'Wins + half of ties − expected wins',
'9 wins against 7.4 expected: 9 − 7.4 = +1.6 wins',
'A season''s luck usually lands within 2.3 wins either way and doesn''t carry into the next season, so a big number one year says nothing about the next.',
'decimal', 'games', '-4.4 to +3.8', null, null, 'decimal_1', 9,
'It mixes two kinds of luck, who you drew and how close games broke; the Schedule column shows the first on its own. Until 2026-11 this was wins minus Pythagorean wins, which counted points against as earned and missed most schedule luck.'),

('career_luck', 'Career luck', 'manager_performance',
'Career wins minus expected wins. Above zero means the schedule and close games broke your way.',
'Every season''s luck added up: regular-season wins, a tie as half, minus the expected wins your scores earned against the whole league each week. The season in progress counts as it''s played, like the career record.',
'Sum over every season of (wins + half of ties − expected wins)',
'125.5 regular-season wins against 113.1 expected: 125.5 − 113.1 = +12.4 wins',
'Luck doesn''t carry from one season to the next, but it does pile up. Over a long career, 6 wins either way is ordinary and past 10 is a lot of breaks.',
'decimal', 'games', '-13 to +13', null, null, 'decimal_1', 30,
'A sum, so it grows with the number of seasons played: compare managers with similar careers. Until 2026-11 it was built on Pythagorean wins and missed most schedule luck.'),

('points_vs_league', 'Pts vs league', 'team_performance',
'Points per game above the other teams'' average that same week.',
'Each game, your score minus the average of every other team''s score that week, then averaged over your games. Comparing within the week takes out bye weeks, rule changes and high- or low-scoring years, so managers from different eras compare fairly.',
'Average over games of (your score − the other teams'' average that week)',
'You score 130 in a week the other nine average 122, so that game is +8. Average that over every game you''ve played.',
'Zero is an average team. Over a career, +3 a game is a strong scorer. A single season tells you much less, since it barely carries into the next one, so read a season''s number as a result and not a rating.',
'decimal', 'points_per_game', '-5 to +6 career; -17 to +17 a season', 3.0000, 5.0000, 'decimal_1', 33,
'One season''s number carries into the next at only r = 0.10, because rosters reset every year. It compares a team with the league it played in, so it can''t say whether one era''s league was stronger than another''s.'),

('schedule_vs_league', 'Schedule', 'team_performance',
'How much your opponents scored against you, per game, compared with the other teams that week.',
'Each game, your opponent''s score minus the average of every other team''s score that week, averaged over your games. There''s no defense in fantasy, so this is pure draw: above zero means your opponents happened to have big weeks against you.',
'Average over games of (your opponent''s score − the other teams'' average that week)',
'Your opponent scores 140 in a week the other nine average 122, so that game is +18. Average that over every game you''ve played.',
'Positive is a tough draw and negative an easy one. A season usually lands within 10 points a game either way, and it doesn''t carry into the next season.',
'decimal', 'points_per_game', '-6 to +4 career; -10 to +10 a season', null, null, 'decimal_1', 34,
'It says how many points you faced, not whether you lost: a big opponent week against your bigger week costs nothing. Luck shows the effect on wins.'),

('power_score', 'Power score', 'team_performance',
'A 0 to 1 rating of how good a team is right now, built from its record, its season scoring and its last three weeks.',
'45% win %, 35% points per game as a share of the league''s best, and 20% the average of the last three weeks as a share of the best. It blends what a team has done with how it''s scoring, so it mostly follows the standings and lifts a team whose points say it''s better than its record.',
'0.45 × win % + 0.35 × (pts/g ÷ top pts/g) + 0.20 × (last 3 weeks'' average ÷ top last-3 average)',
'A 6-2 team (0.750) scoring 132 a game against a top of 140, and 128 over the last three weeks against a top of 145: 0.45 × 0.750 + 0.35 × 0.943 + 0.20 × 0.883 = 0.84',
'Higher is better, and 1.00 would mean leading in all three. Read it next to the record and Luck: a team ranked above its record has been scoring well and losing close or unlucky games.',
'decimal', 'score', '0.47-0.90 from week 5 on', 0.7500, 0.8500, 'decimal_3', 17,
'The weights are a choice, not fitted, and the scoring terms are relative to the week''s best team, so a score isn''t comparable across seasons. It follows the standings closely (rank match 0.95). Before 2026-11 its points term read only that week''s score and it added a schedule term that was really the team''s own record again.'),

('pythagorean_wins', 'Expected (rivalries)', 'head_to_head',
'In a rivalry, the wins each side''s points should have earned.',
'Uses the points each manager scored in their games against each other. In a rivalry both scores are real output, so points for and against both count, which isn''t true across a whole season.',
'Games × PF² ÷ (PF² + PA²)',
'10 meetings, 1,300 points to 1,250: about 5.2 expected wins',
'Compare it with the rivalry record. The difference is the Luckier column.',
'decimal', 'games', '0-17', null, null, 'decimal_1', 8,
'Rivalries are a handful of games, so it moves a lot on one result. It uses an exponent of 2, flatter than the 6.26 that fits whole seasons here, so the better scorer in a rivalry tends to read as lucky.'),

('rivalry_luck', 'Luckier', 'head_to_head',
'Which side of a rivalry has won more games than its points earned.',
'Each side''s wins in the rivalry minus its expected wins from the points scored in those games. The column shows the luckier side.',
'Rivalry wins + half of ties − rivalry expected wins',
'7 wins against 5.2 expected: +1.8 for that side',
'Close games decide it. A rivalry with lots of one-score games can run 2 wins lucky without either side playing better.',
'decimal', 'games', '-3 to +3', null, null, 'decimal_1', 35,
'Rivalries are short, so a single close game moves it a lot.')
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
    limitations = EXCLUDED.limitations,
    updated_at = CURRENT_TIMESTAMP;

-- The head-to-head API's luck field is rivalry luck, not season luck.
UPDATE meta_data.field_definitions SET metric_id = 'rivalry_luck'
WHERE field_id = 'h2h_luck_factor';

-- Luck now comes from all-play wins, and the power score no longer reads
-- the schedule.
DELETE FROM meta_data.metric_relationships
WHERE (primary_metric_id, related_metric_id) IN (
    ('luck_factor', 'pythagorean_wins'), ('power_score', 'strength_of_schedule'));
INSERT INTO meta_data.metric_relationships
    (primary_metric_id, related_metric_id, relationship_type, relationship_description, strength)
VALUES
    ('luck_factor', 'all_play_wins', 'derived_from', 'Luck is wins minus all-play (expected) wins', 10),
    ('rivalry_luck', 'pythagorean_wins', 'derived_from', 'Rivalry luck is rivalry wins minus rivalry expected wins', 10)
ON CONFLICT (primary_metric_id, related_metric_id, relationship_type) DO NOTHING;

COMMIT;
