-- A card for This Season's Best all-play record tile.
--
-- The tile replaced "Points leaders who won the title" and first borrowed the
-- all_play_wins card (Expected wins). That card's numbers are on another
-- scale (7.0 expected wins over a 14-week season), while the tile prints a
-- record across every all-play game (57-33 over 10 weeks). This card explains
-- the record the tile shows. The 58% to 81% range is each finished season's
-- best all-play share, 2005 to 2025, regular-season weeks only.
--
-- populate_metric_definitions.sql and add_metric_limitations.sql carry the
-- same copy, so a rebuild or a re-run of either one keeps it.
--
-- Idempotent: safe to re-run. A re-run brings the copy up to date.

BEGIN;

INSERT INTO meta_data.metric_definitions (
    metric_id, metric_name, metric_category, short_description, detailed_description,
    calculation_formula, example_calculation, interpretation_guide, limitations,
    data_type, unit_of_measure, typical_range, good_value_threshold, excellent_value_threshold,
    display_format, sort_order
) VALUES
('best_all_play_record', 'Best all-play record', 'team_performance',
'This season''s best record if every team had played all the others every week.',
'Each week, a team''s score plays every other team''s score that week, a tie counting half. Added up over the regular season, it''s the record the scoring earned no matter who the team drew. The tile shows the team with the highest share of those games won, so a team a game behind on the schedule isn''t ranked lower for it. Teams on the same record share the tile.',
'Wins: sum over weeks of (teams outscored + half of teams tied). Losses: the same for teams that outscored it.',
'In a 10-team league, a team''s week 1 score beats 7 of the other 9: 7-2. In week 2 it beats 3, ties 1 and loses to 5: 3.5-5.5. After two weeks: 7 + 3.5 = 10.5 wins and 2 + 5.5 = 7.5 losses, so 10.5-7.5.',
'Each week counts as a game against every other team, so 9 games a week in a 10-team league. A season''s best here has won 58% to 81% of them, about 73 to 102 wins over 14 weeks. It''s what the scoring earned, not the standings, and the gap between the two is Luck.',
'It looks back over the regular season to date and leaves out playoff weeks. Every week counts the same, so a week when the whole league scored close together moves it as much as a week with big gaps. It comes from the same scores as the playoff odds, so it only shows once the season''s first week is in.',
'text', 'record', '58% to 81% of games for a season''s best', null, null, 'text', 35)
ON CONFLICT (metric_id) DO UPDATE SET
    metric_name = EXCLUDED.metric_name,
    metric_category = EXCLUDED.metric_category,
    short_description = EXCLUDED.short_description,
    detailed_description = EXCLUDED.detailed_description,
    calculation_formula = EXCLUDED.calculation_formula,
    example_calculation = EXCLUDED.example_calculation,
    interpretation_guide = EXCLUDED.interpretation_guide,
    limitations = EXCLUDED.limitations,
    data_type = EXCLUDED.data_type,
    unit_of_measure = EXCLUDED.unit_of_measure,
    typical_range = EXCLUDED.typical_range,
    good_value_threshold = EXCLUDED.good_value_threshold,
    excellent_value_threshold = EXCLUDED.excellent_value_threshold,
    display_format = EXCLUDED.display_format,
    sort_order = EXCLUDED.sort_order,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO meta_data.metric_relationships (primary_metric_id, related_metric_id, relationship_type, relationship_description, strength)
SELECT 'best_all_play_record', 'all_play_wins', 'related_to', 'The same all-play games, as a record instead of expected wins', 9
WHERE NOT EXISTS (
    SELECT 1 FROM meta_data.metric_relationships
    WHERE primary_metric_id = 'best_all_play_record' AND related_metric_id = 'all_play_wins'
);

COMMIT;
