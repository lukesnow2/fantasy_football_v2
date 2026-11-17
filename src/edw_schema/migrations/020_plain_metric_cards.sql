-- Rewrite every metric card in plain language (docs/metric-card-guide.md).
--
-- A November 2026 research pass (NN/g, Carbon, W3C, FanGraphs, FiveThirtyEight,
-- the Winton Centre's risk-communication work) set the rules in the guide. Each
-- row below was then checked against the code that computes the number, and
-- several described something the code doesn't do:
--   - playoff_probability: prose in the monospace Calculation box, the retired
--     ladder's reading guide, a made-up example and ladder limitations.
--   - Ties and strength of schedule: the code changes with this copy (the
--     ETL in the same PR). Rivalry luck and the stored career win rate now
--     count a tie as half a win, like season Luck, the League Table and Yahoo,
--     so a rivalry's two lucks cancel and every page shows one win rate.
--     Strength of schedule now leaves each opponent's games against the team
--     out of its record, so a team's own results no longer move it (it ran
--     -0.75 against the team's record; now about 0). The pipeline rebuilds
--     those marts on its next run.
--   - season_consistency_score's limitation described the Hall of Fame API.
--   - production_differential, points_per_week, points_above_replacement,
--     trade_winner, draft_value_score and waiver_activity_index described
--     formulas nothing runs.
--   - career_win_percentage's "industry benchmark" bands had no source.
-- Good and Excellent bands now come from this league's own history, and chances
-- and zero-sum numbers (odds, luck, schedule) have none.
--
-- hall_of_fame_index is left out: its formula is changing in separate work
-- (migration 016), and its card should change with it.
--
-- populate_metric_definitions.sql and add_metric_limitations.sql are the
-- baseline a rebuild starts from; every rebuild then applies the migrations,
-- so this file is where the current copy lives.
--
-- Idempotent: safe to re-run.

BEGIN;

-- playoff_probability
UPDATE meta_data.metric_definitions SET
    metric_name = 'Playoff odds',
    short_description = 'The chance a team makes the playoffs, from playing out the rest of the season 10,000 times.',
    detailed_description = 'The rest of the regular season is played out 10,000 times. In each one, every team scores around its weekly average so far, and the final standings sort by wins, then points for, the league''s own tiebreak. A team''s odds are the share of those seasons where it finishes in a playoff spot. Early in the year a team''s average is pulled toward the league''s, because a few games don''t say much: after three weeks its own scores count for half, after twelve for 80%. A team''s level also shifts a little from week to week in each run, the way injuries and trades change a roster, so a two-game lead built on a low score isn''t treated as a lock. The table shows 100% only once a team has clinched and 0% only once it''s eliminated; short of that it shows >99% or <1%.',
    calculation_formula = 'Seasons finished in a playoff spot ÷ 10,000 simulated seasons',
    example_calculation = 'It''s week 10 of 14. A team is 6-4 and has averaged 131 points a game, against a league average of 120. Ten games is a decent sample, so the model trusts about three quarters of that gap (10 ÷ 13) and expects the team to score about 128 a week from here. Each of the 10,000 runs plays the last four weeks with the team scoring around 128 (a big week here, a bad one there), adds those results to its real 6-4, and ranks the final standings by wins, then points for. The team finishes in the top six in 8,412 runs: 8,412 ÷ 10,000 = 84.1%.',
    interpretation_guide = 'A team at 70% still misses about 3 seasons in 10. The games left are predicted from points scored, not wins, so a team winning on a low score rates below its record. The odds have held up: teams given 85 to 95% made it 203 times in 226.',
    limitations = 'It looks forward from the games played so far this season, and it only sees points. Injuries, bye weeks, trades and next week''s lineups don''t count until they show up in a team''s scores, so a team that just lost its best player looks as strong as it did last week. Early in the season the odds lean on the league average, because two or three games can''t tell a good team from a lucky one. The middle of the scale has run a little high: teams given 50 to 70% made it 159 times in 295, or 54%. It beats a simpler model that uses only record and schedule, but not by much (Brier score 0.145 against 0.148, lower is better). All of these checks replay 2008 to 2025 from week 3 on, with at least two weeks left.',
    typical_range = '0 to 100%',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'playoff_probability';

-- luck_factor
UPDATE meta_data.metric_definitions SET
    metric_name = 'Luck',
    short_description = 'How many more wins a team has than its weekly scores earned against the whole league.',
    detailed_description = 'Regular-season wins, a tie as half, minus expected wins. Expected wins credit a team each week with the share of the other teams it outscored, so they only look at its own scores. What''s left in the gap is who it drew and how the close games went. Each week the league hands out as many expected wins as real ones, so the league''s luck adds up to zero. It''s season to date: the season in progress updates every week.',
    calculation_formula = 'Wins + half of ties − expected wins',
    example_calculation = 'A 9-5 team in a 10-team league outscored 67 of the 126 other scores it was up against over 14 weeks (9 a week). Expected wins: 67 ÷ 9 = 7.44. Luck: 9 − 7.44 = +1.56.',
    interpretation_guide = 'Zero means the record matches the scoring; +1.56 is about one and a half wins more than the scores earned. Nine seasons in 10 land within 2.3 wins either way. It doesn''t carry into next season, so it isn''t a skill.',
    limitations = 'It mixes two kinds of luck: who a team drew and how the close games broke. Regular season only. Early in a season one game can move it by most of a win. A team''s luck in one season says nothing about the next: across back-to-back seasons for the same manager the two are slightly opposed (correlation −0.21). Before November 2026 it was wins minus a formula on points for and against (Pythagorean wins), which counted points against as earned and missed most schedule luck.',
    typical_range = '-4.4 to +3.8 a season',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'luck_factor';

-- power_score
UPDATE meta_data.metric_definitions SET
    metric_name = 'Power score',
    short_description = 'A 0 to 1 rating of a team''s strength right now, from its record, season scoring and last three weeks.',
    detailed_description = '45% win percentage (a tie as half), 35% the team''s points per game this season as a share of the league''s best, and 20% its average over the last three weeks as a share of the best over the same stretch. Both scoring terms compare a team with that week''s leader, so the leader on each gets the full weight. It blends what a team has done with how it''s scoring, so it mostly follows the standings and lifts a team whose points say it''s better than its record. It''s recomputed every week of the regular season, the season in progress included.',
    calculation_formula = '0.45 × win % + 0.35 × (points per game ÷ league''s best) + 0.20 × (last 3 weeks'' average ÷ league''s best)',
    example_calculation = 'A 6-2 team has a .750 win %. It scores 132 a game against a league best of 140 (132 ÷ 140 = 0.943) and 128 over the last three weeks against a best of 145 (128 ÷ 145 = 0.883). 0.45 × 0.750 + 0.35 × 0.943 + 0.20 × 0.883 = 0.338 + 0.330 + 0.177 = 0.844.',
    interpretation_guide = 'Higher is better: from week 5 on, the middle team sits near 0.700 and the top tenth above 0.860. It''s a score out of 1, not a chance of winning. Ranked above its record means strong scoring and close losses.',
    limitations = 'It looks back: it''s a rating of games played, not a forecast, though from week 5 on the higher score has won the next week''s game 57% of the time (the better record, 56%). The weights are a choice, not fitted. The scoring terms are relative to that week''s best team, so a score isn''t comparable across seasons. From week 5 on its order matches the standings closely (rank correlation 0.97). In the first few weeks one game swings the win % term a lot. Before November 2026 its points term read only that week''s score and it added a schedule term that was really the team''s own record again.',
    typical_range = '0.360 to 1.000 from week 5 on',
    good_value_threshold = 0.76,
    excellent_value_threshold = 0.86,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'power_score';

-- strength_of_schedule
UPDATE meta_data.metric_definitions SET
    metric_name = 'Strength of schedule',
    short_description = 'How good a team''s opponents have been, by their records in games against everyone else.',
    detailed_description = 'For each game a team has played, the opponent''s record so far in its other games, with a tie as half a win and every game against this team left out. Those records are averaged over the team''s games, so an opponent met twice counts twice. Leaving out the games against the team keeps its own results from moving the number: a win doesn''t make the schedule look easier and a loss doesn''t make it look harder. It''s season to date, regular season only, and it''s stored with the weekly power rankings but isn''t shown on any page.',
    calculation_formula = 'Average over games of (opponent''s win % in its games against everyone else)',
    example_calculation = 'Three weeks in, a team has played A, B and C. Leaving out their games against this team, A is 2-0 (1.000), B is 1-1 (.500) and C is 0-2 (.000): (1.000 + .500 + .000) ÷ 3 = .500.',
    interpretation_guide = '0.500 is an average schedule and higher is tougher. The team''s own games are left out of its opponents'' records, so beating them doesn''t make the schedule look easier. It measures opponents'' records, not how much they scored against you.',
    limitations = 'Early on it rests on a game or two per opponent, and in week 1 no opponent has another game yet, so it reads .500. It uses each opponent''s record as it stands through the latest week, not as it was the day you played them. It sees wins, not points: Schedule, which compares what your opponents scored against you with the league''s average that week, says more about who you drew.',
    typical_range = '0.439 to 0.568 at season''s end',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'strength_of_schedule';

-- all_play_wins
UPDATE meta_data.metric_definitions SET
    metric_name = 'Expected wins',
    short_description = 'The wins a team''s scores would have earned if it had played every other team every week.',
    detailed_description = 'Each week, the share of the other teams a team outscored, a tie as half: beating 7 of 9 is worth 0.78. Added up over the regular season, it''s the record the scoring earned no matter who the team drew, on the same number of games it actually played. Each week the league hands out as many expected wins as real ones. Career figures add up every season, the one in progress included.',
    calculation_formula = 'Sum over weeks of (teams outscored + half of teams tied) ÷ (teams − 1)',
    example_calculation = 'In a 10-team league, a team''s week 1 score beats 7 of the other 9: 7 ÷ 9 = 0.78. Its week 2 score beats 3 of 9: 3 ÷ 9 = 0.33. After two weeks: 0.78 + 0.33 = 1.1 expected wins.',
    interpretation_guide = 'An average team earns half its games: 7.0 over a 14-week season. It''s the record the scoring earned, not a forecast. Wins above it show up as Luck.',
    limitations = 'Every week counts the same, so a week when the whole league scored close together moves it as much as a week with big gaps. It looks back and counts the regular season only, the season in progress as it''s played. A season''s total depends on its length (13 or 14 weeks here), and a career''s on how many seasons were played, so compare like with like.',
    typical_range = '1.9 to 11.3 a season',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'all_play_wins';

-- points_vs_league
UPDATE meta_data.metric_definitions SET
    metric_name = 'Pts vs league',
    short_description = 'How many points a game a team scores above the other teams'' average that same week.',
    detailed_description = 'Each game, the team''s score minus the average of every other team''s score that week, opponent included, then averaged over its games so far this season. Comparing within the week takes out bye weeks, rule changes and high- or low-scoring years, so seasons from different eras compare fairly. Regular season only. It''s stored with the weekly power rankings but isn''t shown on any page yet.',
    calculation_formula = 'Average over games of (your score − the other teams'' average that week)',
    example_calculation = 'Week 1: a team scores 130 and the other nine average 122, so +8.0. Week 2: it scores 121 and the others average 118, so +3.0. (8.0 + 3.0) ÷ 2 = +5.5.',
    interpretation_guide = 'Zero is an average scorer and higher is better, in points a game. +5.2 is the top third of seasons here and +13.1 the top tenth. A season barely carries into the next, so read it as a result, not a rating.',
    limitations = 'It looks back and covers one season to date. One season''s number barely predicts the next (correlation 0.10 for the same manager, back to back), because rosters reset every year. It compares a team with the league it played in, so it can''t say whether one era''s league was stronger than another''s. In the 8-team seasons (2005 and 2007) each other team is a bigger share of the average, so the extremes run wider.',
    typical_range = '-35.8 to +31.8 a season',
    good_value_threshold = 5.2,
    excellent_value_threshold = 13.1,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'points_vs_league';

-- schedule_vs_league
UPDATE meta_data.metric_definitions SET
    metric_name = 'Schedule',
    short_description = 'How many more points a game a team''s opponents scored against it than the rest of the league that week.',
    detailed_description = 'Each game, the opponent''s score minus the average of every team but this one that week, then averaged over the team''s games so far this season. There''s no defense in fantasy, so this is pure draw: above zero means opponents happened to have big weeks against this team. Regular season only. It''s stored with the weekly power rankings but isn''t shown on any page yet.',
    calculation_formula = 'Average over games of (your opponent''s score − the other teams'' average that week)',
    example_calculation = 'Week 1: the opponent scores 140 and the other nine teams average 122, so +18.0. Week 2: the opponent scores 105 and the others average 119, so −14.0. (18.0 − 14.0) ÷ 2 = +2.0.',
    interpretation_guide = 'Zero is an average draw; positive means opponents scored more than usual, a tougher draw. Nine seasons in 10 land within 10 points a game either way. It''s luck of the draw, so it doesn''t carry into next season.',
    limitations = 'It says how many points a team faced, not whether it lost: a big opponent week against a bigger week of its own costs nothing. Luck shows the effect on wins, and the two line up (correlation −0.67 at season''s end). The ''other teams'' average includes the opponent itself, which pulls the number slightly toward zero. It looks back and covers one season to date.',
    typical_range = '-16.6 to +18.7 a season',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'schedule_vs_league';

-- pythagorean_wins
UPDATE meta_data.metric_definitions SET
    metric_name = 'Expected',
    short_description = 'In a rivalry, the wins each side''s points in those games would usually produce.',
    detailed_description = 'Uses the total points each manager scored in every game they''ve played against each other: regular season, playoffs and consolation games, the season in progress included. Each side gets a share of the games from its points, the record those points would usually produce. In a rivalry both scores are real output, so points for and against both count, which isn''t true across a whole season. The two sides add up to the number of games. The formula is the Pythagorean expectation with an exponent of 2.',
    calculation_formula = 'Games × your points squared ÷ (your points squared + their points squared)',
    example_calculation = '10 meetings, 1,300 points to 1,250. 1,300 × 1,300 = 1,690,000 and 1,250 × 1,250 = 1,562,500. 10 × 1,690,000 ÷ (1,690,000 + 1,562,500) = 5.2, and the other side gets 10 − 5.2 = 4.8. The column prints 5.2-4.8.',
    interpretation_guide = 'The two numbers add up to the games played, so 5.2-4.8 is close to even. Compare it with the series record; the gap is Luckier. Squaring the points pulls both sides toward even, so the higher scorer''s expected wins understate its real edge.',
    limitations = 'Rivalries here run 7 to 41 games, so one result moves it a lot. It squares the points, which is flatter than the power of 6.26 that fits whole seasons in this league, so the side that outscores the other usually reads as lucky. It looks back over every meeting and isn''t a prediction of the next one.',
    typical_range = '2.6 to 21.8',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'pythagorean_wins';

-- rivalry_luck
UPDATE meta_data.metric_definitions SET
    metric_name = 'Luckier',
    short_description = 'Which side of a rivalry has won more games than its points earned, and by how many.',
    detailed_description = 'Each side''s wins in the rivalry minus its expected wins from the points scored in those games. The column names the side with the higher number and shows it to two decimals, or Even when the two match. A tied game counts as half a win for each side, so the two sides'' wins add up to the games played, the same as their expected wins, and the two numbers always mirror each other. Every meeting counts: regular season, playoffs and consolation games, the season in progress included.',
    calculation_formula = 'Wins + half of ties − expected wins in the series',
    example_calculation = '10 meetings: one side has won 7 against 5.2 expected, so 7 − 5.2 = +1.80. The other side is 3 − 4.8 = −1.80. The column names the first side with +1.80.',
    interpretation_guide = 'Zero is even; +1.80 is about two more wins than the points earned. Close games decide it, so it isn''t skill. The better scorer usually reads luckier, because expected wins here lean toward even.',
    limitations = 'Rivalries here run 7 to 41 games, so a single close game moves it a lot. Expected wins square the points, flatter than the 6.26 power that fits whole seasons here, so the side with more points in the series tends to show as luckier (a side''s share of the points and its luck correlate at 0.59). It looks back over every meeting and doesn''t predict the next.',
    typical_range = '0 to +4.9 for the luckier side',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'rivalry_luck';

-- career_luck
UPDATE meta_data.metric_definitions SET
    metric_name = 'Career luck',
    short_description = 'How many more regular-season wins a manager has than their weekly scores earned, over a whole career.',
    detailed_description = 'Every season''s luck added up: regular-season wins, a tie as half, minus the expected wins the manager''s scores earned against the whole league each week. Each season''s figure is taken from its last ranked week, so the season in progress counts as it''s played, like the career record. A manager who ran two teams in one season gets both.',
    calculation_formula = 'Sum over every season of (wins + half of ties − expected wins)',
    example_calculation = 'Over 16 seasons a manager has 112.0 regular-season wins, ties as half, against 108.4 expected wins. 112.0 − 108.4 = +3.6 W.',
    interpretation_guide = 'Zero means the record matches the scoring; +3.6 W is three or four wins more than the scores earned. Luck doesn''t carry from season to season, but it piles up. Over 15 or more seasons, 6 wins either way is ordinary and past 10 is rare.',
    limitations = 'A sum, so it grows with the number of seasons played: compare managers with similar careers. It includes the season in progress, so it moves every week during the season. Chance alone spreads one season''s luck by about 1.4 wins, so a 16-season career by about 6. Before November 2026 it was built on a formula from points for and against (Pythagorean wins) and missed most schedule luck.',
    typical_range = '-13 to +13',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'career_luck';

-- career_win_percentage
UPDATE meta_data.metric_definitions SET
    metric_name = 'Career win %',
    short_description = 'The share of a manager''s games they''ve won, every game counted, with a tie as half a win.',
    detailed_description = 'Wins plus half of ties, divided by every game the manager''s teams have played: regular season, playoff, title and consolation games, the season in progress included. A manager who ran two teams in one season has both teams'' games counted. The League Table, the Managers page, the profile''s Win rate and the Consistency badge all show this same rate.',
    calculation_formula = '(Wins + half of ties) ÷ all games, regular season, playoffs and consolation',
    example_calculation = '150 wins, 91 losses and 1 tie in 242 games: (150 + 0.5) ÷ 242 = .622',
    interpretation_guide = 'Average is .500 and higher is better: .550 means winning 11 of every 20 games. Playoff and consolation games count too, so it won''t match a regular-season record. One season rarely moves a 15-season rate by more than .020.',
    limitations = 'Looks back over a whole career and counts the season in progress, so it moves every week during a season. Playoff, title and consolation games all count, so a deep playoff run raises it and a consolation game counts the same as a regular-season one. Every game weighs the same across eras, league sizes and scoring rules.',
    typical_range = '43% to 62%',
    good_value_threshold = 0.51,
    excellent_value_threshold = 0.56,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'career_win_percentage';

-- badge_championships
UPDATE meta_data.metric_definitions SET
    metric_name = 'Championships badge',
    short_description = 'A badge named for how many league titles a manager has won.',
    detailed_description = 'Counts the title games a manager''s teams have won across every season, from the warehouse''s manager career table. Three or more titles reads Dynasty builder, two Repeat champion, one Champion. A manager with no title gets no badge, and the row reads None. A title counts once its title game is played, so the season in progress can''t add one before then.',
    calculation_formula = 'Titles won: 3 or more Dynasty builder, 2 Repeat champion, 1 Champion',
    example_calculation = 'A manager with 2 titles: 2 is under 3, so the badge reads Repeat champion',
    interpretation_guide = 'No title, no badge: the row reads None. Three titles and seven get the same badge; the Championships rank beside it tells them apart.',
    limitations = 'Counts titles only: runner-up finishes, playoff trips and records don''t move it. It tops out at three titles, so it can''t separate a three-time champion from a seven-time one. A title counts only once its title game is played.',
    typical_range = NULL,
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'badge_championships';

-- badge_scoring
UPDATE meta_data.metric_definitions SET
    metric_name = 'Scoring badge',
    short_description = 'A badge named for how many points a manager''s teams score per game over a career.',
    detailed_description = 'Total points a manager''s teams have scored divided by the games they''ve played: regular season, playoff, title and consolation, the season in progress included. 130 or more points a game reads Offensive powerhouse, 120 or more High scorer, and below 120 there''s no badge. It''s the same average the profile shows as Points per game, compared before rounding.',
    calculation_formula = 'Career points ÷ career games: 130 or more Offensive powerhouse, 120 or more High scorer',
    example_calculation = '38,435.55 points over 303 games: 38,435.55 ÷ 303 = 126.85 points a game, 120 or more but under 130: High scorer',
    interpretation_guide = 'No badge below 120 points a game; it''s the same number as the profile''s Points per game. It''s raw scoring, not wins: a high scorer can still have a losing record.',
    limitations = 'Counts the whole career, the season in progress included, and every kind of game, consolation too. Scoring across the league was lower in the first seasons (teams averaged 106 and 99 points a game in 2005 and 2006, and over 115 every season since), so careers that began early read lower. The badge reads the unrounded average, so 119.96 prints as 120.0 points per game but earns no badge.',
    typical_range = NULL,
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'badge_scoring';

-- badge_consistency
UPDATE meta_data.metric_definitions SET
    metric_name = 'Consistency badge',
    short_description = 'A badge named for how often a manager''s teams win, over every game of a career.',
    detailed_description = 'Reads the career win rate stored in the warehouse''s manager career table: wins divided by every game played (regular season, playoff, title and consolation), the season in progress included, with a tie as half a win. .700 or better reads Dominance, .600 or better Consistent winner, .500 or better Above average, and below .500 there''s no badge. It''s the same rate the profile shows as Win rate and the League Table shows as Win %.',
    calculation_formula = '(Wins + half of ties) ÷ all games: .700 or better Dominance, .600 or better Consistent winner, .500 or better Above average',
    example_calculation = '160 wins and 143 losses in 303 games: 160 ÷ 303 = .528, at least .500 but under .600: Above average',
    interpretation_guide = 'No badge below .500. Despite the name it reads win rate, not how steady seasons are; Year-to-year swing on the Managers page measures that.',
    limitations = 'Counts the season in progress, so a manager near a cut line can gain or lose the badge during a season. Playoff and consolation games count alongside the regular season.',
    typical_range = NULL,
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'badge_consistency';

-- badge_longevity
UPDATE meta_data.metric_definitions SET
    metric_name = 'Longevity badge',
    short_description = 'A badge named for how many seasons a manager has played in the league.',
    detailed_description = 'Counts the seasons in which a manager''s team played at least one game, the season in progress included; two teams in one season count once, and seasons don''t have to be in a row. 15 or more reads League veteran, 10 or more Long timer, 5 or more Established, and fewer than 5 Newcomer, so every manager has one.',
    calculation_formula = 'Seasons played: 15 or more League veteran, 10 or more Long timer, 5 or more Established, fewer Newcomer',
    example_calculation = 'A manager with 12 seasons played: 12 is 10 or more but under 15: Long timer',
    interpretation_guide = 'Every manager has one, and it counts seasons, not results. The season in progress counts once its first game is played.',
    limitations = 'Seasons don''t have to be in a row: a manager who left and came back keeps every season. A season counts in full from its first game, so it can move a manager up a badge in week one. It says nothing about how those seasons went.',
    typical_range = NULL,
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'badge_longevity';

-- playoff_win_percentage
UPDATE meta_data.metric_definitions SET
    metric_name = 'Playoff win %',
    short_description = 'The share of winners'' bracket games a manager has won, title games included.',
    detailed_description = 'Every winners'' bracket game a manager''s teams have played, the title game included: wins divided by wins plus losses. Consolation games don''t count, and a first-round bye isn''t a game, so it counts as neither. Bracket games in the season in progress count as they''re played. The League Table works the rate out from the playoff record, so the two always agree; the profile reads the same count stored to four places. A manager with no bracket games reads 0.0%.',
    calculation_formula = 'Playoff and title-game wins ÷ (those wins + those losses)',
    example_calculation = '20 bracket wins and 12 losses: 20 ÷ 32 = 62.5%',
    interpretation_guide = 'Every bracket game has a winner and a loser, so the league as a whole sits at 50%; higher is better. Read it next to playoff trips: a high rate over two trips says less than a good one over ten.',
    limitations = 'Small samples: a career holds roughly 10 to 45 bracket games, so at 20 games one loss turned into a win moves it from 50.0% to 55.0%. It only counts games a manager got to play: missing the playoffs doesn''t lower it, and a first-round bye isn''t a win. A manager with no bracket games reads 0.0%, not None, because the stored rate falls back to zero. Counts the season in progress once its bracket starts.',
    typical_range = '25% to 67%',
    good_value_threshold = 0.52,
    excellent_value_threshold = 0.6,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'playoff_win_percentage';

-- draft_value_score
UPDATE meta_data.metric_definitions SET
    metric_name = 'Points per draft slot',
    short_description = 'A player''s season points divided by where the player has usually been drafted in this league, career-wide.',
    detailed_description = 'Stored for each player-season in the player value table: the points the player scored that season in weeks on a roster in this league, divided by the player''s average overall pick across every season anyone drafted that player here (never less than 1). It isn''t compared with what that slot usually scores, so later picks get bigger numbers from the division alone. Seasons the player wasn''t drafted still use the career average pick. Players never drafted, or who scored nothing, get 0. No page shows it.',
    calculation_formula = 'Season points while rostered ÷ average overall pick across every season the player was drafted',
    example_calculation = 'A receiver scores 165.1 points in a season, and that player''s picks in this league average 37.43 overall: 165.1 ÷ 37.43 = 4.41',
    interpretation_guide = 'Higher means more points for how early the player usually went, but it isn''t a value score: a late pick scores high from the division alone. No page shows it.',
    limitations = 'Divides by pick number, not by what that pick usually scores, so a 10th-round flier and a first-rounder with the same points differ about tenfold. The average pick covers every season the player was drafted, including later ones, so a season can be judged by a draft that came after it. Not position adjusted. Points count only weeks the player was on a roster in this league. Players never drafted read 0.',
    typical_range = '0 to 80; most between 0.5 and 7',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    unit_of_measure = 'points_per_slot',
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'draft_value_score';

-- points_per_week
UPDATE meta_data.metric_definitions SET
    metric_name = 'Points per week',
    short_description = 'A drafted player''s season points spread over the full season, whether or not the player played every week.',
    detailed_description = 'Stored for every draft pick: the points the player scored that season in weeks the player was on a roster in this league, divided by a fixed season length of 15 weeks through 2009, 16 through 2021 and 17 from 2022. It isn''t an average of the weeks the player played. The draft API returns it, but no page shows it.',
    calculation_formula = 'Season points while rostered ÷ weeks in that season (15, 16 or 17)',
    example_calculation = 'A receiver with 153.0 points in a 17-week season: 153.0 ÷ 17 = 9.0',
    interpretation_guide = 'It''s a season total in weekly terms, not the average of the weeks the player played, so a player hurt in week 4 reads low. Quarterbacks run highest, so compare within a position. No page shows it.',
    limitations = 'Divides by a fixed 15, 16 or 17 weeks, not the weeks the player was rostered or started, and those lengths are set by year rather than read from each season''s schedule. The season in progress is divided by 17 too, so its values read low until it ends. Points count only weeks the player was on a roster in this league. Not position adjusted.',
    typical_range = '0 to 31',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'points_per_week';

-- draft_grade
UPDATE meta_data.metric_definitions SET
    metric_name = 'Draft grade',
    short_description = 'How clearly a manager''s drafts have beaten or missed what their picks usually score, across finished seasons.',
    detailed_description = 'Each finished season''s draft gets a draft value: its picks'' points over what players at the same position, taken around the same round, usually score in this league, counted in average picks. The career grade divides a manager''s average draft value by its margin of error: the spread of every finished season''s draft value in league history, divided by the square root of the manager''s seasons. That way one big year can''t carry it, and a short career stays near C. A score of 2 or more is an A and 0.75 or more a B; within 0.75 of zero either way is a C; the same distances below zero give D and F. The season in progress joins once its title game is played.',
    calculation_formula = 'Average draft value per season ÷ (spread of finished seasons'' draft values ÷ square root of seasons)',
    example_calculation = '15 finished drafts averaging +0.94, league spread 2.72: 2.72 ÷ 3.87 (the square root of 15) = 0.70, then 0.94 ÷ 0.70 = 1.34, a B',
    interpretation_guide = 'C means too close to average to call; most managers land there. A score of 0.75 or more earns a B, 2 or more an A, and the same below zero gives D and F. It grades how settled the record is, not how big the edge is.',
    limitations = 'Looks back over finished seasons only; the draft in progress joins once its title game is played. One season''s draft says little about the next (a correlation of 0.19 across 198 manager-seasons, 2005 to 2025), though a season''s draft does line up with that season''s wins (0.53). Expectations come from this league''s own picks, not public rankings, and points count only weeks a player was on a roster in this league. Kickers and defenses count toward it.',
    typical_range = 'A to F; most managers get a C',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'draft_grade';

-- draft_value_per_season
UPDATE meta_data.metric_definitions SET
    metric_name = 'Draft value per season',
    short_description = 'How many average picks'' worth of points a manager''s drafts beat or missed their slots by, per finished season.',
    detailed_description = 'Every pick is judged against what players at the same position, taken within a round either way of it, have scored in this league''s finished seasons, measured against each season''s average drafted player so seasons with different scoring compare. A position with few picks near a round is pulled toward that round''s overall average. A season''s draft value adds up each pick''s points minus its expected points and divides by what that season''s average pick in the first 15 rounds scored. Fifteen rounds is the shortest finished draft, so the 24- and 25-round drafts of 2005 to 2007 aren''t measured in a smaller unit. The career number is the plain average over a manager''s finished seasons.',
    calculation_formula = '(Sum of each pick''s points − its expected points) ÷ season''s average pick points in rounds 1 to 15, averaged over finished seasons',
    example_calculation = 'One season''s picks beat their slots by 357 points when the average pick scored 165: 357 ÷ 165 = +2.16. With two other seasons at −1.40 and +0.50: (2.16 − 1.40 + 0.50) ÷ 3 = +0.42',
    interpretation_guide = '0 is a draft that got what its slots usually return; above 0 beat them. +1.00 is about one extra average pick a season, roughly 150 points. Most managers sit within one average pick of zero.',
    limitations = 'Looks back over finished seasons only, and weighs every season the same. Expectations come from how this league''s own picks have scored, so it measures beating this room, not public rankings. Points count only weeks a player was on a roster in this league, so a pick cut early scores little. Kickers and defenses count. One season''s draft value says little about the next.',
    typical_range = '-1 to +1 for a career; -8 to +18 for one season',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'draft_value_per_season';

-- draft_hit_rate
UPDATE meta_data.metric_definitions SET
    metric_name = 'Hit rate',
    short_description = 'The share of a manager''s picks that scored at least 80% of what their slot usually scores.',
    detailed_description = 'A pick''s expectation is what players at the same position, taken within a round either way, have scored in this league''s finished seasons, scaled to that season''s average pick in the first 15 rounds. A hit is any pick that reached at least 80% of it. Every pick counts, kickers and defenses included, from every season including the one in progress, which is judged on points so far.',
    calculation_formula = 'Picks that scored at least 0.8 × their expected points ÷ all picks',
    example_calculation = '195 hits from 329 picks: 195 ÷ 329 = 59%',
    interpretation_guide = 'Most managers hit on 57% to 64% of their picks. It counts how often picks paid off, not by how much, so one huge steal counts the same as a modest hit. Read it next to steals and busts.',
    limitations = 'Includes the season in progress on points so far, so it moves until the title game. A pick cut early counts only the weeks it was rostered and is usually a miss. With about 300 picks a career, managers a few points apart can''t really be told apart. The 80% line is a choice; a stricter line would lower everyone''s rate.',
    typical_range = '57% to 64%',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'draft_hit_rate';

-- points_above_replacement
UPDATE meta_data.metric_definitions SET
    metric_name = 'Points above replacement',
    short_description = 'How many points a player scored in a week over a fill-in-level player at a similar spot.',
    detailed_description = 'Stored for every rostered player-week: the player''s points that week minus a replacement level. Replacement is the season total of the player three quarters of the way down that season''s list for the player''s group, divided by 17. The groups are offense (quarterbacks, running backs, receivers and tight ends together), kickers, team defenses and individual defenders. No page shows it.',
    calculation_formula = 'Weekly points − (season points of the player three quarters down the group''s list ÷ 17)',
    example_calculation = 'A receiver scores 18.0 in a week. The offensive player three quarters down that season''s list scored 102.0: 102.0 ÷ 17 = 6.0, and 18.0 − 6.0 = 12.0',
    interpretation_guide = '0 is a fill-in''s week, and higher is better. Quarterbacks, backs, receivers and tight ends share one baseline, so it isn''t a comparison within a position. No page shows it.',
    limitations = 'One offensive baseline covers four positions, so quarterbacks rate high and tight ends low. Divides by 17 even in 15- and 16-week seasons. The list holds only players rostered in this league, not the free-agent pool. It''s a weekly number, not a season total.',
    typical_range = '-11 to +62 a week',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'points_above_replacement';

-- production_differential
UPDATE meta_data.metric_definitions SET
    metric_name = 'Average impact',
    short_description = 'On average, how many more points one side''s new players scored than the other side''s after a trade.',
    detailed_description = 'For each trade, add up the fantasy points the players each side received scored from the trade''s week to the end of that season, in any week they were on a roster in this league, bench weeks included. The gap between the two sides is the trade''s swing. Average impact is the average swing across the trades in view, all seasons or one, ignoring which side came out ahead.',
    calculation_formula = 'Sum of each trade''s swing ÷ trades, where swing = points one side''s new players scored − points the other side''s scored, sign dropped',
    example_calculation = 'Three trades with swings of 23.7, 105.1 and 1.4 points: (23.7 + 105.1 + 1.4) ÷ 3 = 43.4',
    interpretation_guide = 'Bigger means trades tend to be more lopsided on points. It''s a size, not a direction, so it doesn''t say who won. Points run from the trade''s week to season''s end, bench weeks included.',
    limitations = 'Counts points only through the end of the season the trade was made in. Counts weeks a player sat on a bench and weeks after the player moved on to a third team, but not weeks the player went unrostered. The trade''s own week counts in full, even games played before the deal. Draft picks and FAAB in a deal aren''t valued. Trades in the season in progress count on points so far.',
    typical_range = '0 to 309 for one trade; 14 to 157 for a season''s average',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'production_differential';

-- trade_winner
UPDATE meta_data.metric_definitions SET
    metric_name = 'Trade winner',
    short_description = 'Which side came out ahead on a weighted trade score, or Even Trade when the scores are close.',
    detailed_description = 'Each side gets a trade score out of 100 from four parts. Points: 50 plus 0.15 for every point its new players outscored the other side''s, from the trade''s week to season''s end. Playoffs: 85 if it won the title that season, 70 if it made the playoffs and the other side didn''t, 30 for the reverse, else 50. Recent form: 50 plus 2 for every point a week the players it got averaged over the ones it gave up, in the four weeks before the deal. Timing: 50, or 55 from week 6 and 62.5 from week 10, the same for both sides. The side 8 or more ahead wins; anything closer is an Even Trade.',
    calculation_formula = 'Each side: 0.40 × points score + 0.30 × playoff score + 0.20 × recent form score + 0.10 × timing score. Winner leads by 8 or more, else Even Trade',
    example_calculation = 'Week 8. Side A''s new players outscore B''s by 60 and averaged 2 more a week before the deal; no playoff edge. A: 0.40 × 59 + 0.30 × 50 + 0.20 × 54 + 0.10 × 55 = 54.9. B: 0.40 × 41 + 0.30 × 50 + 0.20 × 46 + 0.10 × 55 = 46.1. A leads by 8.8, so A''s manager is the winner',
    interpretation_guide = 'Even Trade means the two scores finished within 8. Points weigh most, but a playoff berth or title can outweigh them, so a winner''s players can score fewer. Trades this season can still flip.',
    limitations = 'The playoff and title parts judge the whole team''s season, of which the trade is one piece. Timing adds the same to both sides, so it never decides a winner. Draft picks and FAAB in a deal count for nothing, and points only run to the end of that season. The 8-point line is fixed, so trades near it can flip on small scoring changes. Trades in the season in progress are judged on points so far.',
    typical_range = 'A manager or Even Trade; scores 0 to 100',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'trade_winner';

-- transactions
UPDATE meta_data.metric_definitions SET
    metric_name = 'Moves',
    short_description = 'How many players a manager has brought onto their roster: free-agent adds, waiver claims and players received in trades.',
    detailed_description = 'Every transaction in league history that sent a player to this manager: a free-agent add, a waiver claim, or a player received in a trade, each player counted once. Drops don''t count. Moves per season divides that total by the seasons the manager has played, including the one in progress.',
    calculation_formula = 'Total moves = adds + waiver claims + players received in trades. Moves per season = total moves ÷ seasons played',
    example_calculation = '409 moves over 16 seasons: 409 ÷ 16 = 25.6 moves per season',
    interpretation_guide = 'More moves means a busier manager, not a better one. A 2-for-1 trade counts as one move for one side and two for the other. Read it next to FAAB efficiency to see whether the activity paid off.',
    limitations = 'Counts the season in progress in both the total and the seasons, so the per-season figure dips early in a year. Free-agent adds and waiver claims count the same, whatever they cost. A player added and dropped the same week still counts as a move.',
    typical_range = 'About 240 to 780 total; 14 to 35 a season',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'transactions';

-- faab_efficiency_rating
UPDATE meta_data.metric_definitions SET
    metric_name = 'FAAB efficiency',
    short_description = 'Starter points a manager''s paid waiver claims produced for each FAAB dollar spent.',
    detailed_description = 'For every waiver claim a manager paid FAAB for, the fantasy points that player scored in the manager''s starting lineup from the claim''s week until the same team paid for that player again. Those points are added up over every paid claim and divided by every dollar spent. Bench weeks earn nothing, since the question is what the money put in the lineup. $0 claims are left out, and the season in progress counts. The league has used FAAB since 2022.',
    calculation_formula = 'Starter points from paid claims ÷ FAAB dollars spent',
    example_calculation = '$40 on a running back who started 6 weeks for 72 points, and $10 on a receiver who never started: (72 + 0) ÷ (40 + 10) = 1.44 pts/$',
    interpretation_guide = 'Higher is better. Most careers here run 1.5 to 2.6 points per dollar, so $50 at 2.0 put about 100 points in lineups. It credits every starting point to the claim, not just the gain over who''d have started.',
    limitations = 'Credits all of a player''s starting points to the claim, with no allowance for who would have started otherwise, so filling an empty slot scores the same as a real upgrade. A player dropped and later re-added for free still credits the original paid claim. $0 claims are left out, so winning players for nothing earns no credit here. Covers 2022 on, when the league started using FAAB, and counts the season in progress.',
    typical_range = '0.2 to 3.4',
    good_value_threshold = 2.5,
    excellent_value_threshold = 3.0,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'faab_efficiency_rating';

-- waiver_activity_index
UPDATE meta_data.metric_definitions SET
    metric_name = 'Waiver activity index',
    short_description = 'A planned measure of how busy the waiver wire is; nothing calculates it, so no page shows it.',
    detailed_description = 'This entry has no calculation behind it. No warehouse table, view or page computes a waiver activity index, and the season summary table doesn''t carry the column. For how busy managers are on the wire, see Moves.',
    calculation_formula = 'None: no calculation exists',
    example_calculation = 'None: there''s no value to work through',
    interpretation_guide = 'There''s no number to read. For waiver and trade activity, use moves per season on a manager''s page.',
    limitations = 'Nothing computes it. An old view definition in the schema file refers to the column, but the warehouse doesn''t have it.',
    typical_range = NULL,
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'waiver_activity_index';

-- competitiveness_index
UPDATE meta_data.metric_definitions SET
    metric_name = 'Competitiveness index',
    short_description = 'How evenly matched a season was, as the average of four 0 to 100 balance measures.',
    detailed_description = 'Four parts, each on a 0 to 100 scale, are averaged with equal weight. Win parity scores how tightly the teams'' win rates bunched together. Point spread scores the gap between the top and bottom scoring teams'' season totals against the average total. The playoff race part counts the teams within one win of sixth place or better and gives 10 points for each. The close games part is the percent of the season''s games decided by 15 points or less. Records, points and games all include playoff and consolation games, and the season in progress is scored with the games played so far.',
    calculation_formula = '(Win parity + Point spread + Teams within a win of 6th × 10 + % of games won by 15 or less) ÷ 4',
    example_calculation = 'A ten-team season. Win parity: the teams'' win rates had a standard deviation of 0.1646, so 100 − 0.1646 × 300 = 50.6. Point spread: the top and bottom season totals were 2,175.7 and 1,669.0 against an average of 1,922.5, so 100 − (506.7 ÷ 1,922.5 × 100) = 73.6. Playoff race: sixth place won 8 games and seven teams won 7 or more, so 7 × 10 = 70. Close games: 23 of 76 games were decided by 15 points or less, which is 30.3. Index: (50.6 + 73.6 + 70 + 30.3) ÷ 4 = 56.1.',
    interpretation_guide = 'Higher means a more evenly matched season. Finished seasons here have run 39.0 to 65.9, and the middle one is 56.5. It''s a 0 to 100 score, not a percent, and a strong part can hide a weak one.',
    limitations = 'The four parts are weighted equally by choice, not because the data says they matter equally. The playoff race part always measures from sixth place, whatever the playoff field was that year, and the top six teams always count, so with six or more teams it can''t drop below 60. Records and season points include playoff and consolation games, and teams don''t all play the same number of those. Win parity and point spread stop at zero, though no season here has come close. It includes the season in progress, which moves every week and reads low early on, when a few games spread records wide. It describes a season after the fact and isn''t a forecast.',
    typical_range = '39.0 to 65.9 (finished seasons, 2005 to 2025)',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'competitiveness_index';

-- win_parity_score
UPDATE meta_data.metric_definitions SET
    metric_name = 'Win parity',
    short_description = 'How evenly a season''s wins were spread across the teams, scored 0 to 100.',
    detailed_description = 'Each team''s win rate for the season is its wins plus half its ties, divided by its games, with playoff and consolation games included. The score takes how widely those win rates spread out (their standard deviation), multiplies it by 300 and subtracts it from 100, stopping at zero. A season where every team finished with the same record would score 100. It''s one of the four equal parts of the competitiveness index.',
    calculation_formula = '100 − Standard deviation of team win rates × 300',
    example_calculation = 'A ten-team season, playoff games counted. Records ran from 11-3-1 (a .767 win rate, a tie counting as half a win) down to 3-12 (.200). The ten win rates have a standard deviation of 0.1646. 0.1646 × 300 = 49.4, and 100 − 49.4 = 50.6.',
    interpretation_guide = 'Higher means wins were shared more evenly; 100 would be every team on the same record. Finished seasons here have run 30.1 to 63.6. Each 10 points lower puts the typical team about half a win further from .500 over 15 games.',
    limitations = 'The × 300 scale is arbitrary, picked to land ordinary seasons in a readable range. It stops at zero once the spread passes about .333 of win rate, though no season here has gotten near that. Win rates include playoff and consolation games. It includes the season in progress, where a few games spread records wide, so it reads low early on. It only looks at one season, so it can''t tell whether the same team tops the standings every year.',
    typical_range = '30.1 to 63.6 (finished seasons, 2005 to 2025)',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'win_parity_score';

-- point_spread_score
UPDATE meta_data.metric_definitions SET
    metric_name = 'Point spread',
    short_description = 'How close the top and bottom scoring teams finished in season points, scored 0 to 100.',
    detailed_description = 'Each team''s season points are added up over every game it played, playoff and consolation games included. The gap between the highest and lowest totals is taken as a percent of the average team''s total and subtracted from 100, stopping at zero. A season where every team scored the same total would score 100. Only the top and bottom teams set the gap. It''s one of the four equal parts of the competitiveness index.',
    calculation_formula = '100 − (Most season points − Fewest season points) ÷ Average season points × 100',
    example_calculation = 'A ten-team season. The top scorer finished with 2,175.7 points, the bottom one with 1,669.0, and the average team with 1,922.5. The gap is 2,175.7 − 1,669.0 = 506.7, and 506.7 ÷ 1,922.5 × 100 = 26.4. 100 − 26.4 = 73.6.',
    interpretation_guide = 'Higher means season scoring was more bunched; 100 would be every team on the same total. Finished seasons here have run 32.6 to 83.2. A 75 means the top scorer finished a quarter of an average team''s total ahead of the bottom one.',
    limitations = 'Only the highest and lowest scoring teams count, so one outlier sets the number and the middle of the table is invisible. Season totals include playoff and consolation games, and teams don''t all play the same number of those, which can widen the gap. It stops at zero once the gap passes the average total, though no season here has come close. It includes the season in progress, scored on the games played so far.',
    typical_range = '32.6 to 83.2 (finished seasons, 2005 to 2025)',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'point_spread_score';

-- season_consistency_score
UPDATE meta_data.metric_definitions SET
    metric_name = 'Year-to-year swing',
    short_description = 'How far a manager''s win rate in a typical finished season lands from their own average.',
    detailed_description = 'For every finished season, a manager''s win rate is their wins divided by the games their team played, playoff and consolation games included, with a tie as half a win. The swing is the standard deviation of those season win rates: the typical distance between one season and their average. A season counts once its championship game has been played, so the season in progress never moves it. A manager who ran two teams in one season gets one win rate for each team. The page prints it to three places with a ± sign.',
    calculation_formula = 'Standard deviation of (Wins ÷ Games played), one per finished season',
    example_calculation = 'A manager''s five finished seasons went 10-6, 6-9, 9-7, 4-11 and 8-8, playoff games included. Their win rates are .625, .400, .563, .267 and .500, which average .471. The gaps from that average are +.154, −.071, +.092, −.204 and +.029. Squared and added, they come to .0797. .0797 ÷ 4 (seasons minus one) = .0199, and its square root is .141, shown as ±.141.',
    interpretation_guide = 'Lower means steadier, the opposite of most columns; careers here run ±.112 to ±.194 of win rate. ±.150 means a typical season lands about two wins above or below their average over 15 games. It''s steadiness, not quality: always going 5-10 scores as well as always going 10-5.',
    limitations = 'It only counts finished seasons, so the season in progress doesn''t move it until its title game is played. Win rates include playoff and consolation games, which a playoff team plays more of. It needs several seasons to mean much: with one finished season there''s no spread to measure and it shows as ±.000. A manager who ran two teams in one season gets two win rates for that year. It looks back over a career and says nothing about next season.',
    typical_range = '±.112 to ±.194 (career)',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'season_consistency_score';

-- consistency_rating
UPDATE meta_data.metric_definitions SET
    metric_name = 'Consistency rating',
    short_description = 'How much a player''s weekly fantasy points bounced around over one season.',
    detailed_description = 'For each player and season, it takes the player''s weekly fantasy points in the league''s scoring, leaves out every week where the player scored zero or less, and measures how widely the rest spread out (their standard deviation), in points. A player with one scoring week, or none, gets 0. It''s kept per player-season in the warehouse.',
    calculation_formula = 'Standard deviation of a player''s weekly points, weeks above zero only',
    example_calculation = 'A receiver scores 12.4, 3.1, 0 (a bye), 18.6 and 9.5 over five weeks. The zero drops out, leaving four weeks that average 10.9. The gaps from that average are +1.5, −7.8, +7.7 and −1.4. Squared and added, they come to 124.3. 124.3 ÷ 3 (weeks minus one) = 41.4, and its square root is 6.4.',
    interpretation_guide = 'Lower means steadier weekly scoring, in fantasy points. The middle 80% of player-seasons here run 3.4 to 10.1, with a typical one at 6.6. High scorers swing more by nature, so compare players at the same position.',
    limitations = 'Weeks at zero or below are dropped, so a player who missed games or posted a real zero looks steadier than they were. It isn''t scale free: a player who averages 20 points will swing more than one who averages 6, so it can''t rank a quarterback against a kicker. A player with a single scoring week gets 0, which reads as perfectly steady. It covers one season at a time and looks back only.',
    typical_range = '3.4 to 10.1 (middle 80% of player-seasons)',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'consistency_rating';

-- biggest_win_margin
UPDATE meta_data.metric_definitions SET
    metric_name = 'Biggest blowout',
    short_description = 'The widest winning margin in any game between two of the league''s tracked managers.',
    detailed_description = 'Every game between two tracked managers counts: regular season, playoffs and consolation, including games already played this season. Tracked managers are everyone in the league now plus former managers with five or more seasons. For each game the margin is the winner''s points minus the loser''s, and the tile shows the largest one with the manager who won it. Games against managers with fewer than five seasons, and against Yahoo''s hidden accounts, aren''t counted.',
    calculation_formula = 'Largest (Winner''s points − Loser''s points) across games between tracked managers',
    example_calculation = 'A team scores 180.4 and its opponent 55.6, both run by tracked managers. The margin is 180.4 − 55.6 = 124.8. If no other game between tracked managers has a wider margin, the tile reads 124.8 with the winner''s name.',
    interpretation_guide = 'Points, from a single game: how far the winner finished ahead of the loser. It can sit below the Record Book''s biggest blowout, because games against managers with fewer than five seasons don''t count here.',
    limitations = 'One game sets it, so it says nothing about how often anyone wins big. It leaves out games against short-stint managers (fewer than five seasons) and Yahoo''s hidden accounts, so the Record Book, which counts every game, can show a bigger number. It counts playoff and consolation games and the season in progress, so a game played this week can take the record. When two games tie for the largest margin, the tile names only one winner.',
    typical_range = '77.7 to 124.8 (each tracked manager''s own biggest win)',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'biggest_win_margin';

-- most_lopsided_game
UPDATE meta_data.metric_definitions SET
    metric_name = 'Best win',
    short_description = 'The widest margin in any game between two managers, whichever of them won it.',
    detailed_description = 'For one pair of tracked managers, it looks at every game they''ve played against each other, regular season, playoffs and consolation, including this season, and keeps the largest gap between winner and loser. It doesn''t say which of the two won that game. Tracked managers are everyone in the league now plus former managers with five or more seasons. It shows in the Every rivalry table and on the face-off as the biggest winning margin.',
    calculation_formula = 'Largest (Winner''s points − Loser''s points) over every game the pair has played',
    example_calculation = 'Two managers have met 25 times. Their widest game was a 151.3 to 73.4 win. 151.3 − 73.4 = 77.9, the number in the Best win column.',
    interpretation_guide = 'Points, from the pair''s single most one-sided game, not their usual margin. Across rivalries it runs 33.0 to 124.8, and the middle pair''s is 77.9. It''s the bigger of the two managers'' best wins, so it doesn''t say who won it.',
    limitations = 'One game sets it, so pairs who''ve played more often (anywhere from 7 to 41 games here) have had more chances at a big one. It counts playoff and consolation games and the season in progress, so it can grow this week. Only pairs with at least five games together are listed, and games involving managers with fewer than five seasons aren''t counted at all.',
    typical_range = '33.0 to 124.8 (across rivalries)',
    good_value_threshold = NULL,
    excellent_value_threshold = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'most_lopsided_game';

COMMIT;
